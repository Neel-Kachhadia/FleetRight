"""Live negative tests: stale/replayed Pod CREATE and legacy workloads are refused."""

from __future__ import annotations

import json
import os
import subprocess
import time
import uuid

import pytest
from fleetright.execution import FleetRightWorkloadTarget, TargetPreconditionFailed

pytestmark = pytest.mark.skipif(
    os.getenv("FLEETRIGHT_RUN_KIND_E2E") != "1",
    reason="set FLEETRIGHT_RUN_KIND_E2E=1 for live admission negative tests",
)

NAMESPACE = "fleetright-v2"
CONTROLLER_USER = "system:serviceaccount:fleetright-v2:fleetright-workload-controller"
EXECUTOR_USER = "system:serviceaccount:fleetright:fleetright-controller"


def _kubectl(
    kubeconfig: str, context: str, *args: str, stdin: str | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["kubectl", f"--kubeconfig={kubeconfig}", f"--context={context}", *args],
        input=stdin,
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )


def _workload(name: str, nodes: list[dict[str, str]]) -> dict[str, object]:
    return {
        "apiVersion": "fleetright.io/v1alpha1",
        "kind": "FleetRightWorkload",
        "metadata": {"name": name, "namespace": NAMESPACE},
        "spec": {
            "workloadID": name,
            "executionProtocol": "monotonic-scale-up-v1",
            "podCreateProtocol": "reserved-create-v1",
            "podCreateAdmissionProtocol": "claimed-create-v1",
            "replicas": 2,
            "availabilityFloor": 1,
            "surgeBudget": 2,
            "networkMbps": 0,
            "disruption": {"maxConcurrentDisruptions": 1},
            "placement": {
                "allowedNodes": nodes,
                "topology": {"key": "kubernetes.io/hostname", "maxSkew": 1, "minDomains": 2},
                "requirePodAntiAffinity": True,
            },
            "podTemplate": {
                "containers": [
                    {
                        "name": "app",
                        "image": "fleetright-workload:local",
                        "imagePullPolicy": "IfNotPresent",
                        "ports": [{"containerPort": 8080, "protocol": "TCP"}],
                        "readinessProbe": {"httpGet": {"path": "/ready", "port": 8080}},
                        "resources": {"requests": {"cpu": "100m", "memory": "64Mi"}},
                    }
                ],
            },
        },
    }


def _forged_pod(name: str, owner: dict[str, str], create_id: str) -> dict[str, object]:
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": NAMESPACE,
            "annotations": {"fleetright.io/create-id": create_id},
            "ownerReferences": [
                {
                    "apiVersion": "fleetright.io/v1alpha1",
                    "kind": "FleetRightWorkload",
                    "name": owner["name"],
                    "uid": owner["uid"],
                    "controller": True,
                    "blockOwnerDeletion": True,
                }
            ],
        },
        "spec": {
            "restartPolicy": "Always",
            "securityContext": {
                "runAsNonRoot": True,
                "runAsUser": 10001,
                "seccompProfile": {"type": "RuntimeDefault"},
            },
            "containers": [
                {
                    "name": "app",
                    "image": "fleetright-workload:local",
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "capabilities": {"drop": ["ALL"]},
                        "runAsNonRoot": True,
                        "runAsUser": 10001,
                    },
                }
            ],
        },
    }


def test_live_stale_or_forged_pod_create_is_denied_without_reservation() -> None:
    kubeconfig = os.getenv(
        "FLEETRIGHT_LAB_ADMIN_KUBECONFIG", "/tmp/fleetright-prompt2b-lab.kubeconfig"
    )
    context = os.getenv("FLEETRIGHT_LAB_CONTEXT", "kind-fleetright-prompt2b-lab")

    def kubectl(*args: str, stdin: str | None = None) -> subprocess.CompletedProcess[str]:
        return _kubectl(kubeconfig, context, *args, stdin=stdin)

    nodes = json.loads(kubectl("get", "nodes", "-o", "json").stdout)["items"]
    workers = [
        {"name": node["metadata"]["name"], "uid": node["metadata"]["uid"]}
        for node in nodes
        if not any(key.endswith("/control-plane") for key in node["metadata"].get("labels", {}))
        and not node.get("spec", {}).get("unschedulable", False)
    ]
    assert len(workers) >= 2
    name = f"p2b-stale-create-{uuid.uuid4().hex[:8]}"
    target = FleetRightWorkloadTarget(kubeconfig=kubeconfig, namespace=NAMESPACE)
    forged_names: list[str] = []
    try:
        created = kubectl("apply", "-f", "-", stdin=json.dumps(_workload(name, workers)))
        assert created.returncode == 0, created.stderr
        deadline = time.monotonic() + 180
        observed = target.observe_scale(name)
        while observed["ready_replicas"] != 2:
            assert time.monotonic() < deadline, observed
            time.sleep(1)
            observed = target.observe_scale(name)
        owner = {"name": name, "uid": observed["uid"]}
        assert observed["pending_pod_create"] is None  # reservation already consumed

        def pods() -> list[str]:
            listing = json.loads(
                kubectl(
                    "get",
                    "pods",
                    "-n",
                    NAMESPACE,
                    "-l",
                    f"fleetright.io/workload-uid={owner['uid']}",
                    "-o",
                    "json",
                ).stdout
            )
            return sorted(pod["metadata"]["name"] for pod in listing["items"])

        before = pods()
        assert len(before) == 2
        for index, (user, expected) in enumerate(
            (
                (CONTROLLER_USER, "no current durable reservation"),
                (None, "only the FleetRight workload controller may create owned Pods"),
            )
        ):
            forged = f"frw-{name}-r{9 + index}"
            forged_names.append(forged)
            args = ["create", "-f", "-"]
            if user is not None:
                args.insert(0, f"--as={user}")
            denied = kubectl(*args, stdin=json.dumps(_forged_pod(forged, owner, uuid.uuid4().hex)))
            assert denied.returncode != 0, denied.stdout
            assert expected in denied.stderr, denied.stderr
        victim = before[0]
        eviction = {
            "apiVersion": "policy/v1",
            "kind": "Eviction",
            "metadata": {"name": victim, "namespace": NAMESPACE},
            "deleteOptions": {"preconditions": {"uid": "not-the-source-uid"}},
        }
        forged_eviction = kubectl(
            f"--as={EXECUTOR_USER}",
            "create",
            f"--raw=/api/v1/namespaces/{NAMESPACE}/pods/{victim}/eviction",
            "-f",
            "-",
            stdin=json.dumps(eviction),
        )
        assert forged_eviction.returncode != 0, forged_eviction.stdout
        assert "pod-create.fleetright.io" in forged_eviction.stderr, forged_eviction.stderr
        time.sleep(3)
        assert pods() == before
        survivor = json.loads(kubectl("get", "pod", victim, "-n", NAMESPACE, "-o", "json").stdout)
        assert survivor["metadata"].get("deletionTimestamp") is None
        after = target.observe_scale(name)
        assert after["pending_pod_create"] is None and after["replicas"] == 2
    finally:
        for forged in forged_names:
            kubectl("delete", "pod", forged, "-n", NAMESPACE, "--ignore-not-found=true")
        kubectl(
            "delete", "fleetrightworkload", name, "-n", NAMESPACE, "--wait=true", "--timeout=120s"
        )


def test_live_legacy_workload_is_refused_before_any_mutation() -> None:
    kubeconfig = os.getenv(
        "FLEETRIGHT_LEGACY_LAB_KUBECONFIG", "/home/neel/.cache/fleetright/safety-lab.kubeconfig"
    )
    context = os.getenv("FLEETRIGHT_LEGACY_LAB_CONTEXT", "kind-fleetright-safety-lab")

    def version() -> str:
        result = _kubectl(
            kubeconfig,
            context,
            "get",
            "fleetrightworkload",
            "cpu-api",
            "-n",
            NAMESPACE,
            "-o",
            "jsonpath={.metadata.uid}/{.metadata.resourceVersion}/{.spec.replicas}",
        )
        assert result.returncode == 0, result.stderr
        return result.stdout

    before = version()
    target = FleetRightWorkloadTarget(kubeconfig=kubeconfig, namespace=NAMESPACE)
    with pytest.raises(TargetPreconditionFailed):
        target.observe_scale("cpu-api")
    assert version() == before


def test_live_lost_create_response_is_adopted_without_duplicate_pod() -> None:
    from fleetright.workload_controller import _pod_create_intent, _pod_object

    kubeconfig = os.getenv(
        "FLEETRIGHT_LAB_ADMIN_KUBECONFIG", "/tmp/fleetright-prompt2b-lab.kubeconfig"
    )
    context = os.getenv("FLEETRIGHT_LAB_CONTEXT", "kind-fleetright-prompt2b-lab")

    def kubectl(*args: str, stdin: str | None = None) -> subprocess.CompletedProcess[str]:
        return _kubectl(kubeconfig, context, *args, stdin=stdin)

    def get_workload(name: str) -> dict[str, object]:
        result = kubectl("get", "fleetrightworkload", name, "-n", NAMESPACE, "-o", "json")
        assert result.returncode == 0, result.stderr
        return json.loads(result.stdout)

    def scale_controller(replicas: int) -> None:
        result = kubectl(
            "-n",
            NAMESPACE,
            "scale",
            "deployment/fleetright-workload-controller",
            f"--replicas={replicas}",
        )
        assert result.returncode == 0, result.stderr
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            listing = json.loads(
                kubectl(
                    "get",
                    "pods",
                    "-n",
                    NAMESPACE,
                    "-l",
                    "app.kubernetes.io/name=fleetright-workload-controller",
                    "-o",
                    "json",
                ).stdout
            )["items"]
            running = [
                pod
                for pod in listing
                if pod["metadata"].get("deletionTimestamp") is None
                and any(
                    c.get("type") == "Ready" and c.get("status") == "True"
                    for c in pod.get("status", {}).get("conditions", [])
                )
            ]
            if replicas == 0 and not listing:
                return
            if replicas > 0 and len(running) == replicas:
                return
            time.sleep(1)
        pytest.fail(f"workload controller did not reach {replicas} replicas")

    nodes = json.loads(kubectl("get", "nodes", "-o", "json").stdout)["items"]
    workers = [
        {"name": node["metadata"]["name"], "uid": node["metadata"]["uid"]}
        for node in nodes
        if not any(key.endswith("/control-plane") for key in node["metadata"].get("labels", {}))
        and not node.get("spec", {}).get("unschedulable", False)
    ]
    assert len(workers) >= 3
    name = f"p2b-lost-create-{uuid.uuid4().hex[:8]}"
    target = FleetRightWorkloadTarget(kubeconfig=kubeconfig, namespace=NAMESPACE)
    try:
        assert (
            kubectl("apply", "-f", "-", stdin=json.dumps(_workload(name, workers))).returncode == 0
        )
        deadline = time.monotonic() + 180
        while target.observe_scale(name)["ready_replicas"] != 2:
            assert time.monotonic() < deadline
            time.sleep(1)

        scale_controller(0)
        baseline = target.observe_scale(name)
        target.authorize_scale(
            name,
            baseline["fencing_token"] + 1,
            expected={
                "uid": baseline["uid"],
                "generation": baseline["generation"],
                "replicas": baseline["replicas"],
            },
            plan_id="p2b-lost-create-plan",
            operation_id="p2b-lost-create-operation",
            target_replicas=3,
            destination_nodes=workers,
            purpose="staged-replacement-v1",
            minimum_ready_replicas=2,
        )
        workload = get_workload(name)
        intent = _pod_create_intent(workload, 2)
        reserved = kubectl(
            "patch",
            "fleetrightworkload",
            name,
            "-n",
            NAMESPACE,
            "--subresource=status",
            "--type=merge",
            "-p",
            json.dumps({"status": {"pendingPodCreate": intent}}),
        )
        assert reserved.returncode == 0, reserved.stderr
        workload = get_workload(name)
        # The controller's CREATE is admitted (claiming the reservation) but its response is lost.
        pod = _pod_object(workload, 2, intent)
        sent = kubectl(
            f"--as={CONTROLLER_USER}", "create", "-n", NAMESPACE, "-f", "-", stdin=json.dumps(pod)
        )
        assert sent.returncode == 0, sent.stderr
        created_uid = json.loads(
            kubectl("get", "pod", intent["podName"], "-n", NAMESPACE, "-o", "json").stdout
        )["metadata"]["uid"]
        claimed = get_workload(name)["status"]["pendingPodCreate"]
        assert claimed["createID"] == intent["createID"] and claimed.get("admissionUID")

        scale_controller(1)
        deadline = time.monotonic() + 180
        while True:
            status = get_workload(name).get("status", {})
            if (
                status.get("pendingPodCreate") is None
                and status.get("readyReplicas") == 3
                and any(
                    record.get("createID") == intent["createID"]
                    for record in status.get("authorizedPodCreates", [])
                )
            ):
                break
            assert time.monotonic() < deadline, status
            time.sleep(2)
        status = get_workload(name)["status"]
        record = next(
            item
            for item in status["authorizedPodCreates"]
            if item["createID"] == intent["createID"]
        )
        assert record["podUID"] == created_uid  # adopted, not recreated
        listing = json.loads(
            kubectl(
                "get",
                "pods",
                "-n",
                NAMESPACE,
                "-l",
                f"fleetright.io/workload-uid={workload['metadata']['uid']}",
                "-o",
                "json",
            ).stdout
        )["items"]
        assert len(listing) == 3
        assert (
            sum(
                item["metadata"]["labels"]["fleetright.io/replica-id"].endswith("-2")
                for item in listing
            )
            == 1
        )
    finally:
        scale_controller(1)
        kubectl(
            "delete", "fleetrightworkload", name, "-n", NAMESPACE, "--wait=true", "--timeout=120s"
        )
