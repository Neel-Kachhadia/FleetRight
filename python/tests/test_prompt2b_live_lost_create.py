"""Live lost/delayed Pod CREATE across a real workload-controller takeover.

Controller A is a real container whose only route to the API server is a disposable TLS proxy on
the Kind bridge gateway. The proxy lets the API server commit the replacement CREATE (or holds the
request past takeover) and withholds the response from A; A is then SIGKILLed and the in-cluster
controller (B) takes over. No host networking outside that bridge address is touched.
"""

from __future__ import annotations

import base64
import json
import os
import socket
import subprocess
import time
import uuid
from pathlib import Path

import pytest
import yaml
from fault_proxy import FaultProxy
from fleetright.execution import FleetRightWorkloadTarget

pytestmark = pytest.mark.skipif(
    os.getenv("FLEETRIGHT_RUN_KIND_E2E") != "1",
    reason="set FLEETRIGHT_RUN_KIND_E2E=1 for the live lost-CREATE fault-injection test",
)

NAMESPACE = "fleetright-v2"
DEPLOYMENT = "fleetright-workload-controller"
CONTROLLER_LABEL = "app.kubernetes.io/name=fleetright-workload-controller"


def _run(*args: str, check: bool = True, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, capture_output=True, text=True, check=check, timeout=timeout)


@pytest.mark.parametrize("mode", ["drop_response", "hold_request"])
def test_live_lost_or_delayed_pod_create_across_controller_takeover(
    tmp_path: Path, mode: str
) -> None:
    kubeconfig = os.getenv(
        "FLEETRIGHT_LAB_ADMIN_KUBECONFIG", "/tmp/fleetright-prompt2b-lab.kubeconfig"
    )
    context = os.getenv("FLEETRIGHT_LAB_CONTEXT", "kind-fleetright-prompt2b-lab")

    def kubectl(*args: str, check: bool = True, timeout: int = 60, input: str | None = None):
        return subprocess.run(
            ["kubectl", f"--kubeconfig={kubeconfig}", f"--context={context}", *args],
            capture_output=True,
            text=True,
            check=check,
            timeout=timeout,
            input=input,
        )

    assert kubectl("config", "current-context").stdout.strip() == context
    cluster = json.loads(kubectl("config", "view", "--minify", "--raw", "-o", "json").stdout)[
        "clusters"
    ][0]["cluster"]
    upstream_ca = tmp_path / "upstream-ca.crt"
    upstream_ca.write_bytes(base64.b64decode(cluster["certificate-authority-data"]))
    gateway = _run(
        "docker",
        "network",
        "inspect",
        "kind",
        "--format",
        "{{range .IPAM.Config}}{{.Gateway}} {{end}}",
    ).stdout.split()
    bridge = next(item for item in gateway if "." in item)
    with socket.socket() as probe:
        probe.bind((bridge, 0))
        port = probe.getsockname()[1]
    cert, key = tmp_path / "proxy.crt", tmp_path / "proxy.key"
    _run(
        "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
        "-keyout", str(key), "-out", str(cert), "-subj", "/CN=fleetright-lost-create-proxy",
        "-addext", f"subjectAltName=IP:{bridge}",
        "-addext", "basicConstraints=critical,CA:TRUE",
        "-addext", "keyUsage=critical,digitalSignature,keyEncipherment,keyCertSign",
        "-addext", "extendedKeyUsage=serverAuth",
    )  # fmt: skip
    token = tmp_path / "token"
    token.write_text(
        kubectl("create", "token", DEPLOYMENT, "-n", NAMESPACE, "--duration=1h").stdout.strip()
    )
    for item in (cert, token):
        item.chmod(0o644)  # read by uid 10001 inside the disposable container only

    proxy = FaultProxy((bridge, port), cluster["server"], str(upstream_ca), str(cert), str(key))
    container = f"fleetright-lost-create-a-{uuid.uuid4().hex[:8]}"
    workload_id = f"p2b-lost-create-{uuid.uuid4().hex[:8]}"
    plan_id, operation_id = f"{workload_id}-plan", f"{workload_id}-op"
    target = FleetRightWorkloadTarget(kubeconfig=kubeconfig, namespace=NAMESPACE)

    def controller_b(replicas: int) -> None:
        kubectl("scale", f"deployment/{DEPLOYMENT}", "-n", NAMESPACE, f"--replicas={replicas}")
        if replicas:
            kubectl(
                "rollout", "status", f"deployment/{DEPLOYMENT}", "-n", NAMESPACE,
                "--timeout=120s", timeout=130,
            )  # fmt: skip
        else:
            kubectl(
                "wait", "--for=delete", "pod", "-l", CONTROLLER_LABEL, "-n", NAMESPACE,
                "--timeout=90s", check=False, timeout=100,
            )  # fmt: skip

    def pods_of(uid: str) -> list[dict]:
        return json.loads(
            kubectl(
                "get", "pods", "-n", NAMESPACE, "-l", f"fleetright.io/workload-uid={uid}",
                "-o", "json",
            ).stdout
        )["items"]  # fmt: skip

    nodes = json.loads(kubectl("get", "nodes", "-o", "json").stdout)["items"]
    workers = [
        {"name": n["metadata"]["name"], "uid": n["metadata"]["uid"]}
        for n in nodes
        if not any(k.endswith("/control-plane") for k in n["metadata"].get("labels", {}))
        and not n.get("spec", {}).get("unschedulable", False)
    ]
    assert len(workers) >= 4
    workload = {
        "apiVersion": "fleetright.io/v1alpha1",
        "kind": "FleetRightWorkload",
        "metadata": {"name": workload_id, "namespace": NAMESPACE},
        "spec": {
            "workloadID": workload_id,
            "executionProtocol": "monotonic-scale-up-v1",
            "podCreateProtocol": "reserved-create-v1",
            "podCreateAdmissionProtocol": "claimed-create-v1",
            "replicas": 1,
            "availabilityFloor": 1,
            "surgeBudget": 2,
            "networkMbps": 0,
            "disruption": {"maxConcurrentDisruptions": 1},
            "placement": {
                "allowedNodes": workers,
                "topology": {"key": "kubernetes.io/hostname", "minDomains": 2, "maxSkew": 1},
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
                ]
            },
        },
    }
    controller_b(0)  # only A (behind the proxy) may mutate while the fault is armed
    try:
        proxy.start()
        service_account = "/var/run/secrets/kubernetes.io/serviceaccount"
        _run(
            "docker", "run", "-d", "--name", container, "--network", "kind", "--read-only",
            "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--memory", "128m",
            "--label", "fleetright.test=lost-create",
            "-e", f"KUBERNETES_SERVICE_HOST={bridge}",
            "-e", f"KUBERNETES_SERVICE_PORT_HTTPS={port}",
            "-e", f"POD_NAMESPACE={NAMESPACE}",
            "-v", f"{token}:{service_account}/token:ro",
            "-v", f"{cert}:{service_account}/ca.crt:ro",
            "fleetright-workload-controller:local",
        )  # fmt: skip
        kubectl("apply", "-f", "-", input=yaml.safe_dump(workload, sort_keys=False))
        deadline = time.monotonic() + 120
        current = target.observe_scale(workload_id)
        while current["ready_replicas"] != 1 or current["available_replicas"] != 1:
            assert time.monotonic() < deadline, json.dumps(current, sort_keys=True)
            time.sleep(1)
            current = target.observe_scale(workload_id)
        assert proxy.events == [] or all(e["mode"] == "pass" for e in proxy.events)
        owner_uid = current["uid"]
        source_slot = f"{workload_id}-0"
        source_record = next(
            r for r in current["authorized_pod_creates"] if r["replicaID"] == source_slot
        )
        source = json.loads(
            kubectl("get", "pod", source_record["podName"], "-n", NAMESPACE, "-o", "json").stdout
        )
        source_node = source["spec"]["nodeName"]
        destinations = [w for w in workers if w["name"] != source_node]
        payload = {
            "operation_id": operation_id,
            "replica_id": source_slot,
            "source_pod_name": source["metadata"]["name"],
            "source_pod_uid": source["metadata"]["uid"],
            "source_controller_uid": owner_uid,
            "source_node_id": source_node,
            "source_node_uid": next(w["uid"] for w in workers if w["name"] == source_node),
            "source_owner_kind": "FleetRightWorkload",
            "source_owner_name": workload_id,
            "source_owner_uid": owner_uid,
            "source_logical_slot_id": source_slot,
            "destination_node_ids": [w["name"] for w in destinations],
            "destination_nodes": destinations,
            "workload_generation": current["generation"],
            "baseline_replicas": 1,
            "surge_desired_replicas": 2,
            "final_desired_replicas": 2,
            "maximum_live_replicas": 3,
            "maximum_temporary_surge_replicas": 2,
            "profile_required_replicas": 1,
            "readiness_stabilization_ms": 0,
            "topology_key": "kubernetes.io/hostname",
            "minimum_topology_domains": 2,
            "maximum_topology_skew": 1,
        }

        def prepare() -> dict[str, object]:
            try:
                return target.prepare_source_retirement(
                    workload_id, plan_id, payload, fencing_token=21
                )
            except Exception as error:  # status CAS races retry; real faults fail the assert
                if "resource version changed" not in str(error):
                    raise
                return {"stage": "STATUS_CAS_RETRY"}

        # --- fault: the replacement CREATE leaves controller A and its outcome is withheld ---
        proxy.arm(mode)
        first = prepare()
        deadline = time.monotonic() + 60
        while not proxy.fault_seen.is_set():
            assert time.monotonic() < deadline, first
            first = prepare()
            time.sleep(0.5)
        fault = next(e for e in proxy.events if e["mode"] == mode)
        if mode == "drop_response":
            deadline = time.monotonic() + 30
            while not any(e.get("stage") == "upstream-completed" for e in proxy.events):
                assert time.monotonic() < deadline, proxy.events
                time.sleep(0.2)
        _run("docker", "kill", container)  # controller A terminates with no conclusive response
        pending = target.observe_scale(workload_id)["pending_pod_create"]
        assert pending is not None and pending["createID"] == fault["create_id"]
        assert pending["podName"] == fault["pod_name"]

        controller_b(1)  # controller B takes authority
        if mode == "hold_request":
            # The CREATE is still in flight: B must neither duplicate it nor claim readiness.
            for _ in range(8):
                stage = prepare()["stage"]
                assert stage in {"REPLACEMENT_CREATE_PENDING", "STATUS_CAS_RETRY"}, stage
                time.sleep(1)
            assert [p["metadata"]["name"] for p in pods_of(owner_uid)] == [source_record["podName"]]
            assert target.observe_scale(workload_id)["retired_replica_slots"] == []
            proxy.proceed.set()  # the old request now reaches the API server, after takeover
        else:
            proxy.proceed.set()  # RST toward the dead client; the server already committed

        result = prepare()
        deadline = time.monotonic() + 150
        while result["stage"] != "READY_FOR_SAFE_EVICTION":
            assert time.monotonic() < deadline, result
            time.sleep(1)
            result = prepare()

        completed = next(
            e for e in proxy.events if e["mode"] == mode and e.get("stage") == "upstream-completed"
        )
        assert completed["upstream_status"] == 201
        # Exactly the Pod A created is the replacement; nothing was duplicated.
        assert result["replacement_pod_uid"] == completed["pod_uid"]
        assert result["replacement_pod_name"] == fault["pod_name"]
        assert result["replacement_create_id"] == fault["create_id"]
        assert result["workload_uid"] == owner_uid
        assert result["replacement_replica_id"] == f"{workload_id}-1"
        pods = pods_of(owner_uid)
        assert sorted(p["metadata"]["name"] for p in pods) == sorted(
            [source_record["podName"], fault["pod_name"]]
        )
        assert all(p["metadata"].get("deletionTimestamp") is None for p in pods)
        replacement = next(p for p in pods if p["metadata"]["name"] == fault["pod_name"])
        assert replacement["spec"]["nodeName"] in {d["name"] for d in destinations}
        assert any(
            c["type"] == "Ready" and c["status"] == "True"
            for c in replacement["status"]["conditions"]
        )  # tombstone exists only because the late Pod really became Ready
        observed = target.observe_scale(workload_id)
        assert observed["pending_pod_create"] is None
        assert [r["podUID"] for r in observed["retired_replica_slots"]] == [
            source["metadata"]["uid"]
        ]
        assert observed["retired_replica_slots"][0]["replacementPodUID"] == completed["pod_uid"]
        records = [
            r for r in observed["authorized_pod_creates"] if r["operationID"] == operation_id
        ]
        assert [r["podUID"] for r in records] == [completed["pod_uid"]]
        # No eviction was ever sent: the source Pod is untouched.
        survivor = target.get_pod(source["metadata"]["name"])
        assert survivor is not None and survivor["metadata"].get("deletionTimestamp") is None
    finally:
        _run("docker", "rm", "-f", container, check=False)
        proxy.stop()
        kubectl(
            "delete", "fleetrightworkload", workload_id, "-n", NAMESPACE, "--wait=true",
            "--timeout=60s", check=False, timeout=70,
        )  # fmt: skip
        controller_b(1)
