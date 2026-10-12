from __future__ import annotations

import json
import os
import subprocess
import threading
import time
import uuid
from pathlib import Path

import pytest
import yaml
from fleetright.execution import (
    ActionBlocked,
    FleetRightWorkloadTarget,
    OutcomeUnknown,
    TargetError,
    TargetPreconditionFailed,
)
from fleetright.workload_controller import REPLICA_ID_LABEL


@pytest.mark.skipif(
    os.getenv("FLEETRIGHT_RUN_KIND_E2E") != "1",
    reason="set FLEETRIGHT_RUN_KIND_E2E=1 for the isolated live replacement test",
)
@pytest.mark.parametrize("pdb_blocked", [False, True], ids=["pdb-permitted", "pdb-blocked"])
def test_live_workload_prepares_and_evacuates_source_with_real_pod_identity(
    tmp_path: Path, pdb_blocked: bool
) -> None:
    kubeconfig = os.getenv(
        "FLEETRIGHT_LAB_ADMIN_KUBECONFIG",
        "/home/neel/.cache/fleetright/safety-lab.kubeconfig",
    )
    context = os.getenv("FLEETRIGHT_LAB_CONTEXT", "kind-fleetright-safety-lab")

    def kubectl(
        *args: str, check: bool = True, **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["kubectl", f"--kubeconfig={kubeconfig}", f"--context={context}", *args],
            capture_output=True,
            check=check,
            text=True,
            timeout=kwargs.pop("timeout", 30),  # type: ignore[arg-type]
            **kwargs,
        )

    def restart_workload_controller() -> str:
        label = "app.kubernetes.io/name=fleetright-workload-controller"
        current_pods = json.loads(
            kubectl("get", "pods", "-n", "fleetright-v2", "-l", label, "-o", "json").stdout
        )["items"]
        current = next(
            item
            for item in current_pods
            if item.get("metadata", {}).get("deletionTimestamp") is None
        )
        current_name, current_uid = current["metadata"]["name"], current["metadata"]["uid"]
        deleted = kubectl(
            "delete",
            "pod",
            current_name,
            "-n",
            "fleetright-v2",
            "--grace-period=1",
            "--wait=true",
            "--timeout=30s",
            check=False,
        )
        assert deleted.returncode == 0, deleted.stderr
        rollout = kubectl(
            "rollout",
            "status",
            "deployment/fleetright-workload-controller",
            "-n",
            "fleetright-v2",
            "--timeout=60s",
            check=False,
            timeout=70,
        )
        assert rollout.returncode == 0, rollout.stderr
        deadline = time.monotonic() + 60
        while True:
            pods = json.loads(
                kubectl("get", "pods", "-n", "fleetright-v2", "-l", label, "-o", "json").stdout
            )["items"]
            replacement = next(
                (
                    item
                    for item in pods
                    if item.get("metadata", {}).get("uid") != current_uid
                    and item.get("metadata", {}).get("deletionTimestamp") is None
                ),
                None,
            )
            if replacement is not None and any(
                condition.get("type") == "Ready" and condition.get("status") == "True"
                for condition in replacement.get("status", {}).get("conditions", [])
            ):
                return replacement["metadata"]["uid"]
            assert time.monotonic() < deadline, pods
            time.sleep(1)

    assert kubectl("config", "current-context").stdout.strip() == context
    nodes = json.loads(kubectl("get", "nodes", "-o", "json").stdout)["items"]
    workers = [
        item
        for item in nodes
        if not any(key.endswith("/control-plane") for key in item["metadata"].get("labels", {}))
        and any(
            condition.get("type") == "Ready" and condition.get("status") == "True"
            for condition in item.get("status", {}).get("conditions", [])
        )
    ]
    assert len(workers) >= 2
    workload_id = f"p2b-live-{uuid.uuid4().hex[:10]}"
    plan_id = f"p2b-live-plan-{uuid.uuid4().hex[:10]}"
    operation_id = f"p2b-live-op-{uuid.uuid4().hex[:10]}"
    role_name = f"p2b-executor-{uuid.uuid4().hex[:8]}"
    workload = {
        "apiVersion": "fleetright.io/v1alpha1",
        "kind": "FleetRightWorkload",
        "metadata": {"name": workload_id, "namespace": "fleetright-v2"},
        "spec": {
            "workloadID": workload_id,
            "executionProtocol": "monotonic-scale-up-v1",
            "podCreateProtocol": "reserved-create-v1",
            "podCreateAdmissionProtocol": "claimed-create-v1",
            "replicas": 1,
            "availabilityFloor": 1,
            "surgeBudget": 2,
            "networkMbps": 0,
            "disruption": {"maxConcurrentDisruptions": 0 if pdb_blocked else 1},
            "placement": {
                "allowedNodes": [
                    {"name": node["metadata"]["name"], "uid": node["metadata"]["uid"]}
                    for node in workers
                ],
                "topology": {
                    "key": "kubernetes.io/hostname",
                    "minDomains": 2,
                    "maxSkew": 1,
                },
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
    kubectl(
        "apply",
        "-f",
        "-",
        input=yaml.safe_dump(workload, sort_keys=False),
        timeout=30,
    )
    target = FleetRightWorkloadTarget(kubeconfig=kubeconfig, namespace="fleetright-v2")
    try:
        deadline = time.monotonic() + 120
        current = target.observe_scale(workload_id)
        while current["ready_replicas"] != 1 or current["available_replicas"] != 1:
            assert time.monotonic() < deadline, json.dumps(current, sort_keys=True)
            time.sleep(1)
            current = target.observe_scale(workload_id)
        owner_uid = current["uid"]
        source_record = next(
            record
            for record in current["authorized_pod_creates"]
            if record["workloadUID"] == owner_uid and record["replicaID"] == f"{workload_id}-0"
        )
        source = json.loads(
            kubectl(
                "get",
                "pod",
                source_record["podName"],
                "-n",
                "fleetright-v2",
                "-o",
                "json",
            ).stdout
        )
        source_node_name = source["spec"]["nodeName"]
        destination_nodes = [
            {"name": node["metadata"]["name"], "uid": node["metadata"]["uid"]}
            for node in workers
            if node["metadata"]["name"] != source_node_name
        ]
        assert len(destination_nodes) >= 2
        payload = {
            "operation_id": operation_id,
            "replica_id": source_record["replicaID"],
            "source_pod_name": source["metadata"]["name"],
            "source_pod_uid": source["metadata"]["uid"],
            "source_controller_uid": owner_uid,
            "source_node_id": source_node_name,
            "source_node_uid": next(
                node["metadata"]["uid"]
                for node in workers
                if node["metadata"]["name"] == source_node_name
            ),
            "source_owner_kind": "FleetRightWorkload",
            "source_owner_name": workload_id,
            "source_owner_uid": owner_uid,
            "source_logical_slot_id": source_record["replicaID"],
            "destination_node_ids": [node["name"] for node in destination_nodes],
            "destination_nodes": destination_nodes,
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

        def prepare_after_observation() -> dict[str, object]:
            try:
                return target.prepare_source_retirement(
                    workload_id, plan_id, payload, fencing_token=13
                )
            except TargetPreconditionFailed as error:
                if str(error) != "target resource version changed":
                    raise
                return {"stage": "STATUS_CAS_RETRY"}

        result = prepare_after_observation()
        deadline = time.monotonic() + 120
        while result["stage"] not in {"READY_FOR_SAFE_EVICTION", "PDB_BLOCKED"}:
            assert time.monotonic() < deadline, result
            time.sleep(1)
            result = prepare_after_observation()

        if pdb_blocked:
            assert result["stage"] == "PDB_BLOCKED", result
            observed = target.observe_scale(workload_id)
            assert observed["retired_replica_slots"] == []
            source_after = target.get_pod(source["metadata"]["name"])
            assert source_after is not None
            assert source_after["metadata"].get("deletionTimestamp") is None
            return

        assert result["workload_uid"] == owner_uid
        assert result["source_pod_uid"] == source["metadata"]["uid"]
        assert result["source_node_id"] == source_node_name
        assert result["source_node_uid"] == payload["source_node_uid"]
        assert result["source_pod_resource_version"] == source["metadata"]["resourceVersion"]
        assert result["replacement_pod_uid"]
        approved_destinations = {node["name"]: node["uid"] for node in destination_nodes}
        assert (
            approved_destinations[result["replacement_node_id"]] == result["replacement_node_uid"]
        )
        assert result["replacement_pod_uid"] != source["metadata"]["uid"]
        observed = target.observe_scale(workload_id)
        assert observed["retired_replica_slots"] == [
            {
                "workloadUID": owner_uid,
                "replicaID": f"{workload_id}-0",
                "podUID": source["metadata"]["uid"],
                "workloadGeneration": 1,
                "planID": plan_id,
                "operationID": operation_id,
                "sourceNodeName": source_node_name,
                "sourceNodeUID": payload["source_node_uid"],
                "replacementPodUID": result["replacement_pod_uid"],
                "replacementPodName": result["replacement_pod_name"],
                "replacementReplicaID": result["replacement_replica_id"],
                "replacementNodeName": result["replacement_node_id"],
                "replacementNodeUID": result["replacement_node_uid"],
                "replacementCreateID": result["replacement_create_id"],
                "replacementWorkloadGeneration": result["replacement_workload_generation"],
                "readinessStabilizationMs": result["readiness_stabilization_ms"],
            }
        ]
        replacement_controller_uid = restart_workload_controller()
        after_restart = target.observe_scale(workload_id)
        assert after_restart["uid"] == owner_uid
        assert after_restart["ready_replicas"] == 2
        assert after_restart["pending_pod_create"] is None
        replacement_records = [
            item
            for item in after_restart["authorized_pod_creates"]
            if item["planID"] == plan_id and item["operationID"] == operation_id
        ]
        assert len(replacement_records) == 1
        assert replacement_records[0]["podUID"] == result["replacement_pod_uid"]

        role = {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "Role",
            "metadata": {"name": role_name, "namespace": "fleetright-v2"},
            "rules": [
                {
                    "apiGroups": ["fleetright.io"],
                    "resources": ["fleetrightworkloads", "fleetrightworkloads/status"],
                    "resourceNames": [workload_id],
                    "verbs": ["get", "patch"],
                },
                {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list"]},
                {
                    "apiGroups": [""],
                    "resources": ["pods/eviction"],
                    "resourceNames": [source["metadata"]["name"]],
                    "verbs": ["create"],
                },
                {
                    "apiGroups": ["policy"],
                    "resources": ["poddisruptionbudgets"],
                    "resourceNames": [workload_id],
                    "verbs": ["get"],
                },
            ],
        }
        role_binding = {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "RoleBinding",
            "metadata": {"name": role_name, "namespace": "fleetright-v2"},
            "subjects": [
                {
                    "kind": "ServiceAccount",
                    "name": "fleetright-controller",
                    "namespace": "fleetright",
                }
            ],
            "roleRef": {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "Role",
                "name": role_name,
            },
        }
        kubectl(
            "apply",
            "-f",
            "-",
            input=yaml.safe_dump_all([role, role_binding], sort_keys=False),
        )
        token = kubectl(
            "create",
            "token",
            "fleetright-controller",
            "-n",
            "fleetright",
            "--duration=30m",
        ).stdout.strip()
        admin_config = yaml.safe_load(Path(kubeconfig).read_text(encoding="utf-8"))
        context_entry = next(item for item in admin_config["contexts"] if item["name"] == context)
        cluster_name = context_entry["context"]["cluster"]
        cluster_entry = next(
            item for item in admin_config["clusters"] if item["name"] == cluster_name
        )
        cluster = cluster_entry["cluster"]
        certificate = cluster.get("certificate-authority-data")
        if certificate is None:
            certificate = Path(cluster["certificate-authority"]).read_bytes().decode("ascii")
            import base64

            certificate = base64.b64encode(certificate.encode("ascii")).decode("ascii")
        executor_config = tmp_path / "executor.kubeconfig"
        executor_config.write_text(
            yaml.safe_dump(
                {
                    "apiVersion": "v1",
                    "kind": "Config",
                    "clusters": [
                        {
                            "name": "safety-lab",
                            "cluster": {
                                "server": cluster["server"],
                                "certificate-authority-data": certificate,
                            },
                        }
                    ],
                    "users": [{"name": "fleetright-executor", "user": {"token": token}}],
                    "contexts": [
                        {
                            "name": "fleetright-executor",
                            "context": {
                                "cluster": "safety-lab",
                                "user": "fleetright-executor",
                                "namespace": "fleetright-v2",
                            },
                        }
                    ],
                    "current-context": "fleetright-executor",
                }
            ),
            encoding="utf-8",
        )
        executor_config.chmod(0o600)

        def can_i(*args: str) -> subprocess.CompletedProcess[str]:
            access = subprocess.run(
                [
                    "kubectl",
                    f"--kubeconfig={executor_config}",
                    "auth",
                    "can-i",
                    *args,
                    "--namespace=fleetright-v2",
                ],
                capture_output=True,
                check=False,
                text=True,
                timeout=10,
            )
            return access

        # kubectl auth can-i has no resource-name flag; prove named authorization
        # through the real source Eviction below and check broad access stays denied.
        denied = can_i("create", "pods/eviction")
        assert (denied.returncode, denied.stdout.strip()) == (1, "no"), denied.stderr
        denied = can_i("delete", "pods")
        assert (denied.returncode, denied.stdout.strip()) == (1, "no"), denied.stderr
        denied = can_i("patch", "nodes")
        assert (denied.returncode, denied.stdout.strip()) == (1, "no"), denied.stderr

        executor_target = FleetRightWorkloadTarget(
            kubeconfig=str(executor_config), namespace="fleetright-v2"
        )
        takeover_target = FleetRightWorkloadTarget(
            kubeconfig=str(executor_config), namespace="fleetright-v2"
        )

        pdb = json.loads(
            kubectl("get", "pdb", workload_id, "-n", "fleetright-v2", "-o", "json").stdout
        )
        assert pdb["status"]["disruptionsAllowed"] >= 1

        real_kubectl = executor_target._kubectl
        eviction_ready = threading.Event()
        release_eviction = threading.Event()
        stale_outcome: list[Exception] = []

        def delay_stale_eviction(*args: str, stdin: str | None = None):
            if any(arg.endswith("/eviction") for arg in args):
                eviction_ready.set()
                if not release_eviction.wait(30):
                    raise TimeoutError("stale Eviction request release timed out")
            return real_kubectl(*args, stdin=stdin)

        def send_stale_eviction() -> None:
            try:
                executor_target.evict_source(
                    workload_id,
                    plan_id,
                    payload,
                    result,
                    fencing_token=13,
                    before_dispatch=lambda: True,
                )
            except Exception as error:
                stale_outcome.append(error)

        executor_target._kubectl = delay_stale_eviction  # type: ignore[method-assign]
        stale_request = threading.Thread(target=send_stale_eviction, daemon=True)
        stale_request.start()
        assert eviction_ready.wait(30), "Controller A never reached the Eviction request"

        before_takeover = takeover_target.observe_scale(workload_id)
        takeover_target.authorize_scale(
            workload_id,
            14,
            expected=before_takeover,
            plan_id=plan_id,
            operation_id=operation_id,
            target_replicas=2,
            destination_nodes=payload["destination_nodes"],
            purpose="staged-replacement-v1",
            minimum_ready_replicas=1,
            source_replica_id=payload["source_logical_slot_id"],
            source_pod_uid=payload["source_pod_uid"],
        )
        deadline = time.monotonic() + 60
        current = takeover_target.observe_scale(workload_id)
        while (
            current["fencing_token"] != 14
            or current["observed_generation"] != current["generation"]
            or current["execution"].get("phase") != "APPLIED"
        ):
            assert time.monotonic() < deadline, current
            time.sleep(1)
            current = takeover_target.observe_scale(workload_id)
        recovered = takeover_target.observe_source_retirement(
            workload_id, plan_id, payload, result, fencing_token=14
        )
        assert recovered["stage"] == "EVICTION_RETRY_SAFE", recovered

        release_eviction.set()
        stale_request.join(timeout=30)
        executor_target._kubectl = real_kubectl  # type: ignore[method-assign]
        assert not stale_request.is_alive(), "stale Eviction request did not finish"
        assert len(stale_outcome) == 1 and isinstance(stale_outcome[0], TargetError)
        source_after_stale = takeover_target.get_pod(source["metadata"]["name"])
        assert source_after_stale is not None
        assert source_after_stale["metadata"].get("deletionTimestamp") is None

        prepared_after_takeover = {**recovered, "stage": "READY_FOR_SAFE_EVICTION"}
        takeover_kubectl = takeover_target._kubectl

        def lose_eviction_response(*args: str, stdin: str | None = None):
            response = takeover_kubectl(*args, stdin=stdin)
            if any(arg.endswith("/eviction") for arg in args):
                raise OutcomeUnknown("simulated lost response after Kubernetes Eviction")
            return response

        takeover_target._kubectl = lose_eviction_response  # type: ignore[method-assign]
        with pytest.raises(OutcomeUnknown, match="simulated lost response"):
            takeover_target.evict_source(
                workload_id,
                plan_id,
                payload,
                prepared_after_takeover,
                fencing_token=14,
                before_dispatch=lambda: True,
            )
        takeover_target._kubectl = takeover_kubectl  # type: ignore[method-assign]
        restarted_controller_uid = restart_workload_controller()
        assert restarted_controller_uid != replacement_controller_uid

        deadline = time.monotonic() + 120
        retired = takeover_target.observe_source_retirement(
            workload_id, plan_id, payload, prepared_after_takeover, fencing_token=14
        )
        while retired["stage"] != "RETIREMENT_COMPLETE":
            assert time.monotonic() < deadline, retired
            if retired["stage"] == "EVICTION_RETRY_SAFE":
                retry_prepared = {
                    **prepared_after_takeover,
                    **retired,
                    "stage": "READY_FOR_SAFE_EVICTION",
                }
                takeover_target.evict_source(
                    workload_id,
                    plan_id,
                    payload,
                    retry_prepared,
                    fencing_token=14,
                    before_dispatch=lambda: True,
                )
            time.sleep(1)
            retired = takeover_target.observe_source_retirement(
                workload_id, plan_id, payload, prepared_after_takeover, fencing_token=14
            )
        assert retired["source_pod_uid"] == source["metadata"]["uid"]
        assert retired["final_desired_replicas"] == 2
        assert retired["observation"]["ready_replicas"] == 2

        recovery_nodes = [
            {"name": node["metadata"]["name"], "uid": node["metadata"]["uid"]} for node in workers
        ]
        before_recovery = takeover_target.observe_scale(workload_id)
        recovery_operation_id = f"p2b-live-recovery-{uuid.uuid4().hex[:10]}"
        takeover_target.authorize_scale(
            workload_id,
            15,
            expected=before_recovery,
            plan_id=plan_id,
            operation_id=recovery_operation_id,
            target_replicas=3,
            destination_nodes=recovery_nodes,
        )
        deadline = time.monotonic() + 120
        recovered_workload = takeover_target.observe_scale(workload_id)
        while (
            recovered_workload["ready_replicas"] != 3
            or recovered_workload["available_replicas"] != 3
            or recovered_workload["observed_generation"] != recovered_workload["generation"]
            or recovered_workload["execution"].get("phase") != "APPLIED"
        ):
            assert time.monotonic() < deadline, recovered_workload
            time.sleep(1)
            recovered_workload = takeover_target.observe_scale(workload_id)
        recovery_action = {
            "intent_id": recovery_operation_id,
            "workload_id": workload_id,
            "payload": {
                "target_precondition": {
                    key: before_recovery[key] for key in ("uid", "generation", "replicas")
                },
                "current_replicas": 2,
                "proposed_replicas": 3,
                "destination_nodes": recovery_nodes,
                "topology_key": "kubernetes.io/hostname",
                "minimum_topology_domains": 2,
                "maximum_topology_skew": 1,
            },
        }
        try:
            takeover_target.verify_recovery(plan_id, recovery_action, recovered_workload)
        except ActionBlocked as error:
            pods = json.loads(
                kubectl(
                    "get",
                    "pods",
                    "-n",
                    "fleetright-v2",
                    "-l",
                    f"fleetright.io/workload-uid={owner_uid}",
                    "-o",
                    "json",
                ).stdout
            )["items"]
            placement = [
                {
                    "slot": pod.get("metadata", {}).get("labels", {}).get(REPLICA_ID_LABEL),
                    "node": pod.get("spec", {}).get("nodeName"),
                    "phase": pod.get("status", {}).get("phase"),
                }
                for pod in pods
            ]
            pytest.fail(
                f"recovery verification failed: {error.reason}; "
                f"topology={recovered_workload.get('placement_topology')}; pods={placement}"
            )
    finally:
        kubectl(
            "delete",
            "rolebinding",
            role_name,
            "-n",
            "fleetright-v2",
            "--ignore-not-found=true",
            check=False,
        )
        kubectl(
            "delete",
            "role",
            role_name,
            "-n",
            "fleetright-v2",
            "--ignore-not-found=true",
            check=False,
        )
        kubectl(
            "delete",
            "fleetrightworkload",
            workload_id,
            "-n",
            "fleetright-v2",
            "--wait=true",
            "--ignore-not-found=true",
            check=False,
            timeout=60,
        )
        kubectl(
            "delete",
            "pods",
            "-n",
            "fleetright-v2",
            "-l",
            f"fleetright.io/workload-id={workload_id}",
            "--wait=true",
            "--ignore-not-found=true",
            check=False,
            timeout=60,
        )


@pytest.mark.skipif(
    os.getenv("FLEETRIGHT_RUN_KIND_E2E") != "1",
    reason="set FLEETRIGHT_RUN_KIND_E2E=1 to test the live Binding admission fence",
)
def test_live_pod_binding_admission_blocks_fleetright_cordoned_node() -> None:
    kubeconfig = os.getenv(
        "FLEETRIGHT_LAB_ADMIN_KUBECONFIG", "/tmp/fleetright-prompt2b-lab.kubeconfig"
    )
    context = os.getenv("FLEETRIGHT_LAB_CONTEXT", "kind-fleetright-prompt2b-lab")
    namespace = "fleetright-v2"

    def kubectl(
        *args: str, check: bool = True, **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["kubectl", f"--kubeconfig={kubeconfig}", f"--context={context}", *args],
            capture_output=True,
            check=check,
            text=True,
            timeout=kwargs.pop("timeout", 30),  # type: ignore[arg-type]
            **kwargs,
        )

    assert kubectl("config", "current-context").stdout.strip() == context
    namespace_object = json.loads(kubectl("get", "namespace", namespace, "-o", "json").stdout)
    assert namespace_object["metadata"]["uid"] == "df488d28-0a9a-4b1c-ba5d-ff4632be9b29"
    configuration = json.loads(
        kubectl(
            "get",
            "validatingwebhookconfiguration",
            "fleetright-pod-admission",
            "-o",
            "json",
        ).stdout
    )
    assert any(
        "pods/binding" in rule.get("resources", [])
        for webhook in configuration["webhooks"]
        for rule in webhook["rules"]
    )

    nodes = json.loads(kubectl("get", "nodes", "-o", "json").stdout)["items"]
    workers = [
        node
        for node in nodes
        if not any(key.endswith("/control-plane") for key in node["metadata"].get("labels", {}))
        and any(
            condition.get("type") == "Ready" and condition.get("status") == "True"
            for condition in node.get("status", {}).get("conditions", [])
        )
        and not node.get("spec", {}).get("unschedulable", False)
    ]
    pods = json.loads(kubectl("get", "pods", "-A", "-o", "json").stdout)["items"]

    def has_only_exempt_pods(node_name: str) -> bool:
        for pod in pods:
            if pod.get("spec", {}).get("nodeName") != node_name:
                continue
            metadata = pod.get("metadata", {})
            if metadata.get("annotations", {}).get("kubernetes.io/config.mirror"):
                continue
            if any(
                owner.get("kind") == "DaemonSet" and owner.get("controller") is True
                for owner in metadata.get("ownerReferences", [])
            ):
                continue
            return False
        return True

    idle_workers = [node for node in workers if has_only_exempt_pods(node["metadata"]["name"])]
    assert len(idle_workers) >= 1
    source_node = idle_workers[0]
    destination_node = next(
        node for node in workers if node["metadata"]["name"] != source_node["metadata"]["name"]
    )
    source_name = source_node["metadata"]["name"]
    source_uid = source_node["metadata"]["uid"]
    plan_id = f"p2b-binding-{uuid.uuid4().hex[:12]}"
    operation_id = f"binding-{uuid.uuid4().hex[:12]}"
    pod_name = f"binding-probe-{uuid.uuid4().hex[:12]}"
    original_annotations = source_node["metadata"].get("annotations", {})
    original_unschedulable_present = "unschedulable" in source_node.get("spec", {})
    original_unschedulable = source_node.get("spec", {}).get("unschedulable", False)
    cordon_attempted = False
    pod_created = False
    target = FleetRightWorkloadTarget(kubeconfig=kubeconfig, namespace=namespace)
    payload = {
        "operation_id": operation_id,
        "source_node_id": source_name,
        "source_node_uid": source_uid,
    }

    try:
        pod = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": pod_name, "namespace": namespace},
            "spec": {
                "schedulerName": "fleetright-p2b-binding-probe",
                "restartPolicy": "Never",
                "securityContext": {
                    "runAsNonRoot": True,
                    "runAsUser": 10001,
                    "seccompProfile": {"type": "RuntimeDefault"},
                },
                "containers": [
                    {
                        "name": "probe",
                        "image": "registry.k8s.io/pause:3.10",
                        "imagePullPolicy": "IfNotPresent",
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
        direct_assignment = json.loads(json.dumps(pod))
        direct_assignment["metadata"]["name"] = f"{pod_name}-direct"
        direct_assignment["spec"]["nodeName"] = source_name
        denied_create = kubectl(
            "apply", "-f", "-", input=json.dumps(direct_assignment), check=False
        )
        assert denied_create.returncode != 0
        assert "Pod CREATE cannot assign a Node directly" in denied_create.stderr
        kubectl("apply", "-f", "-", input=json.dumps(pod))
        pod_created = True
        pending = json.loads(kubectl("get", "pod", pod_name, "-n", namespace, "-o", "json").stdout)
        assert pending["spec"].get("nodeName", "") == ""
        assert pending["status"].get("phase") == "Pending"
        pod_uid = pending["metadata"]["uid"]

        cordon_attempted = True
        target.cordon_source_node(plan_id, payload, fencing_token=1)

        def bind(node_name: str, expected_uid: str) -> subprocess.CompletedProcess[str]:
            binding = {
                "apiVersion": "v1",
                "kind": "Binding",
                "metadata": {"name": pod_name, "namespace": namespace, "uid": expected_uid},
                "target": {"apiVersion": "v1", "kind": "Node", "name": node_name},
            }
            return kubectl(
                "create",
                f"--raw=/api/v1/namespaces/{namespace}/pods/{pod_name}/binding",
                "-f",
                "-",
                input=json.dumps(binding),
                check=False,
            )

        denied = bind(source_name, pod_uid)
        assert denied.returncode != 0, denied.stdout
        assert "Binding to a cordoned or FleetRight draining Node is denied" in denied.stderr
        allowed = bind(destination_node["metadata"]["name"], pod_uid)
        assert allowed.returncode == 0, allowed.stderr
        bound = json.loads(kubectl("get", "pod", pod_name, "-n", namespace, "-o", "json").stdout)
        assert bound["spec"]["nodeName"] == destination_node["metadata"]["name"]
        kubectl("delete", "pod", pod_name, "-n", namespace, "--wait=true")
        kubectl("apply", "-f", "-", input=json.dumps(pod))
        recreated = json.loads(
            kubectl("get", "pod", pod_name, "-n", namespace, "-o", "json").stdout
        )
        new_uid = recreated["metadata"]["uid"]
        assert new_uid != pod_uid
        stale_bind = bind(destination_node["metadata"]["name"], pod_uid)
        assert stale_bind.returncode != 0, stale_bind.stdout
        assert "Binding Pod is missing, changed, deleting, or already assigned" in stale_bind.stderr
        current_bind = bind(destination_node["metadata"]["name"], new_uid)
        assert current_bind.returncode == 0, current_bind.stderr
    finally:
        if pod_created:
            kubectl(
                "delete",
                "pod",
                pod_name,
                "-n",
                namespace,
                "--wait=true",
                "--ignore-not-found=true",
                check=False,
                timeout=30,
            )
        if cordon_attempted:
            current = target._json("get", "node", source_name, "-o", "json")
            metadata, spec = current.get("metadata", {}), current.get("spec", {})
            annotations = metadata.get("annotations", {})
            if (
                metadata.get("uid") == source_uid
                and annotations.get("fleetright.io/plan-id") == plan_id
                and annotations.get("fleetright.io/operation-id") == operation_id
                and annotations.get("fleetright.io/fencing-token") == "1"
            ):
                restored_annotations = dict(annotations)
                for key in (
                    "fleetright.io/plan-id",
                    "fleetright.io/operation-id",
                    "fleetright.io/fencing-token",
                ):
                    if key in original_annotations:
                        restored_annotations[key] = original_annotations[key]
                    else:
                        restored_annotations.pop(key, None)
                patch = [
                    {"op": "test", "path": "/metadata/uid", "value": source_uid},
                    {
                        "op": "test",
                        "path": "/metadata/resourceVersion",
                        "value": metadata["resourceVersion"],
                    },
                ]
                if restored_annotations:
                    patch.append(
                        {
                            "op": "replace" if "annotations" in metadata else "add",
                            "path": "/metadata/annotations",
                            "value": restored_annotations,
                        }
                    )
                elif "annotations" in metadata:
                    patch.append({"op": "remove", "path": "/metadata/annotations"})
                if original_unschedulable_present:
                    patch.append(
                        {
                            "op": "replace" if "unschedulable" in spec else "add",
                            "path": "/spec/unschedulable",
                            "value": original_unschedulable,
                        }
                    )
                elif "unschedulable" in spec:
                    patch.append({"op": "remove", "path": "/spec/unschedulable"})
                target._json("patch", "node", source_name, "--type=json", "-p", json.dumps(patch))
