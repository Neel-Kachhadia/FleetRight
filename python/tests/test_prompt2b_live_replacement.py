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
from fleetright.execution import FleetRightWorkloadTarget, OutcomeUnknown, TargetError


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
    context = "kind-fleetright-safety-lab"

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
                "topology": {"key": "kubernetes.io/hostname", "maxSkew": 1},
                "requirePodAntiAffinity": False,
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
        destination_node = next(
            node for node in workers if node["metadata"]["name"] != source_node_name
        )
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
            "destination_node_ids": [destination_node["metadata"]["name"]],
            "destination_nodes": [
                {
                    "name": destination_node["metadata"]["name"],
                    "uid": destination_node["metadata"]["uid"],
                }
            ],
            "workload_generation": current["generation"],
            "baseline_replicas": 1,
            "surge_desired_replicas": 2,
            "final_desired_replicas": 2,
            "maximum_live_replicas": 3,
            "maximum_temporary_surge_replicas": 2,
            "profile_required_replicas": 1,
            "readiness_stabilization_ms": 0,
        }
        result = target.prepare_source_retirement(workload_id, plan_id, payload, fencing_token=13)
        deadline = time.monotonic() + 120
        while result["stage"] not in {"READY_FOR_SAFE_EVICTION", "PDB_BLOCKED"}:
            assert time.monotonic() < deadline, result
            time.sleep(1)
            result = target.prepare_source_retirement(
                workload_id, plan_id, payload, fencing_token=13
            )

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
        assert result["replacement_node_id"] == destination_node["metadata"]["name"]
        assert result["replacement_node_uid"] == destination_node["metadata"]["uid"]
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
                    workload_id, plan_id, payload, result, fencing_token=13
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
                )
            time.sleep(1)
            retired = takeover_target.observe_source_retirement(
                workload_id, plan_id, payload, prepared_after_takeover, fencing_token=14
            )
        assert retired["source_pod_uid"] == source["metadata"]["uid"]
        assert retired["final_desired_replicas"] == 2
        assert retired["observation"]["ready_replicas"] == 2
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
