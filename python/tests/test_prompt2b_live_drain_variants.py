"""Live authenticated Node drains: a two-Pod drain and a PDB-blocked drain (Kind, PostgreSQL)."""

from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from copy import deepcopy
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient
from fleetright.api import create_app
from fleetright.execution import FleetRightWorkloadTarget
from fleetright.workload_controller import REPLICA_ID_LABEL, _placement_fingerprint
from test_prompt2a_control_api import (  # noqa: F401 - fixtures resolved by pytest
    _LiveHealthyObserver,
    _tokens,
    clean_database,
    control_database,
)

NAMESPACE = "fleetright-v2"
TAINT = "fleetright.io/p2b-drain-variant"
TRANSIENT = (
    "fleetright.io/plan-id",
    "fleetright.io/operation-id",
    "fleetright.io/cordon-owned",
)


@pytest.mark.skipif(
    os.getenv("FLEETRIGHT_RUN_KIND_E2E") != "1",
    reason="set FLEETRIGHT_RUN_KIND_E2E=1 for live API-to-workload execution",
)
@pytest.mark.parametrize("pdb_blocked", [False, True], ids=["two-pod-drain", "pdb-blocked-drain"])
def test_live_v2_node_drain_two_pods_and_pdb_blocked(
    clean_database,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    pdb_blocked: bool,
) -> None:
    from test_evacuation_v2 import _v2_snapshot

    kubeconfig = os.getenv(
        "FLEETRIGHT_LAB_ADMIN_KUBECONFIG", "/tmp/fleetright-prompt2b-lab.kubeconfig"
    )
    context = os.getenv("FLEETRIGHT_LAB_CONTEXT", "kind-fleetright-prompt2b-lab")

    def kubectl(*args: str, check: bool = True, input: str | None = None):
        result = subprocess.run(
            ["kubectl", f"--kubeconfig={kubeconfig}", f"--context={context}", *args],
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
            input=input,
        )
        if check and result.returncode:
            pytest.fail(f"kubectl {' '.join(args)} failed: {result.stderr.strip()}")
        return result

    def kjson(*args: str) -> dict:
        return json.loads(kubectl(*args, "-o", "json").stdout)

    nodes = kjson("get", "nodes")["items"]
    workers = sorted(
        (
            node
            for node in nodes
            if any(
                c.get("type") == "Ready" and c.get("status") == "True"
                for c in node.get("status", {}).get("conditions", [])
            )
            and not any(k.endswith("/control-plane") for k in node["metadata"].get("labels", {}))
            and not node.get("spec", {}).get("unschedulable", False)
            and not node.get("spec", {}).get("taints")
        ),
        key=lambda node: node["metadata"]["name"],
    )
    assert len(workers) == 4
    worker_names = [node["metadata"]["name"] for node in workers]

    def wait_for_quiet_lab() -> None:
        # Pods of earlier tests may still be terminating; they would hide drainable Nodes.
        deadline = time.monotonic() + 180
        while any(
            pod["metadata"]["name"].startswith("frw-")
            for pod in kjson("get", "pods", "-n", NAMESPACE)["items"]
        ):
            assert time.monotonic() < deadline, "previous test Pods are still terminating"
            time.sleep(2)

    wait_for_quiet_lab()
    all_pods = kjson("get", "pods", "-A")["items"]

    def drainable(name: str) -> bool:
        return all(
            pod["metadata"].get("annotations", {}).get("kubernetes.io/config.mirror")
            or any(o.get("kind") == "DaemonSet" for o in pod["metadata"].get("ownerReferences", []))
            for pod in all_pods
            if pod.get("spec", {}).get("nodeName") == name
        )

    # Initial Pods go on two Nodes hosting only DaemonSet/static Pods; the rest are destinations.
    clean_nodes = [name for name in worker_names if drainable(name)]
    assert len(clean_nodes) >= 2
    initial = clean_nodes[:2]
    destinations = [name for name in worker_names if name not in initial]

    ids = [f"p2b-drain2-{uuid.uuid4().hex[:8]}-{i}" for i in range(2)]
    allowed = [{"name": n["metadata"]["name"], "uid": n["metadata"]["uid"]} for n in workers]

    def workload(name: str) -> dict:
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
                # A live PDB that allows no disruption: the drain must wait, never bypass it.
                "disruption": {"maxConcurrentDisruptions": 0 if pdb_blocked else 1},
                "placement": {
                    "allowedNodes": allowed,
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

    source_node = initial[0]
    target = None
    try:
        for name in destinations:
            kubectl("taint", "node", name, f"{TAINT}=true:NoSchedule")
        for name in ids:
            kubectl("apply", "-f", "-", input=json.dumps(workload(name)))
        deadline = time.monotonic() + 180
        objects: dict[str, dict] = {}
        pods_by_workload: dict[str, list[dict]] = {}
        while True:
            ready = True
            for name in ids:
                obj = kjson("get", "fleetrightworkload", name, "-n", NAMESPACE)
                pods = [
                    pod
                    for pod in kjson(
                        "get",
                        "pods",
                        "-n",
                        NAMESPACE,
                        "-l",
                        f"fleetright.io/workload-uid={obj['metadata']['uid']}",
                    )["items"]  # fmt: skip
                    if pod["metadata"].get("deletionTimestamp") is None
                    and any(
                        c.get("type") == "Ready" and c.get("status") == "True"
                        for c in pod.get("status", {}).get("conditions", [])
                    )
                ]
                if len(pods) != 2 or {p["spec"]["nodeName"] for p in pods} != set(initial):
                    ready = False
                    break
                objects[name], pods_by_workload[name] = obj, pods
            if ready:
                break
            assert time.monotonic() < deadline, f"initial workloads not Ready: {ids}"
            time.sleep(1)
        for name in destinations:
            kubectl("taint", "node", name, f"{TAINT}-")

        nodes_now = {n["metadata"]["name"]: n for n in kjson("get", "nodes")["items"]}
        ready_workers = [nodes_now[name] for name in worker_names]
        now_ms = time.time_ns() // 1_000_000
        snapshot, _, _ = _v2_snapshot(now_ms)
        snapshot["snapshot_id"] = f"node-drain-variant-{uuid.uuid4().hex}"
        snapshot["nodes"] = snapshot["nodes"][:4]
        for model, node in zip(snapshot["nodes"], ready_workers, strict=True):
            model.update(
                node_id=node["metadata"]["name"],
                active=True,
                health="NODE_HEALTH_HEALTHY",
                labels=[("kubernetes.io/hostname", node["metadata"]["name"])],
            )
        model_workloads, profiles, controllers, inventory, placements = [], [], [], [], []
        for name in ids:
            obj = objects[name]
            uid, spec = obj["metadata"]["uid"], obj["spec"]
            model = deepcopy(snapshot["workloads"][0])
            model.update(
                workload_id=name,
                current_resources={"cpu_millicores": 100, "memory_mib": 64, "network_mbps": 0},
                topology_key="kubernetes.io/hostname",
                min_topology_domains=2,
                max_topology_skew=1,
                availability_floor=1,
            )
            model_workloads.append(model)
            profile = deepcopy(snapshot["capacity_profiles"][0])
            profile.update(
                workload_id=name,
                profile_id=f"{name}-profile",
                allocation=deepcopy(model["current_resources"]),
            )
            profiles.append(profile)
            pdb = kjson("get", "pdb", name, "-n", NAMESPACE)
            live_allowed = pdb["status"]["disruptionsAllowed"]
            assert live_allowed == (0 if pdb_blocked else 1), pdb["status"]
            controllers.append(
                {
                    "workload_id": name,
                    "namespace": NAMESPACE,
                    "owner_kind": "FleetRightWorkload",
                    "owner_name": name,
                    "owner_uid": uid,
                    "owner_resource_version": obj["metadata"]["resourceVersion"],
                    "owner_generation": obj["metadata"]["generation"],
                    "controller_type": "fleetright-workload-controller/v1",
                    "execution_protocol": "monotonic-scale-up-v1",
                    "pod_template_sha256": _placement_fingerprint(spec, spec["podTemplate"]),
                    "desired_replicas": 2,
                    # The inventory was observed before the budget tightened: stale by design.
                    "pdb_allowed_disruptions": 1,
                    "pdb_resource_version": pdb["metadata"]["resourceVersion"],
                    "observed_at_ms": now_ms,
                }
            )
            for pod in pods_by_workload[name]:
                replica = pod["metadata"]["labels"][REPLICA_ID_LABEL]
                placements.append(
                    {
                        "workload_id": name,
                        "replica_id": replica,
                        "node_id": pod["spec"]["nodeName"],
                    }
                )
                inventory.append(
                    {
                        **placements[-1],
                        "pod_name": pod["metadata"]["name"],
                        "pod_uid": pod["metadata"]["uid"],
                        "owner_kind": "FleetRightWorkload",
                        "owner_name": name,
                        "owner_uid": uid,
                        "logical_slot_id": replica,
                        "ready": True,
                        "terminating": False,
                    }
                )
        snapshot["workloads"], snapshot["capacity_profiles"] = model_workloads, profiles
        snapshot["current_placements"] = placements
        snapshot["evacuation_v2"] = {
            **snapshot["evacuation_v2"],
            "cluster_uid": kjson("get", "namespace", "kube-system")["metadata"]["uid"],
            "namespace": NAMESPACE,
            "inventory_observed_at_ms": now_ms,
            "controllers": controllers,
            "pods": inventory,
            "nodes": [
                {
                    "node_id": n["metadata"]["name"],
                    "node_uid": n["metadata"]["uid"],
                    "resource_version": n["metadata"]["resourceVersion"],
                }
                for n in ready_workers
            ],
        }
        monkeypatch.setenv("FLEETRIGHT_KUBERNETES_WORKLOADS", ",".join(ids))

        cluster = json.loads(kubectl("config", "view", "--minify", "--raw", "-o", "json").stdout)[
            "clusters"
        ][0]["cluster"]
        executor_config = tmp_path / "executor.kubeconfig"
        executor_config.write_text(
            yaml.safe_dump(
                {
                    "apiVersion": "v1",
                    "kind": "Config",
                    "clusters": [
                        {
                            "name": "lab",
                            "cluster": {
                                "server": cluster["server"],
                                "certificate-authority-data": cluster["certificate-authority-data"],
                            },
                        }
                    ],
                    "users": [
                        {
                            "name": "executor",
                            "user": {
                                "token": kubectl(
                                    "create",
                                    "token",
                                    "fleetright-controller",
                                    "-n",
                                    "fleetright",
                                    "--duration=30m",
                                ).stdout.strip()  # fmt: skip
                            },
                        }
                    ],
                    "contexts": [
                        {
                            "name": "executor",
                            "context": {
                                "cluster": "lab",
                                "user": "executor",
                                "namespace": NAMESPACE,
                            },
                        }
                    ],
                    "current-context": "executor",
                }
            )
        )
        executor_config.chmod(0o600)
        target = FleetRightWorkloadTarget(kubeconfig=str(executor_config), namespace=NAMESPACE)
        evictions: list[str] = []
        real_kubectl = target._kubectl

        def trace(*args: str, stdin: str | None = None):
            evictions.extend(arg for arg in args if arg.endswith("/eviction"))
            return real_kubectl(*args, stdin=stdin)

        target._kubectl = trace  # type: ignore[method-assign]
        tokens, raw_tokens = _tokens()
        monkeypatch.setenv("FLEETRIGHT_READINESS_TIMEOUT_SECONDS", "300")
        app = create_app(
            clean_database.repository,
            tokens=tokens,
            mode="EXECUTE",
            execution_target="kubernetes-workload",
            target_adapter=target,
            health_observer=_LiveHealthyObserver(),
            start_execution_worker=False,
        )
        operator = {"Authorization": f"Bearer {raw_tokens['operator']}"}
        viewer = {"Authorization": f"Bearer {raw_tokens['viewer']}"}
        with TestClient(app) as client:
            optimized = client.post(
                "/v1/optimize",
                headers={
                    "Authorization": f"Bearer {raw_tokens['recommender']}",
                    "Idempotency-Key": f"drain-variant-{uuid.uuid4().hex}",
                },
                json={"snapshot": {**snapshot, "plan_schema_version": 2}},
            )
            assert optimized.status_code == 201, optimized.text
            plan = optimized.json()
            plan_id = plan["plan_id"]
            assert plan["verification_accepted"] is True and plan["admission_accepted"] is True
            types = [a["action_type"] for a in plan["candidate_actions"]]
            assert types.count("CORDON_NODE") == 1 and types.count("VERIFY_NODE_DRAIN") == 1
            assert types.count("PREPARE_SOURCE_RETIREMENT") == 2  # a two-Pod drain
            drained_node = {
                a["payload"]["source_node_id"]
                for a in plan["candidate_actions"]
                if a["action_type"] == "PREPARE_SOURCE_RETIREMENT"
            }
            assert len(drained_node) == 1
            source_node = drained_node.pop()
            source_uid = nodes_now[source_node]["metadata"]["uid"]
            sources = {
                a["payload"]["source_pod_name"]: a["payload"]["source_pod_uid"]
                for a in plan["candidate_actions"]
                if a["action_type"] == "PREPARE_SOURCE_RETIREMENT"
            }
            for step, headers_key in (("approve", "approve"), ("execute", "execute")):
                response = client.post(
                    f"/v1/plans/{plan_id}/{step}",
                    headers={**operator, "Idempotency-Key": f"{headers_key}-{plan_id}"},
                    json={},
                )
                assert response.status_code in {200, 202}, response.text

            def detail() -> dict:
                return client.get(f"/v1/plans/{plan_id}", headers=viewer).json()

            def stages(action: dict) -> list[str]:
                return [
                    item["result"].get("stage")
                    for item in (action.get("execution_result") or {}).get("progress", [])
                    if isinstance(item, dict) and isinstance(item.get("result"), dict)
                ]

            controller = app.state.execution_controller
            if not pdb_blocked:
                deadline = time.monotonic() + 420
                while detail()["execution"]["status"] != "COMMITTED":
                    assert time.monotonic() < deadline, detail()["execution"]
                    assert detail()["execution"]["status"] not in {"FAILED_SAFE", "COMPENSATING"}
                    controller.run_once()
                    time.sleep(0.5)
                final = detail()
                assert all(a["execution_state"] == "SUCCEEDED" for a in final["candidate_actions"])
                assert sorted(evictions) == sorted(
                    f"/api/v1/namespaces/{NAMESPACE}/pods/{name}/eviction" for name in sources
                )
                assert all(target.get_pod(name) is None for name in sources)
                node = kjson("get", "node", source_node)
                assert node["metadata"]["uid"] == source_uid
                assert node["spec"].get("unschedulable") is True
                assert not [k for k in node["metadata"].get("annotations", {}) if k in TRANSIENT]
                cordon = next(
                    a for a in final["candidate_actions"] if a["action_type"] == "CORDON_NODE"
                )
                assert cordon["execution_result"]["target_observation"]["cordon_owned"] is True
                leftover = [
                    p["metadata"]["name"]
                    for p in kjson("get", "pods", "-n", NAMESPACE)["items"]
                    if p["spec"].get("nodeName") == source_node
                    and p["metadata"]["labels"].get("fleetright.io/workload") in ids
                ]
                assert leftover == []  # drained: nothing of ours remains on the Node
                return

            # --- PDB-blocked: the live budget forbids disruption; the drain must wait ---
            deadline = time.monotonic() + 240
            blocked = False
            while time.monotonic() < deadline and not blocked:
                controller.run_once()
                current = detail()
                assert current["execution"]["status"] == "EXECUTING", current["execution"]
                blocked = any(
                    "PDB_BLOCKED" in stages(a)
                    for a in current["candidate_actions"]
                    if a["action_type"] == "PREPARE_SOURCE_RETIREMENT"
                )
                time.sleep(0.5)
            assert blocked, "drain never reached the PDB gate"
            for _ in range(10):  # waiting is stable: no eviction, no extra surge, no completion
                controller.run_once()
                time.sleep(0.5)
            current = detail()
            assert current["execution"]["status"] == "EXECUTING"
            by_type = {a["action_type"]: a for a in current["candidate_actions"]}
            assert by_type["VERIFY_NODE_DRAIN"]["execution_state"] == "NOT_STARTED"
            assert by_type["CORDON_NODE"]["execution_state"] == "SUCCEEDED"
            assert evictions == []  # the PDB was never bypassed, no Eviction even attempted
            for name, uid in sources.items():
                pod = target.get_pod(name)
                assert pod is not None and pod["metadata"]["uid"] == uid
                assert pod["metadata"].get("deletionTimestamp") is None
            counts = {
                obj_name: len(
                    kjson(
                        "get",
                        "pods",
                        "-n",
                        NAMESPACE,
                        "-l",
                        f"fleetright.io/workload-uid={objects[obj_name]['metadata']['uid']}",
                    )["items"]  # fmt: skip
                )
                for obj_name in ids
            }
            assert sorted(counts.values()) in ([2, 3], [2, 2]), counts  # at most one surge Pod
            assert all("RETIREMENT_COMPLETE" not in stages(a) for a in current["candidate_actions"])
    finally:
        for name in destinations:
            kubectl("taint", "node", name, f"{TAINT}-", check=False)
        for name in ids:
            kubectl(
                "delete", "fleetrightworkload", name, "-n", NAMESPACE, "--wait=true",
                "--ignore-not-found=true", check=False,
            )  # fmt: skip
        # A drain leaves its Node cordoned by design; restore the lab.
        kubectl("uncordon", source_node, check=False)
        kubectl(
            "annotate",
            "node",
            source_node,
            *(f"{k}-" for k in (*TRANSIENT, "fleetright.io/fencing-token")),
            check=False,
        )
