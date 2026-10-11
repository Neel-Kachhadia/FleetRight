from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import fleetright.execution as execution_module
import pytest
from fleetright.api import create_app
from fleetright.control import Repository
from fleetright.execution import (
    _WORKLOAD_AUTHORIZED_POD_CREATES_RULE,
    _WORKLOAD_CREATE_HISTORY_UNCERTAIN_RULE,
    _WORKLOAD_EXECUTION_RULES,
    _WORKLOAD_PENDING_CREATE_RULE,
    _WORKLOAD_POD_CREATE_PROTOCOL_RULE,
    _WORKLOAD_RETIRED_SLOTS_RULE,
    ActionBlocked,
    DockerComposeTarget,
    FleetRightWorkloadTarget,
    KubernetesTarget,
    TargetError,
    TargetPreconditionFailed,
    TargetUnavailable,
    WorkloadHealthObserver,
    _run,
    bounded_execution_health_policy,
    valid_execution_health_policy,
)
from fleetright.reconciler import ExecutionController
from fleetright.workload_controller import (
    POD_CREATE_ATTEMPT_ANNOTATION,
    POD_CREATE_OPERATION_ANNOTATION,
    POD_CREATE_PLAN_ANNOTATION,
    WORKLOAD_GENERATION_ANNOTATION,
    _pod_name,
    _pod_object,
)


def _skip_fencing_crd_preflight(
    monkeypatch: pytest.MonkeyPatch, target: FleetRightWorkloadTarget
) -> None:
    monkeypatch.setattr(target, "_require_fencing_schema", lambda: None)


def test_execution_health_policy_is_capped_by_server_workload_contract() -> None:
    policy = bounded_execution_health_policy("cpu-api", 10_000, 1_000_000)
    assert policy == {
        "p99_limit_ms": 120,
        "error_slo_ppm": 1_000,
        "max_queue_growth_milli": 0,
    }
    assert valid_execution_health_policy("cpu-api", policy)
    assert not valid_execution_health_policy("cpu-api", {**policy, "p99_limit_ms": 10_000})


def test_workload_execute_requires_the_supported_explicit_namespace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FLEETRIGHT_KUBERNETES_KUBECONFIG", "/tmp/fleetright-kubeconfig")
    monkeypatch.delenv("FLEETRIGHT_KUBERNETES_NAMESPACE", raising=False)
    with pytest.raises(RuntimeError, match="namespace fleetright-v2"):
        create_app(mode="EXECUTE", execution_target="kubernetes-workload")


def test_direct_deployment_execution_target_is_disabled() -> None:
    with pytest.raises(RuntimeError, match="docker or kubernetes-workload"):
        create_app(mode="EXECUTE", execution_target="kubernetes")


def test_kubernetes_scale_patch_tests_uid_and_resource_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = KubernetesTarget(namespace="fleetright")
    observed = {
        "uid": "deployment-uid",
        "generation": 3,
        "resource_version": "17",
        "replicas": 8,
        "ready_replicas": 8,
        "available_replicas": 8,
        "fencing_token": 0,
        "annotations_present": False,
    }
    monkeypatch.setattr(target, "observe_scale", lambda _: dict(observed))
    calls: list[list[str]] = []

    def apply(*args: str, **kwargs: object) -> dict[str, object]:
        calls.append(list(args))
        return {
            "metadata": {
                "uid": "deployment-uid",
                "generation": 4,
                "resourceVersion": "18",
            }
        }

    monkeypatch.setattr(target, "_json", apply)
    result = target.set_replicas("cpu-api", 9, observed, expected_replicas=8, fencing_token=1)
    assert result["replicas"] == 9
    patch_index = calls[0].index("-p")
    patch = json.loads(calls[0][patch_index + 1])
    assert patch == [
        {"op": "test", "path": "/metadata/resourceVersion", "value": "17"},
        {"op": "test", "path": "/metadata/uid", "value": "deployment-uid"},
        {"op": "test", "path": "/metadata/generation", "value": 3},
        {
            "op": "add",
            "path": "/metadata/annotations",
            "value": {"fleetright.io/fencing-token": "1"},
        },
        {"op": "replace", "path": "/spec/replicas", "value": 9},
    ]


def test_kubernetes_refuses_external_replica_change(monkeypatch: pytest.MonkeyPatch) -> None:
    target = KubernetesTarget(namespace="fleetright")
    observed = {
        "uid": "uid-1",
        "replicas": 9,
        "resource_version": "20",
        "generation": 1,
        "fencing_token": 0,
    }
    monkeypatch.setattr(target, "observe_scale", lambda _: dict(observed))
    with pytest.raises(TargetPreconditionFailed, match="replica precondition changed"):
        target.set_replicas("cpu-api", 7, observed, expected_replicas=8, fencing_token=1)


def test_kubernetes_fence_receipt_uses_atomic_patch_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = KubernetesTarget(namespace="fleetright")
    before = {
        "uid": "uid-1",
        "generation": 7,
        "resource_version": "20",
        "replicas": 3,
        "ready_replicas": 3,
        "available_replicas": 3,
        "observed_generation": 7,
        "fencing_token": 0,
        "annotations_present": True,
    }
    later_external_state = {**before, "generation": 9, "resource_version": "22"}
    observations = [before, later_external_state]
    monkeypatch.setattr(target, "observe_scale", lambda _: observations.pop(0))
    monkeypatch.setattr(
        target,
        "_json",
        lambda *_args: {
            "metadata": {
                "uid": "uid-1",
                "generation": 8,
                "resourceVersion": "21",
                "annotations": {"fleetright.io/fencing-token": "4"},
            },
            "spec": {"replicas": 3},
            "status": {"readyReplicas": 3, "availableReplicas": 3, "observedGeneration": 8},
        },
    )

    result = target.fence_scale("cpu-api", 4, expected=before)

    assert result["generation"] == 8
    assert result["resource_version"] == "21"
    assert result["fencing_token"] == 4
    assert result["fence_precondition"]["generation"] == 7
    assert len(observations) == 1


def test_kubernetes_stale_scale_patch_loses_to_newer_target_fence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = KubernetesTarget(namespace="fleetright")
    deployment = {
        "metadata": {
            "uid": "deployment-uid",
            "generation": 3,
            "resourceVersion": "17",
            "annotations": {"fleetright.io/fencing-token": "1"},
        },
        "spec": {"replicas": 8},
        "status": {"readyReplicas": 8, "availableReplicas": 8, "observedGeneration": 3},
    }

    monkeypatch.setattr(
        target,
        "observe_scale",
        lambda _workload: KubernetesTarget._scale_observation(deepcopy(deployment)),
    )
    takeover = False

    def patch(*args: str) -> dict[str, object]:
        nonlocal takeover
        operations = json.loads(args[args.index("-p") + 1])
        if not takeover and any(item.get("path") == "/spec/replicas" for item in operations):
            takeover = True
            target.fence_scale("cpu-api", 2, expected=target.observe_scale("cpu-api"))

        def read(path: str) -> object:
            current: object = deployment
            for part in path.split("/")[1:]:
                current = current[part.replace("~1", "/")]  # type: ignore[index]
            return current

        for operation in operations:
            if operation["op"] == "test" and read(operation["path"]) != operation["value"]:
                raise TargetError("target rejected the operation")
        for operation in operations:
            path = operation["path"]
            if path == "/metadata/annotations/fleetright.io~1fencing-token":
                deployment["metadata"]["annotations"]["fleetright.io/fencing-token"] = operation[
                    "value"
                ]
            elif path == "/spec/replicas":
                deployment["spec"]["replicas"] = operation["value"]
                deployment["metadata"]["generation"] += 1
        deployment["metadata"]["resourceVersion"] = str(
            int(deployment["metadata"]["resourceVersion"]) + 1
        )
        return deepcopy(deployment)

    monkeypatch.setattr(target, "_json", patch)
    observed = target.observe_scale("cpu-api")

    with pytest.raises(TargetError, match="target rejected the operation"):
        target.set_replicas("cpu-api", 9, observed, expected_replicas=8, fencing_token=1)

    assert takeover
    assert deployment["spec"]["replicas"] == 8
    assert deployment["metadata"]["annotations"]["fleetright.io/fencing-token"] == "2"


def test_docker_process_fence_rejects_a_stale_controller(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.setenv("FLEETRIGHT_DOCKER_FENCE_DIR", str(tmp_path / "fences"))
    target = DockerComposeTarget(project="fleetright-test")
    current = {
        "uid": "fleetright-test/cpu-service",
        "replicas": 0,
        "container_ids": [],
        "ready_replicas": 0,
    }
    mutations: list[int] = []

    def observe(_workload: str) -> dict[str, object]:
        return {
            **current,
            "generation": len(current["container_ids"]),
            "observed_generation": len(current["container_ids"]),
        }

    def compose(*args: str, **_kwargs: object) -> object:
        replicas = int(args[args.index("--scale") + 1].split("=", 1)[1])
        mutations.append(replicas)
        current["replicas"] = replicas
        current["ready_replicas"] = replicas
        current["container_ids"] = ["b" * 12 for _ in range(replicas)]
        return object()

    monkeypatch.setattr(target, "observe_scale", observe)
    monkeypatch.setattr(target, "_compose", compose)
    initial = target.fence_scale("cpu-api", 10)
    target.set_replicas("cpu-api", 1, initial, expected_replicas=0, fencing_token=10)
    target.fence_scale("cpu-api", 11)
    with pytest.raises(TargetPreconditionFailed, match="newer controller"):
        target.set_replicas("cpu-api", 0, initial, expected_replicas=1, fencing_token=10)
    assert mutations == [1]


def test_docker_refuses_replica_counts_that_conflict_on_the_published_port(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.setenv("FLEETRIGHT_DOCKER_FENCE_DIR", str(tmp_path / "fences"))
    target = DockerComposeTarget(project="fleetright-test")
    current = {
        "uid": "fleetright-test/cpu-service",
        "generation": 1,
        "replicas": 1,
        "ready_replicas": 1,
    }
    monkeypatch.setattr(target, "observe_scale", lambda _workload: current)
    calls: list[tuple[object, ...]] = []

    def compose(*args: object, **_kwargs: object) -> SimpleNamespace:
        calls.append(args)
        return SimpleNamespace(
            stdout=json.dumps({"services": {"cpu-service": {"ports": [{"published": "8081"}]}}})
        )

    monkeypatch.setattr(target, "_compose", compose)
    with pytest.raises(TargetPreconditionFailed, match="without published host ports"):
        target.set_replicas("cpu-api", 2, current, expected_replicas=1, fencing_token=1)
    assert calls == [("config", "--no-interpolate", "--format", "json")]


def test_docker_allows_multiple_replicas_for_portless_compose_service(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.setenv("FLEETRIGHT_DOCKER_FENCE_DIR", str(tmp_path / "fences"))
    target = DockerComposeTarget(project="fleetright-test")
    current = {
        "uid": "fleetright-test/cpu-service",
        "replicas": 3,
        "container_ids": ["a" * 12, "b" * 12, "c" * 12],
        "ready_replicas": 3,
    }
    calls: list[tuple[object, ...]] = []

    def compose(*args: object, **_kwargs: object) -> SimpleNamespace:
        calls.append(args)
        if args[0] == "config":
            return SimpleNamespace(stdout=json.dumps({"services": {"cpu-service": {}}}))
        current.update(
            replicas=4,
            container_ids=["a" * 12, "b" * 12, "c" * 12, "d" * 12],
            ready_replicas=4,
        )
        return SimpleNamespace(stdout="")

    def observe(_workload: str) -> dict[str, object]:
        return {**current, "generation": len(current["container_ids"])}

    monkeypatch.setattr(target, "observe_scale", observe)
    monkeypatch.setattr(target, "_compose", compose)
    result = target.set_replicas(
        "cpu-api", 4, observe("cpu-api"), expected_replicas=3, fencing_token=1
    )

    assert result["replicas"] == 4
    assert result["applied"] is True
    assert calls[0] == ("config", "--no-interpolate", "--format", "json")
    assert calls[1][0] == "up"


def test_docker_fails_closed_when_compose_ports_cannot_be_verified(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.setenv("FLEETRIGHT_DOCKER_FENCE_DIR", str(tmp_path / "fences"))
    target = DockerComposeTarget(project="fleetright-test")
    observed = {"uid": "fleetright-test/cpu-service", "generation": 1, "replicas": 3}
    monkeypatch.setattr(target, "observe_scale", lambda _workload: observed)
    monkeypatch.setattr(
        target,
        "_compose",
        lambda *_args, **_kwargs: SimpleNamespace(stdout="not-json"),
    )

    with pytest.raises(TargetPreconditionFailed, match="cannot verify Compose service ports"):
        target.set_replicas("cpu-api", 4, observed, expected_replicas=3, fencing_token=1)


def test_docker_observation_counts_compose_healthchecks_as_readiness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = DockerComposeTarget(project="fleetright-test")
    ids = ["a" * 12, "b" * 12]
    states = [
        {"State": {"Running": True, "Health": {"Status": "healthy"}}},
        {"State": {"Running": True, "Health": {"Status": "starting"}}},
    ]
    monkeypatch.setattr(target, "_compose", lambda *_args: SimpleNamespace(stdout="\n".join(ids)))
    monkeypatch.setattr(
        execution_module,
        "_run",
        lambda *_args: SimpleNamespace(stdout=json.dumps(states)),
    )

    observed = target.observe_scale("cpu-api")

    assert observed["replicas"] == 2
    assert observed["ready_replicas"] == 1


def test_kubernetes_target_has_no_pod_eviction_or_node_cordon_mutators() -> None:
    target = KubernetesTarget(namespace="fleetright")

    assert not hasattr(target, "evict_one")
    assert not hasattr(target, "cordon")


def test_kubernetes_roles_publish_requests_and_leave_pod_creation_to_controller() -> None:
    root = Path(__file__).resolve().parents[2]
    api_rbac = (root / "deploy/kubernetes/rbac.yaml").read_text()
    workload_rbac = (root / "deploy/kubernetes/fleetright-workload-controller.yaml").read_text()

    assert 'resources: ["fleetrightworkloads"]' in api_rbac
    assert 'resources: ["fleetrightworkloads/status"]' in api_rbac
    assert 'verbs: ["get", "patch"]' in api_rbac
    assert 'resourceNames: ["fleetrightworkloads.fleetright.io"]' in api_rbac
    assert 'resources: ["customresourcedefinitions"]' in api_rbac
    assert 'resources: ["pods"]\n    verbs: ["get", "list"]' in api_rbac
    assert 'resources: ["nodes"]\n    verbs: ["get"]' in api_rbac
    assert 'resources: ["deployments"]' not in api_rbac
    assert 'resources: ["pods/eviction"]\n    verbs: ["create"]' in api_rbac
    assert 'resources: ["pods"]\n    verbs: ["get", "list", "create"]' in workload_rbac
    assert 'verbs: ["get", "list", "create", "delete"]' not in workload_rbac
    assert 'resources: ["pods/eviction"]' not in workload_rbac
    assert "type: Recreate" in workload_rbac


def _workload_object() -> dict[str, object]:
    return {
        "metadata": {
            "name": "cpu-api",
            "uid": "workload-uid",
            "generation": 4,
            "resourceVersion": "7",
        },
        "spec": {
            "workloadID": "cpu-api",
            "executionProtocol": "monotonic-scale-up-v1",
            "podCreateProtocol": "reserved-create-v1",
            "podCreateAdmissionProtocol": "claimed-create-v1",
            "surgeBudget": 2,
            "networkMbps": 0,
            "disruption": {"maxConcurrentDisruptions": 1},
            "placement": {
                "allowedNodes": [
                    {"name": "worker-a", "uid": "node-uid"},
                    {"name": "worker-b", "uid": "worker-b-uid"},
                ]
            },
            "replicas": 2,
            "availabilityFloor": 1,
            "podTemplate": {
                "containers": [
                    {
                        "name": "api",
                        "image": "fleetright-workload:local",
                        "resources": {"requests": {"cpu": "100m", "memory": "64Mi"}},
                        "readinessProbe": {"httpGet": {"path": "/ready", "port": 8080}},
                    }
                ]
            },
        },
        "status": {
            "observedGeneration": 4,
            "replicas": 2,
            "readyReplicas": 2,
            "availableReplicas": 2,
            "conditions": [{"type": "Reconciled", "status": "True"}],
            "authorizedPodCreates": [],
        },
    }


def _retirement_payload(**changes: object) -> dict[str, object]:
    return {
        "operation_id": "verified-operation",
        "replica_id": "cpu-api-0",
        "source_pod_name": _pod_name("workload-uid", 0),
        "source_pod_uid": "source-pod-uid",
        "source_controller_uid": "workload-uid",
        "source_node_id": "worker-a",
        "source_node_uid": "node-uid",
        "source_owner_kind": "FleetRightWorkload",
        "source_owner_name": "cpu-api",
        "source_owner_uid": "workload-uid",
        "source_logical_slot_id": "cpu-api-0",
        "destination_node_ids": ["worker-b"],
        "destination_nodes": [{"name": "worker-b", "uid": "worker-b-uid"}],
        "workload_generation": 4,
        "baseline_replicas": 2,
        "surge_desired_replicas": 3,
        "final_desired_replicas": 3,
        "maximum_live_replicas": 4,
        "maximum_temporary_surge_replicas": 2,
        "profile_required_replicas": 2,
        "readiness_stabilization_ms": 0,
        **changes,
    }


def _install_source_observation(
    monkeypatch: pytest.MonkeyPatch,
    target: FleetRightWorkloadTarget,
    *,
    pod_uid: str = "source-pod-uid",
    node_uid: str = "node-uid",
    workload: dict[str, object] | None = None,
    replacement_pod: dict[str, object] | None = None,
) -> None:
    source_name = _pod_name("workload-uid", 0)
    source_intent = {
        "workloadUID": "workload-uid",
        "workloadGeneration": 4,
        "podName": source_name,
        "replicaID": "cpu-api-0",
        "planID": "initial-plan",
        "operationID": "initial-operation",
        "createID": "source-create",
    }
    source_workload = deepcopy(workload or _workload_object())
    source_workload["metadata"]["generation"] = 4  # type: ignore[index]
    source_workload["spec"].pop("executionRequest", None)  # type: ignore[union-attr]
    source = _pod_object(source_workload, 0, source_intent)
    source["metadata"].update(name=source_name, uid=pod_uid, resourceVersion="3")
    source["spec"]["nodeName"] = "worker-a"
    source["status"] = {
        "phase": "Running",
        "conditions": [{"type": "Ready", "status": "True"}],
    }
    objects = {
        "node": {"metadata": {"name": "worker-a", "uid": node_uid}},
        "pdb": {
            "metadata": {
                "name": "cpu-api",
                "generation": 2,
                "annotations": {WORKLOAD_GENERATION_ANNOTATION: "5"},
                "ownerReferences": [
                    {
                        "apiVersion": "fleetright.io/v1alpha1",
                        "kind": "FleetRightWorkload",
                        "name": "cpu-api",
                        "uid": "workload-uid",
                        "controller": True,
                    }
                ],
            },
            "spec": {
                "selector": {"matchLabels": {"fleetright.io/workload-uid": "workload-uid"}},
                "unhealthyPodEvictionPolicy": "IfHealthyBudget",
            },
            "status": {
                "observedGeneration": 2,
                "desiredHealthy": 2,
                "currentHealthy": 3,
                "disruptionsAllowed": 1,
                "expectedPods": 3,
                "disruptedPods": {},
            },
        },
    }
    if workload is not None:
        status = workload["status"]
        assert isinstance(status, dict)
        records = status.setdefault("authorizedPodCreates", [])
        assert isinstance(records, list)
        if not any(record.get("podUID") == pod_uid for record in records):
            records.append({**source_intent, "podUID": pod_uid})

    def read_object(*args: str) -> dict[str, object]:
        if args[:2] == ("get", "fleetrightworkload") and workload is not None:
            return deepcopy(workload)
        if args[:2] == ("patch", "fleetrightworkload") and workload is not None:
            patch = json.loads(args[args.index("-p") + 1])
            metadata = workload["metadata"]
            spec = workload["spec"]
            assert isinstance(metadata, dict) and isinstance(spec, dict)
            if patch["metadata"]["resourceVersion"] != metadata["resourceVersion"]:
                raise TargetPreconditionFailed("workload resourceVersion changed")
            if "--subresource=status" in args:
                status = workload["status"]
                assert isinstance(status, dict)
                status.update(patch["status"])
            else:
                spec.update(patch["spec"])
                metadata["generation"] += 1
            metadata["resourceVersion"] = str(int(metadata["resourceVersion"]) + 1)
            return deepcopy(workload)
        if args[:2] == ("get", "nodes"):
            return {
                "items": [
                    {"metadata": {"name": "worker-a", "uid": node_uid}},
                    {"metadata": {"name": "worker-b", "uid": "worker-b-uid"}},
                ]
            }
        if args[:2] == ("get", "pdb"):
            pdb = deepcopy(objects["pdb"])
            if workload is not None:
                pdb["metadata"]["annotations"][WORKLOAD_GENERATION_ANNOTATION] = str(
                    workload["metadata"]["generation"]
                )
            return pdb
        if args[:2] == ("get", "pod"):
            if replacement_pod is not None and args[2] == replacement_pod["metadata"]["name"]:  # type: ignore[index]
                return deepcopy(replacement_pod)
            if args[2] == source_name:
                return deepcopy(source)
        if args[:2] == ("get", "node") and args[2] in {"worker-a", "worker-b"}:
            uid = objects["node"]["metadata"]["uid"] if args[2] == "worker-a" else "worker-b-uid"  # type: ignore[index]
            return {"metadata": {"name": args[2], "uid": uid}}
        return deepcopy(objects.get(args[1], {}))

    monkeypatch.setattr(target, "_json", read_object)


@pytest.mark.parametrize("replacement_already_authorized", [False, True])
def test_source_retirement_takeover_publishes_a_target_fence_before_claim(
    monkeypatch: pytest.MonkeyPatch, replacement_already_authorized: bool
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    _skip_fencing_crd_preflight(monkeypatch, target)
    state = _workload_object()
    pre_observation = target._observe(state, "cpu-api")
    if replacement_already_authorized:
        state["metadata"].update(generation=5, resourceVersion="9")  # type: ignore[index]
        state["spec"]["replicas"] = 3  # type: ignore[index]
        state["spec"]["executionRequest"] = {
            "planID": "verified-plan",
            "operationID": "verified-operation",
            "generation": 13,
            "expectedReplicas": 2,
            "targetReplicas": 3,
            "minimumReadyReplicas": 2,
            "destinationNodes": [{"name": "worker-b", "uid": "worker-b-uid"}],
            "purpose": "staged-replacement-v1",
        }  # type: ignore[index]
        state["spec"]["executionFence"] = {
            "planID": "verified-plan",
            "operationID": "verified-operation",
            "generation": 13,
            "expectedGeneration": 4,
            "expectedReplicas": 2,
            "fencedGeneration": 5,
        }  # type: ignore[index]
        state["status"].update(  # type: ignore[index]
            observedGeneration=5,
            replicas=3,
            readyReplicas=3,
            availableReplicas=3,
            execution={
                "planID": "verified-plan",
                "operationID": "verified-operation",
                "generation": 13,
                "targetReplicas": 3,
                "purpose": "staged-replacement-v1",
                "phase": "APPLIED",
            },
        )
    _install_source_observation(monkeypatch, target, workload=state)
    action = {
        "intent_id": "verified-operation",
        "action_type": "PREPARE_SOURCE_RETIREMENT",
        "workload_id": "cpu-api",
        "execution_state": "RUNNING",
        "execution_result": {"pre_observation": pre_observation},
        "payload": _retirement_payload(),
    }
    records: list[tuple[object, ...]] = []
    monkeypatch.setattr(Repository, "prepare_execution_takeover", lambda *_: 14)
    monkeypatch.setattr(Repository, "incomplete_actions", lambda *_: [action])
    monkeypatch.setattr(Repository, "execution_fence_receipt", lambda *_: None)
    monkeypatch.setattr(Repository, "record_execution_fence", lambda *args: records.append(args))
    monkeypatch.setattr(Repository, "claim_execution_controller", lambda *_: 14)
    controller = ExecutionController(
        SimpleNamespace(transaction=lambda: nullcontext(object())),
        target,
        SimpleNamespace(),
        "controller-b",
    )

    token = controller._claim(
        {
            "plan_id": "verified-plan",
            "controller_id": "controller-a",
            "controller_lease_expired": True,
            "controller_fencing_token": 13,
        }
    )

    assert token == 14
    assert state["spec"]["executionFence"]["generation"] == 14  # type: ignore[index]
    assert state["metadata"]["generation"] == 5 + int(replacement_already_authorized)  # type: ignore[index]
    receipt = records[-1][-1]
    assert receipt["state"] == "COMPLETE"  # type: ignore[index]
    assert receipt["token"] == 14  # type: ignore[index]
    assert receipt["observation"]["fencing_token"] == 14  # type: ignore[index]


def test_source_retirement_publishes_plan_bounded_replacement_intent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    _skip_fencing_crd_preflight(monkeypatch, target)
    state = _workload_object()
    _install_source_observation(monkeypatch, target, workload=state)

    result = target.prepare_source_retirement(
        "cpu-api", "verified-plan", _retirement_payload(), fencing_token=13
    )

    assert result["stage"] == "REPLACEMENT_REQUESTED"
    assert state["spec"]["replicas"] == 3  # type: ignore[index]
    assert state["spec"]["executionRequest"] == {  # type: ignore[index]
        "planID": "verified-plan",
        "operationID": "verified-operation",
        "generation": 13,
        "expectedReplicas": 2,
        "targetReplicas": 3,
        "minimumReadyReplicas": 2,
        "destinationNodes": [{"name": "worker-b", "uid": "worker-b-uid"}],
        "purpose": "staged-replacement-v1",
    }


def test_takeover_target_fence_rejects_a_delayed_replacement_patch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    _skip_fencing_crd_preflight(monkeypatch, target)
    state = _workload_object()
    _install_source_observation(monkeypatch, target, workload=state)
    apply_patch = target._json
    old_patch_waiting = threading.Event()
    release_old_patch = threading.Event()
    old_errors: list[Exception] = []

    def pause_old_patch(*args: str) -> dict[str, object]:
        if (
            args[:2] == ("patch", "fleetrightworkload")
            and threading.current_thread().name == "controller-a"
        ):
            old_patch_waiting.set()
            assert release_old_patch.wait(timeout=5)
        return apply_patch(*args)

    monkeypatch.setattr(target, "_json", pause_old_patch)

    def old_authorization() -> None:
        try:
            target.prepare_source_retirement(
                "cpu-api", "verified-plan", _retirement_payload(), fencing_token=13
            )
        except Exception as error:
            old_errors.append(error)

    old_controller = threading.Thread(name="controller-a", target=old_authorization)
    old_controller.start()
    assert old_patch_waiting.wait(timeout=5)
    before_fence = target.observe_scale("cpu-api")
    target.fence_scale(
        "cpu-api",
        14,
        expected=before_fence,
        plan_id="verified-plan",
        operation_id="verified-operation",
        target_replicas=3,
        destination_nodes=[{"name": "worker-b", "uid": "worker-b-uid"}],
        purpose="staged-replacement-v1",
        minimum_ready_replicas=2,
    )
    release_old_patch.set()
    old_controller.join(timeout=5)

    assert not old_controller.is_alive()
    assert len(old_errors) == 1 and isinstance(old_errors[0], TargetPreconditionFailed)
    assert state["spec"]["replicas"] == 2  # type: ignore[index]
    assert "executionRequest" not in state["spec"]  # type: ignore[operator]
    assert state["spec"]["executionFence"]["generation"] == 14  # type: ignore[index]


@pytest.mark.parametrize(
    ("fencing_token", "deleting", "pdb_state", "source_uid_changed"),
    [
        (13, False, "ready", False),
        (13, True, "ready", False),
        (14, False, "ready", False),
        (13, False, "stale", False),
        (13, False, "blocked", False),
        (13, False, "ready", True),
    ],
)
def test_source_retirement_binds_observed_ready_replacement_uid_and_node(
    monkeypatch: pytest.MonkeyPatch,
    fencing_token: int,
    deleting: bool,
    pdb_state: str,
    source_uid_changed: bool,
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    _skip_fencing_crd_preflight(monkeypatch, target)
    state = _workload_object()
    state["metadata"].update(generation=5, resourceVersion="9")  # type: ignore[index]
    state["spec"]["replicas"] = 3  # type: ignore[index]
    state["spec"]["executionRequest"] = {  # type: ignore[index]
        "planID": "verified-plan",
        "operationID": "verified-operation",
        "generation": 13,
        "expectedReplicas": 2,
        "targetReplicas": 3,
        "minimumReadyReplicas": 2,
        "destinationNodes": [{"name": "worker-b", "uid": "worker-b-uid"}],
        "purpose": "staged-replacement-v1",
    }
    state["spec"]["executionFence"] = {  # type: ignore[index]
        "planID": "verified-plan",
        "operationID": "verified-operation",
        "generation": 13,
        "expectedGeneration": 4,
        "expectedReplicas": 2,
        "fencedGeneration": 5,
    }
    create = {
        "workloadUID": "workload-uid",
        "podUID": "replacement-pod-uid",
        "podName": _pod_name("workload-uid", 2),
        "replicaID": "cpu-api-2",
        "workloadGeneration": 5,
        "planID": "verified-plan",
        "operationID": "verified-operation",
        "createID": "create-attempt-2",
    }
    state["status"].update(  # type: ignore[index]
        observedGeneration=5,
        replicas=3,
        readyReplicas=3,
        availableReplicas=3,
        authorizedPodCreates=[create],
        execution={
            "planID": "verified-plan",
            "operationID": "verified-operation",
            "generation": 13,
            "targetReplicas": 3,
            "purpose": "staged-replacement-v1",
            "phase": "APPLIED",
        },
    )
    replacement = {
        "metadata": {
            "name": create["podName"],
            "uid": create["podUID"],
            "labels": {
                "fleetright.io/workload-uid": "workload-uid",
                "fleetright.io/replica-id": "cpu-api-2",
            },
            "annotations": {
                POD_CREATE_PLAN_ANNOTATION: "verified-plan",
                POD_CREATE_OPERATION_ANNOTATION: "verified-operation",
                POD_CREATE_ATTEMPT_ANNOTATION: "create-attempt-2",
                WORKLOAD_GENERATION_ANNOTATION: "5",
            },
            "ownerReferences": [
                {
                    "kind": "FleetRightWorkload",
                    "name": "cpu-api",
                    "uid": "workload-uid",
                    "controller": True,
                }
            ],
        },
        "spec": {"nodeName": "worker-b"},
        "status": {
            "phase": "Running",
            "conditions": [
                {
                    "type": "Ready",
                    "status": "True",
                    "lastTransitionTime": "2000-01-01T00:00:00Z",
                }
            ],
        },
    }
    if deleting:
        replacement["metadata"]["deletionTimestamp"] = "2026-10-11T00:00:00Z"
    _install_source_observation(monkeypatch, target, workload=state, replacement_pod=replacement)

    original_json = target._json
    source_reads = 0

    def retirement_observation(*args: str) -> dict[str, object]:
        nonlocal source_reads
        value = original_json(*args)
        if args[:2] == ("get", "pod") and args[2] == _pod_name("workload-uid", 0):
            source_reads += 1
            if source_uid_changed and source_reads == 2:
                value["metadata"]["uid"] = "new-source-pod-uid"  # type: ignore[index]
        if args[:2] == ("get", "pdb") and pdb_state != "ready":
            status = value["status"]
            assert isinstance(status, dict)
            status.update(
                observedGeneration=1 if pdb_state == "stale" else 2,
                desiredHealthy=1 if pdb_state == "stale" else 2,
                currentHealthy=1 if pdb_state == "stale" else 2,
                disruptionsAllowed=0,
            )
        return value

    monkeypatch.setattr(target, "_json", retirement_observation)

    if source_uid_changed:
        with pytest.raises(ActionBlocked, match="SOURCE_POD_UID_CHANGED"):
            target.prepare_source_retirement(
                "cpu-api", "verified-plan", _retirement_payload(), fencing_token=fencing_token
            )
        assert state["status"].get("retiredReplicaSlots", []) == []  # type: ignore[index]
        return

    result = target.prepare_source_retirement(
        "cpu-api", "verified-plan", _retirement_payload(), fencing_token=fencing_token
    )

    if fencing_token == 14:
        assert result == {"stage": "REPLACEMENT_REQUESTED", "surge_replicas": 3}
        request = state["spec"]["executionRequest"]  # type: ignore[index]
        assert request["expectedReplicas"] == 2
        assert request["targetReplicas"] == 3
        assert request["generation"] == 14
        assert state["spec"]["replicas"] == 3  # type: ignore[index]
        assert create in state["status"]["authorizedPodCreates"]  # type: ignore[index]
        return
    if deleting:
        assert result == {"stage": "REPLACEMENT_WAITING_READY", "replica_id": "cpu-api-2"}
        return
    if pdb_state == "stale":
        assert result["stage"] == "PDB_STATUS_PENDING"
        assert state["status"].get("retiredReplicaSlots", []) == []  # type: ignore[index]
        return
    if pdb_state == "blocked":
        assert result["stage"] == "PDB_BLOCKED"
        assert state["status"].get("retiredReplicaSlots", []) == []  # type: ignore[index]
        return
    assert result == {
        "stage": "READY_FOR_SAFE_EVICTION",
        "workload_uid": "workload-uid",
        "workload_generation": 5,
        "source_pod_name": _pod_name("workload-uid", 0),
        "source_pod_uid": "source-pod-uid",
        "source_pod_resource_version": "3",
        "source_node_id": "worker-a",
        "source_node_uid": "node-uid",
        "replacement_pod_name": create["podName"],
        "replacement_pod_uid": "replacement-pod-uid",
        "replacement_replica_id": "cpu-api-2",
        "replacement_node_id": "worker-b",
        "replacement_node_uid": "worker-b-uid",
        "replacement_workload_generation": 5,
        "replacement_create_id": "create-attempt-2",
        "ready_since": "2000-01-01T00:00:00Z",
        "source_retirement_intent_recorded": True,
    }
    tombstone = state["status"]["retiredReplicaSlots"][0]  # type: ignore[index]
    assert tombstone["replicaID"] == "cpu-api-0"
    assert tombstone["podUID"] == "source-pod-uid"
    assert tombstone["planID"] == "verified-plan"
    assert tombstone["operationID"] == "verified-operation"
    assert tombstone["sourceNodeName"] == "worker-a"
    assert tombstone["sourceNodeUID"] == "node-uid"
    assert tombstone["replacementPodUID"] == "replacement-pod-uid"
    assert tombstone["replacementNodeName"] == "worker-b"
    assert tombstone["replacementNodeUID"] == "worker-b-uid"
    captured: dict[str, object] = {}

    def send_eviction(*args: str, stdin: str | None = None) -> SimpleNamespace:
        captured["args"] = args
        captured["body"] = json.loads(stdin or "{}")
        return SimpleNamespace(stdout='{"kind":"Status","status":"Success"}')

    monkeypatch.setattr(target, "_kubectl", send_eviction)
    eviction_result = target.evict_source(
        "cpu-api",
        "verified-plan",
        _retirement_payload(),
        result,
        fencing_token=13,
    )
    assert eviction_result["stage"] == "EVICTION_ACCEPTED"
    assert any(
        "/pods/" + _pod_name("workload-uid", 0) + "/eviction" in arg for arg in captured["args"]
    )
    eviction = captured["body"]
    assert eviction["apiVersion"] == "policy/v1"
    assert eviction["kind"] == "Eviction"
    assert eviction["deleteOptions"]["preconditions"] == {
        "uid": "source-pod-uid",
        "resourceVersion": "3",
    }
    assert eviction["metadata"]["annotations"]["fleetright.io/fencing-token"] == "13"


def test_source_retirement_rejects_unapproved_replacement_destination(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    _skip_fencing_crd_preflight(monkeypatch, target)
    state = _workload_object()
    _install_source_observation(monkeypatch, target, workload=state)

    with pytest.raises(ActionBlocked, match="DESTINATION_NODE_OUTSIDE_WORKLOAD_PLACEMENT"):
        target.prepare_source_retirement(
            "cpu-api",
            "verified-plan",
            _retirement_payload(destination_nodes=[{"name": "worker-c", "uid": "worker-c-uid"}]),
            fencing_token=13,
        )
    assert "executionRequest" not in state["spec"]  # type: ignore[operator]


def test_source_retirement_target_rejects_changed_source_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    observed = target._observe(_workload_object(), "cpu-api")
    monkeypatch.setattr(target, "observe_scale", lambda _workload: observed)

    with pytest.raises(ActionBlocked, match="SOURCE_WORKLOAD_UID_CHANGED"):
        target.prepare_source_retirement(
            "cpu-api",
            "verified-plan",
            _retirement_payload(source_owner_uid="old-workload-uid"),
        )


@pytest.mark.parametrize(
    ("pod_uid", "node_uid", "reason"),
    [
        ("late-pod-uid", "node-uid", "SOURCE_POD_UID_CHANGED"),
        ("source-pod-uid", "replacement-node-uid", "SOURCE_NODE_UID_CHANGED"),
    ],
)
def test_source_retirement_target_revalidates_live_source_pod_and_node_uid(
    monkeypatch: pytest.MonkeyPatch, pod_uid: str, node_uid: str, reason: str
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    observed = target._observe(_workload_object(), "cpu-api")
    monkeypatch.setattr(target, "observe_scale", lambda _workload: observed)
    _install_source_observation(monkeypatch, target, pod_uid=pod_uid, node_uid=node_uid)
    assert target._json("get", "node", "worker-a", "-o", "json")["metadata"]["uid"] == node_uid

    with pytest.raises(ActionBlocked, match=reason):
        target.prepare_source_retirement("cpu-api", "verified-plan", _retirement_payload())


@pytest.mark.parametrize(
    ("status_update", "protocol", "reason"),
    [
        (
            {
                "pendingPodCreate": {
                    "workloadUID": "workload-uid",
                    "workloadGeneration": 4,
                    "podName": "frw-workload-r2",
                    "replicaID": "cpu-api-2",
                    "planID": "plan-1",
                    "operationID": "action-1",
                    "createID": "attempt-1",
                }
            },
            "reserved-create-v1",
            "POD_CREATE_OUTCOME_UNKNOWN",
        ),
        ({"podCreateHistoryUncertain": True}, "reserved-create-v1", "POD_CREATE_HISTORY_UNCERTAIN"),
        ({}, None, "UNSUPPORTED_POD_CREATE_PROTOCOL"),
    ],
)
def test_source_retirement_target_fails_closed_on_unresolved_or_legacy_create(
    monkeypatch: pytest.MonkeyPatch,
    status_update: dict[str, object],
    protocol: str | None,
    reason: str,
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    state = _workload_object()
    state["status"].update(status_update)
    if protocol is None:
        del state["spec"]["podCreateProtocol"]
        with pytest.raises(TargetPreconditionFailed, match="Pod CREATE history is uncertain"):
            target._observe(state, "cpu-api")
        return
    observed = target._observe(state, "cpu-api")
    monkeypatch.setattr(target, "observe_scale", lambda _workload: observed)

    with pytest.raises(ActionBlocked, match=reason):
        target.prepare_source_retirement(
            "cpu-api",
            "verified-plan",
            _retirement_payload(),
        )


def test_workload_request_cas_binds_identity_generation_and_replica_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    _skip_fencing_crd_preflight(monkeypatch, target)
    state = _workload_object()
    patches: list[dict[str, object]] = []

    def apply(*args: str) -> dict[str, object]:
        if args[0] == "get":
            return deepcopy(state)
        patch = json.loads(args[args.index("-p") + 1])
        patches.append(patch)
        if patch["metadata"]["resourceVersion"] != state["metadata"]["resourceVersion"]:  # type: ignore[index]
            raise TargetPreconditionFailed("resourceVersion compare-and-swap failed")
        state["spec"].update(patch["spec"])  # type: ignore[index]
        state["metadata"]["generation"] += 1  # type: ignore[index]
        state["metadata"]["resourceVersion"] = str(  # type: ignore[index]
            int(state["metadata"]["resourceVersion"]) + 1  # type: ignore[index]
        )
        return deepcopy(state)

    monkeypatch.setattr(target, "_json", apply)
    before = target.observe_scale("cpu-api")
    result = target.authorize_scale(
        "cpu-api", 12, expected=before, plan_id="plan-1", operation_id="action-1", target_replicas=3
    )

    assert result["uid"] == "workload-uid"
    assert result["replicas"] == 3
    assert result["fencing_token"] == 12
    assert result["execution_request"] == {
        "planID": "plan-1",
        "operationID": "action-1",
        "generation": 12,
        "expectedReplicas": 2,
        "targetReplicas": 3,
    }
    assert patches[0] == {
        "metadata": {"resourceVersion": "7"},
        "spec": {
            "executionRequest": {
                "planID": "plan-1",
                "operationID": "action-1",
                "generation": 12,
                "expectedReplicas": 2,
                "targetReplicas": 3,
            },
            "executionFence": {
                "planID": "plan-1",
                "operationID": "action-1",
                "generation": 12,
                "expectedGeneration": 4,
                "expectedReplicas": 2,
                "fencedGeneration": 5,
            },
            "replicas": 3,
        },
    }


def _execution_crd(rules: set[str]) -> dict[str, object]:
    return {
        "spec": {
            "versions": [
                {
                    "name": "v1alpha1",
                    "schema": {
                        "openAPIV3Schema": {
                            "x-kubernetes-validations": [
                                {"rule": rule}
                                for rule in (
                                    _WORKLOAD_PENDING_CREATE_RULE,
                                    _WORKLOAD_AUTHORIZED_POD_CREATES_RULE,
                                    _WORKLOAD_RETIRED_SLOTS_RULE,
                                    _WORKLOAD_CREATE_HISTORY_UNCERTAIN_RULE,
                                )
                            ],
                            "properties": {
                                "spec": {
                                    "properties": {
                                        "executionProtocol": {"enum": ["monotonic-scale-up-v1"]},
                                        "podCreateProtocol": {"enum": ["reserved-create-v1"]},
                                        "executionRequest": {
                                            "required": [
                                                "planID",
                                                "operationID",
                                                "generation",
                                                "expectedReplicas",
                                                "targetReplicas",
                                            ]
                                        },
                                        "executionFence": {
                                            "required": [
                                                "planID",
                                                "operationID",
                                                "generation",
                                                "expectedGeneration",
                                                "expectedReplicas",
                                                "fencedGeneration",
                                            ]
                                        },
                                    },
                                    "x-kubernetes-validations": [
                                        {
                                            "rule": rule,
                                            **(
                                                {"optionalOldSelf": True}
                                                if rule == _WORKLOAD_POD_CREATE_PROTOCOL_RULE
                                                else {}
                                            ),
                                        }
                                        for rule in {*rules, _WORKLOAD_POD_CREATE_PROTOCOL_RULE}
                                    ],
                                },
                                "status": {
                                    "properties": {
                                        "pendingPodCreate": {
                                            "required": [
                                                "workloadUID",
                                                "workloadGeneration",
                                                "podName",
                                                "replicaID",
                                                "planID",
                                                "operationID",
                                                "createID",
                                            ]
                                        },
                                        "authorizedPodCreates": {
                                            "x-kubernetes-list-type": "map",
                                            "x-kubernetes-list-map-keys": ["podUID"],
                                            "maxItems": 1_000,
                                            "items": {
                                                "required": [
                                                    "workloadUID",
                                                    "podUID",
                                                    "podName",
                                                    "replicaID",
                                                    "workloadGeneration",
                                                    "planID",
                                                    "operationID",
                                                    "createID",
                                                ]
                                            },
                                        },
                                        "retiredReplicaSlots": {
                                            "x-kubernetes-list-type": "map",
                                            "x-kubernetes-list-map-keys": ["replicaID"],
                                            "maxItems": 1_000,
                                            "items": {
                                                "required": [
                                                    "workloadUID",
                                                    "replicaID",
                                                    "podUID",
                                                    "workloadGeneration",
                                                    "planID",
                                                    "operationID",
                                                ]
                                            },
                                        },
                                    }
                                },
                            },
                        }
                    },
                }
            ]
        }
    }


def test_workload_preflight_requires_old_fence_advance_rule(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    advance_rule = (
        "!has(oldSelf.executionFence) || !has(self.executionRequest) || "
        "(has(oldSelf.executionRequest) && self.executionRequest == oldSelf.executionRequest) || "
        "self.executionRequest.generation > oldSelf.executionFence.generation"
    )
    assert advance_rule in _WORKLOAD_EXECUTION_RULES
    crd = _execution_crd(_WORKLOAD_EXECUTION_RULES - {advance_rule})
    monkeypatch.setattr(target, "_json", lambda *_args: crd)

    with pytest.raises(TargetPreconditionFailed, match="execution CRD is not installed"):
        target._require_fencing_schema()


def test_workload_preflight_requires_create_time_pod_protocol_rule(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    crd = _execution_crd(_WORKLOAD_EXECUTION_RULES)
    spec = crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
    for rule in spec["x-kubernetes-validations"]:
        if rule["rule"] == _WORKLOAD_POD_CREATE_PROTOCOL_RULE:
            del rule["optionalOldSelf"]
    monkeypatch.setattr(target, "_json", lambda *_args: crd)

    with pytest.raises(TargetPreconditionFailed, match="execution CRD is not installed"):
        target._require_fencing_schema()


@pytest.mark.parametrize(
    "missing_rule",
    [
        "self.replicas >= oldSelf.replicas",
        "self.placement == oldSelf.placement",
        "self.podTemplate == oldSelf.podTemplate",
    ],
)
def test_workload_preflight_requires_immutable_monotonic_contract_rules(
    monkeypatch: pytest.MonkeyPatch, missing_rule: str
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    assert missing_rule in _WORKLOAD_EXECUTION_RULES
    crd = _execution_crd(_WORKLOAD_EXECUTION_RULES - {missing_rule})
    monkeypatch.setattr(target, "_json", lambda *_args: crd)

    with pytest.raises(TargetPreconditionFailed, match="execution CRD is not installed"):
        target._require_fencing_schema()


def test_workload_preflight_requires_complete_execution_request_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    crd = _execution_crd(_WORKLOAD_EXECUTION_RULES)
    version = crd["spec"]["versions"][0]
    spec = version["schema"]["openAPIV3Schema"]["properties"]["spec"]
    spec["properties"]["executionRequest"]["required"].remove("targetReplicas")
    monkeypatch.setattr(target, "_json", lambda *_args: crd)

    with pytest.raises(TargetPreconditionFailed, match="execution CRD is not installed"):
        target._require_fencing_schema()


def test_workload_preflight_requires_pending_pod_create_status_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    crd = _execution_crd(_WORKLOAD_EXECUTION_RULES)
    status = crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["status"]
    status["properties"]["pendingPodCreate"]["required"].remove("podName")
    monkeypatch.setattr(target, "_json", lambda *_args: crd)

    with pytest.raises(TargetPreconditionFailed, match="execution CRD is not installed"):
        target._require_fencing_schema()


def test_workload_preflight_requires_root_pending_create_spec_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    crd = _execution_crd(_WORKLOAD_EXECUTION_RULES)
    root = crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]
    root["x-kubernetes-validations"].clear()
    monkeypatch.setattr(target, "_json", lambda *_args: crd)

    with pytest.raises(TargetPreconditionFailed, match="execution CRD is not installed"):
        target._require_fencing_schema()


@pytest.mark.parametrize(
    "missing_rule",
    [
        _WORKLOAD_AUTHORIZED_POD_CREATES_RULE,
        _WORKLOAD_RETIRED_SLOTS_RULE,
        _WORKLOAD_CREATE_HISTORY_UNCERTAIN_RULE,
    ],
)
def test_workload_preflight_requires_durable_create_and_retirement_guards(
    monkeypatch: pytest.MonkeyPatch, missing_rule: str
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    crd = _execution_crd(_WORKLOAD_EXECUTION_RULES)
    root = crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]
    root["x-kubernetes-validations"] = [
        rule for rule in root["x-kubernetes-validations"] if rule["rule"] != missing_rule
    ]
    monkeypatch.setattr(target, "_json", lambda *_args: crd)

    with pytest.raises(TargetPreconditionFailed, match="execution CRD is not installed"):
        target._require_fencing_schema()


@pytest.mark.parametrize("method", ["fence_scale", "authorize_scale"])
def test_workload_authority_blocks_while_pod_create_outcome_is_unknown(
    monkeypatch: pytest.MonkeyPatch, method: str
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    state = _workload_object()
    state["status"]["pendingPodCreate"] = {
        "workloadUID": "workload-uid",
        "workloadGeneration": 4,
        "podName": "frw-workload-r2",
        "replicaID": "cpu-api-2",
        "planID": "plan-1",
        "operationID": "action-1",
        "createID": "attempt-1",
    }
    observed = target._observe(state, "cpu-api")
    monkeypatch.setattr(target, "observe_scale", lambda _: observed)
    monkeypatch.setattr(target, "_require_fencing_schema", lambda: None)

    with pytest.raises(ActionBlocked, match="POD_CREATE_OUTCOME_UNKNOWN"):
        getattr(target, method)(
            "cpu-api",
            12,
            expected=observed,
            plan_id="plan-1",
            operation_id="action-1",
            target_replicas=3,
        )


@pytest.mark.parametrize("method", ["fence_scale", "authorize_scale"])
@pytest.mark.parametrize(
    ("status_update", "reason"),
    [
        ({"podCreateHistoryUncertain": True}, "POD_CREATE_HISTORY_UNCERTAIN"),
        ({"unverifiedPodCreates": 1}, "POD_CREATE_HISTORY_UNCERTAIN"),
    ],
)
def test_workload_authority_stays_blocked_after_stale_pod_disappears(
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    status_update: dict[str, object],
    reason: str,
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    state = _workload_object()
    state["status"].update(status_update)
    observed = target._observe(state, "cpu-api")
    monkeypatch.setattr(target, "observe_scale", lambda _: observed)
    monkeypatch.setattr(target, "_require_fencing_schema", lambda: None)

    with pytest.raises(ActionBlocked, match=reason):
        getattr(target, method)(
            "cpu-api",
            12,
            expected=observed,
            plan_id="plan-1",
            operation_id="action-1",
            target_replicas=3,
        )


def test_retirement_history_fences_only_the_exact_logical_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    state = _workload_object()
    state["status"]["retiredReplicaSlots"] = [
        {
            "workloadUID": "workload-uid",
            "replicaID": "cpu-api-1",
            "podUID": "source-pod-uid",
            "workloadGeneration": 4,
            "planID": "old-plan",
            "operationID": "old-operation",
        }
    ]
    observed = target._observe(state, "cpu-api")
    monkeypatch.setattr(target, "observe_scale", lambda _: observed)
    monkeypatch.setattr(target, "_require_fencing_schema", lambda: None)

    with pytest.raises(ActionBlocked, match="SOURCE_REPLICA_ALREADY_RETIRED"):
        target.authorize_scale(
            "cpu-api",
            12,
            expected=observed,
            plan_id="new-plan",
            operation_id="new-operation",
            target_replicas=3,
            purpose="staged-replacement-v1",
            minimum_ready_replicas=1,
            source_replica_id="cpu-api-1",
            source_pod_uid="source-pod-uid",
        )


def test_legacy_workload_cannot_be_promoted_for_execution() -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    state = _workload_object()
    del state["spec"]["podCreateProtocol"]
    state["status"]["podCreateHistoryUncertain"] = True

    with pytest.raises(TargetPreconditionFailed, match="Pod CREATE history is uncertain"):
        target._observe(state, "cpu-api")


def test_pending_create_reservation_wins_authority_resource_version_cas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    _skip_fencing_crd_preflight(monkeypatch, target)
    state = _workload_object()

    def apply(*args: str) -> dict[str, object]:
        if args[0] == "get":
            return deepcopy(state)
        patch = json.loads(args[args.index("-p") + 1])
        state["status"]["pendingPodCreate"] = {
            "workloadUID": "workload-uid",
            "workloadGeneration": 4,
            "podName": "frw-workload-r2",
            "replicaID": "cpu-api-2",
            "planID": "plan-1",
            "operationID": "action-1",
            "createID": "attempt-1",
        }  # type: ignore[index]
        state["metadata"]["resourceVersion"] = "8"  # type: ignore[index]
        if patch["metadata"]["resourceVersion"] != "8":
            raise TargetPreconditionFailed("target resource version changed")
        state["spec"].update(patch["spec"])  # type: ignore[index]
        return deepcopy(state)

    monkeypatch.setattr(target, "_json", apply)
    before = target.observe_scale("cpu-api")

    with pytest.raises(TargetPreconditionFailed, match="resource version changed"):
        target.authorize_scale(
            "cpu-api",
            12,
            expected=before,
            plan_id="plan-1",
            operation_id="action-1",
            target_replicas=3,
        )

    assert state["spec"]["replicas"] == 2  # type: ignore[index]
    assert "executionRequest" not in state["spec"]  # type: ignore[operator]


def test_authority_patch_wins_and_stale_create_reservation_cannot_post(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fleetright.workload_controller import ApiError as ControllerApiError
    from fleetright.workload_controller import _write_pending_pod_create

    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    _skip_fencing_crd_preflight(monkeypatch, target)
    state = _workload_object()

    def apply(*args: str) -> dict[str, object]:
        if args[0] == "get":
            return deepcopy(state)
        patch = json.loads(args[args.index("-p") + 1])
        if patch["metadata"]["resourceVersion"] != state["metadata"]["resourceVersion"]:  # type: ignore[index]
            raise TargetPreconditionFailed("target resource version changed")
        state["spec"].update(patch["spec"])  # type: ignore[index]
        state["metadata"]["generation"] += 1  # type: ignore[index]
        state["metadata"]["resourceVersion"] = str(  # type: ignore[index]
            int(state["metadata"]["resourceVersion"]) + 1  # type: ignore[index]
        )
        return deepcopy(state)

    monkeypatch.setattr(target, "_json", apply)
    before = target.observe_scale("cpu-api")
    target.authorize_scale(
        "cpu-api",
        12,
        expected=before,
        plan_id="plan-1",
        operation_id="action-1",
        target_replicas=3,
    )
    create_calls = 0

    class StatusAPI:
        def patch_status(
            self, _name: str, resource_version: str, _status: dict[str, object]
        ) -> dict[str, object]:
            if resource_version != state["metadata"]["resourceVersion"]:  # type: ignore[index]
                raise ControllerApiError(409, "resourceVersion conflict")
            return {"metadata": {"resourceVersion": "9"}, "status": {}}

        def create_pod(self, _pod: dict[str, object]) -> None:
            nonlocal create_calls
            create_calls += 1

    workload = {
        "metadata": {"name": "cpu-api", "resourceVersion": "7"},
        "status": {},
    }
    with pytest.raises(ControllerApiError, match="resourceVersion conflict"):
        _write_pending_pod_create(
            StatusAPI(),
            workload,
            {
                "workloadUID": "workload-uid",
                "workloadGeneration": 4,
                "podName": "frw-workload-r2",
                "replicaID": "cpu-api-2",
            },
        )

    assert create_calls == 0
    assert state["spec"]["replicas"] == 3  # type: ignore[index]


def test_workload_takeover_fence_changes_authority_without_scaling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    _skip_fencing_crd_preflight(monkeypatch, target)
    state = _workload_object()
    patches: list[dict[str, object]] = []

    def apply(*args: str) -> dict[str, object]:
        if args[0] == "get":
            return deepcopy(state)
        patch = json.loads(args[args.index("-p") + 1])
        patches.append(patch)
        state["spec"].update(patch["spec"])  # type: ignore[index]
        state["metadata"]["generation"] += 1  # type: ignore[index]
        state["metadata"]["resourceVersion"] = str(  # type: ignore[index]
            int(state["metadata"]["resourceVersion"]) + 1  # type: ignore[index]
        )
        return deepcopy(state)

    monkeypatch.setattr(target, "_json", apply)
    before = target.observe_scale("cpu-api")
    result = target.fence_scale(
        "cpu-api",
        12,
        expected=before,
        plan_id="plan-1",
        operation_id="action-1",
        target_replicas=3,
    )
    repeated = target.fence_scale(
        "cpu-api",
        12,
        expected=before,
        plan_id="plan-1",
        operation_id="action-1",
        target_replicas=3,
    )
    advanced = target.fence_scale(
        "cpu-api",
        13,
        expected=repeated,
        plan_id="plan-1",
        operation_id="action-1",
        target_replicas=3,
    )
    authorized = target.authorize_scale(
        "cpu-api",
        14,
        expected=advanced,
        plan_id="plan-1",
        operation_id="action-1",
        target_replicas=3,
    )

    assert result["replicas"] == repeated["replicas"] == advanced["replicas"] == 2
    assert result["fencing_token"] == repeated["fencing_token"] == 12
    assert advanced["fencing_token"] == 13
    assert authorized["replicas"] == 3
    assert authorized["execution_fence"]["generation"] == 14
    assert authorized["execution_fence"]["expectedGeneration"] == 4
    assert authorized["execution_fence"]["expectedReplicas"] == 2
    assert result["execution_request"] is None
    assert result["execution_fence"] == {
        "planID": "plan-1",
        "operationID": "action-1",
        "generation": 12,
        "expectedGeneration": 4,
        "expectedReplicas": 2,
        "fencedGeneration": 5,
    }
    assert patches[:2] == [
        {
            "metadata": {"resourceVersion": "7"},
            "spec": {"executionFence": result["execution_fence"]},
        },
        {
            "metadata": {"resourceVersion": "8"},
            "spec": {
                "executionFence": {
                    "planID": "plan-1",
                    "operationID": "action-1",
                    "generation": 13,
                    "expectedGeneration": 4,
                    "expectedReplicas": 2,
                    "fencedGeneration": 6,
                }
            },
        },
    ]


def test_workload_takeover_fence_replay_rejects_later_spec_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    _skip_fencing_crd_preflight(monkeypatch, target)
    state = _workload_object()

    def apply(*args: str) -> dict[str, object]:
        if args[0] == "get":
            return deepcopy(state)
        patch = json.loads(args[args.index("-p") + 1])
        state["spec"].update(patch["spec"])  # type: ignore[index]
        state["metadata"]["generation"] += 1  # type: ignore[index]
        state["metadata"]["resourceVersion"] = str(  # type: ignore[index]
            int(state["metadata"]["resourceVersion"]) + 1  # type: ignore[index]
        )
        return deepcopy(state)

    monkeypatch.setattr(target, "_json", apply)
    before = target.observe_scale("cpu-api")
    target.fence_scale(
        "cpu-api",
        12,
        expected=before,
        plan_id="plan-1",
        operation_id="action-1",
        target_replicas=3,
    )
    state["metadata"]["generation"] += 1  # type: ignore[index]
    state["metadata"]["resourceVersion"] = "external-spec-update"  # type: ignore[index]

    with pytest.raises(TargetPreconditionFailed, match="changed after the authority fence"):
        target.fence_scale(
            "cpu-api",
            12,
            expected=before,
            plan_id="plan-1",
            operation_id="action-1",
            target_replicas=3,
        )


def test_takeover_fences_an_already_published_request_without_replaying_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    _skip_fencing_crd_preflight(monkeypatch, target)
    state = _workload_object()
    state["metadata"]["generation"] = 5  # type: ignore[index]
    state["spec"].update(  # type: ignore[index]
        replicas=3,
        executionRequest={
            "planID": "plan-1",
            "operationID": "action-1",
            "generation": 10,
            "expectedReplicas": 2,
            "targetReplicas": 3,
        },
    )
    state["status"].update(replicas=2, observedGeneration=4)  # type: ignore[index]
    patches: list[dict[str, object]] = []

    def apply(*args: str) -> dict[str, object]:
        if args[0] == "get":
            return deepcopy(state)
        patch = json.loads(args[args.index("-p") + 1])
        patches.append(patch)
        state["spec"].update(patch["spec"])  # type: ignore[index]
        state["metadata"]["generation"] += 1  # type: ignore[index]
        state["metadata"]["resourceVersion"] = str(  # type: ignore[index]
            int(state["metadata"]["resourceVersion"]) + 1  # type: ignore[index]
        )
        return deepcopy(state)

    monkeypatch.setattr(target, "_json", apply)
    stale_expected = {"uid": "workload-uid", "generation": 4, "replicas": 2}
    with pytest.raises(TargetPreconditionFailed, match="changed before fencing"):
        target.fence_scale(
            "cpu-api",
            12,
            expected=stale_expected,
            plan_id="plan-1",
            operation_id="action-1",
            target_replicas=3,
        )
    fenced = target.fence_scale(
        "cpu-api",
        12,
        expected=target.observe_scale("cpu-api"),
        plan_id="plan-1",
        operation_id="action-1",
        target_replicas=3,
    )
    replayed = target.fence_scale(
        "cpu-api",
        12,
        expected=stale_expected,
        plan_id="plan-1",
        operation_id="action-1",
        target_replicas=3,
    )

    assert fenced["replicas"] == 3
    assert fenced["operation_applied_before_fence"] is True
    assert replayed["fence_replayed"] is True
    assert replayed["fence_precondition"] == fenced["fence_precondition"]
    assert fenced["fence_precondition"] == {
        "uid": "workload-uid",
        "generation": 5,
        "replicas": 3,
    }
    assert fenced["execution_request"]["generation"] == 10
    assert fenced["execution_fence"]["generation"] == 12
    assert patches == [
        {
            "metadata": {"resourceVersion": "7"},
            "spec": {
                "executionFence": {
                    "planID": "plan-1",
                    "operationID": "action-1",
                    "generation": 12,
                    "expectedGeneration": 4,
                    "expectedReplicas": 2,
                    "fencedGeneration": 6,
                }
            },
        }
    ]


def test_workload_mutation_fails_closed_when_fencing_crd_is_old(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    state = _workload_object()
    patches: list[dict[str, object]] = []

    def apply(*args: str) -> dict[str, object]:
        if args[0] == "get" and args[1] == "fleetrightworkload":
            return deepcopy(state)
        if args[0] == "get" and args[1] == "crd":
            return {
                "spec": {
                    "versions": [
                        {
                            "name": "v1alpha1",
                            "schema": {
                                "openAPIV3Schema": {"properties": {"spec": {"properties": {}}}}
                            },
                        }
                    ]
                }
            }
        patches.append(json.loads(args[args.index("-p") + 1]))
        return deepcopy(state)

    monkeypatch.setattr(target, "_json", apply)
    before = target.observe_scale("cpu-api")
    with pytest.raises(TargetPreconditionFailed, match="execution CRD is not installed"):
        target.authorize_scale(
            "cpu-api",
            12,
            expected=before,
            plan_id="plan-1",
            operation_id="action-1",
            target_replicas=3,
        )
    assert patches == []


def test_workload_scale_down_is_rejected_before_any_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    before = target._observe(_workload_object(), "cpu-api")
    monkeypatch.setattr(target, "observe_scale", lambda _name: before)
    monkeypatch.setattr(
        target, "_json", lambda *_args: pytest.fail("scale-down patched Kubernetes")
    )

    with pytest.raises(ActionBlocked, match="WORKLOAD_SCALE_DOWN_DISABLED"):
        target.authorize_scale(
            "cpu-api",
            13,
            expected=before,
            plan_id="plan-1",
            operation_id="action-1",
            target_replicas=1,
        )


def test_workload_scale_down_is_blocked_while_pod_create_is_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    state = _workload_object()
    state["status"]["pendingPodCreate"] = {
        "workloadUID": "workload-uid",
        "workloadGeneration": 4,
        "podName": "frw-pending",
        "replicaID": "cpu-api-2",
        "planID": "plan-1",
        "operationID": "action-1",
        "createID": "attempt-1",
    }
    before = target._observe(state, "cpu-api")
    monkeypatch.setattr(target, "observe_scale", lambda _name: before)
    monkeypatch.setattr(
        target, "_json", lambda *_args: pytest.fail("pending CREATE reached a Kubernetes patch")
    )

    with pytest.raises(ActionBlocked, match="POD_CREATE_OUTCOME_UNKNOWN"):
        target.authorize_scale(
            "cpu-api",
            13,
            expected=before,
            plan_id="plan-1",
            operation_id="retire-1",
            target_replicas=1,
        )


def test_workload_takeover_blocks_new_operation_until_prior_request_is_applied(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    _skip_fencing_crd_preflight(monkeypatch, target)
    value = _workload_object()
    value["spec"].update(
        replicas=3,
        executionRequest={
            "planID": "plan-1",
            "operationID": "action-1",
            "generation": 100,
            "expectedReplicas": 2,
            "targetReplicas": 3,
        },
    )
    value["status"].update(
        execution={
            "planID": "plan-1",
            "operationID": "action-1",
            "generation": 100,
            "targetReplicas": 3,
            "phase": "RECONCILING",
        }
    )
    current = target._observe(value, "cpu-api")
    monkeypatch.setattr(target, "observe_scale", lambda _name: current)
    monkeypatch.setattr(
        target, "_json", lambda *_args: pytest.fail("new request bypassed takeover")
    )

    with pytest.raises(ActionBlocked, match="PRIOR_WORKLOAD_OPERATION_UNRESOLVED"):
        target.authorize_scale(
            "cpu-api",
            101,
            expected=current,
            plan_id="plan-2",
            operation_id="action-2",
            target_replicas=4,
        )


def test_workload_authorization_fails_closed_on_unreconciled_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    _skip_fencing_crd_preflight(monkeypatch, target)
    value = _workload_object()
    value["status"]["conditions"] = [{"type": "Reconciled", "status": "False"}]
    current = target._observe(value, "cpu-api")
    monkeypatch.setattr(target, "observe_scale", lambda _name: current)
    monkeypatch.setattr(
        target, "_json", lambda *_args: pytest.fail("unreconciled workload was authorized")
    )

    with pytest.raises(ActionBlocked, match="WORKLOAD_NOT_RECONCILED"):
        target.authorize_scale(
            "cpu-api",
            13,
            expected=current,
            plan_id="plan-1",
            operation_id="action-1",
            target_replicas=3,
        )


def test_workload_action_is_not_complete_until_controller_status_is_applied() -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    value = _workload_object()
    value["spec"]["executionRequest"] = {
        "planID": "plan-1",
        "operationID": "action-1",
        "generation": 12,
        "expectedReplicas": 2,
        "targetReplicas": 2,
    }
    value["status"]["execution"] = {
        "planID": "plan-1",
        "operationID": "action-1",
        "generation": 12,
        "targetReplicas": 2,
        "phase": "APPLIED",
    }
    observed = target._observe(value, "cpu-api")
    assert ExecutionController._capacity_ready(observed)

    pending = deepcopy(value)
    pending["status"]["execution"]["phase"] = "RECONCILING"
    assert not ExecutionController._capacity_ready(target._observe(pending, "cpu-api"))

    incomplete = deepcopy(value)
    incomplete["status"]["replicas"] = 1
    assert not ExecutionController._capacity_ready(target._observe(incomplete, "cpu-api"))


def test_legacy_workload_cannot_be_targeted_by_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    value = _workload_object()
    del value["spec"]["executionProtocol"]
    monkeypatch.setattr(target, "_json", lambda *_args: value)

    with pytest.raises(TargetPreconditionFailed, match="not created for monotonic scale-up"):
        target.observe_scale("cpu-api")


def test_concurrent_workload_takeover_uses_resource_version_cas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    _skip_fencing_crd_preflight(monkeypatch, target)
    state = _workload_object()
    barrier = threading.Barrier(2)
    lock = threading.Lock()
    gets = 0

    def apply(*args: str) -> dict[str, object]:
        nonlocal gets
        if args[0] == "get":
            with lock:
                wait = gets < 2
                gets += 1
                result = deepcopy(state)
            if wait:
                barrier.wait(timeout=2)
            return result
        patch = json.loads(args[args.index("-p") + 1])
        with lock:
            if patch["metadata"]["resourceVersion"] != state["metadata"]["resourceVersion"]:  # type: ignore[index]
                raise TargetPreconditionFailed("resourceVersion compare-and-swap lost")
            state["spec"].update(patch["spec"])  # type: ignore[index]
            state["metadata"]["generation"] += 1  # type: ignore[index]
            state["metadata"]["resourceVersion"] = str(  # type: ignore[index]
                int(state["metadata"]["resourceVersion"]) + 1  # type: ignore[index]
            )
            return deepcopy(state)

    monkeypatch.setattr(target, "_json", apply)
    before = target._observe(state, "cpu-api")

    def takeover(token: int) -> object:
        try:
            return target.authorize_scale(
                "cpu-api",
                token,
                expected=before,
                plan_id="plan-1",
                operation_id="action-1",
                target_replicas=3,
            )
        except TargetPreconditionFailed as error:
            return error

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(takeover, (100, 101)))

    winner = max(
        (value for value in outcomes if isinstance(value, dict)),
        key=lambda value: value["fencing_token"],
    )
    if winner["fencing_token"] < 101:
        current = target.observe_scale("cpu-api")
        winner = target.authorize_scale(
            "cpu-api",
            101,
            expected=current,
            plan_id="plan-1",
            operation_id="action-1",
            target_replicas=3,
        )
    assert winner["fencing_token"] == 101
    assert winner["replicas"] == 3
    assert winner["execution_request"]["operationID"] == "action-1"


def test_eviction_distinguishes_pdb_429_from_api_throttling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "fleetright.execution.subprocess.run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=1,
            stderr="Error from server (TooManyRequests): Cannot evict pod; disruption budget",
        ),
    )
    with pytest.raises(ActionBlocked, match="PDB_BLOCKED"):
        _run(["kubectl", "create"])

    monkeypatch.setattr(
        "fleetright.execution.subprocess.run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=1,
            stderr="Error from server (TooManyRequests): API rate limit exceeded",
        ),
    )
    with pytest.raises(TargetUnavailable, match="throttled"):
        _run(["kubectl", "create"])


def test_health_observer_requires_readiness_slo_and_fresh_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metrics = """\
fleetright_request_duration_seconds_bucket{le="0.12"} 99
fleetright_request_duration_seconds_bucket{le="0.16"} 100
fleetright_request_duration_seconds_bucket{le="+Inf"} 100
fleetright_request_duration_seconds_count 100
fleetright_requests_total{result="error"} 0
fleetright_queue_growth_milli_requests_per_second 0
"""

    observer = WorkloadHealthObserver({"cpu-api": "http://127.0.0.1"})
    monkeypatch.setattr(
        observer, "_get", lambda url: (200, "" if url.endswith("/ready") else metrics)
    )
    result = observer.observe("cpu-api", p99_limit_ms=120, error_slo_ppm=1000)
    assert result["healthy"] is True
    assert result["p99_within_slo"] is True
    assert result["error_rate_ppm"] == 0
    assert result["observed_at_ms"] > 0


def test_health_observer_fails_on_latency_and_error_regression(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metrics = """\
fleetright_request_duration_seconds_bucket{le="0.12"} 90
fleetright_request_duration_seconds_bucket{le="0.16"} 100
fleetright_request_duration_seconds_bucket{le="+Inf"} 100
fleetright_request_duration_seconds_count 100
fleetright_requests_total{result="error"} 2
fleetright_queue_growth_milli_requests_per_second 5
"""

    observer = WorkloadHealthObserver({"cpu-api": "http://127.0.0.1"})
    monkeypatch.setattr(
        observer, "_get", lambda url: (200, "" if url.endswith("/ready") else metrics)
    )
    result = observer.observe("cpu-api", p99_limit_ms=120, error_slo_ppm=1000)
    assert result["healthy"] is False
    assert result["p99_within_slo"] is False
    assert result["error_rate_ppm"] == 20_000
    assert result["queue_growth_within_limit"] is False


def test_pdb_block_response_is_classified(monkeypatch: pytest.MonkeyPatch) -> None:
    import subprocess

    from fleetright import execution

    monkeypatch.setattr(
        execution.subprocess,
        "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            args="kubectl", returncode=1, stdout="", stderr="Too many requests: disruption budget"
        ),
    )

    with pytest.raises(ActionBlocked, match="PDB_BLOCKED"):
        execution._run(["kubectl", "create", "--raw=/pods/x/eviction"])
