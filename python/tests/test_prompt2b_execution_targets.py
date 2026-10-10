from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest
from fleetright.api import create_app
from fleetright.execution import (
    _WORKLOAD_FENCE_RULES,
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
    monkeypatch.setattr(target, "_compose", lambda *_args, **_kwargs: pytest.fail("mutated"))
    with pytest.raises(TargetPreconditionFailed, match="supports one replica"):
        target.set_replicas("cpu-api", 2, current, expected_replicas=1, fencing_token=1)


def test_kubernetes_target_has_no_pod_eviction_or_node_cordon_mutators() -> None:
    target = KubernetesTarget(namespace="fleetright")

    assert not hasattr(target, "evict_one")
    assert not hasattr(target, "cordon")


def test_kubernetes_roles_publish_requests_and_leave_pod_creation_to_controller() -> None:
    root = Path(__file__).resolve().parents[2]
    api_rbac = (root / "deploy/kubernetes/rbac.yaml").read_text()
    workload_rbac = (root / "deploy/kubernetes/fleetright-workload-controller.yaml").read_text()

    assert 'resources: ["fleetrightworkloads"]' in api_rbac
    assert 'verbs: ["get", "patch"]' in api_rbac
    assert 'resourceNames: ["fleetrightworkloads.fleetright.io"]' in api_rbac
    assert 'resources: ["customresourcedefinitions"]' in api_rbac
    assert 'resources: ["deployments"]' not in api_rbac
    assert 'resources: ["pods/eviction"]' not in api_rbac
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
            "replicas": 2,
            "availabilityFloor": 1,
        },
        "status": {
            "observedGeneration": 4,
            "replicas": 2,
            "readyReplicas": 2,
            "availableReplicas": 2,
            "conditions": [{"type": "Reconciled", "status": "True"}],
        },
    }


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


def test_workload_preflight_requires_old_fence_advance_rule(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    advance_rule = (
        "!has(oldSelf.executionFence) || !has(self.executionRequest) || "
        "(has(oldSelf.executionRequest) && self.executionRequest == oldSelf.executionRequest) || "
        "self.executionRequest.generation > oldSelf.executionFence.generation"
    )
    assert advance_rule in _WORKLOAD_FENCE_RULES
    crd = {
        "spec": {
            "versions": [
                {
                    "name": "v1alpha1",
                    "schema": {
                        "openAPIV3Schema": {
                            "properties": {
                                "spec": {
                                    "properties": {
                                        "executionFence": {
                                            "required": [
                                                "planID",
                                                "operationID",
                                                "generation",
                                                "expectedGeneration",
                                                "expectedReplicas",
                                                "fencedGeneration",
                                            ]
                                        }
                                    },
                                    "x-kubernetes-validations": [
                                        {"rule": rule}
                                        for rule in _WORKLOAD_FENCE_RULES
                                        if rule != advance_rule
                                    ],
                                }
                            }
                        }
                    },
                }
            ]
        }
    }
    monkeypatch.setattr(target, "_json", lambda *_args: crd)

    with pytest.raises(TargetPreconditionFailed, match="fencing CRD is not installed"):
        target._require_fencing_schema()


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
    with pytest.raises(TargetPreconditionFailed, match="fencing CRD is not installed"):
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
