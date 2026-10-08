from __future__ import annotations

import json

import pytest
from fleetright.api import create_app
from fleetright.execution import (
    ActionBlocked,
    DockerComposeTarget,
    KubernetesTarget,
    TargetPreconditionFailed,
    WorkloadHealthObserver,
    bounded_execution_health_policy,
    valid_execution_health_policy,
)


def test_execution_health_policy_is_capped_by_server_workload_contract() -> None:
    policy = bounded_execution_health_policy("cpu-api", 10_000, 1_000_000)
    assert policy == {
        "p99_limit_ms": 120,
        "error_slo_ppm": 1_000,
        "max_queue_growth_milli": 0,
    }
    assert valid_execution_health_policy("cpu-api", policy)
    assert not valid_execution_health_policy("cpu-api", {**policy, "p99_limit_ms": 10_000})


def test_kubernetes_execute_requires_the_supported_explicit_namespace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FLEETRIGHT_KUBERNETES_KUBECONFIG", "/tmp/fleetright-kubeconfig")
    monkeypatch.delenv("FLEETRIGHT_KUBERNETES_NAMESPACE", raising=False)
    with pytest.raises(RuntimeError, match="namespace fleetright"):
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


def test_eviction_uses_policy_api_with_uid_precondition(monkeypatch: pytest.MonkeyPatch) -> None:
    target = KubernetesTarget(namespace="fleetright")
    monkeypatch.setattr(
        target,
        "_json",
        lambda *args: {
            "metadata": {
                "uid": "01234567-89ab-cdef-0123-456789abcdef",
                "resourceVersion": "1",
                "labels": {"fleetright.io/workload": "cpu-api"},
                "ownerReferences": [{"kind": "ReplicaSet"}],
            }
        },
    )
    monkeypatch.setattr(target, "_require_schedulable_node", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(target, "_pdb_allows", lambda _labels: None)
    monkeypatch.setattr(
        target,
        "fence_pod",
        lambda _name, uid, token: {"uid": uid, "resource_version": "2", "fencing_token": token},
    )
    calls: list[tuple[list[str], str | None]] = []

    def run(*args: str, stdin: str | None = None) -> object:
        calls.append((list(args), stdin))
        return object()

    monkeypatch.setattr(target, "_kubectl", run)
    target.evict_one(
        "cpu-api-abcde",
        "01234567-89ab-cdef-0123-456789abcdef",
        1,
        destination_node="node-a",
    )
    args, body = calls[0]
    assert args[0] == "create"
    assert args[1] == "--raw=/api/v1/namespaces/fleetright/pods/cpu-api-abcde/eviction"
    preconditions = json.loads(body or "{}")["deleteOptions"]["preconditions"]
    assert preconditions["uid"] == "01234567-89ab-cdef-0123-456789abcdef"
    assert preconditions["resourceVersion"] == "2"


def test_eviction_rejects_pod_identity_change(monkeypatch: pytest.MonkeyPatch) -> None:
    target = KubernetesTarget(namespace="fleetright")
    monkeypatch.setattr(target, "_json", lambda *args: {"metadata": {"uid": "different"}})
    with pytest.raises(TargetPreconditionFailed, match="identity changed"):
        target.evict_one(
            "cpu-api-abcde",
            "01234567-89ab-cdef-0123-456789abcdef",
            1,
            destination_node="node-a",
        )


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
