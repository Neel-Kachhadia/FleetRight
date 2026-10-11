from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from io import BytesIO
from pathlib import Path
from threading import Event
from typing import Any
from urllib.error import HTTPError

import pytest
import yaml
from fleetright.execution import ActionBlocked, FleetRightWorkloadTarget
from fleetright.workload_controller import (
    POD_ADMISSION_PROBE_MARKER,
    POD_CREATE_ADMISSION_PROTOCOL,
    POD_CREATE_ADMISSION_PROTOCOL_RULE,
    POD_CREATE_ATTEMPT_ANNOTATION,
    POD_CREATE_OPERATION_ANNOTATION,
    POD_CREATE_PLAN_ANNOTATION,
    POD_CREATE_PROTOCOL,
    POD_CREATE_REJECTION_RULE,
    POD_CREATE_RESOLUTION_RULE,
    REPLICA_ID_LABEL,
    TEMPLATE_HASH_ANNOTATION,
    WORKLOAD_GENERATION_ANNOTATION,
    WORKLOAD_UID_LABEL,
    ApiError,
    KubernetesAPI,
    SafetyError,
    _ensure_pdb,
    _pod_name,
    _pod_object,
    _validate_spec,
    _write_pending_pod_create,
    _write_retired_slot_tombstone,
    reconcile_workload,
)


def _workload(**spec_overrides: Any) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "workloadID": "api",
        "podCreateProtocol": POD_CREATE_PROTOCOL,
        "podCreateAdmissionProtocol": POD_CREATE_ADMISSION_PROTOCOL,
        "replicas": 1,
        "availabilityFloor": 1,
        "surgeBudget": 0,
        "networkMbps": 0,
        "disruption": {"maxConcurrentDisruptions": 1},
        "placement": {
            "allowedNodes": [
                {"name": "worker-a", "uid": "worker-a-uid"},
                {"name": "worker-b", "uid": "worker-b-uid"},
            ],
            "topology": {"key": "kubernetes.io/hostname", "maxSkew": 1},
            "requirePodAntiAffinity": True,
        },
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
    }
    spec.update(spec_overrides)
    return {
        "apiVersion": "fleetright.io/v1alpha1",
        "kind": "FleetRightWorkload",
        "metadata": {"name": "api", "uid": "workload-uid", "resourceVersion": "1", "generation": 1},
        "spec": spec,
    }


def _nodes() -> list[dict[str, Any]]:
    return [
        {"metadata": {"name": "worker-a", "uid": "worker-a-uid"}},
        {"metadata": {"name": "worker-b", "uid": "worker-b-uid"}},
    ]


def test_deterministic_pod_name_fits_kubernetes_subdomain_limit() -> None:
    assert len(_pod_name("workload-uid", 1_000)) <= 253


class FakeAPI:
    def __init__(self) -> None:
        self.pods: dict[str, dict[str, Any]] = {}
        self.pdbs: dict[str, dict[str, Any]] = {}
        self.status: dict[str, Any] | None = None
        self.next_uid = 0

    def list_pods(self, workload_uid: str) -> list[dict[str, Any]]:
        return [
            deepcopy(pod)
            for pod in self.pods.values()
            if pod.get("metadata", {}).get("labels", {}).get(WORKLOAD_UID_LABEL) == workload_uid
        ]

    def get_pod(self, name: str) -> dict[str, Any] | None:
        pod = self.pods.get(name)
        return deepcopy(pod) if pod else None

    def create_pod(self, pod: dict[str, Any]) -> dict[str, Any]:
        name = pod["metadata"]["name"]
        if name in self.pods:
            raise ApiError(409, "already exists")
        self.next_uid += 1
        value = deepcopy(pod)
        value["metadata"].update(uid=f"pod-uid-{self.next_uid}", resourceVersion="1")
        value["status"] = {"phase": "Pending", "conditions": []}
        self.pods[name] = value
        return deepcopy(value)

    def get_pdb(self, name: str) -> dict[str, Any] | None:
        value = self.pdbs.get(name)
        return deepcopy(value) if value else None

    def create_pdb(self, pdb: dict[str, Any]) -> dict[str, Any]:
        value = deepcopy(pdb)
        value["metadata"].update(uid="pdb-uid", resourceVersion="1")
        self.pdbs[value["metadata"]["name"]] = value
        return deepcopy(value)

    def patch_pdb(
        self,
        name: str,
        resource_version: str,
        spec: dict[str, Any],
        annotations: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        metadata = self.pdbs[name]["metadata"]
        if metadata.get("resourceVersion") != resource_version:
            raise ApiError(409, "resourceVersion conflict")
        self.pdbs[name]["spec"] = deepcopy(spec)
        if annotations is not None:
            metadata.setdefault("annotations", {}).update(deepcopy(annotations))
        metadata["resourceVersion"] = str(int(resource_version) + 1)
        return deepcopy(self.pdbs[name])

    def patch_status(
        self, name: str, resource_version: str, status: dict[str, Any]
    ) -> dict[str, Any]:
        old = self.status or {}
        updated = deepcopy(old)
        for key, value in status.items():
            if value is None:
                updated.pop(key, None)
            else:
                updated[key] = deepcopy(value)
        pending = old.get("pendingPodCreate")
        rejected = updated.get("lastRejectedPodCreate")
        if rejected is not None and rejected.get("httpStatus") not in {400, 403, 422, 429}:
            raise ApiError(422, "Pod create rejection must be definitive")
        if pending is not None and updated.get("pendingPodCreate") != pending:
            next_pending = updated.get("pendingPodCreate")
            claim = (
                isinstance(next_pending, dict)
                and "admissionUID" not in pending
                and isinstance(next_pending.get("admissionUID"), str)
                and 0 < len(next_pending["admissionUID"]) <= 64
                and {key: value for key, value in next_pending.items() if key != "admissionUID"}
                == pending
            )
            authorized = any(
                all(
                    record.get(key) == value
                    for key, value in pending.items()
                    if key != "admissionUID"
                )
                for record in updated.get("authorizedPodCreates", [])
            )
            definitive_rejection = rejected is not None and rejected.get("intent") == pending
            if not claim and (next_pending is not None or not (authorized or definitive_rejection)):
                raise ApiError(422, "pending Pod create has no exact resolution")
        old_rejected = old.get("lastRejectedPodCreate")
        if (
            old_rejected is not None
            and rejected != old_rejected
            and not (
                rejected is not None and pending is not None and rejected.get("intent") == pending
            )
        ):
            raise ApiError(422, "Pod create rejection evidence cannot be removed or replaced")
        self.status = updated
        return {
            "metadata": {"name": name, "resourceVersion": str(int(resource_version) + 1)},
            "status": deepcopy(self.status),
        }


class CompareAndSwapStatusAPI(FakeAPI):
    def __init__(self) -> None:
        super().__init__()
        self.status_resource_version = "1"
        self.status_lock = threading.Lock()

    def patch_status(
        self, name: str, resource_version: str, status: dict[str, Any]
    ) -> dict[str, Any]:
        with self.status_lock:
            if resource_version != self.status_resource_version:
                raise ApiError(409, "status resourceVersion conflict")
            result = super().patch_status(name, resource_version, status)
            self.status_resource_version = result["metadata"]["resourceVersion"]
            return result


def test_creates_owned_pod_with_hard_allowed_set_topology_and_pdb_selector() -> None:
    api = FakeAPI()
    workload = _workload()

    reconcile_workload(api, workload, _nodes())

    assert len(api.pods) == 1
    pod = next(iter(api.pods.values()))
    assert pod["metadata"]["ownerReferences"] == [
        {
            "apiVersion": "fleetright.io/v1alpha1",
            "kind": "FleetRightWorkload",
            "name": "api",
            "uid": "workload-uid",
            "controller": True,
            "blockOwnerDeletion": False,
        }
    ]
    assert pod["metadata"]["labels"][REPLICA_ID_LABEL] == "api-0"
    assert pod["spec"]["containers"][0]["resources"]["requests"] == {
        "cpu": "100m",
        "memory": "64Mi",
    }
    assert pod["spec"]["securityContext"] == {
        "runAsNonRoot": True,
        "runAsUser": 10001,
        "runAsGroup": 10001,
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    assert pod["spec"]["containers"][0]["securityContext"] == {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
    }
    node_terms = pod["spec"]["affinity"]["nodeAffinity"][
        "requiredDuringSchedulingIgnoredDuringExecution"
    ]["nodeSelectorTerms"]
    assert len(node_terms) == 2
    assert {
        tuple(term["matchFields"][0]["values"])
        for term in node_terms
        if len(term["matchFields"]) == 1
        and term["matchFields"][0]["key"] == "metadata.name"
        and term["matchFields"][0]["operator"] == "In"
    } == {("worker-a",), ("worker-b",)}
    topology = pod["spec"]["topologySpreadConstraints"][0]
    assert topology["whenUnsatisfiable"] == "DoNotSchedule"
    assert topology["labelSelector"]["matchLabels"][WORKLOAD_UID_LABEL] == "workload-uid"
    assert (
        "requiredDuringSchedulingIgnoredDuringExecution"
        in pod["spec"]["affinity"]["podAntiAffinity"]
    )
    assert "nodeName" not in pod["spec"]
    assert "preferredDuringSchedulingIgnoredDuringExecution" not in pod["spec"]["affinity"]

    pdb = api.pdbs["api"]
    assert pdb["spec"]["selector"] == {"matchLabels": {WORKLOAD_UID_LABEL: "workload-uid"}}
    assert pdb["spec"]["maxUnavailable"] == 0
    assert pdb["spec"]["unhealthyPodEvictionPolicy"] == "IfHealthyBudget"
    assert pdb["metadata"]["annotations"][WORKLOAD_GENERATION_ANNOTATION] == "1"
    assert api.status["labelSelector"] == f"{WORKLOAD_UID_LABEL}=workload-uid"
    assert api.status["readyReplicas"] == 0


def test_stale_workload_snapshot_cannot_loosen_a_newer_managed_pdb() -> None:
    api = FakeAPI()
    current = _workload(replicas=2, availabilityFloor=1)
    current["metadata"]["generation"] = 3
    _ensure_pdb(api, current)
    api.pdbs["api"]["spec"]["maxUnavailable"] = 1

    stale = deepcopy(current)
    stale["metadata"]["generation"] = 2
    stale["spec"]["replicas"] = 5
    stale["spec"]["disruption"]["maxConcurrentDisruptions"] = 5

    with pytest.raises(SafetyError, match="stale workload snapshot"):
        _ensure_pdb(api, stale)

    assert api.pdbs["api"]["spec"]["maxUnavailable"] == 1
    assert api.pdbs["api"]["metadata"]["annotations"][WORKLOAD_GENERATION_ANNOTATION] == "3"


def test_stale_generation_cannot_create_pods_after_a_newer_workload_update() -> None:
    api = FakeAPI()
    current = _workload(replicas=1)
    current["metadata"]["generation"] = 3
    reconcile_workload(api, current, _nodes())
    original_pdb = deepcopy(api.pdbs["api"])
    original_pods = deepcopy(api.pods)

    def reject_create(_pod: dict[str, Any]) -> dict[str, Any]:
        pytest.fail("stale workload snapshot reached Pod creation")

    def reject_delete(_pod: dict[str, Any]) -> None:
        pytest.fail("stale workload snapshot reached Pod deletion")

    api.create_pod = reject_create
    api.delete_pod = reject_delete
    stale = _workload(replicas=2)
    stale["metadata"]["generation"] = 2

    reconcile_workload(api, stale, _nodes())

    assert api.pods == original_pods
    assert api.pdbs["api"] == original_pdb
    assert api.status["conditions"][2]["reason"] == "StaleWorkloadGeneration"


def test_takeover_fence_blocks_a_superseded_workload_request() -> None:
    api = FakeAPI()
    workload = _workload(
        executionProtocol="monotonic-scale-up-v1",
        replicas=2,
        surgeBudget=1,
        executionFence={
            "planID": "plan-1",
            "operationID": "action-1",
            "generation": 14,
            "expectedGeneration": 1,
            "expectedReplicas": 1,
            "fencedGeneration": 2,
        },
        executionRequest={
            "planID": "plan-1",
            "operationID": "action-1",
            "generation": 13,
            "expectedReplicas": 1,
            "targetReplicas": 2,
        },
    )
    workload["metadata"]["generation"] = 2
    workload["status"] = {
        "observedGeneration": 2,
        "replicas": 2,
        "readyReplicas": 2,
        "availableReplicas": 2,
        "execution": {
            "planID": "plan-1",
            "operationID": "action-1",
            "generation": 13,
            "targetReplicas": 2,
            "phase": "APPLIED",
        },
        "conditions": [{"type": "Reconciled", "status": "True"}],
    }

    reconcile_workload(api, workload, _nodes())

    assert api.pods == {}
    assert any(
        condition.get("reason") == "StaleExecutionAuthority"
        for condition in api.status["conditions"]
    )


def test_delayed_create_is_journaled_and_takeover_resolves_exact_pod() -> None:
    api = CompareAndSwapStatusAPI()
    initial = _workload()
    initial["spec"]["executionProtocol"] = "monotonic-scale-up-v1"
    reconcile_workload(api, initial, _nodes())
    api.pods[next(iter(api.pods))]["status"] = {
        "phase": "Running",
        "conditions": [{"type": "Ready", "status": "True"}],
    }
    initial["metadata"]["resourceVersion"] = api.status_resource_version
    reconcile_workload(api, initial, _nodes())
    api.status_resource_version = str(int(api.status_resource_version) + 1)
    delayed = deepcopy(initial)
    delayed["metadata"].update(generation=2, resourceVersion=api.status_resource_version)
    delayed["spec"].update(
        replicas=2,
        executionFence={
            "planID": "plan-1",
            "operationID": "action-1",
            "generation": 100,
            "expectedGeneration": 1,
            "expectedReplicas": 1,
            "fencedGeneration": 2,
        },
        executionRequest={
            "planID": "plan-1",
            "operationID": "action-1",
            "generation": 100,
            "expectedReplicas": 1,
            "targetReplicas": 2,
        },
    )
    delayed["status"] = deepcopy(api.status)
    entered_post, release_post = Event(), Event()
    create = api.create_pod

    def delayed_create(pod: dict[str, Any]) -> dict[str, Any]:
        if (
            threading.current_thread().name.startswith("old-controller")
            and pod["metadata"]["labels"][REPLICA_ID_LABEL] == "api-1"
        ):
            entered_post.set()
            assert release_post.wait(timeout=3)
        return create(pod)

    api.create_pod = delayed_create
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="old-controller") as pool:
        old = pool.submit(reconcile_workload, api, delayed, _nodes())
        assert entered_post.wait(timeout=3)

        takeover = deepcopy(delayed)
        takeover["metadata"]["resourceVersion"] = api.status_resource_version
        takeover["status"] = deepcopy(api.status)
        reconcile_workload(api, takeover, _nodes())
        slot_name = _pod_name("workload-uid", 1)
        source_pod = next(iter(api.pods.values()))
        assert api.get_pod(slot_name) is None
        assert api.status["pendingPodCreate"]["podName"] == slot_name
        assert api.status["execution"]["phase"] == "RECONCILING"
        retirement_target = FleetRightWorkloadTarget(namespace="default")
        retirement_observation = retirement_target._observe(takeover, "api")
        retirement_target.observe_scale = lambda _workload: retirement_observation
        with pytest.raises(ActionBlocked, match="POD_CREATE_OUTCOME_UNKNOWN"):
            retirement_target.prepare_source_retirement(
                "api",
                "plan-retirement",
                {
                    "operation_id": "retire-1",
                    "source_owner_uid": "workload-uid",
                    "workload_generation": 2,
                },
            )
        assert not api.status.get("retiredReplicaSlots")
        assert (
            api.pods[source_pod["metadata"]["name"]]["metadata"]["uid"]
            == source_pod["metadata"]["uid"]
        )
        release_post.set()
        old.result(timeout=3)

    created_uid = api.pods[slot_name]["metadata"]["uid"]
    assert "pendingPodCreate" not in api.status
    recovered = deepcopy(delayed)
    recovered["metadata"]["resourceVersion"] = api.status_resource_version
    recovered["status"] = deepcopy(api.status)
    reconcile_workload(api, recovered, _nodes())

    assert len(api.pods) == 2
    assert api.pods[slot_name]["metadata"]["uid"] == created_uid
    assert "pendingPodCreate" not in api.status
    assert api.status["execution"]["phase"] == "APPLIED"
    authorization = next(
        item for item in api.status["authorizedPodCreates"] if item["podUID"] == created_uid
    )
    assert (authorization["planID"], authorization["operationID"]) == ("plan-1", "action-1")
    assert (
        authorization["createID"]
        == api.pods[slot_name]["metadata"]["annotations"][POD_CREATE_ATTEMPT_ANNOTATION]
    )
    assert api.status["conditions"][2]["reason"] == "Reconciled"
    assert not api.status.get("retiredReplicaSlots")


def test_controller_refreshes_webhook_claim_resource_version_before_authorizing_created_pod() -> (
    None
):
    class ClaimedAPIFake(FakeAPI):
        def __init__(self) -> None:
            super().__init__()
            self.version = "1"
            self.workload = _workload()

        def get_workload(self, name: str) -> dict[str, Any]:
            assert name == "api"
            return deepcopy(self.workload)

        def patch_status(
            self, name: str, resource_version: str, status: dict[str, Any]
        ) -> dict[str, Any]:
            if resource_version != self.version:
                raise ApiError(409, "status resourceVersion conflict")
            result = super().patch_status(name, resource_version, status)
            self.version = result["metadata"]["resourceVersion"]
            self.workload["metadata"]["resourceVersion"] = self.version
            self.workload["status"] = deepcopy(result["status"])
            return result

        def create_pod(self, pod: dict[str, Any]) -> dict[str, Any]:
            pending = self.status["pendingPodCreate"]
            self.patch_status(
                "api",
                self.version,
                {"pendingPodCreate": {**pending, "admissionUID": "admission-1"}},
            )
            return super().create_pod(pod)

    api = ClaimedAPIFake()
    reconcile_workload(api, _workload(), _nodes())

    assert "pendingPodCreate" not in api.status
    assert api.status["authorizedPodCreates"][0]["podUID"] == "pod-uid-1"


def test_stored_create_is_not_reused_as_replacement_during_response_delay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored, release_response = Event(), Event()

    class DelayedResponseAPI(CompareAndSwapStatusAPI):
        create_calls = 0

        def create_pod(self, pod: dict[str, Any]) -> dict[str, Any]:
            self.create_calls += 1
            claimed = {**self.status["pendingPodCreate"], "admissionUID": "admission-1"}
            self.patch_status("api", self.status_resource_version, {"pendingPodCreate": claimed})
            result = super().create_pod(pod)
            stored.set()
            assert release_response.wait(timeout=3)
            return result

    api = DelayedResponseAPI()
    workload = _workload(executionProtocol="monotonic-scale-up-v1")
    workload["spec"]["surgeBudget"] = 2
    with ThreadPoolExecutor(max_workers=1) as pool:
        old_controller = pool.submit(reconcile_workload, api, workload, _nodes())
        try:
            assert stored.wait(timeout=3)
            assert api.status["pendingPodCreate"]["admissionUID"] == "admission-1"
            source_name = _pod_name("workload-uid", 0)
            source = api.pods[source_name]
            source["spec"]["nodeName"] = "worker-a"
            source["status"] = {
                "phase": "Running",
                "conditions": [{"type": "Ready", "status": "True"}],
            }
            takeover = deepcopy(workload)
            takeover["metadata"]["resourceVersion"] = api.status_resource_version
            takeover["status"] = deepcopy(api.status)
            reconcile_workload(api, takeover, _nodes())

            assert "pendingPodCreate" not in api.status
            assert api.status["authorizedPodCreates"][0]["podUID"] == source["metadata"]["uid"]
            assert api.status["readyReplicas"] == 1
            target = FleetRightWorkloadTarget(namespace="default")
            observed = target._observe(takeover, "api")
            monkeypatch.setattr(target, "observe_scale", lambda _workload: observed)
            monkeypatch.setattr(target, "_require_fencing_schema", lambda: None)

            def read_source(*args: str) -> dict[str, Any]:
                if args[:2] == ("patch", "fleetrightworkload"):
                    patch = json.loads(args[args.index("-p") + 1])
                    metadata = takeover["metadata"]
                    if patch["metadata"]["resourceVersion"] != metadata["resourceVersion"]:
                        raise ApiError(409, "workload resourceVersion conflict")
                    takeover["spec"].update(patch["spec"])
                    metadata["generation"] += 1
                    metadata["resourceVersion"] = str(int(metadata["resourceVersion"]) + 1)
                    return deepcopy(takeover)
                if args[:2] == ("get", "pod"):
                    return deepcopy(source)
                assert args[:2] == ("get", "node")
                return _nodes()[0]

            monkeypatch.setattr(target, "_json", read_source)
            prepared = target.prepare_source_retirement(
                "api",
                "plan-retirement",
                {
                    "operation_id": "retire-1",
                    "source_owner_kind": "FleetRightWorkload",
                    "source_owner_name": "api",
                    "source_owner_uid": "workload-uid",
                    "workload_generation": 1,
                    "source_pod_name": source_name,
                    "source_pod_uid": source["metadata"]["uid"],
                    "source_node_id": "worker-a",
                    "source_node_uid": "worker-a-uid",
                    "source_logical_slot_id": "api-0",
                    "destination_nodes": [{"name": "worker-b", "uid": "worker-b-uid"}],
                    "baseline_replicas": 1,
                    "surge_desired_replicas": 2,
                    "final_desired_replicas": 2,
                    "maximum_live_replicas": 3,
                    "maximum_temporary_surge_replicas": 2,
                    "profile_required_replicas": 1,
                    "readiness_stabilization_ms": 0,
                },
                fencing_token=10,
            )
            assert prepared["stage"] == "REPLACEMENT_REQUESTED"
            assert takeover["spec"]["executionRequest"]["targetReplicas"] == 2
            settled_status = deepcopy(api.status)
            assert not api.status.get("retiredReplicaSlots")
        finally:
            release_response.set()
        with pytest.raises(ApiError, match="status resourceVersion conflict"):
            old_controller.result(timeout=3)

    assert api.create_calls == 1
    assert api.status == settled_status
    assert api.pods[source_name]["metadata"]["uid"] == source["metadata"]["uid"]


@pytest.mark.parametrize("writer", ["legacy", "unreserved-current-protocol"])
def test_pre_upgrade_inflight_create_can_recreate_a_retired_slot(writer: str) -> None:
    api = CompareAndSwapStatusAPI()
    initial = _workload()
    initial["spec"]["executionProtocol"] = "monotonic-scale-up-v1"
    reconcile_workload(api, initial, _nodes())
    api.pods[next(iter(api.pods))]["status"] = {
        "phase": "Running",
        "conditions": [{"type": "Ready", "status": "True"}],
    }
    initial["metadata"]["resourceVersion"] = api.status_resource_version
    reconcile_workload(api, initial, _nodes())

    old_snapshot = deepcopy(initial)
    api.status_resource_version = str(int(api.status_resource_version) + 1)
    old_snapshot["metadata"].update(generation=2, resourceVersion=api.status_resource_version)
    old_snapshot["spec"].update(
        replicas=2,
        executionFence={
            "planID": "plan-1",
            "operationID": "action-1",
            "generation": 100,
            "expectedGeneration": 1,
            "expectedReplicas": 1,
            "fencedGeneration": 2,
        },
        executionRequest={
            "planID": "plan-1",
            "operationID": "action-1",
            "generation": 100,
            "expectedReplicas": 1,
            "targetReplicas": 2,
        },
    )
    old_pod = _pod_object(old_snapshot, 1)
    if writer == "legacy":
        for annotation in (
            POD_CREATE_ATTEMPT_ANNOTATION,
            POD_CREATE_OPERATION_ANNOTATION,
            POD_CREATE_PLAN_ANNOTATION,
        ):
            old_pod["metadata"]["annotations"].pop(annotation)
    entered_post, release_post = Event(), Event()

    def delayed_old_create() -> None:
        # Admission has finished; storage only compares the Pod name, never CR status.
        entered_post.set()
        assert release_post.wait(timeout=3)
        api.create_pod(old_pod)

    with ThreadPoolExecutor(max_workers=1) as pool:
        old_request = pool.submit(delayed_old_create)
        assert entered_post.wait(timeout=3)

        takeover = deepcopy(old_snapshot)
        takeover["metadata"]["generation"] = 2
        takeover["status"] = deepcopy(api.status)
        reconcile_workload(api, takeover, _nodes())
        retired_name = _pod_name("workload-uid", 1)
        assert "pendingPodCreate" not in api.status
        retired_uid = api.pods[retired_name]["metadata"]["uid"]
        source_node = "worker-a"
        api.pods[retired_name]["spec"]["nodeName"] = source_node

        next_operation = deepcopy(takeover)
        next_operation["metadata"].update(
            generation=3,
            resourceVersion=str(int(takeover["metadata"]["resourceVersion"]) + 1),
        )
        next_operation["spec"].update(
            replicas=3,
            executionFence={
                "planID": "plan-2",
                "operationID": "action-2",
                "generation": 101,
                "expectedGeneration": 2,
                "expectedReplicas": 2,
                "fencedGeneration": 3,
            },
            executionRequest={
                "planID": "plan-2",
                "operationID": "action-2",
                "generation": 101,
                "expectedReplicas": 2,
                "targetReplicas": 3,
            },
        )
        next_operation["status"] = deepcopy(api.status)
        api.status_resource_version = next_operation["metadata"]["resourceVersion"]
        reconcile_workload(api, next_operation, _nodes())
        _write_retired_slot_tombstone(
            api,
            next_operation,
            api.pods[retired_name],
            "plan-retire",
            "retire-1",
            _nodes(),
        )
        # Model successful source eviction while the pre-upgrade CREATE remains in flight.
        del api.pods[retired_name]
        assert api.get_pod(retired_name) is None
        release_post.set()
        old_request.result(timeout=3)

    recreated = api.pods[retired_name]
    assert recreated["metadata"]["uid"] != retired_uid
    node_terms = recreated["spec"]["affinity"]["nodeAffinity"][
        "requiredDuringSchedulingIgnoredDuringExecution"
    ]["nodeSelectorTerms"]
    assert any(
        term["matchFields"][0]["values"] == [source_node]
        for term in node_terms
        if term.get("matchFields")
    )
    assert recreated["metadata"]["annotations"][WORKLOAD_GENERATION_ANNOTATION] == "2"

    observed = deepcopy(next_operation)
    observed["metadata"]["resourceVersion"] = api.status_resource_version
    observed["status"] = deepcopy(api.status)
    reconcile_workload(api, observed, _nodes())
    assert api.status["unverifiedPodCreates"] == 1
    assert api.status["podCreateHistoryUncertain"] is True
    assert api.status["readyReplicas"] == 1
    assert api.status["execution"]["phase"] == "BLOCKED"
    assert api.status["conditions"][2]["reason"] == "RetiredReplicaPodObserved"


def test_retired_slot_tombstone_survives_takeover_and_uses_a_new_logical_slot() -> None:
    class LostRetirementResponseAPI(FakeAPI):
        lose_response = True

        def patch_status(
            self, name: str, resource_version: str, status: dict[str, Any]
        ) -> dict[str, Any]:
            result = super().patch_status(name, resource_version, status)
            if self.lose_response and "retiredReplicaSlots" in status:
                self.lose_response = False
                raise RuntimeError("controller lost the tombstone response")
            return result

    api = LostRetirementResponseAPI()
    workload = _workload()
    reconcile_workload(api, workload, _nodes())
    source = next(iter(api.pods.values()))
    source["spec"]["nodeName"] = "worker-a"
    source["status"] = {
        "phase": "Running",
        "conditions": [{"type": "Ready", "status": "True"}],
    }
    with pytest.raises(RuntimeError, match="lost the tombstone response"):
        _write_retired_slot_tombstone(api, workload, source, "plan-retire", "retire-1", _nodes())

    interim = deepcopy(workload)
    interim["metadata"]["resourceVersion"] = str(int(workload["metadata"]["resourceVersion"]) + 1)
    interim["status"] = deepcopy(api.status)
    _write_retired_slot_tombstone(api, interim, source, "plan-retire", "retire-1", _nodes())
    reconcile_workload(api, interim, _nodes())

    assert api.pods[source["metadata"]["name"]]["metadata"]["uid"] == source["metadata"]["uid"]
    assert api.status.get("podCreateHistoryUncertain") is not True
    assert api.status["conditions"][2]["reason"] == "Reconciled"
    del api.pods[source["metadata"]["name"]]

    _write_retired_slot_tombstone(api, interim, source, "plan-retire", "retire-1", _nodes())

    takeover = deepcopy(interim)
    takeover["status"] = deepcopy(api.status)
    reconcile_workload(api, takeover, _nodes())

    assert list(api.pods) == [_pod_name("workload-uid", 1)]
    assert api.pods[_pod_name("workload-uid", 1)]["metadata"]["labels"][REPLICA_ID_LABEL] == "api-1"
    assert api.status["retiredReplicaSlots"][0]["replicaID"] == "api-0"
    assert api.status["conditions"][2]["reason"] == "Reconciled"


def test_retirement_replay_revalidates_live_source_uid() -> None:
    api = FakeAPI()
    workload = _workload()
    reconcile_workload(api, workload, _nodes())
    source = deepcopy(next(iter(api.pods.values())))
    source["spec"]["nodeName"] = "worker-a"
    _write_retired_slot_tombstone(api, workload, source, "plan-retire", "retire-1", _nodes())

    api.pods[source["metadata"]["name"]]["metadata"]["uid"] = "replacement-pod-uid"

    with pytest.raises(SafetyError, match="source Pod UID changed"):
        _write_retired_slot_tombstone(api, workload, source, "plan-retire", "retire-1", _nodes())


def test_retirement_rejects_unresolved_current_create() -> None:
    api = FakeAPI()
    workload = _workload()
    reconcile_workload(api, workload, _nodes())
    source = deepcopy(next(iter(api.pods.values())))
    workload["status"]["pendingPodCreate"] = {
        "workloadUID": workload["metadata"]["uid"],
        "workloadGeneration": workload["metadata"]["generation"],
        "podName": _pod_name(workload["metadata"]["uid"], 1),
        "replicaID": "api-1",
        "planID": "plan-1",
        "operationID": "scale-up-1",
        "createID": "attempt-1",
    }

    with pytest.raises(SafetyError, match="unresolved Pod create history blocks retirement"):
        _write_retired_slot_tombstone(api, workload, source, "plan-retire", "retire-1", _nodes())

    assert not api.status.get("retiredReplicaSlots")


def test_retirement_rejects_legacy_create_provenance() -> None:
    api = FakeAPI()
    workload = _workload()
    reconcile_workload(api, workload, _nodes())
    source = deepcopy(next(iter(api.pods.values())))
    legacy = deepcopy(workload)
    legacy["spec"].pop("podCreateProtocol")

    with pytest.raises(SafetyError, match="legacy workload UID has untracked Pod CREATE history"):
        _write_retired_slot_tombstone(api, legacy, source, "plan-retire", "retire-1", _nodes())

    assert not api.status.get("retiredReplicaSlots")


@pytest.mark.parametrize("winner", ["retirement", "reservation"])
def test_retirement_and_create_reservation_race_uses_status_cas(winner: str) -> None:
    class CASAPI(FakeAPI):
        def __init__(self) -> None:
            super().__init__()
            self.cas_version: str | None = None
            self.cas_lock = threading.Lock()
            self.entered = {name: Event() for name in ("retirement", "reservation")}
            self.release = {name: Event() for name in ("retirement", "reservation")}

        def patch_status(
            self, name: str, resource_version: str, status: dict[str, Any]
        ) -> dict[str, Any]:
            if self.cas_version is None:
                return super().patch_status(name, resource_version, status)
            kind = "retirement" if "retiredReplicaSlots" in status else "reservation"
            self.entered[kind].set()
            assert self.release[kind].wait(timeout=3)
            with self.cas_lock:
                if resource_version != self.cas_version:
                    raise ApiError(409, "resourceVersion conflict")
                result = super().patch_status(name, resource_version, status)
                self.cas_version = str(int(resource_version) + 1)
                return result

    api = CASAPI()
    workload = _workload()
    workload["spec"]["executionProtocol"] = "monotonic-scale-up-v1"
    reconcile_workload(api, workload, _nodes())
    api.cas_version = workload["metadata"]["resourceVersion"]
    source = next(iter(api.pods.values()))
    source["spec"]["nodeName"] = "worker-a"
    retirement_workload = deepcopy(workload)
    reservation_workload = deepcopy(workload)
    intent = {
        "workloadUID": workload["metadata"]["uid"],
        "workloadGeneration": workload["metadata"]["generation"],
        "podName": _pod_name(workload["metadata"]["uid"], 1),
        "replicaID": "api-1",
        "planID": "plan-scale-up",
        "operationID": "scale-up-1",
        "createID": "attempt-1",
    }

    def persist_retirement() -> str:
        try:
            _write_retired_slot_tombstone(
                api, retirement_workload, source, "plan-retire", "retire-1", _nodes()
            )
            return "persisted"
        except ApiError as error:
            assert error.status == 409
            return "conflict"

    def persist_reservation() -> str:
        try:
            _write_pending_pod_create(api, reservation_workload, intent)
            return "persisted"
        except ApiError as error:
            assert error.status == 409
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as pool:
        retirement = pool.submit(persist_retirement)
        reservation = pool.submit(persist_reservation)
        assert api.entered["retirement"].wait(timeout=3)
        assert api.entered["reservation"].wait(timeout=3)
        api.release[winner].set()
        winner_future = reservation if winner == "reservation" else retirement
        assert winner_future.result(timeout=3) == "persisted"
        loser = retirement if winner == "reservation" else reservation
        api.release["retirement" if winner == "reservation" else "reservation"].set()
        assert loser.result(timeout=3) == "conflict"

    assert len(api.pods) == 1
    assert bool(api.status.get("retiredReplicaSlots")) == (winner == "retirement")
    assert (api.status.get("pendingPodCreate") is not None) == (winner == "reservation")
    target = FleetRightWorkloadTarget(namespace="default")
    current = deepcopy(workload)
    current["status"] = deepcopy(api.status)
    observed = target._observe(current, "api")
    target.observe_scale = lambda _workload: observed
    reason = (
        "RETIRED_SLOT_HISTORY_PRESENT" if winner == "retirement" else "POD_CREATE_OUTCOME_UNKNOWN"
    )
    with pytest.raises(ActionBlocked, match=reason):
        target.prepare_source_retirement(
            "api",
            "plan-retirement",
            {
                "operation_id": "retire-1",
                "source_owner_uid": "workload-uid",
                "workload_generation": 1,
            },
        )


def test_authorized_create_history_capacity_blocks_before_pod_post() -> None:
    class PostTrackingAPI(FakeAPI):
        create_calls = 0

        def create_pod(self, pod: dict[str, Any]) -> dict[str, Any]:
            self.create_calls += 1
            return super().create_pod(pod)

    api = PostTrackingAPI()
    workload = _workload()
    workload["status"] = {
        "authorizedPodCreates": [
            {
                "workloadUID": "workload-uid",
                "podUID": f"old-pod-{index}",
                "podName": f"old-pod-{index}",
                "replicaID": f"api-{index}",
                "workloadGeneration": 1,
                "planID": "plan-old",
                "operationID": "operation-old",
                "createID": f"attempt-{index}",
            }
            for index in range(1_000)
        ]
    }

    reconcile_workload(api, workload, _nodes())

    assert api.create_calls == 0
    assert api.pods == {}
    assert api.status["conditions"][2]["reason"] == "PodCreateHistoryFull"


@pytest.mark.parametrize(
    ("annotation", "value"),
    [
        (WORKLOAD_GENERATION_ANNOTATION, "2"),
        (POD_CREATE_OPERATION_ANNOTATION, "old-operation"),
        (POD_CREATE_ATTEMPT_ANNOTATION, "old-attempt"),
    ],
)
def test_same_ordinal_stale_pod_uid_and_create_identity_are_unverified(
    annotation: str, value: str
) -> None:
    api = FakeAPI()
    workload = _workload()
    reconcile_workload(api, workload, _nodes())
    name = _pod_name("workload-uid", 0)
    stale = deepcopy(api.pods[name])
    stale["metadata"]["uid"] = "pod-uid-from-stale-create"
    stale["metadata"]["annotations"][annotation] = value
    api.pods[name] = stale

    takeover = deepcopy(workload)
    takeover["metadata"]["resourceVersion"] = "2"
    takeover["status"] = deepcopy(api.status)
    reconcile_workload(api, takeover, _nodes())

    assert api.status["replicas"] == 0
    assert api.status["readyReplicas"] == 0
    assert api.status["unverifiedPodCreates"] == 1
    assert api.status["podCreateHistoryUncertain"] is True


def test_legacy_workload_uid_is_durably_uncertain() -> None:
    api = FakeAPI()
    legacy = _workload()
    del legacy["spec"]["podCreateProtocol"]

    reconcile_workload(api, legacy, _nodes())

    assert api.pods == {}
    assert api.status["podCreateHistoryUncertain"] is True
    assert api.status["conditions"][2]["reason"] == "LegacyPodCreateUncertain"


def test_stale_pod_history_stays_blocked_after_stale_pod_disappears() -> None:
    api = FakeAPI()
    workload = _workload()
    reconcile_workload(api, workload, _nodes())
    name = _pod_name("workload-uid", 0)
    stale = deepcopy(api.pods[name])
    stale["metadata"]["uid"] = "pod-uid-from-stale-create"
    stale["metadata"]["annotations"][POD_CREATE_ATTEMPT_ANNOTATION] = "old-attempt"
    api.pods[name] = stale

    observed = deepcopy(workload)
    observed["metadata"]["resourceVersion"] = "2"
    observed["status"] = deepcopy(api.status)
    reconcile_workload(api, observed, _nodes())
    assert api.status["podCreateHistoryUncertain"] is True
    del api.pods[name]

    takeover = deepcopy(workload)
    takeover["metadata"]["resourceVersion"] = str(int(observed["metadata"]["resourceVersion"]) + 1)
    takeover["status"] = deepcopy(api.status)
    reconcile_workload(api, takeover, _nodes())

    assert api.pods == {}
    assert api.status["podCreateHistoryUncertain"] is True
    assert api.status["conditions"][2]["reason"] == "PodCreateHistoryUncertain"


def test_stale_detection_and_blocked_phase_persist_atomically_across_restart() -> None:
    class CrashAfterSafetyWriteAPI(FakeAPI):
        crash_after_block = True

        def patch_status(
            self, name: str, resource_version: str, status: dict[str, Any]
        ) -> dict[str, Any]:
            result = super().patch_status(name, resource_version, status)
            if (
                self.crash_after_block
                and status.get("podCreateHistoryUncertain") is True
                and status.get("execution", {}).get("phase") == "BLOCKED"
            ):
                self.crash_after_block = False
                raise RuntimeError("controller process stopped after durable status write")
            return result

    api = CrashAfterSafetyWriteAPI()
    workload = _workload()
    reconcile_workload(api, workload, _nodes())
    pod = next(iter(api.pods.values()))
    pod["status"] = {
        "phase": "Running",
        "conditions": [{"type": "Ready", "status": "True"}],
    }
    reconcile_workload(api, workload, _nodes())
    workload["metadata"]["generation"] = 2
    workload["spec"].update(
        executionProtocol="monotonic-scale-up-v1",
        executionFence={
            "planID": "plan-1",
            "operationID": "action-1",
            "generation": 1,
            "expectedGeneration": 1,
            "expectedReplicas": 1,
            "fencedGeneration": 2,
        },
        executionRequest={
            "planID": "plan-1",
            "operationID": "action-1",
            "generation": 1,
            "expectedReplicas": 1,
            "targetReplicas": 1,
        },
    )
    name = _pod_name("workload-uid", 0)
    stale = deepcopy(api.pods[name])
    stale["metadata"]["uid"] = "late-pod-uid"
    stale["metadata"]["annotations"][POD_CREATE_ATTEMPT_ANNOTATION] = "old-attempt"
    api.pods[name] = stale
    workload["status"] = deepcopy(api.status)

    with pytest.raises(RuntimeError, match="process stopped"):
        reconcile_workload(api, workload, _nodes())

    assert api.status["podCreateHistoryUncertain"] is True
    assert api.status["execution"]["phase"] == "BLOCKED"
    del api.pods[name]

    takeover = deepcopy(workload)
    takeover["metadata"]["resourceVersion"] = "99"
    takeover["status"] = deepcopy(api.status)
    reconcile_workload(api, takeover, _nodes())

    assert api.pods == {}
    assert api.status["podCreateHistoryUncertain"] is True
    assert api.status["execution"]["phase"] == "BLOCKED"


def test_delayed_duplicate_create_cannot_match_a_new_attempt_for_the_same_slot() -> None:
    class LateDuplicateAPI(FakeAPI):
        delayed_pod: dict[str, Any] | None = None

        def create_pod(self, pod: dict[str, Any]) -> dict[str, Any]:
            if self.delayed_pod is not None:
                name = pod["metadata"]["name"]
                stale = deepcopy(self.delayed_pod)
                stale["metadata"]["uid"] = "pod-uid-late-duplicate"
                self.pods[name] = stale
                self.delayed_pod = None
                raise ApiError(409, "late duplicate CREATE occupied the name")
            return super().create_pod(pod)

    api = LateDuplicateAPI()
    workload = _workload()
    reconcile_workload(api, workload, _nodes())
    name = _pod_name("workload-uid", 0)
    previous = deepcopy(api.pods[name])
    previous_attempt = previous["metadata"]["annotations"][POD_CREATE_ATTEMPT_ANNOTATION]
    del api.pods[name]
    api.delayed_pod = previous

    takeover = deepcopy(workload)
    takeover["metadata"]["resourceVersion"] = str(int(workload["metadata"]["resourceVersion"]) + 1)
    takeover["status"] = deepcopy(api.status)
    reconcile_workload(api, takeover, _nodes())

    assert api.status["pendingPodCreate"]["createID"] != previous_attempt
    assert api.status["podCreateHistoryUncertain"] is True
    assert api.status["unverifiedPodCreates"] == 1
    assert api.status["conditions"][2]["reason"] == "PodCreateIdentityConflict"


@pytest.mark.parametrize(
    "generation",
    ["01", "١", "0", "9" * 5_000, str(2**63)],
    ids=["leading-zero", "non-ascii", "zero", "overlong", "overflow"],
)
def test_invalid_pod_generation_persists_uncertainty_without_raising(generation: str) -> None:
    api = FakeAPI()
    workload = _workload()
    reconcile_workload(api, workload, _nodes())
    pod = next(iter(api.pods.values()))
    pod["metadata"]["annotations"][WORKLOAD_GENERATION_ANNOTATION] = generation

    reconcile_workload(api, workload, _nodes())

    assert api.status["podCreateHistoryUncertain"] is True
    assert api.status["unverifiedPodCreates"] == 1
    assert api.status["readyReplicas"] == 0
    assert api.status["conditions"][2]["reason"] == "PodCreateIdentityConflict"
    assert all(condition["status"] == "False" for condition in api.status["conditions"])


@pytest.mark.parametrize(
    "mutation", ["erase", "replace", "wrong-authorization", "wrong-rejection", "unknown-rejection"]
)
def test_unresolved_create_cannot_be_erased_or_replaced(mutation: str) -> None:
    api = CompareAndSwapStatusAPI()
    workload = _workload()
    intent = {
        "workloadUID": "workload-uid",
        "workloadGeneration": 1,
        "podName": _pod_name("workload-uid", 0),
        "replicaID": "api-0",
        "planID": "plan-1",
        "operationID": "operation-1",
        "createID": "attempt-1",
    }
    _write_pending_pod_create(api, workload, intent)
    before = deepcopy(api.status)
    version = api.status_resource_version
    patch: dict[str, Any] = {"pendingPodCreate": None}
    other_attempt = {**intent, "createID": "attempt-2"}
    if mutation == "replace":
        patch["pendingPodCreate"] = other_attempt
    elif mutation == "wrong-authorization":
        patch["authorizedPodCreates"] = [{**other_attempt, "podUID": "pod-uid"}]
    elif mutation == "wrong-rejection":
        patch["lastRejectedPodCreate"] = {"intent": other_attempt, "httpStatus": 422}
    elif mutation == "unknown-rejection":
        patch["lastRejectedPodCreate"] = {"intent": intent, "httpStatus": 503}

    with pytest.raises(ApiError) as error:
        api.patch_status("api", version, patch)
    assert error.value.status == 422
    assert api.status == before
    assert api.status_resource_version == version

    takeover = deepcopy(workload)
    takeover["status"] = deepcopy(api.status)
    reconcile_workload(api, takeover, _nodes())

    assert api.pods == {}
    assert api.status["pendingPodCreate"] == intent
    assert api.status_resource_version == version


def test_crd_requires_exact_pending_resolution_and_bounded_rejection_evidence() -> None:
    root = Path(__file__).resolve().parents[2]
    crd = yaml.safe_load((root / "deploy/kubernetes/fleetrightworkload-crd.yaml").read_text())
    schema = crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]
    rules = {entry["rule"] for entry in schema["x-kubernetes-validations"]}
    assert {POD_CREATE_RESOLUTION_RULE, POD_CREATE_REJECTION_RULE} <= rules
    status = schema["properties"]["status"]["properties"]
    assert status["execution"]["properties"]["purpose"]["enum"] == ["staged-replacement-v1"]
    rejection = status["lastRejectedPodCreate"]
    assert set(rejection["required"]) == {"intent", "httpStatus"}
    assert rejection["properties"]["httpStatus"]["enum"] == [400, 403, 422, 429]
    assert rejection["properties"]["intent"] == status["pendingPodCreate"]
    spec = schema["properties"]["spec"]
    protocol_rule = next(
        entry
        for entry in spec["x-kubernetes-validations"]
        if entry["rule"] == POD_CREATE_ADMISSION_PROTOCOL_RULE
    )
    request = spec["properties"]["executionRequest"]
    assert request["properties"]["minimumReadyReplicas"]["type"] == "integer"
    assert any(
        "minimumReadyReplicas" in entry["rule"]
        and "approved ready replica floor" in entry["message"]
        for entry in spec["x-kubernetes-validations"]
    )
    assert protocol_rule["optionalOldSelf"] is True
    assert spec["properties"]["podCreateAdmissionProtocol"]["enum"] == [
        POD_CREATE_ADMISSION_PROTOCOL
    ]
    destinations = spec["properties"]["executionRequest"]["properties"]["destinationNodes"]
    assert destinations["x-kubernetes-list-type"] == "map"
    assert destinations["x-kubernetes-list-map-keys"] == ["name"]
    assert set(destinations["items"]["required"]) == {"name", "uid"}


@pytest.mark.parametrize(
    "change", ["replace-claim", "erase-claim", "change-attempt", "empty-claim"]
)
def test_admission_claim_is_set_once_without_replacing_intent(change: str) -> None:
    api = CompareAndSwapStatusAPI()
    workload = _workload()
    intent = {
        "workloadUID": "workload-uid",
        "workloadGeneration": 1,
        "podName": _pod_name("workload-uid", 0),
        "replicaID": "api-0",
        "planID": "plan-1",
        "operationID": "operation-1",
        "createID": "attempt-1",
    }
    _write_pending_pod_create(api, workload, intent)
    claimed = {**intent, "admissionUID": "admission-1"}
    if change == "empty-claim":
        invalid = {**intent, "admissionUID": ""}
    else:
        api.patch_status("api", api.status_resource_version, {"pendingPodCreate": claimed})
        invalid = dict(claimed)
        if change == "replace-claim":
            invalid["admissionUID"] = "admission-2"
        elif change == "erase-claim":
            invalid.pop("admissionUID")
        else:
            invalid["createID"] = "attempt-2"
    before = deepcopy(api.status)
    version = api.status_resource_version

    with pytest.raises(ApiError) as error:
        api.patch_status("api", version, {"pendingPodCreate": invalid})
    assert error.value.status == 422
    assert api.status == before
    assert api.status_resource_version == version


def test_rejection_after_admission_claim_preserves_the_unresolved_winner() -> None:
    class ClaimedRejectionAPI(CompareAndSwapStatusAPI):
        create_calls = 0

        def create_pod(self, pod: dict[str, Any]) -> dict[str, Any]:
            self.create_calls += 1
            claimed = {**self.status["pendingPodCreate"], "admissionUID": "admission-1"}
            self.patch_status("api", self.status_resource_version, {"pendingPodCreate": claimed})
            raise ApiError(403, "a distinct CREATE request was denied after another claim won")

    api = ClaimedRejectionAPI()
    workload = _workload()

    reconcile_workload(api, workload, _nodes())

    pending = deepcopy(api.status["pendingPodCreate"])
    assert pending["admissionUID"] == "admission-1"
    assert not api.status.get("lastRejectedPodCreate")
    takeover = deepcopy(workload)
    takeover["metadata"]["resourceVersion"] = api.status_resource_version
    takeover["status"] = deepcopy(api.status)
    reconcile_workload(api, takeover, _nodes())

    assert api.create_calls == 1
    assert api.pods == {}
    assert api.status["pendingPodCreate"] == pending


@pytest.mark.parametrize("placement", ["changed-allowed-set", "scheduled-outside-set"])
def test_invalid_create_response_placement_keeps_the_intent_unresolved(placement: str) -> None:
    class MutatedCreateAPI(FakeAPI):
        def create_pod(self, pod: dict[str, Any]) -> dict[str, Any]:
            changed = deepcopy(pod)
            if placement == "changed-allowed-set":
                terms = changed["spec"]["affinity"]["nodeAffinity"][
                    "requiredDuringSchedulingIgnoredDuringExecution"
                ]["nodeSelectorTerms"]
                terms[0]["matchFields"][0]["values"] = ["unapproved-node"]
            else:
                changed["spec"]["nodeName"] = "unapproved-node"
            return super().create_pod(changed)

    api = MutatedCreateAPI()
    workload = _workload()

    reconcile_workload(api, workload, _nodes())

    assert api.status["pendingPodCreate"]["replicaID"] == "api-0"
    assert not api.status.get("authorizedPodCreates")
    assert api.status["podCreateHistoryUncertain"] is True
    assert api.status["conditions"][2]["reason"] == "PodOutsideAllowedNodes"


@pytest.mark.parametrize(
    "probe", ["required-denial", "accepted", "unrelated-denial", "wrong-status", "network-error"]
)
def test_real_create_requires_the_unique_dry_run_admission_denial(probe: str) -> None:
    calls: list[str] = []

    class ProbeAPI(FakeAPI):
        namespace = "default"

        def create_pod(self, pod: dict[str, Any]) -> dict[str, Any]:
            return KubernetesAPI.create_pod(self, pod)

        def request(self, method: str, path: str, body: dict[str, Any]) -> dict[str, Any]:
            assert method == "POST"
            calls.append(path)
            if path.endswith("?dryRun=All"):
                assert body["metadata"]["name"] == "fleetright-pod-admission-probe"
                if probe == "required-denial":
                    raise ApiError(403, "Forbidden", status_message=POD_ADMISSION_PROBE_MARKER)
                if probe == "unrelated-denial":
                    raise ApiError(403, "Forbidden")
                if probe == "wrong-status":
                    raise ApiError(422, "Invalid", status_message=POD_ADMISSION_PROBE_MARKER)
                if probe == "network-error":
                    raise RuntimeError("probe request failed")
                return body
            assert probe == "required-denial"
            return super().create_pod(body)

    api = ProbeAPI()
    workload = _workload()
    if probe == "network-error":
        with pytest.raises(RuntimeError, match="probe request failed"):
            reconcile_workload(api, workload, _nodes())
    else:
        reconcile_workload(api, workload, _nodes())

    if probe == "required-denial":
        assert len(api.pods) == 1
        assert "pendingPodCreate" not in api.status
        assert calls == [
            "/api/v1/namespaces/default/pods?dryRun=All",
            "/api/v1/namespaces/default/pods",
        ]
    else:
        assert calls == ["/api/v1/namespaces/default/pods?dryRun=All"]
        assert api.pods == {}
        assert api.status["pendingPodCreate"]["replicaID"] == "api-0"
        assert not api.status.get("lastRejectedPodCreate")


def test_http_error_retains_only_the_sanitized_probe_marker(tmp_path: Path) -> None:
    token_path = tmp_path / "token"
    token_path.write_text("private-token", encoding="utf-8")

    class DeniedOpener:
        def open(self, request: Any, timeout: int) -> None:
            body = json.dumps(
                {
                    "message": f"denied: {POD_ADMISSION_PROBE_MARKER}; private-admission-detail",
                    "details": {"private": "private-response-detail"},
                }
            ).encode()
            raise HTTPError(request.full_url, 403, "Forbidden", {}, BytesIO(body))

    api = object.__new__(KubernetesAPI)
    api.base_url = "https://kubernetes.default.svc"
    api.token_path = str(token_path)
    api.opener = DeniedOpener()

    with pytest.raises(ApiError) as error:
        api.request("POST", "/pods?dryRun=All", {})

    assert error.value.status == 403
    assert error.value.status_message == POD_ADMISSION_PROBE_MARKER
    assert str(error.value) == "Kubernetes API returned HTTP 403"
    assert "private" not in repr(error.value.__dict__)


def test_status_reservation_conflict_stops_before_pod_create() -> None:
    class ConflictAPI(FakeAPI):
        def patch_status(
            self, _name: str, _resource_version: str, _status: dict[str, Any]
        ) -> dict[str, Any]:
            raise ApiError(409, "resourceVersion conflict")

        def create_pod(self, _pod: dict[str, Any]) -> dict[str, Any]:
            pytest.fail("Pod CREATE must follow a successful durable reservation")

    with pytest.raises(ApiError, match="resourceVersion conflict"):
        reconcile_workload(ConflictAPI(), _workload(), _nodes())


def test_lost_reservation_response_never_posts_or_retries_create() -> None:
    class ReservationResponseLostAPI(FakeAPI):
        status_resource_version = "1"

        def patch_status(
            self, name: str, resource_version: str, status: dict[str, Any]
        ) -> dict[str, Any]:
            if "pendingPodCreate" in status:
                self.status = deepcopy(status)
                self.status_resource_version = str(int(resource_version) + 1)
                raise RuntimeError("status response lost")
            return super().patch_status(name, resource_version, status)

        def create_pod(self, _pod: dict[str, Any]) -> dict[str, Any]:
            pytest.fail("Pod CREATE must follow a confirmed durable reservation")

    api = ReservationResponseLostAPI()
    workload = _workload()
    with pytest.raises(RuntimeError, match="status response lost"):
        reconcile_workload(api, workload, _nodes())

    assert api.pods == {}
    assert api.status["pendingPodCreate"]["podName"] == _pod_name("workload-uid", 0)
    recovery = deepcopy(workload)
    recovery["metadata"]["resourceVersion"] = api.status_resource_version
    recovery["status"] = deepcopy(api.status)
    reconcile_workload(api, recovery, _nodes())

    assert api.pods == {}
    assert api.status["pendingPodCreate"]["podName"] == _pod_name("workload-uid", 0)


def test_lost_create_response_is_reconciled_without_resending() -> None:
    class CreateResponseLostAPI(FakeAPI):
        create_calls = 0

        def create_pod(self, pod: dict[str, Any]) -> dict[str, Any]:
            self.create_calls += 1
            super().create_pod(pod)
            raise RuntimeError("create response lost")

    api = CreateResponseLostAPI()
    workload = _workload()
    with pytest.raises(RuntimeError, match="create response lost"):
        reconcile_workload(api, workload, _nodes())

    name = _pod_name("workload-uid", 0)
    created_uid = api.pods[name]["metadata"]["uid"]
    assert api.status["pendingPodCreate"]["podName"] == name
    reconcile_workload(api, workload, _nodes())

    assert api.create_calls == 1
    assert api.pods[name]["metadata"]["uid"] == created_uid
    assert "pendingPodCreate" not in api.status


def test_pending_create_rejects_wrong_generation_without_clearing_intent() -> None:
    api = FakeAPI()
    workload = _workload()
    reconcile_workload(api, workload, _nodes())
    name = _pod_name("workload-uid", 0)
    pending = {
        "workloadUID": "workload-uid",
        "workloadGeneration": 1,
        "podName": name,
        "replicaID": "api-0",
        "planID": "plan-0",
        "operationID": "operation-0",
        "createID": "attempt-0",
    }
    api.status["pendingPodCreate"] = pending
    api.pods[name]["metadata"]["annotations"][WORKLOAD_GENERATION_ANNOTATION] = "2"

    recovery = deepcopy(workload)
    recovery["status"] = deepcopy(api.status)
    reconcile_workload(api, recovery, _nodes())

    assert api.status["pendingPodCreate"] == pending
    assert api.status["conditions"][2]["reason"] == "PodCreateIdentityConflict"


def test_execution_request_on_unreconciled_create_cannot_start_pod_creation() -> None:
    api = FakeAPI()
    workload = _workload(
        executionProtocol="monotonic-scale-up-v1",
        executionRequest={
            "planID": "forged-plan",
            "operationID": "forged-operation",
            "generation": 42,
            "expectedReplicas": 0,
            "targetReplicas": 1,
        },
    )

    reconcile_workload(api, workload, _nodes())

    assert api.pods == {}
    assert api.status["execution"]["operationID"] == "forged-operation"
    assert api.status["execution"]["phase"] == "BLOCKED"
    assert api.status["conditions"][2]["reason"] == "WorkloadOperationUnreconciled"


def test_execution_request_reconciles_after_fence_without_intermediate_status_write() -> None:
    api = FakeAPI()
    workload = _workload(executionProtocol="monotonic-scale-up-v1")
    reconcile_workload(api, workload, _nodes())
    for pod in api.pods.values():
        pod["status"] = {
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True"}],
        }
    reconcile_workload(api, workload, _nodes())
    assert api.status["conditions"][2]["status"] == "True"

    authorized = deepcopy(workload)
    authorized["metadata"].update(generation=3, resourceVersion="3")
    authorized["spec"].update(
        replicas=2,
        executionFence={
            "planID": "plan-1",
            "operationID": "action-1",
            "generation": 101,
            "expectedGeneration": 1,
            "expectedReplicas": 1,
            "fencedGeneration": 3,
        },
        executionRequest={
            "planID": "plan-1",
            "operationID": "action-1",
            "generation": 101,
            "expectedReplicas": 1,
            "targetReplicas": 2,
        },
    )
    authorized["status"] = deepcopy(api.status)

    reconcile_workload(api, authorized, _nodes())

    assert len(api.pods) == 2
    assert api.status["execution"]["phase"] == "APPLIED"
    assert api.status["conditions"][2]["reason"] == "Reconciled"


def test_workload_controller_rejects_stale_fence_generation() -> None:
    workload = _workload(
        executionProtocol="monotonic-scale-up-v1",
        replicas=2,
        executionFence={
            "planID": "plan-1",
            "operationID": "action-1",
            "generation": 101,
            "expectedGeneration": 1,
            "expectedReplicas": 1,
            "fencedGeneration": 2,
        },
        executionRequest={
            "planID": "plan-1",
            "operationID": "action-1",
            "generation": 101,
            "expectedReplicas": 1,
            "targetReplicas": 2,
        },
    )
    workload["metadata"]["generation"] = 3

    with pytest.raises(SafetyError, match="execution fence is invalid"):
        _validate_spec(workload, _nodes())


@pytest.mark.parametrize(
    "destinations",
    [
        [],
        [{"name": "unknown-node", "uid": "unknown-uid"}],
        [{"name": "worker-b", "uid": "wrong-uid"}],
        [{"name": "worker-b", "uid": "worker-b-uid"}] * 2,
        "worker-b",
    ],
)
def test_execution_destinations_must_match_the_immutable_node_policy(destinations: Any) -> None:
    workload = _workload(
        executionProtocol="monotonic-scale-up-v1",
        executionRequest={
            "planID": "plan-1",
            "operationID": "operation-1",
            "generation": 1,
            "expectedReplicas": 0,
            "targetReplicas": 1,
            "destinationNodes": destinations,
        },
    )

    with pytest.raises(SafetyError, match="destination Nodes must be a nonempty subset"):
        _validate_spec(workload, _nodes())


def test_new_replica_uses_only_request_destinations_and_preserves_existing_source() -> None:
    api = CompareAndSwapStatusAPI()
    initial = _workload(executionProtocol="monotonic-scale-up-v1")
    reconcile_workload(api, initial, _nodes())
    source_name = _pod_name("workload-uid", 0)
    api.pods[source_name]["spec"]["nodeName"] = "worker-a"
    api.pods[source_name]["status"] = {
        "phase": "Running",
        "conditions": [{"type": "Ready", "status": "True"}],
    }
    reconcile_workload(api, initial, _nodes())
    source_uid = api.pods[source_name]["metadata"]["uid"]
    authorized = deepcopy(initial)
    api.status_resource_version = str(int(api.status_resource_version) + 1)
    authorized["metadata"].update(generation=2, resourceVersion=api.status_resource_version)
    authorized["spec"].update(
        replicas=2,
        executionRequest={
            "planID": "plan-1",
            "operationID": "operation-1",
            "generation": 100,
            "expectedReplicas": 1,
            "targetReplicas": 2,
            "destinationNodes": [{"name": "worker-b", "uid": "worker-b-uid"}],
        },
        executionFence={
            "planID": "plan-1",
            "operationID": "operation-1",
            "generation": 100,
            "expectedGeneration": 1,
            "expectedReplicas": 1,
            "fencedGeneration": 2,
        },
    )

    reconcile_workload(api, authorized, _nodes())

    replacement = api.pods[_pod_name("workload-uid", 1)]
    terms = replacement["spec"]["affinity"]["nodeAffinity"][
        "requiredDuringSchedulingIgnoredDuringExecution"
    ]["nodeSelectorTerms"]
    assert [term["matchFields"][0]["values"] for term in terms] == [["worker-b"]]
    assert api.pods[source_name]["metadata"]["uid"] == source_uid
    assert api.status["execution"]["phase"] == "APPLIED"
    replacement["spec"]["nodeName"] = "worker-b"
    replacement["status"] = {
        "phase": "Running",
        "conditions": [{"type": "Ready", "status": "True"}],
    }
    reconcile_workload(api, authorized, _nodes())

    assert api.status["readyReplicas"] == 2
    assert len(api.pods) == 2
    assert api.pods[source_name]["metadata"]["uid"] == source_uid


def test_workload_without_claimed_admission_protocol_remains_legacy() -> None:
    api = FakeAPI()
    workload = _workload()
    workload["spec"].pop("podCreateAdmissionProtocol")

    reconcile_workload(api, workload, _nodes())

    assert api.pods == {}
    assert api.status["podCreateHistoryUncertain"] is True
    assert api.status["conditions"][2]["reason"] == "LegacyPodCreateUncertain"


def test_stale_pdb_get_cannot_overwrite_a_newer_write() -> None:
    api = FakeAPI()
    stale = _workload(replicas=2, availabilityFloor=1)
    stale["metadata"]["generation"] = 2
    _ensure_pdb(api, stale)
    stale_pdb = api.get_pdb("api")
    assert stale_pdb is not None

    current = api.pdbs["api"]
    current["metadata"]["resourceVersion"] = "2"
    current["metadata"]["annotations"][WORKLOAD_GENERATION_ANNOTATION] = "3"
    current["spec"]["maxUnavailable"] = 2
    api.get_pdb = lambda _name: deepcopy(stale_pdb)
    stale["spec"]["replicas"] = 5
    stale["spec"]["disruption"]["maxConcurrentDisruptions"] = 5

    with pytest.raises(ApiError, match="resourceVersion conflict"):
        _ensure_pdb(api, stale)

    assert current["spec"]["maxUnavailable"] == 2
    assert current["metadata"]["annotations"][WORKLOAD_GENERATION_ANNOTATION] == "3"


def test_newer_workload_generation_advances_managed_pdb_budget() -> None:
    api = FakeAPI()
    old = _workload(replicas=2, availabilityFloor=1)
    old["metadata"]["generation"] = 2
    _ensure_pdb(api, old)
    old["metadata"]["resourceVersion"] = "2"
    old["spec"]["replicas"] = 4
    old["spec"]["disruption"]["maxConcurrentDisruptions"] = 2
    old["metadata"]["generation"] = 3

    _ensure_pdb(api, old)

    assert api.pdbs["api"]["spec"]["maxUnavailable"] == 2
    assert api.pdbs["api"]["metadata"]["annotations"][WORKLOAD_GENERATION_ANNOTATION] == "3"


def test_unversioned_managed_pdb_fails_closed_without_changing_budget() -> None:
    api = FakeAPI()
    old = _workload(replicas=3, availabilityFloor=2)
    _ensure_pdb(api, old)
    del api.pdbs["api"]["metadata"]["annotations"][WORKLOAD_GENERATION_ANNOTATION]
    old["spec"]["availabilityFloor"] = 1
    old["spec"]["disruption"]["maxConcurrentDisruptions"] = 2
    old["metadata"]["generation"] = 2
    original_spec = deepcopy(api.pdbs["api"]["spec"])

    with pytest.raises(SafetyError, match="no trusted workload generation"):
        _ensure_pdb(api, old)

    assert api.pdbs["api"]["spec"] == original_spec
    assert WORKLOAD_GENERATION_ANNOTATION not in api.pdbs["api"]["metadata"]["annotations"]


def test_managed_pdb_repairs_a_permissive_unhealthy_pod_eviction_policy() -> None:
    api = FakeAPI()
    workload = _workload()
    _ensure_pdb(api, workload)
    api.pdbs["api"]["spec"]["unhealthyPodEvictionPolicy"] = "AlwaysAllow"

    _ensure_pdb(api, workload)

    assert api.pdbs["api"]["spec"]["unhealthyPodEvictionPolicy"] == "IfHealthyBudget"


def test_staged_replacement_pdb_carries_the_approved_ready_floor() -> None:
    api = FakeAPI()
    workload = _workload(
        replicas=3,
        availabilityFloor=1,
        disruption={"maxConcurrentDisruptions": 2},
        executionProtocol="monotonic-scale-up-v1",
        executionRequest={
            "planID": "plan-1",
            "operationID": "move-1",
            "generation": 7,
            "expectedReplicas": 2,
            "targetReplicas": 3,
            "purpose": "staged-replacement-v1",
            "minimumReadyReplicas": 2,
        },
    )

    _ensure_pdb(api, workload)

    assert api.pdbs["api"]["spec"]["maxUnavailable"] == 1
    assert api.pdbs["api"]["spec"]["unhealthyPodEvictionPolicy"] == "IfHealthyBudget"


def test_reconcile_after_restart_reconstructs_existing_replica_without_duplicate() -> None:
    api = FakeAPI()
    workload = _workload()
    reconcile_workload(api, workload, _nodes())

    # A fresh reconcile has no in-memory state and discovers the Pod from the API.
    reconcile_workload(api, workload, _nodes())

    assert len(api.pods) == 1
    assert api.status["conditions"][0]["reason"] == "ReplicasNotReady"
    assert api.pods[next(iter(api.pods))]["metadata"]["ownerReferences"][0]["uid"] == "workload-uid"


def test_existing_pending_pod_is_observed_without_duplicate() -> None:
    api = FakeAPI()
    workload = _workload()
    reconcile_workload(api, workload, _nodes())
    pod = next(iter(api.pods.values()))
    assert pod["status"]["phase"] == "Pending"

    reconcile_workload(api, workload, _nodes())

    assert len(api.pods) == 1
    assert api.status["readyReplicas"] == 0
    assert api.status["conditions"][0]["status"] == "False"


def test_node_uid_change_blocks_new_pod_without_using_mutable_uid_label() -> None:
    api = FakeAPI()
    workload = _workload()
    changed_nodes = [{"metadata": {"name": "worker-a", "uid": "replacement-uid"}}, _nodes()[1]]

    reconcile_workload(api, workload, changed_nodes)

    assert api.pods == {}
    assert api.status["conditions"][0]["reason"] == "NodeIdentityChanged"


def test_restart_blocks_scheduled_pod_after_approved_node_uid_changes() -> None:
    api = FakeAPI()
    workload = _workload()
    reconcile_workload(api, workload, _nodes())
    pod = next(iter(api.pods.values()))
    pod["spec"]["nodeName"] = "worker-a"
    replacement_inventory = [
        {"metadata": {"name": "worker-a", "uid": "replacement-worker-a-uid"}},
        _nodes()[1],
    ]

    # A fresh controller pass observes the current Node object after the Pod was scheduled.
    reconcile_workload(api, workload, replacement_inventory)

    assert len(api.pods) == 1
    assert pod["metadata"].get("deletionTimestamp") is None
    assert api.status["conditions"][0]["reason"] == "NodeIdentityChanged"


def test_conflicting_pod_owner_or_replica_identity_is_rejected() -> None:
    api = FakeAPI()
    workload = _workload()
    api.pods["foreign"] = {
        "metadata": {
            "name": "foreign",
            "labels": {WORKLOAD_UID_LABEL: "workload-uid", "fleetright.io/workload-id": "api"},
            "ownerReferences": [{"apiVersion": "apps/v1", "kind": "ReplicaSet", "uid": "other"}],
        },
        "spec": {},
        "status": {
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True"}],
        },
    }

    reconcile_workload(api, workload, _nodes())

    assert len(api.pods) == 1
    assert api.status["conditions"][0]["reason"] == "PodIdentityConflict"
    assert api.status["replicas"] == 0
    assert api.status["readyReplicas"] == 0
    assert api.status["availableReplicas"] == 0


def test_nonzero_network_request_is_rejected_until_scheduler_resource_exists() -> None:
    api = FakeAPI()

    reconcile_workload(api, _workload(networkMbps=10), _nodes())

    assert api.pods == {}
    assert api.pdbs == {}
    assert api.status["conditions"][0]["reason"] == "UnsupportedNetworkBandwidth"


def test_security_context_is_hardened_and_root_overrides_are_rejected() -> None:
    for security_context in (
        {"runAsUser": 0},
        {"runAsGroup": 0},
        {"runAsNonRoot": False},
        {"runAsNonRoot": True},
    ):
        api = FakeAPI()
        workload = _workload()
        workload["spec"]["podTemplate"]["securityContext"] = security_context

        reconcile_workload(api, workload, _nodes())

        assert api.pods == {}
        assert api.status["conditions"][0]["reason"] == "UnsafeSecurityContext"


def test_workload_deletion_timestamp_stops_dependent_reconciliation() -> None:
    api = FakeAPI()
    workload = _workload()
    reconcile_workload(api, workload, _nodes())
    existing_pods = deepcopy(api.pods)
    existing_pdbs = deepcopy(api.pdbs)
    existing_status = deepcopy(api.status)
    deleting = deepcopy(workload)
    deleting["metadata"]["deletionTimestamp"] = "2026-10-08T00:00:00Z"
    deleting["spec"]["replicas"] = 2
    deleting["spec"]["disruption"]["maxConcurrentDisruptions"] = 0

    reconcile_workload(api, deleting, _nodes())

    assert api.pods == existing_pods
    assert api.pdbs == existing_pdbs
    assert api.status == existing_status


def test_api_rejected_pod_create_sets_sanitized_condition() -> None:
    class RejectingAPI(FakeAPI):
        def create_pod(self, pod: dict[str, Any]) -> dict[str, Any]:
            raise ApiError(422, "untrusted raw admission details")

    api = RejectingAPI()

    reconcile_workload(api, _workload(), _nodes())

    assert api.pods == {}
    assert "pendingPodCreate" not in api.status
    assert api.status["conditions"][0]["reason"] == "PodCreateRejectedByAPIServer"
    assert (
        api.status["conditions"][0]["message"] == "Kubernetes API rejected Pod creation (HTTP 422)"
    )
    assert "untrusted" not in api.status["conditions"][0]["message"]


def test_definitive_create_rejection_clears_intent_for_safe_retry() -> None:
    class RejectOnceAPI(FakeAPI):
        rejected = False

        def create_pod(self, pod: dict[str, Any]) -> dict[str, Any]:
            if not self.rejected:
                self.rejected = True
                raise ApiError(422, "invalid Pod")
            return super().create_pod(pod)

    api = RejectOnceAPI()
    workload = _workload()
    reconcile_workload(api, workload, _nodes())

    assert api.pods == {}
    assert "pendingPodCreate" not in api.status
    assert api.status["conditions"][2]["reason"] == "PodCreateRejectedByAPIServer"

    workload["metadata"]["resourceVersion"] = "4"
    reconcile_workload(api, workload, _nodes())

    assert len(api.pods) == 1
    assert "pendingPodCreate" not in api.status


def test_unknown_create_and_failed_status_write_errors_escape() -> None:
    class RateLimitedAPI(FakeAPI):
        def create_pod(self, pod: dict[str, Any]) -> dict[str, Any]:
            raise ApiError(503, "response outcome unknown")

    rate_limited = RateLimitedAPI()
    try:
        reconcile_workload(rate_limited, _workload(), _nodes())
    except ApiError as error:
        assert error.status == 503
    else:
        raise AssertionError("unknown Kubernetes API outcomes must escape reconciliation")
    assert rate_limited.status["pendingPodCreate"]["podName"] == _pod_name("workload-uid", 0)

    class StatusWriteFailureAPI(FakeAPI):
        def create_pod(self, pod: dict[str, Any]) -> dict[str, Any]:
            raise ApiError(422, "invalid Pod")

        def patch_status(
            self, name: str, resource_version: str, status: dict[str, Any]
        ) -> dict[str, Any]:
            raise ApiError(503, "status endpoint unavailable")

    status_failure = StatusWriteFailureAPI()
    try:
        reconcile_workload(status_failure, _workload(), _nodes())
    except ApiError as error:
        assert error.status == 503
    else:
        raise AssertionError("failed status writes must escape reconciliation")


def test_pod_template_fingerprint_prevents_silent_in_place_replacement() -> None:
    api = FakeAPI()
    workload = _workload()
    reconcile_workload(api, workload, _nodes())
    pod = next(iter(api.pods.values()))
    assert pod["metadata"]["annotations"][TEMPLATE_HASH_ANNOTATION]

    changed = _workload()
    changed["spec"]["podTemplate"]["containers"][0]["image"] = "other:local"
    reconcile_workload(api, changed, _nodes())

    assert len(api.pods) == 1
    assert api.status["conditions"][0]["reason"] == "PodTemplateChanged"


def test_map_list_reordering_does_not_block_scale_up() -> None:
    api = FakeAPI()
    workload = _workload(surgeBudget=1)
    workload["spec"]["podTemplate"]["containers"].append(
        {
            "name": "sidecar",
            "image": "sidecar:local",
            "resources": {"requests": {"cpu": "20m", "memory": "32Mi"}},
            "env": [{"name": "A", "value": "a"}, {"name": "B", "value": "b"}],
            "ports": [
                {"containerPort": 8081, "protocol": "TCP"},
                {"containerPort": 8082, "protocol": "TCP"},
            ],
        }
    )
    reconcile_workload(api, workload, _nodes())

    reordered = deepcopy(workload)
    reordered["spec"]["replicas"] = 2
    reordered["spec"]["placement"]["allowedNodes"].reverse()
    reordered["spec"]["podTemplate"]["containers"].reverse()
    for container in reordered["spec"]["podTemplate"]["containers"]:
        container.get("ports", []).reverse()
    reconcile_workload(api, reordered, _nodes())

    assert len(api.pods) == 2
    assert api.status["conditions"][0]["reason"] != "PodTemplateChanged"


def test_env_order_change_is_detected_as_pod_template_change() -> None:
    api = FakeAPI()
    workload = _workload()
    workload["spec"]["podTemplate"]["containers"][0]["env"] = [
        {"name": "BASE", "value": "first"},
        {"name": "DEPENDENT", "value": "$(BASE)"},
    ]
    reconcile_workload(api, workload, _nodes())

    reordered = deepcopy(workload)
    reordered["spec"]["podTemplate"]["containers"][0]["env"].reverse()
    reconcile_workload(api, reordered, _nodes())

    assert len(api.pods) == 1
    assert api.status["conditions"][0]["reason"] == "PodTemplateChanged"


def test_terminal_pod_is_preserved_until_explicit_replacement_protocol() -> None:
    api = FakeAPI()
    workload = _workload()
    reconcile_workload(api, workload, _nodes())
    name, pod = next(iter(api.pods.items()))
    pod["status"]["phase"] = "Failed"

    reconcile_workload(api, workload, _nodes())

    assert len(api.pods) == 1
    assert api.pods[name]["status"]["phase"] == "Failed"
    assert api.status["conditions"][2]["reason"] == "TerminalPodRequiresStagedExecution"


def test_terminal_pod_replacement_uses_same_deterministic_slot_after_removal() -> None:
    api = FakeAPI()
    workload = _workload()
    reconcile_workload(api, workload, _nodes())
    name, pod = next(iter(api.pods.items()))
    pod["status"]["phase"] = "Failed"
    del api.pods[name]
    reconcile_workload(api, workload, _nodes())

    assert len(api.pods) == 1
    assert next(iter(api.pods.values()))["metadata"]["uid"] != pod["metadata"]["uid"]


def test_kubernetes_pdb_patch_uses_current_resource_version_and_generation_marker() -> None:
    api = KubernetesAPI.__new__(KubernetesAPI)
    api.namespace = "fleetright-v2"
    requests: list[tuple[str, str, dict[str, Any] | None, str | None]] = []
    api.request = lambda method, path, body=None, *, content_type="application/json": (
        requests.append((method, path, body, content_type)) or {}
    )

    api.patch_pdb(
        "api",
        "resource-version-7",
        {"maxUnavailable": 1},
        {WORKLOAD_GENERATION_ANNOTATION: "3"},
    )

    assert requests == [
        (
            "PATCH",
            "/apis/policy/v1/namespaces/fleetright-v2/poddisruptionbudgets/api",
            {
                "metadata": {
                    "resourceVersion": "resource-version-7",
                    "annotations": {WORKLOAD_GENERATION_ANNOTATION: "3"},
                },
                "spec": {"maxUnavailable": 1},
            },
            "application/merge-patch+json",
        )
    ]


def test_terminating_logical_replica_is_not_recreated_until_absent() -> None:
    api = FakeAPI()
    workload = _workload()
    reconcile_workload(api, workload, _nodes())
    name, pod = next(iter(api.pods.items()))
    pod["metadata"]["deletionTimestamp"] = "terminating"

    reconcile_workload(api, workload, _nodes())

    assert set(api.pods) == {name}
    assert api.pods[name]["metadata"]["uid"] == pod["metadata"]["uid"]

    del api.pods[name]
    reconcile_workload(api, workload, _nodes())

    assert len(api.pods) == 1
    assert next(iter(api.pods.values()))["metadata"]["uid"] != pod["metadata"]["uid"]


def test_count_satisfied_with_ordinal_gap_does_not_delete_or_create_surge() -> None:
    api = FakeAPI()
    workload = _workload(replicas=3, availabilityFloor=3, surgeBudget=1)
    for ordinal in (0, 2, 3):
        pod = _pod_object(workload, ordinal)
        api.create_pod(pod)
        api.pods[pod["metadata"]["name"]]["status"] = {
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True"}],
        }
        api.status = api.status or {}
        api.status.setdefault("authorizedPodCreates", []).append(
            {
                "workloadUID": "workload-uid",
                "podUID": api.pods[pod["metadata"]["name"]]["metadata"]["uid"],
                "podName": pod["metadata"]["name"],
                "replicaID": pod["metadata"]["labels"][REPLICA_ID_LABEL],
                "workloadGeneration": 1,
                "planID": pod["metadata"]["annotations"][POD_CREATE_PLAN_ANNOTATION],
                "operationID": pod["metadata"]["annotations"][POD_CREATE_OPERATION_ANNOTATION],
                "createID": pod["metadata"]["annotations"][POD_CREATE_ATTEMPT_ANNOTATION],
            }
        )
    workload["status"] = deepcopy(api.status)
    expected_names = set(api.pods)

    reconcile_workload(api, workload, _nodes())

    assert set(api.pods) == expected_names
    assert all("deletionTimestamp" not in pod["metadata"] for pod in api.pods.values())
    assert api.status["replicas"] == 3
    assert api.status["readyReplicas"] == 3
    assert api.status["availableReplicas"] == 3
    assert api.status["surgeReplicas"] == 0


def test_scale_down_requires_staged_execution_when_floor_permits() -> None:
    api = FakeAPI()
    workload = _workload(replicas=3, availabilityFloor=2)
    reconcile_workload(api, workload, _nodes())
    for pod in api.pods.values():
        pod["status"] = {
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True"}],
        }
    scaled_down = deepcopy(workload)
    scaled_down["spec"]["replicas"] = 2

    reconcile_workload(api, scaled_down, _nodes())

    assert all("deletionTimestamp" not in pod["metadata"] for pod in api.pods.values())
    assert len(api.pods) == 3
    assert api.status["replicas"] == 3
    assert api.status["readyReplicas"] == 3
    assert api.status["availableReplicas"] == 3
    assert api.status["conditions"][0]["status"] == "True"
    assert api.status["conditions"][1]["status"] == "True"
    assert api.status["conditions"][2]["status"] == "False"
    assert api.status["conditions"][2]["reason"] == "ScaleDownRequiresStagedExecution"
    assert "requires staged" in api.status["conditions"][2]["message"]

    recovered_status = deepcopy(scaled_down)
    recovered_status["status"] = deepcopy(api.status)
    recovered_status["metadata"]["resourceVersion"] = "2"
    reconcile_workload(api, recovered_status, _nodes())

    assert all("deletionTimestamp" not in pod["metadata"] for pod in api.pods.values())
    assert api.status["conditions"][2]["reason"] == "ScaleDownRequiresStagedExecution"


def test_scale_down_waits_for_existing_deletion_without_retiring_another_pod() -> None:
    api = FakeAPI()
    workload = _workload(replicas=3, availabilityFloor=2)
    reconcile_workload(api, workload, _nodes())
    for pod in api.pods.values():
        pod["status"] = {
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True"}],
        }
    deleting_name = _pod_name(workload["metadata"]["uid"], 2)
    api.pods[deleting_name]["metadata"]["deletionTimestamp"] = "in-progress"
    scaled_down = deepcopy(workload)
    scaled_down["spec"]["replicas"] = 2

    reconcile_workload(api, scaled_down, _nodes())

    assert [name for name, pod in api.pods.items() if pod["metadata"].get("deletionTimestamp")] == [
        deleting_name
    ]
    assert api.status["conditions"][2]["status"] == "False"
    assert api.status["conditions"][2]["reason"] == "ScaleDownRequiresStagedExecution"


def test_scale_down_does_not_delete_pending_or_ready_pods() -> None:
    api = FakeAPI()
    workload = _workload(replicas=3, availabilityFloor=2)
    reconcile_workload(api, workload, _nodes())
    for pod in api.pods.values():
        pod["status"] = {"phase": "Pending", "conditions": []}
    for ordinal in (0, 2):
        pod = next(
            pod
            for pod in api.pods.values()
            if pod["metadata"]["labels"][REPLICA_ID_LABEL] == f"api-{ordinal}"
        )
        pod["status"] = {
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True"}],
        }
    api.pods[_pod_name(workload["metadata"]["uid"], 1)]["status"] = {
        "phase": "Running",
        "conditions": [{"type": "Ready", "status": "False"}],
    }
    scaled_down = deepcopy(workload)
    scaled_down["spec"]["replicas"] = 2

    reconcile_workload(api, scaled_down, _nodes())

    assert all("deletionTimestamp" not in pod["metadata"] for pod in api.pods.values())
    ready_ids = {
        pod["metadata"]["labels"][REPLICA_ID_LABEL]
        for pod in api.pods.values()
        if any(
            condition.get("type") == "Ready" and condition.get("status") == "True"
            for condition in pod["status"]["conditions"]
        )
    }
    assert ready_ids == {"api-0", "api-2"}
    assert api.status["readyReplicas"] == 2
    assert all("deletionTimestamp" not in pod["metadata"] for pod in api.pods.values())
    assert api.status["conditions"][2]["reason"] == "ScaleDownRequiresStagedExecution"


def test_zero_configured_disruption_keeps_source_pod_during_scale_down() -> None:
    class NoDeleteAPI(FakeAPI):
        def delete_pod(self, pod: dict[str, Any]) -> None:
            raise AssertionError(
                f"nonterminal Pod deletion bypassed PDB: {pod['metadata']['name']}"
            )

    api = NoDeleteAPI()
    workload = _workload(replicas=3, availabilityFloor=2)
    workload["spec"]["disruption"]["maxConcurrentDisruptions"] = 0
    reconcile_workload(api, workload, _nodes())
    for pod in api.pods.values():
        pod["status"] = {
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True"}],
        }
    scaled_down = deepcopy(workload)
    scaled_down["spec"]["replicas"] = 2

    reconcile_workload(api, scaled_down, _nodes())

    assert len(api.pods) == 3
    assert all("deletionTimestamp" not in pod["metadata"] for pod in api.pods.values())
    assert api.pdbs["api"]["spec"]["maxUnavailable"] == 0
    assert api.status["conditions"][0]["status"] == "True"
    assert api.status["conditions"][1]["status"] == "True"
    assert api.status["conditions"][2]["status"] == "False"
    assert api.status["conditions"][2]["reason"] == "ScaleDownRequiresStagedExecution"


def test_scale_down_does_not_delete_ready_replica_below_availability_floor() -> None:
    api = FakeAPI()
    workload = _workload(replicas=3, availabilityFloor=2)
    reconcile_workload(api, workload, _nodes())
    ready_pod = next(
        pod for pod in api.pods.values() if pod["metadata"]["labels"][REPLICA_ID_LABEL] == "api-2"
    )
    ready_pod["status"] = {
        "phase": "Running",
        "conditions": [{"type": "Ready", "status": "True"}],
    }
    workload["spec"]["replicas"] = 2

    reconcile_workload(api, workload, _nodes())

    assert all("deletionTimestamp" not in pod["metadata"] for pod in api.pods.values())
    assert api.status["conditions"][0]["reason"] == "AvailabilityFloorWouldBeViolated"


def test_api_patch_media_type_and_projected_token_refresh(tmp_path: Path) -> None:
    class Response:
        def __enter__(self) -> Response:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def read(self) -> bytes:
            return b"{}"

    class Opener:
        def __init__(self) -> None:
            self.requests = []

        def open(self, request: Any, timeout: int) -> Response:
            self.requests.append(request)
            return Response()

    token_path = tmp_path / "token"
    token_path.write_text("first-token", encoding="utf-8")
    api = object.__new__(KubernetesAPI)
    api.base_url = "https://kubernetes.default.svc"
    api.token_path = str(token_path)
    api.opener = Opener()
    api.request(
        "PATCH",
        "/status",
        {"status": {"replicas": 1}},
        content_type="application/merge-patch+json",
    )
    token_path.write_text("rotated-token", encoding="utf-8")
    api.request("GET", "/pods")

    patch_request, next_request = api.opener.requests
    assert patch_request.get_header("Content-type") == "application/merge-patch+json"
    assert patch_request.get_header("Authorization") == "Bearer first-token"
    assert next_request.get_header("Authorization") == "Bearer rotated-token"
