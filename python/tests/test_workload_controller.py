from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
from threading import Event
from typing import Any

import pytest
from fleetright.workload_controller import (
    POD_CREATE_ATTEMPT_ANNOTATION,
    POD_CREATE_OPERATION_ANNOTATION,
    POD_CREATE_PLAN_ANNOTATION,
    POD_CREATE_PROTOCOL,
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
    _write_retired_slot_tombstone,
    reconcile_workload,
)


def _workload(**spec_overrides: Any) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "workloadID": "api",
        "podCreateProtocol": POD_CREATE_PROTOCOL,
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
        self.status = deepcopy(self.status or {})
        for key, value in status.items():
            if value is None:
                self.status.pop(key, None)
            else:
                self.status[key] = deepcopy(value)
        return {
            "metadata": {"name": name, "resourceVersion": str(int(resource_version) + 1)},
            "status": deepcopy(status),
        }


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


def test_delayed_create_is_journaled_and_takeover_resolves_exact_pod() -> None:
    class CompareAndSwapStatusAPI(FakeAPI):
        status_resource_version = "1"

        def patch_status(
            self, name: str, resource_version: str, status: dict[str, Any]
        ) -> dict[str, Any]:
            if resource_version != self.status_resource_version:
                raise ApiError(409, "status resourceVersion conflict")
            self.status = deepcopy(self.status or {})
            for key, value in status.items():
                if value is None:
                    self.status.pop(key, None)
                else:
                    self.status[key] = deepcopy(value)
            self.status_resource_version = str(int(resource_version) + 1)
            return {
                "metadata": {"name": name, "resourceVersion": self.status_resource_version},
                "status": deepcopy(self.status),
            }

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
        assert api.get_pod(slot_name) is None
        assert api.status["pendingPodCreate"]["podName"] == slot_name
        assert api.status["execution"]["phase"] == "RECONCILING"
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
    assert (
        authorization["createID"]
        == api.pods[slot_name]["metadata"]["annotations"][POD_CREATE_ATTEMPT_ANNOTATION]
    )
    assert api.status["conditions"][2]["reason"] == "Reconciled"


def test_pre_upgrade_inflight_create_can_recreate_a_retired_slot() -> None:
    api = FakeAPI()
    initial = _workload()
    initial["spec"]["executionProtocol"] = "monotonic-scale-up-v1"
    reconcile_workload(api, initial, _nodes())
    api.pods[next(iter(api.pods))]["status"] = {
        "phase": "Running",
        "conditions": [{"type": "Ready", "status": "True"}],
    }
    initial["metadata"]["resourceVersion"] = "2"
    reconcile_workload(api, initial, _nodes())

    old_snapshot = deepcopy(initial)
    old_snapshot["metadata"]["generation"] = 2
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
    for annotation in (
        POD_CREATE_ATTEMPT_ANNOTATION,
        POD_CREATE_OPERATION_ANNOTATION,
        POD_CREATE_PLAN_ANNOTATION,
    ):
        old_pod["metadata"]["annotations"].pop(annotation)
    entered_post, release_post = Event(), Event()

    def delayed_old_create() -> None:
        # Model a pre-upgrade request admitted before pendingPodCreate existed.
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
    observed["metadata"]["resourceVersion"] = str(
        int(next_operation["metadata"]["resourceVersion"]) + 1
    )
    observed["status"] = deepcopy(api.status)
    reconcile_workload(api, observed, _nodes())
    assert api.status["unverifiedPodCreates"] == 1
    assert api.status["podCreateHistoryUncertain"] is True
    assert api.status["readyReplicas"] == 1
    assert api.status["execution"]["phase"] == "BLOCKED"
    assert api.status["conditions"][2]["reason"] == "RetiredReplicaPodObserved"


def test_retired_slot_tombstone_survives_controller_takeover_and_blocks_recreation() -> None:
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
    with pytest.raises(RuntimeError, match="lost the tombstone response"):
        _write_retired_slot_tombstone(api, workload, source, "plan-retire", "retire-1", _nodes())

    interim = deepcopy(workload)
    interim["metadata"]["resourceVersion"] = str(int(workload["metadata"]["resourceVersion"]) + 1)
    interim["status"] = deepcopy(api.status)
    _write_retired_slot_tombstone(api, interim, source, "plan-retire", "retire-1", _nodes())
    reconcile_workload(api, interim, _nodes())

    assert api.pods[source["metadata"]["name"]]["metadata"]["uid"] == source["metadata"]["uid"]
    assert api.status.get("podCreateHistoryUncertain") is not True
    assert api.status["conditions"][2]["reason"] == "RetiredReplicaRequiresNewOperation"
    del api.pods[source["metadata"]["name"]]

    _write_retired_slot_tombstone(api, interim, source, "plan-retire", "retire-1", _nodes())

    takeover = deepcopy(interim)
    takeover["status"] = deepcopy(api.status)
    reconcile_workload(api, takeover, _nodes())

    assert api.pods == {}
    assert api.status["retiredReplicaSlots"][0]["replicaID"] == "api-0"
    assert api.status["conditions"][2]["reason"] == "RetiredReplicaRequiresNewOperation"


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
