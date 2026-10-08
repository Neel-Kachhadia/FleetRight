from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

from fleetright.workload_controller import (
    REPLICA_ID_LABEL,
    TEMPLATE_HASH_ANNOTATION,
    WORKLOAD_UID_LABEL,
    ApiError,
    KubernetesAPI,
    _pod_name,
    _pod_object,
    reconcile_workload,
)


def _workload(**spec_overrides: Any) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "workloadID": "api",
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

    def delete_pod(self, pod: dict[str, Any]) -> None:
        self.pods[pod["metadata"]["name"]]["metadata"]["deletionTimestamp"] = "now"

    def get_pdb(self, name: str) -> dict[str, Any] | None:
        value = self.pdbs.get(name)
        return deepcopy(value) if value else None

    def create_pdb(self, pdb: dict[str, Any]) -> dict[str, Any]:
        value = deepcopy(pdb)
        value["metadata"].update(uid="pdb-uid", resourceVersion="1")
        self.pdbs[value["metadata"]["name"]] = value
        return deepcopy(value)

    def patch_pdb(self, name: str, resource_version: str, spec: dict[str, Any]) -> dict[str, Any]:
        self.pdbs[name]["spec"] = deepcopy(spec)
        return deepcopy(self.pdbs[name])

    def patch_status(
        self, name: str, resource_version: str, status: dict[str, Any]
    ) -> dict[str, Any]:
        self.status = deepcopy(status)
        return {"status": deepcopy(status)}


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
    assert api.status["labelSelector"] == f"{WORKLOAD_UID_LABEL}=workload-uid"
    assert api.status["readyReplicas"] == 0


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
    assert api.status["conditions"][0]["reason"] == "PodCreateRejectedByAPIServer"
    assert (
        api.status["conditions"][0]["message"] == "Kubernetes API rejected Pod creation (HTTP 422)"
    )
    assert "untrusted" not in api.status["conditions"][0]["message"]


def test_transient_create_and_failed_status_write_errors_escape() -> None:
    class RateLimitedAPI(FakeAPI):
        def create_pod(self, pod: dict[str, Any]) -> dict[str, Any]:
            raise ApiError(429, "rate limited")

    rate_limited = RateLimitedAPI()
    try:
        reconcile_workload(rate_limited, _workload(), _nodes())
    except ApiError as error:
        assert error.status == 429
    else:
        raise AssertionError("transient Kubernetes API errors must escape reconciliation")
    assert rate_limited.status is None

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


def test_terminal_pod_is_deleted_by_uid_and_recreated_only_after_absence() -> None:
    api = FakeAPI()
    workload = _workload()
    reconcile_workload(api, workload, _nodes())
    name, pod = next(iter(api.pods.items()))
    pod["status"]["phase"] = "Failed"

    reconcile_workload(api, workload, _nodes())

    assert len(api.pods) == 1
    assert api.pods[name]["metadata"]["deletionTimestamp"] == "now"
    del api.pods[name]
    reconcile_workload(api, workload, _nodes())

    assert len(api.pods) == 1
    assert next(iter(api.pods.values()))["metadata"]["uid"] != pod["metadata"]["uid"]


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
    expected_names = set(api.pods)

    reconcile_workload(api, workload, _nodes())

    assert set(api.pods) == expected_names
    assert all("deletionTimestamp" not in pod["metadata"] for pod in api.pods.values())
    assert api.status["replicas"] == 3
    assert api.status["readyReplicas"] == 3
    assert api.status["availableReplicas"] == 3
    assert api.status["surgeReplicas"] == 0


def test_scale_down_deletes_only_one_high_ordinal_when_floor_permits() -> None:
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

    deleted = [
        pod
        for pod in api.pods.values()
        if pod["metadata"].get("deletionTimestamp")
    ]
    assert len(deleted) == 1
    assert deleted[0]["metadata"]["labels"][REPLICA_ID_LABEL] == "api-2"
    assert len(api.pods) == 3
    assert api.status["replicas"] == 2
    assert api.status["readyReplicas"] == 2
    assert api.status["availableReplicas"] == 2

    reconcile_workload(api, scaled_down, _nodes())

    still_deleted = [
        pod
        for pod in api.pods.values()
        if pod["metadata"].get("deletionTimestamp")
    ]
    assert len(still_deleted) == 1
    assert still_deleted[0]["metadata"]["labels"][REPLICA_ID_LABEL] == "api-2"


def test_scale_down_skips_high_ready_pod_at_floor_and_removes_lower_pending_pod() -> None:
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
    scaled_down = deepcopy(workload)
    scaled_down["spec"]["replicas"] = 2

    reconcile_workload(api, scaled_down, _nodes())

    deleted = [
        pod
        for pod in api.pods.values()
        if pod["metadata"].get("deletionTimestamp")
    ]
    assert len(deleted) == 1
    assert deleted[0]["metadata"]["labels"][REPLICA_ID_LABEL] == "api-1"
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

    assert ready_pod["metadata"].get("deletionTimestamp") is None
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
