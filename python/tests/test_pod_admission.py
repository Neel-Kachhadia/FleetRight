from __future__ import annotations

import io
import json
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import yaml
from fleetright.pod_admission import (
    EXECUTOR_USERNAME,
    PATH,
    PodCreateAdmission,
    _pod_spec_mismatch_paths,
    make_handler,
)
from fleetright.workload_controller import (
    API_VERSION,
    POD_CREATE_ADMISSION_PROTOCOL,
    POD_CREATE_ATTEMPT_ANNOTATION,
    POD_CREATE_OPERATION_ANNOTATION,
    POD_CREATE_PLAN_ANNOTATION,
    POD_CREATE_PROTOCOL,
    REPLICA_ID_LABEL,
    WORKLOAD_UID_LABEL,
    ApiError,
    _pdb_object,
    _pod_name,
    _pod_object,
)


def _workload() -> dict[str, Any]:
    intent = {
        "workloadUID": "workload-uid",
        "workloadGeneration": 1,
        "podName": _pod_name("workload-uid", 0),
        "replicaID": "api-0",
        "planID": "plan-1",
        "operationID": "operation-1",
        "createID": "create-1",
    }
    return {
        "apiVersion": API_VERSION,
        "kind": "FleetRightWorkload",
        "metadata": {
            "name": "api",
            "uid": "workload-uid",
            "resourceVersion": "7",
            "generation": 1,
        },
        "spec": {
            "workloadID": "api",
            "podCreateProtocol": POD_CREATE_PROTOCOL,
            "podCreateAdmissionProtocol": POD_CREATE_ADMISSION_PROTOCOL,
            "placement": {"allowedNodes": [{"name": "worker-a", "uid": "node-uid"}]},
            "podTemplate": {
                "containers": [
                    {
                        "name": "api",
                        "image": "fleetright-workload:local",
                        "resources": {"requests": {"cpu": "100m", "memory": "64Mi"}},
                    }
                ]
            },
        },
        "status": {"pendingPodCreate": intent},
    }


class AdmissionAPI:
    def __init__(self, workload: dict[str, Any]):
        self.workload = deepcopy(workload)
        self.status_patches = 0

    def request(self, method: str, path: str) -> dict[str, Any]:
        assert method == "GET"
        assert path.endswith("/fleetrightworkloads/api")
        return deepcopy(self.workload)

    def patch_status(
        self, name: str, resource_version: str, status: dict[str, Any]
    ) -> dict[str, Any]:
        assert name == "api"
        if resource_version != self.workload["metadata"]["resourceVersion"]:
            raise ApiError(409, "resourceVersion conflict")
        self.status_patches += 1
        self.workload["metadata"]["resourceVersion"] = str(int(resource_version) + 1)
        self.workload["status"] = deepcopy(status)
        return deepcopy(self.workload)


def _request(pod: dict[str, Any], *, uid: str = "admission-1") -> dict[str, Any]:
    return {
        "uid": uid,
        "namespace": "fleetright-v2",
        "dryRun": False,
        "userInfo": {
            "username": "system:serviceaccount:fleetright-v2:fleetright-workload-controller"
        },
        "object": deepcopy(pod),
    }


def test_admission_dry_run_probe_rejects_without_claiming_or_mutating() -> None:
    api = AdmissionAPI(_workload())
    admission = PodCreateAdmission(api, "fleetright-v2", "fleetright-workload-controller")

    assert admission.admit({"dryRun": True}) == (False, "FLEETRIGHT_POD_ADMISSION_PROBE_REJECTED")
    assert api.status_patches == 0


def test_only_one_exact_pod_create_can_claim_a_durable_reservation() -> None:
    workload = _workload()
    intent = workload["status"]["pendingPodCreate"]
    pod = _pod_object(workload, 0, intent)
    api = AdmissionAPI(workload)
    admission = PodCreateAdmission(api, "fleetright-v2", "fleetright-workload-controller")

    assert admission.admit(_request(pod)) == (True, "")
    assert api.workload["status"]["pendingPodCreate"]["admissionUID"] == "admission-1"
    assert api.status_patches == 1
    assert admission.admit(_request(pod, uid="admission-2"))[0] is False
    assert api.status_patches == 1


def test_pod_create_cannot_reuse_a_retired_logical_slot() -> None:
    workload = _workload()
    workload["status"]["retiredReplicaSlots"] = [{"replicaID": "api-0"}]
    pod = _pod_object(workload, 0, workload["status"]["pendingPodCreate"])
    api = AdmissionAPI(workload)

    assert PodCreateAdmission(api, "fleetright-v2", "fleetright-workload-controller").admit(
        _request(pod)
    ) == (False, "Pod CREATE targets a retired or invalid logical slot")
    assert api.status_patches == 0


def test_admission_rejects_forged_placement_stale_identity_and_old_writer() -> None:
    workload = _workload()
    intent = workload["status"]["pendingPodCreate"]
    pod = _pod_object(workload, 0, intent)
    api = AdmissionAPI(workload)
    admission = PodCreateAdmission(api, "fleetright-v2", "fleetright-workload-controller")

    forbidden = deepcopy(pod)
    forbidden["spec"]["nodeName"] = "worker-b"
    assert admission.admit(_request(forbidden))[0] is False

    stale_owner = deepcopy(pod)
    stale_owner["metadata"]["ownerReferences"][0]["uid"] = "old-workload-uid"
    assert admission.admit(_request(stale_owner))[0] is False

    old_writer = deepcopy(pod)
    old_writer["metadata"]["annotations"][POD_CREATE_ATTEMPT_ANNOTATION] = "unreserved"
    old_writer["metadata"]["annotations"][POD_CREATE_PLAN_ANNOTATION] = "old-plan"
    old_writer["metadata"]["annotations"][POD_CREATE_OPERATION_ANNOTATION] = "old-operation"
    old_writer["metadata"]["labels"][WORKLOAD_UID_LABEL] = "legacy-workload"
    old_writer["metadata"]["labels"][REPLICA_ID_LABEL] = "legacy-0"
    assert admission.admit(_request(old_writer))[0] is False
    assert api.status_patches == 0

    custom_scheduler = deepcopy(pod)
    custom_scheduler["spec"]["schedulerName"] = "other-scheduler"
    assert admission.admit(_request(custom_scheduler))[0] is False

    extra_label = deepcopy(pod)
    extra_label["metadata"]["labels"]["attacker.example/route"] = "elsewhere"
    assert admission.admit(_request(extra_label))[0] is False


def test_admission_rejects_direct_node_assignment_for_every_pod_create() -> None:
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "native-pod"},
        "spec": {"nodeName": "worker-a"},
    }
    request = _request(pod)
    request["userInfo"]["username"] = "system:serviceaccount:fleetright-v2:other"

    assert PodCreateAdmission(AdmissionAPI(_workload()), "fleetright-v2", "controller").admit(
        request
    ) == (False, "Pod CREATE cannot assign a Node directly")


def test_known_api_server_defaults_are_accepted_without_allowing_custom_values() -> None:
    workload = _workload()
    intent = workload["status"]["pendingPodCreate"]
    pod = _pod_object(workload, 0, intent)
    pod["spec"].update(
        dnsPolicy="ClusterFirst",
        enableServiceLinks=True,
        nodeName="",
        preemptionPolicy="PreemptLowerPriority",
        schedulerName="default-scheduler",
        serviceAccountName="default",
        terminationGracePeriodSeconds=30,
    )
    pod["spec"]["containers"][0].update(
        imagePullPolicy="IfNotPresent",
        terminationMessagePath="/dev/termination-log",
        terminationMessagePolicy="File",
    )
    api = AdmissionAPI(workload)
    admission = PodCreateAdmission(api, "fleetright-v2", "fleetright-workload-controller")

    assert admission.admit(_request(pod)) == (True, "")


def _live_defaulted_probe_pod(workload: dict[str, Any]) -> dict[str, Any]:
    pod = _pod_object(workload, 0, workload["status"]["pendingPodCreate"])
    pod["spec"].update(
        priority=0,
        serviceAccount="default",
        serviceAccountName="default",
        tolerations=[
            {
                "key": "node.kubernetes.io/not-ready",
                "operator": "Exists",
                "effect": "NoExecute",
                "tolerationSeconds": 300,
            },
            {
                "key": "node.kubernetes.io/unreachable",
                "operator": "Exists",
                "effect": "NoExecute",
                "tolerationSeconds": 300,
            },
        ],
    )
    for name in ("readinessProbe", "livenessProbe"):
        probe = pod["spec"]["containers"][0][name]
        probe["successThreshold"] = 1
        probe["httpGet"]["scheme"] = "HTTP"
    return pod


def test_live_v137_probe_priority_service_account_and_toleration_defaults_are_accepted() -> None:
    workload = _workload()
    container = workload["spec"]["podTemplate"]["containers"][0]
    for name in ("readinessProbe", "livenessProbe"):
        container[name] = {"httpGet": {"path": "/ready", "port": 8080}}
    pod = _live_defaulted_probe_pod(workload)
    request = _request(pod)
    unmodified = deepcopy(request)
    api = AdmissionAPI(workload)
    admission = PodCreateAdmission(api, "fleetright-v2", "fleetright-workload-controller")

    assert admission.admit(request) == (True, "")
    assert api.status_patches == 1
    assert request == unmodified


@pytest.mark.parametrize(
    "mutation",
    [
        "service-account-alias",
        "service-account-name",
        "priority",
        "priority-class",
        "readiness-scheme",
        "liveness-scheme",
        "success-threshold",
        "toleration-seconds",
        "toleration-effect",
        "extra-toleration",
        "missing-toleration",
        "readiness-path",
        "resources",
    ],
)
def test_live_default_normalization_rejects_every_nondefault_change(mutation: str) -> None:
    workload = _workload()
    container = workload["spec"]["podTemplate"]["containers"][0]
    for name in ("readinessProbe", "livenessProbe"):
        container[name] = {"httpGet": {"path": "/ready", "port": 8080}}
    pod = _live_defaulted_probe_pod(workload)
    spec, container = pod["spec"], pod["spec"]["containers"][0]
    if mutation == "service-account-alias":
        spec["serviceAccount"] = "other-service-account"
    elif mutation == "service-account-name":
        spec["serviceAccountName"] = "other-service-account"
    elif mutation == "priority":
        spec["priority"] = 1
    elif mutation == "priority-class":
        spec["priorityClassName"] = "other-priority-class"
    elif mutation in {"readiness-scheme", "liveness-scheme"}:
        name = "readinessProbe" if mutation == "readiness-scheme" else "livenessProbe"
        container[name]["httpGet"]["scheme"] = "HTTPS"
    elif mutation == "success-threshold":
        container["readinessProbe"]["successThreshold"] = 2
    elif mutation == "toleration-seconds":
        spec["tolerations"][0]["tolerationSeconds"] = 301
    elif mutation == "toleration-effect":
        spec["tolerations"][0]["effect"] = "NoSchedule"
    elif mutation == "extra-toleration":
        spec["tolerations"].append({"operator": "Exists", "effect": "NoSchedule"})
    elif mutation == "missing-toleration":
        spec["tolerations"].pop()
    elif mutation == "readiness-path":
        container["readinessProbe"]["httpGet"]["path"] = "/other"
    elif mutation == "resources":
        container["resources"]["requests"]["cpu"] = "101m"
    api = AdmissionAPI(workload)
    admission = PodCreateAdmission(api, "fleetright-v2", "fleetright-workload-controller")

    allowed, message = admission.admit(_request(pod))

    assert allowed is False
    assert message.startswith("Pod reservation mismatch: pod-spec: fields=spec.")
    assert api.status_patches == 0


def test_spec_mismatch_diagnostics_contain_only_bounded_safe_field_paths() -> None:
    workload = _workload()
    pod = _pod_object(workload, 0, workload["status"]["pendingPodCreate"])
    pod["spec"]["containers"][0]["readinessProbe"] = {"httpGet": {"scheme": "secret-scheme-value"}}
    pod["spec"]["private-field-name"] = "secret-field-value"
    api = AdmissionAPI(workload)
    admission = PodCreateAdmission(api, "fleetright-v2", "fleetright-workload-controller")

    assert admission.admit(_request(pod)) == (
        False,
        "Pod reservation mismatch: pod-spec: "
        "fields=spec.containers[0].readinessProbe,spec.<unknown-field>",
    )
    assert api.status_patches == 0


def test_spec_mismatch_diagnostics_stop_after_eight_paths() -> None:
    actual = {"containers": [{"image": "secret-image-value"}] * 32}
    expected = {"containers": [{"image": "approved-image-value"}] * 32}

    assert _pod_spec_mismatch_paths(actual, expected) == ",".join(
        f"spec.containers[{index}].image" for index in range(8)
    )


def test_extra_containers_are_not_discarded_by_default_normalization() -> None:
    workload = _workload()
    pod = _pod_object(workload, 0, workload["status"]["pendingPodCreate"])
    pod["spec"]["containers"].append(deepcopy(pod["spec"]["containers"][0]))
    api = AdmissionAPI(workload)
    admission = PodCreateAdmission(api, "fleetright-v2", "fleetright-workload-controller")

    assert admission.admit(_request(pod)) == (
        False,
        "Pod reservation mismatch: pod-spec: fields=spec.containers[1]",
    )
    assert api.status_patches == 0


def test_empty_api_server_pod_status_is_accepted_but_nonempty_status_is_rejected() -> None:
    workload = _workload()
    pod = _pod_object(workload, 0, workload["status"]["pendingPodCreate"])
    pod["status"] = {}
    admission = PodCreateAdmission(
        AdmissionAPI(workload), "fleetright-v2", "fleetright-workload-controller"
    )

    assert admission.admit(_request(pod)) == (True, "")

    workload = _workload()
    pod = _pod_object(workload, 0, workload["status"]["pendingPodCreate"])
    pod["status"] = {"phase": "Pending", "conditions": []}
    admission = PodCreateAdmission(
        AdmissionAPI(workload), "fleetright-v2", "fleetright-workload-controller"
    )
    assert admission.admit(_request(pod)) == (True, "")

    workload = _workload()
    pod = _pod_object(workload, 0, workload["status"]["pendingPodCreate"])
    pod["status"] = {"phase": "", "qosClass": "Burstable"}
    admission = PodCreateAdmission(
        AdmissionAPI(workload), "fleetright-v2", "fleetright-workload-controller"
    )
    assert admission.admit(_request(pod)) == (True, "")

    workload = _workload()
    pod = _pod_object(workload, 0, workload["status"]["pendingPodCreate"])
    pod["status"] = {"qosClass": "Guaranteed"}
    admission = PodCreateAdmission(
        AdmissionAPI(workload), "fleetright-v2", "fleetright-workload-controller"
    )
    assert admission.admit(_request(pod))[0] is False


def test_server_owned_api_metadata_is_accepted_but_unapproved_finalizers_are_rejected() -> None:
    workload = _workload()
    pod = _pod_object(workload, 0, workload["status"]["pendingPodCreate"])
    pod["metadata"].update(
        creationTimestamp=None,
        resourceVersion="",
        uid="",
        generation=0,
        managedFields=[],
    )
    admission = PodCreateAdmission(
        AdmissionAPI(workload), "fleetright-v2", "fleetright-workload-controller"
    )
    assert admission.admit(_request(pod)) == (True, "")

    workload = _workload()
    pod = _pod_object(workload, 0, workload["status"]["pendingPodCreate"])
    pod["metadata"].update(
        creationTimestamp="2026-10-11T12:00:00Z",
        uid="server-assigned-pod-uid",
        generation=1,
        managedFields=[{"manager": "fleetright-workload-controller"}],
    )
    admission = PodCreateAdmission(
        AdmissionAPI(workload), "fleetright-v2", "fleetright-workload-controller"
    )
    assert admission.admit(_request(pod)) == (True, "")

    workload = _workload()
    pod = _pod_object(workload, 0, workload["status"]["pendingPodCreate"])
    pod["metadata"]["finalizers"] = ["attacker.example/hold"]
    admission = PodCreateAdmission(
        AdmissionAPI(workload), "fleetright-v2", "fleetright-workload-controller"
    )
    assert admission.admit(_request(pod))[0] is False

    workload = _workload()
    pod = _pod_object(workload, 0, workload["status"]["pendingPodCreate"])
    pod["status"] = {"phase": ""}
    admission = PodCreateAdmission(
        AdmissionAPI(workload), "fleetright-v2", "fleetright-workload-controller"
    )
    assert admission.admit(_request(pod)) == (True, "")

    workload = _workload()
    pod = _pod_object(workload, 0, workload["status"]["pendingPodCreate"])
    pod["status"] = {"phase": "Running"}
    admission = PodCreateAdmission(
        AdmissionAPI(workload), "fleetright-v2", "fleetright-workload-controller"
    )
    assert admission.admit(_request(pod))[0] is False


def test_admission_review_accepts_api_server_timeout_query() -> None:
    workload = _workload()
    intent = workload["status"]["pendingPodCreate"]
    pod = _pod_object(workload, 0, intent)
    api = AdmissionAPI(workload)
    admission = PodCreateAdmission(api, "fleetright-v2", "fleetright-workload-controller")
    body = json.dumps(
        {
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "request": _request(pod),
        }
    ).encode()
    handler_type = make_handler(admission)
    handler = object.__new__(handler_type)
    handler.path = f"{PATH}?timeout=10s"
    handler.headers = {"Content-Length": str(len(body))}
    handler.rfile = io.BytesIO(body)
    handler.wfile = io.BytesIO()
    handler.send_response = lambda _code: None
    handler.send_header = lambda _key, _value: None
    handler.end_headers = lambda: None

    handler_type.do_POST(handler)

    response = json.loads(handler.wfile.getvalue())
    assert response["response"]["uid"] == "admission-1"
    assert response["response"]["allowed"] is True


class EvictionAPI(AdmissionAPI):
    def __init__(self) -> None:
        workload = _workload()
        workload["metadata"]["namespace"] = "fleetright-v2"
        workload["spec"].update(
            executionProtocol="monotonic-scale-up-v1",
            replicas=1,
            availabilityFloor=1,
            surgeBudget=1,
            disruption={"maxConcurrentDisruptions": 1},
        )
        workload["spec"]["placement"]["allowedNodes"] = [
            {"name": "worker-a", "uid": "node-a-uid"},
            {"name": "worker-b", "uid": "node-b-uid"},
        ]
        workload["spec"]["placement"]["requirePodAntiAffinity"] = True
        source_intent = workload["status"].pop("pendingPodCreate")
        self.source = _pod_object(workload, 0, source_intent)
        self.source["metadata"].update(uid="source-uid", resourceVersion="3")
        self.source["spec"]["nodeName"] = "worker-a"
        self.source["status"] = {
            "phase": "Running",
            "conditions": [
                {
                    "type": "Ready",
                    "status": "True",
                    "lastTransitionTime": "2000-01-01T00:00:00Z",
                }
            ],
        }
        workload["metadata"].update(generation=2, resourceVersion="9")
        workload["spec"].update(
            replicas=2,
            executionRequest={
                "planID": "plan-1",
                "operationID": "operation-1",
                "generation": 41,
                "expectedReplicas": 1,
                "targetReplicas": 2,
                "purpose": "staged-replacement-v1",
                "minimumReadyReplicas": 1,
                "destinationNodes": [{"name": "worker-b", "uid": "node-b-uid"}],
            },
            executionFence={
                "planID": "plan-1",
                "operationID": "operation-1",
                "generation": 41,
                "expectedGeneration": 1,
                "expectedReplicas": 1,
                "fencedGeneration": 2,
            },
        )
        replacement_intent = {
            **source_intent,
            "workloadGeneration": 2,
            "podName": _pod_name("workload-uid", 1),
            "replicaID": "api-1",
            "createID": "replacement-create-id",
        }
        self.replacement = _pod_object(workload, 1, replacement_intent)
        self.replacement["metadata"].update(uid="replacement-uid", resourceVersion="4")
        self.replacement["spec"]["nodeName"] = "worker-b"
        self.replacement["status"] = deepcopy(self.source["status"])
        workload["status"].update(
            observedGeneration=2,
            replicas=2,
            readyReplicas=2,
            availableReplicas=2,
            unverifiedPodCreates=0,
            execution={
                "planID": "plan-1",
                "operationID": "operation-1",
                "generation": 41,
                "targetReplicas": 2,
                "phase": "APPLIED",
                "purpose": "staged-replacement-v1",
            },
            authorizedPodCreates=[
                {**source_intent, "podUID": "source-uid"},
                {**replacement_intent, "podUID": "replacement-uid"},
            ],
            retiredReplicaSlots=[
                {
                    "workloadUID": "workload-uid",
                    "replicaID": "api-0",
                    "podUID": "source-uid",
                    "workloadGeneration": 1,
                    "planID": "plan-1",
                    "operationID": "operation-1",
                    "sourceNodeName": "worker-a",
                    "sourceNodeUID": "node-a-uid",
                    "replacementPodUID": "replacement-uid",
                    "replacementPodName": replacement_intent["podName"],
                    "replacementReplicaID": "api-1",
                    "replacementNodeName": "worker-b",
                    "replacementNodeUID": "node-b-uid",
                    "replacementCreateID": "replacement-create-id",
                    "replacementWorkloadGeneration": 2,
                    "readinessStabilizationMs": 0,
                }
            ],
        )
        self.pdb = _pdb_object(workload)
        self.pdb["metadata"].update(namespace="fleetright-v2", uid="pdb-uid", generation=5)
        self.pdb["status"] = {
            "observedGeneration": 5,
            "desiredHealthy": 1,
            "currentHealthy": 2,
            "expectedPods": 2,
            "disruptionsAllowed": 1,
            "disruptedPods": {},
        }
        self.nodes = [
            {
                "metadata": {"name": name, "uid": uid},
                "status": {"conditions": [{"type": "Ready", "status": "True"}]},
            }
            for name, uid in (("worker-a", "node-a-uid"), ("worker-b", "node-b-uid"))
        ]
        super().__init__(workload)
        self.reads: list[str] = []
        self.change_during_read = False

    def request(self, method: str, path: str) -> dict[str, Any]:
        assert method == "GET"
        self.reads.append(path)
        if path.endswith("/fleetrightworkloads/api"):
            if (
                self.change_during_read
                and sum("/fleetrightworkloads/" in p for p in self.reads) == 2
            ):
                self.workload["metadata"]["resourceVersion"] = "10"
                self.workload["spec"]["executionFence"]["generation"] = 42
            return deepcopy(self.workload)
        if path.endswith(f"/pods/{self.source['metadata']['name']}"):
            return deepcopy(self.source)
        if path.endswith(f"/pods/{self.replacement['metadata']['name']}"):
            return deepcopy(self.replacement)
        if path.endswith("/poddisruptionbudgets/api"):
            return deepcopy(self.pdb)
        for node in self.nodes:
            if path.endswith(f"/nodes/{node['metadata']['name']}"):
                return deepcopy(node)
        raise ApiError(404, "evidence not found")


def _eviction_request(api: EvictionAPI) -> dict[str, Any]:
    return {
        "uid": "eviction-admission-uid",
        "namespace": "fleetright-v2",
        "name": api.source["metadata"]["name"],
        "operation": "CREATE",
        "subResource": "eviction",
        "resource": {"group": "", "version": "v1", "resource": "pods"},
        "userInfo": {"username": EXECUTOR_USERNAME},
        "object": {
            "apiVersion": "policy/v1",
            "kind": "Eviction",
            "metadata": {
                "name": api.source["metadata"]["name"],
                "namespace": "fleetright-v2",
                "annotations": {
                    "fleetright.io/plan-id": "plan-1",
                    "fleetright.io/operation-id": "operation-1",
                    "fleetright.io/fencing-token": "41",
                    "fleetright.io/workload-uid": "workload-uid",
                    "fleetright.io/replacement-pod-uid": "replacement-uid",
                    "fleetright.io/replacement-node-uid": "node-b-uid",
                },
            },
            "deleteOptions": {"preconditions": {"uid": "source-uid", "resourceVersion": "3"}},
        },
    }


def test_eviction_requires_current_authority_bound_ready_replacement_and_fresh_pdb() -> None:
    api = EvictionAPI()
    admission = PodCreateAdmission(api, "fleetright-v2", "fleetright-workload-controller")

    assert admission.admit(_eviction_request(api)) == (True, "")
    assert len(api.reads) == 7
    assert api.status_patches == 0


def test_eviction_admission_requires_replacement_readiness_to_remain_stable() -> None:
    api = EvictionAPI()
    api.workload["status"]["retiredReplicaSlots"][0]["readinessStabilizationMs"] = 60_000
    api.replacement["status"]["conditions"][0]["lastTransitionTime"] = (
        datetime.now(UTC).isoformat().replace("+00:00", "Z")
    )
    admission = PodCreateAdmission(api, "fleetright-v2", "fleetright-workload-controller")

    allowed, message = admission.admit(_eviction_request(api))

    assert allowed is False
    assert "readiness" in message


@pytest.mark.parametrize(
    ("target", "path", "value"),
    [
        ("request", ("userInfo", "username"), "other-user"),
        ("request", ("object", "deleteOptions", "preconditions", "uid"), "other-source-uid"),
        ("request", ("object", "deleteOptions", "preconditions", "uid"), None),
        ("request", ("object", "deleteOptions", "preconditions", "resourceVersion"), "2"),
        ("request", ("object", "deleteOptions", "preconditions", "resourceVersion"), None),
        ("request", ("object", "metadata", "annotations", "fleetright.io/plan-id"), "other-plan"),
        (
            "request",
            ("object", "metadata", "annotations", "fleetright.io/operation-id"),
            "other-operation",
        ),
        ("request", ("object", "metadata", "annotations", "fleetright.io/fencing-token"), "40"),
        ("request", ("object", "metadata", "annotations", "fleetright.io/fencing-token"), "041"),
        (
            "request",
            ("object", "metadata", "annotations", "fleetright.io/workload-uid"),
            "old-workload-uid",
        ),
        (
            "request",
            ("object", "metadata", "annotations", "fleetright.io/replacement-pod-uid"),
            "other-replacement-uid",
        ),
        ("workload", ("metadata", "uid"), "new-workload-uid"),
        ("workload", ("spec", "podCreateAdmissionProtocol"), None),
        ("workload", ("spec", "executionFence", "generation"), 42),
        ("workload", ("spec", "executionRequest", "operationID"), "other-operation"),
        ("workload", ("status", "pendingPodCreate"), {"createID": "unknown-create"}),
        ("workload", ("status", "podCreateHistoryUncertain"), True),
        ("workload", ("status", "execution", "phase"), "RECONCILING"),
        ("workload", ("status", "retiredReplicaSlots"), []),
        ("workload", ("status", "retiredReplicaSlots", 0, "sourceNodeUID"), "old-source-node-uid"),
        (
            "workload",
            ("status", "retiredReplicaSlots", 0, "replacementCreateID"),
            "wrong-create-id",
        ),
        ("workload", ("status", "retiredReplicaSlots", 0, "replacementWorkloadGeneration"), 1),
        ("workload", ("status", "retiredReplicaSlots", 0, "readinessStabilizationMs"), None),
        ("workload", ("spec", "executionRequest", "minimumReadyReplicas"), 2),
        ("source", ("metadata", "uid"), "recreated-source-uid"),
        ("source", ("spec", "nodeName"), "worker-b"),
        ("replacement", ("metadata", "uid"), "recreated-replacement-uid"),
        ("replacement", ("metadata", "deletionTimestamp"), "2026-10-11T12:00:00Z"),
        ("replacement", ("spec", "nodeName"), "worker-a"),
        ("replacement", ("status", "conditions", 0, "status"), "False"),
        ("replacement", ("metadata", "ownerReferences", 0, "uid"), "old-workload-uid"),
        ("node", ("metadata", "uid"), "recreated-node-uid"),
        ("node", ("status", "conditions", 0, "status"), "False"),
        ("pdb", ("status", "observedGeneration"), 4),
        ("pdb", ("metadata", "annotations", "fleetright.io/workload-generation"), "1"),
        ("pdb", ("metadata", "ownerReferences", 0, "uid"), "old-workload-uid"),
        ("pdb", ("spec", "maxUnavailable"), 2),
        ("pdb", ("spec", "unhealthyPodEvictionPolicy"), "AlwaysAllow"),
        ("pdb", ("status", "desiredHealthy"), 0),
        ("pdb", ("status", "currentHealthy"), 0),
        ("pdb", ("status", "disruptionsAllowed"), 0),
        ("pdb", ("status", "expectedPods"), 1),
        ("pdb", ("status", "disruptedPods"), {"other-pod": "2026-10-11T12:00:00Z"}),
    ],
)
def test_eviction_rejects_stale_or_invalid_evidence(
    target: str, path: tuple[str | int, ...], value: object
) -> None:
    api = EvictionAPI()
    request = _eviction_request(api)
    objects = {
        "request": request,
        "workload": api.workload,
        "source": api.source,
        "replacement": api.replacement,
        "node": api.nodes[1],
        "pdb": api.pdb,
    }
    item = objects[target]
    for key in path[:-1]:
        item = item[key]
    item[path[-1]] = value
    admission = PodCreateAdmission(api, "fleetright-v2", "fleetright-workload-controller")

    allowed, message = admission.admit(request)

    assert allowed is False
    assert message
    assert api.status_patches == 0


def test_unready_source_eviction_still_preserves_the_effective_pdb_floor() -> None:
    api = EvictionAPI()
    api.source["status"]["conditions"][0]["status"] = "False"
    api.pdb["status"].update(currentHealthy=1, disruptionsAllowed=0)
    admission = PodCreateAdmission(api, "fleetright-v2", "fleetright-workload-controller")

    assert admission.admit(_eviction_request(api)) == (True, "")
    api.pdb["status"]["currentHealthy"] = 0
    assert admission.admit(_eviction_request(api))[0] is False


def test_eviction_requires_the_verified_hard_anti_affinity_contract() -> None:
    api = EvictionAPI()
    api.workload["spec"]["placement"]["requirePodAntiAffinity"] = False
    admission = PodCreateAdmission(api, "fleetright-v2", "fleetright-workload-controller")

    allowed, message = admission.admit(_eviction_request(api))

    assert allowed is False
    assert message == "Eviction has no current workload execution authority"
    assert api.status_patches == 0


def test_eviction_rejects_a_fence_change_during_evidence_reads() -> None:
    api = EvictionAPI()
    api.change_during_read = True
    admission = PodCreateAdmission(api, "fleetright-v2", "fleetright-workload-controller")

    assert admission.admit(_eviction_request(api)) == (
        False,
        "Eviction workload authority changed during admission",
    )


def test_eviction_admission_cannot_revoke_a_request_already_allowed() -> None:
    api = EvictionAPI()
    request = _eviction_request(api)
    admission = PodCreateAdmission(api, "fleetright-v2", "fleetright-workload-controller")
    admitted = admission.admit(request)
    assert admitted == (True, "")
    api.workload["spec"]["executionFence"]["generation"] = 42
    api.workload["metadata"]["resourceVersion"] = "10"
    assert admission.admit(request)[0] is False
    # Storage checks the unchanged Pod UID/RV after the prior Allow, not the later FWW fence.
    assert (
        request["object"]["deleteOptions"]["preconditions"]["uid"] == api.source["metadata"]["uid"]
    )
    assert (
        request["object"]["deleteOptions"]["preconditions"]["resourceVersion"]
        == api.source["metadata"]["resourceVersion"]
    )
    api.source = None
    assert admitted[0] is True
    assert api.source is None


def _binding_admission(monkeypatch: pytest.MonkeyPatch):
    node = {
        "metadata": {"name": "worker-b", "uid": "node-b-uid", "resourceVersion": "21"},
        "spec": {},
    }
    pod = {
        "metadata": {
            "name": "api-pending",
            "namespace": "fleetright-v2",
            "uid": "pod-uid",
            "resourceVersion": "11",
        },
        "spec": {},
    }
    request = {
        "uid": "binding-admission-1",
        "operation": "CREATE",
        "resource": {"group": "", "version": "v1", "resource": "pods"},
        "subResource": "binding",
        "namespace": "fleetright-v2",
        "name": "api-pending",
        "object": {
            "apiVersion": "v1",
            "kind": "Binding",
            "metadata": {
                "name": "api-pending",
                "namespace": "fleetright-v2",
                "uid": "pod-uid",
            },
            "target": {"kind": "Node", "name": "worker-b"},
        },
    }
    api = AdmissionAPI(_workload())
    api.pod = pod

    def get_evidence(method: str, path: str):
        assert method == "GET"
        if path == "/api/v1/nodes/worker-b":
            return deepcopy(node)
        assert path == "/api/v1/namespaces/fleetright-v2/pods/api-pending"
        return deepcopy(pod)

    monkeypatch.setattr(api, "request", get_evidence)
    admission = PodCreateAdmission(api, "fleetright-v2", "fleetright-workload-controller")
    return admission, node, request, api


@pytest.mark.parametrize(
    "annotations",
    [
        {"fleetright.io/plan-id": "plan-1", "fleetright.io/operation-id": "drain-1"},
        {"fleetright.io/plan-id": "plan-1"},
    ],
)
def test_binding_to_fleetright_draining_node_is_denied(monkeypatch, annotations):
    admission, node, request, api = _binding_admission(monkeypatch)
    node["metadata"]["annotations"] = annotations
    assert admission.admit(request)[0] is False
    assert api.status_patches == 0


def test_binding_ignores_released_fencing_token_high_water_mark(monkeypatch):
    admission, node, request, api = _binding_admission(monkeypatch)
    node["metadata"]["annotations"] = {"fleetright.io/fencing-token": "3"}
    assert admission.admit(request) == (True, "")


def test_binding_to_other_schedulable_destination_is_allowed(monkeypatch):
    admission, node, request, api = _binding_admission(monkeypatch)
    node["metadata"]["annotations"] = {"other-controller.example/keep": "yes"}
    assert admission.admit(request) == (True, "")
    assert api.status_patches == 0


def test_binding_allowed_before_cordon_cannot_be_retroactively_revoked(monkeypatch):
    admission, node, request, _ = _binding_admission(monkeypatch)
    accepted = admission.admit(request)
    node["spec"]["unschedulable"] = True
    assert accepted == (True, "")
    assert admission.admit(request)[0] is False


def test_binding_uid_precondition_rejects_a_recreated_pod(monkeypatch):
    admission, _, request, api = _binding_admission(monkeypatch)
    api.pod["metadata"]["uid"] = "replacement-pod-uid"

    assert admission.admit(request) == (
        False,
        "Binding Pod is missing, changed, deleting, or already assigned",
    )


def test_binding_for_a_missing_pod_fails_closed(monkeypatch):
    admission, _, request, api = _binding_admission(monkeypatch)
    get_evidence = api.request

    def missing_pod(method, path):
        if path.endswith("/pods/api-pending"):
            raise ApiError(404, "missing")
        return get_evidence(method, path)

    monkeypatch.setattr(api, "request", missing_pod)

    assert admission.admit(request) == (
        False,
        "Node binding evidence is unavailable or invalid",
    )


def test_binding_requires_live_unbound_pod(monkeypatch):
    admission, _, request, api = _binding_admission(monkeypatch)
    api.pod["metadata"]["deletionTimestamp"] = "2026-10-12T00:00:00Z"

    assert admission.admit(request)[0] is False


@pytest.mark.parametrize(
    ("object_name", "path", "value"),
    [
        ("node", ("metadata", "name"), "different-node"),
        ("node", ("metadata", "uid"), None),
        ("node", ("metadata", "resourceVersion"), ""),
        ("node", ("metadata", "annotations"), []),
        ("node", ("metadata", "deletionTimestamp"), "2026-10-12T00:00:00Z"),
        ("node", ("spec",), None),
        ("node", ("spec", "unschedulable"), "false"),
        ("request", ("object", "target", "uid"), "old-node-uid"),
        ("request", ("object", "target", "kind"), "Pod"),
        ("request", ("object", "target", "apiVersion"), "apps/v1"),
        ("request", ("object", "metadata", "name"), "different-pod"),
        ("request", ("object", "metadata", "uid"), None),
        ("request", ("namespace",), "other-namespace"),
    ],
)
def test_binding_invalid_identity_or_node_evidence_fails_closed(
    monkeypatch, object_name, path, value
):
    admission, node, request, _ = _binding_admission(monkeypatch)
    item = {"node": node, "request": request}[object_name]
    for key in path[:-1]:
        item = item[key]
    item[path[-1]] = value
    allowed, message = admission.admit(request)
    assert allowed is False
    assert message


@pytest.mark.parametrize(
    "error", [ApiError(404, "missing"), ApiError(403, "denied"), RuntimeError("timeout")]
)
def test_binding_missing_or_unavailable_node_fails_closed(monkeypatch, error):
    admission, _, request, api = _binding_admission(monkeypatch)

    def unavailable_node(method, path):
        raise error

    monkeypatch.setattr(api, "request", unavailable_node)
    assert admission.admit(request) == (False, "Node binding evidence is unavailable or invalid")


def test_eviction_webhook_and_rbac_keep_direct_pod_delete_denied() -> None:
    root = Path(__file__).resolve().parents[2]
    configuration = yaml.safe_load(
        (root / "deploy/kubernetes/fleetright-pod-admission-configuration.yaml").read_text()
    )
    webhook = configuration["webhooks"][0]
    assert webhook["failurePolicy"] == "Fail"
    assert any(
        rule["apiGroups"] == [""]
        and rule["operations"] == ["CREATE"]
        and "pods/eviction" in rule["resources"]
        for rule in webhook["rules"]
    )
    assert any(
        rule["apiGroups"] == [""]
        and rule["operations"] == ["CREATE"]
        and "pods/binding" in rule["resources"]
        for rule in webhook["rules"]
    )
    roles = list(yaml.safe_load_all((root / "deploy/kubernetes/rbac.yaml").read_text()))
    executor = next(
        role
        for role in roles
        if role.get("kind") == "Role" and role["metadata"]["name"] == "fleetright-executor"
    )
    assert executor["metadata"]["namespace"] == "fleetright-v2"
    assert {"apiGroups": [""], "resources": ["pods/eviction"], "verbs": ["create"]} in executor[
        "rules"
    ]
    assert all(
        "delete" not in rule["verbs"] and "*" not in rule["verbs"] for rule in executor["rules"]
    )
    webhook_roles = list(
        yaml.safe_load_all(
            (root / "deploy/kubernetes/fleetright-pod-admission-webhook.yaml").read_text()
        )
    )
    role = next(role for role in webhook_roles if role.get("kind") == "Role")
    assert {"apiGroups": [""], "resources": ["pods"], "verbs": ["get"]} in role["rules"]
    assert {
        "apiGroups": ["policy"],
        "resources": ["poddisruptionbudgets"],
        "verbs": ["get"],
    } in role["rules"]
    node_reader = next(role for role in webhook_roles if role.get("kind") == "ClusterRole")
    assert node_reader["rules"] == [{"apiGroups": [""], "resources": ["nodes"], "verbs": ["get"]}]
