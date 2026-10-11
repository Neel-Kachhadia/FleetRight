from __future__ import annotations

import json
import logging
import os
import re
import ssl
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import quote, urlsplit

from fleetright.workload_controller import (
    API_VERSION,
    GROUP_VERSION_PATH,
    POD_CREATE_ADMISSION_PROTOCOL,
    POD_CREATE_ATTEMPT_ANNOTATION,
    POD_CREATE_OPERATION_ANNOTATION,
    POD_CREATE_PLAN_ANNOTATION,
    REPLICA_ID_LABEL,
    WORKLOAD_GENERATION_ANNOTATION,
    WORKLOAD_UID_LABEL,
    ApiError,
    KubernetesAPI,
    SafetyError,
    _has_create_authorization,
    _pdb_object,
    _pod_object,
    _pod_ready,
    _validate_scheduled_pod,
)

LOG = logging.getLogger(__name__)
PATH = "/validate-pod-create"
EXECUTOR_USERNAME = "system:serviceaccount:fleetright:fleetright-controller"
_FENCING_TOKEN = re.compile(r"[1-9][0-9]{0,18}\Z")
_POD_DEFAULTS = {
    "dnsPolicy": "ClusterFirst",
    "enableServiceLinks": True,
    "nodeName": "",
    "preemptionPolicy": "PreemptLowerPriority",
    "priority": 0,
    "schedulerName": "default-scheduler",
    "serviceAccount": "default",
    "serviceAccountName": "default",
    "terminationGracePeriodSeconds": 30,
    "tolerations": [
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
}
_CONTAINER_DEFAULTS = {
    "terminationMessagePath": "/dev/termination-log",
    "terminationMessagePolicy": "File",
}
_POD_STATUS_DEFAULTS = {
    "phase": {"", "Pending"},
    "conditions": [],
    "message": "",
    "reason": "",
    "nominatedNodeName": "",
    "hostIP": "",
    "hostIPs": [],
    "podIP": "",
    "podIPs": [],
    "startTime": None,
    "containerStatuses": [],
    "initContainerStatuses": [],
    "ephemeralContainerStatuses": [],
}
_POD_METADATA_DEFAULTS = {
    "creationTimestamp": None,
    "resourceVersion": "",
    "uid": "",
    "generation": 0,
    "managedFields": [],
    "selfLink": "",
    "deletionTimestamp": None,
    "deletionGracePeriodSeconds": None,
}
_OTHER_POD_STATUS_FIELDS = {"qosClass", "resize", "resourceClaimStatuses"}
_DIAGNOSTIC_SPEC_FIELDS = frozenset(
    "affinity allowPrivilegeEscalation automountServiceAccountToken capabilities command "
    "containerPort containers cpu dnsPolicy drop enableServiceLinks env failureThreshold "
    "fsGroup fsGroupChangePolicy host hostNetwork httpGet image imagePullPolicy initContainers "
    "initialDelaySeconds key labelSelector limits livenessProbe matchExpressions matchFields "
    "matchLabels maxSkew memory metadata.name minDomains name nodeAffinity nodeName "
    "nodeSelectorTerms operator path periodSeconds podAffinity podAntiAffinity port ports "
    "preemptionPolicy priority priorityClassName protocol readinessProbe requests "
    "requiredDuringSchedulingIgnoredDuringExecution resources restartPolicy runAsGroup "
    "runAsNonRoot runAsUser schedulerName scheme seccompProfile securityContext serviceAccount "
    "serviceAccountName startupProbe successThreshold terminationGracePeriodSeconds "
    "terminationMessagePath terminationMessagePolicy timeoutSeconds tolerations "
    "topologyKey topologySpreadConstraints type value values whenUnsatisfiable".split()
)


def _pod_spec_mismatch_paths(actual: object, expected: object) -> str:
    paths: list[str] = []

    def visit(actual: object, expected: object, path: str, depth: int) -> None:
        if actual == expected or len(paths) >= 8:
            return
        if depth < 12 and isinstance(actual, dict) and isinstance(expected, dict):
            for key in sorted(actual.keys() | expected.keys()):
                field = key if key in _DIAGNOSTIC_SPEC_FIELDS else "<unknown-field>"
                child = f"{path}.{field}"
                if key not in actual or key not in expected:
                    if child not in paths:
                        paths.append(child)
                else:
                    visit(actual[key], expected[key], child, depth + 1)
                if len(paths) >= 8:
                    break
        elif depth < 12 and isinstance(actual, list) and isinstance(expected, list):
            for index in range(max(len(actual), len(expected))):
                child = f"{path}[{index}]"
                if index >= len(actual) or index >= len(expected):
                    paths.append(child)
                else:
                    visit(actual[index], expected[index], child, depth + 1)
                if len(paths) >= 8:
                    break
        elif path not in paths:
            paths.append(path)

    visit(actual, expected, "spec", 0)
    return ",".join(paths)


def _default_pull_policy(image: object) -> str | None:
    if not isinstance(image, str) or not image:
        return None
    name = image.split("@", 1)[0].rsplit("/", 1)[-1]
    tag = name.rsplit(":", 1)[1] if ":" in name else "latest"
    return "Always" if tag == "latest" else "IfNotPresent"


def _pod_qos_class(pod_spec: dict[str, Any]) -> str:
    containers = [*pod_spec.get("initContainers", []), *pod_spec.get("containers", [])]
    best_effort, guaranteed = True, bool(containers)
    for container in containers:
        resources = container.get("resources", {})
        requests = resources.get("requests", {})
        limits = resources.get("limits", {})
        for name in ("cpu", "memory"):
            request, limit = requests.get(name), limits.get(name)
            best_effort &= request is None and limit is None
            guaranteed &= request is not None and request == limit
    return "BestEffort" if best_effort else "Guaranteed" if guaranteed else "Burstable"


def _without_apiserver_defaults(
    pod_spec: dict[str, Any], expected_spec: dict[str, Any]
) -> dict[str, Any]:
    result = dict(pod_spec)
    for key, value in _POD_DEFAULTS.items():
        if (
            key not in expected_spec
            and type(result.get(key)) is type(value)
            and result.get(key) == value
        ):
            result.pop(key)
    containers = result.get("containers")
    expected_containers = expected_spec.get("containers")
    if isinstance(containers, list) and isinstance(expected_containers, list):
        normalized = []
        for index, actual in enumerate(containers):
            expected = expected_containers[index] if index < len(expected_containers) else None
            if not isinstance(actual, dict) or not isinstance(expected, dict):
                normalized.append(actual)
                continue
            item = dict(actual)
            defaults = {
                **_CONTAINER_DEFAULTS,
                "imagePullPolicy": _default_pull_policy(expected.get("image")),
            }
            for key, value in defaults.items():
                if value is not None and key not in expected and item.get(key) == value:
                    item.pop(key)
            for probe_name in ("readinessProbe", "livenessProbe"):
                probe, expected_probe = item.get(probe_name), expected.get(probe_name)
                if not isinstance(probe, dict) or not isinstance(expected_probe, dict):
                    continue
                probe = dict(probe)
                if (
                    "successThreshold" not in expected_probe
                    and type(probe.get("successThreshold")) is int
                    and probe.get("successThreshold") == 1
                ):
                    probe.pop("successThreshold")
                http_get, expected_http_get = probe.get("httpGet"), expected_probe.get("httpGet")
                if isinstance(http_get, dict) and isinstance(expected_http_get, dict):
                    http_get = dict(http_get)
                    if "scheme" not in expected_http_get and http_get.get("scheme") == "HTTP":
                        http_get.pop("scheme")
                    probe["httpGet"] = http_get
                item[probe_name] = probe
            normalized.append(item)
        result["containers"] = normalized
    return result


class PodCreateAdmission:
    def __init__(
        self,
        api: KubernetesAPI,
        namespace: str,
        service_account: str,
        *,
        executor_username: str = EXECUTOR_USERNAME,
    ):
        self.api = api
        self.namespace = namespace
        self.controller_username = f"system:serviceaccount:{namespace}:{service_account}"
        self.executor_username = executor_username

    def admit(self, request: dict[str, Any]) -> tuple[bool, str]:
        if request.get("subResource") == "eviction":
            try:
                return self._admit_eviction(request)
            except (
                ApiError,
                RuntimeError,
                SafetyError,
                AttributeError,
                KeyError,
                TypeError,
                ValueError,
            ):
                return False, "FleetRight eviction evidence is unavailable or invalid"
        if request.get("dryRun") is True:
            return False, "FLEETRIGHT_POD_ADMISSION_PROBE_REJECTED"
        pod = request.get("object")
        if not isinstance(pod, dict):
            return False, "Pod CREATE has no object"
        metadata = pod.get("metadata")
        metadata = metadata if isinstance(metadata, dict) else {}
        owner_refs = metadata.get("ownerReferences", [])
        labels = metadata.get("labels", {})
        annotations = metadata.get("annotations", {})
        pod_spec = pod.get("spec", {})
        if not all(
            (
                isinstance(owner_refs, list),
                isinstance(labels, dict),
                isinstance(annotations, dict),
                isinstance(pod_spec, dict),
            )
        ):
            return False, "Pod identity or spec is invalid"
        is_workload_pod = (
            isinstance(owner_refs, list)
            and any(
                isinstance(owner, dict) and owner.get("kind") == "FleetRightWorkload"
                for owner in owner_refs
            )
        ) or (isinstance(labels, dict) and WORKLOAD_UID_LABEL in labels)
        user = request.get("userInfo", {})
        username = user.get("username") if isinstance(user, dict) else None
        if username != self.controller_username:
            return (
                (False, "only the FleetRight workload controller may create owned Pods")
                if is_workload_pod
                else (True, "")
            )
        if request.get("namespace") != self.namespace or not is_workload_pod:
            return False, "FleetRight controller may only create reserved FleetRightWorkload Pods"

        owner = next(
            (
                item
                for item in owner_refs
                if isinstance(item, dict) and item.get("kind") == "FleetRightWorkload"
            ),
            None,
        )
        if (
            not isinstance(owner, dict)
            or owner.get("apiVersion") != API_VERSION
            or owner.get("controller") is not True
            or not all(isinstance(owner.get(key), str) and owner[key] for key in ("name", "uid"))
        ):
            return False, "FleetRight Pod owner identity is invalid"

        workload_name = owner["name"]
        try:
            workload = self.api.request(
                "GET",
                f"{GROUP_VERSION_PATH}/namespaces/{self.namespace}/"
                f"fleetrightworkloads/{workload_name}",
            )
        except ApiError as error:
            return (
                False,
                "FleetRightWorkload owner is unavailable"
                if error.status == 404
                else "FleetRightWorkload lookup failed",
            )
        spec = workload.get("spec", {})
        status = workload.get("status", {})
        pending = status.get("pendingPodCreate") if isinstance(status, dict) else None
        workload_metadata = workload.get("metadata", {})
        pod_name = metadata.get("name")
        replica_id = labels.get(REPLICA_ID_LABEL) if isinstance(labels, dict) else None
        if not isinstance(spec, dict) or not isinstance(workload_metadata, dict):
            return False, "FleetRightWorkload identity is invalid"
        if (
            workload_metadata.get("uid") != owner["uid"]
            or spec.get("podCreateProtocol") != "reserved-create-v1"
            or spec.get("podCreateAdmissionProtocol") != POD_CREATE_ADMISSION_PROTOCOL
            or not isinstance(pending, dict)
            or pending.get("admissionUID") is not None
            or pending.get("workloadUID") != owner["uid"]
            or pending.get("workloadGeneration") != workload_metadata.get("generation")
            or pending.get("podName") != pod_name
            or pending.get("replicaID") != replica_id
            or pending.get("planID") != annotations.get(POD_CREATE_PLAN_ANNOTATION)
            or pending.get("operationID") != annotations.get(POD_CREATE_OPERATION_ANNOTATION)
            or pending.get("createID") != annotations.get(POD_CREATE_ATTEMPT_ANNOTATION)
            or spec.get("workloadID") != workload_name
        ):
            return False, "Pod CREATE has no current durable reservation"

        try:
            ordinal_text = replica_id.rsplit("-", 1)[1]
            if (
                not ordinal_text.isascii()
                or not ordinal_text.isdecimal()
                or str(int(ordinal_text)) != ordinal_text
            ):
                return False, "Pod replica identity is invalid"
            expected = _pod_object(workload, int(ordinal_text), pending)
        except (AttributeError, IndexError, KeyError, TypeError, ValueError):
            return False, "Pod CREATE reservation is invalid"
        expected_metadata = expected["metadata"]
        metadata = dict(metadata)
        # Kubernetes fills these fields before admission; they cannot change the reserved Pod spec.
        if "creationTimestamp" in metadata:
            timestamp = metadata["creationTimestamp"]
            if timestamp is not None:
                try:
                    parsed_timestamp = (
                        datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                        if isinstance(timestamp, str)
                        else None
                    )
                except ValueError:
                    parsed_timestamp = None
                if parsed_timestamp is None or parsed_timestamp.utcoffset() is None:
                    return False, "Pod creation timestamp is invalid"
            metadata.pop("creationTimestamp")
        if "generation" in metadata:
            generation = metadata["generation"]
            if type(generation) is not int or generation not in {0, 1}:
                return False, "Pod generation is invalid"
            metadata.pop("generation")
        if "uid" in metadata:
            pod_uid = metadata["uid"]
            if not isinstance(pod_uid, str) or len(pod_uid) > 128:
                return False, "Pod UID is invalid"
            metadata.pop("uid")
        if "managedFields" in metadata:
            managed_fields = metadata["managedFields"]
            if not isinstance(managed_fields, list) or len(managed_fields) > 32:
                return False, "Pod managed field metadata is invalid"
            metadata.pop("managedFields")
        for key, value in _POD_METADATA_DEFAULTS.items():
            if key in metadata and metadata[key] == value:
                metadata.pop(key)
        metadata_extras = set(metadata) - {
            "name",
            "namespace",
            "labels",
            "annotations",
            "ownerReferences",
        }
        known_metadata_extras = ",".join(
            sorted(key for key in metadata_extras if key in _POD_METADATA_DEFAULTS)
        )
        unknown_metadata_extras = len(metadata_extras - _POD_METADATA_DEFAULTS.keys())
        pod_status = pod.get("status")
        status_is_default = pod_status is None or (
            isinstance(pod_status, dict)
            and all(
                (
                    value == _pod_qos_class(expected["spec"])
                    if key == "qosClass"
                    else key in _POD_STATUS_DEFAULTS
                    and (
                        isinstance(value, str) and value in _POD_STATUS_DEFAULTS[key]
                        if key == "phase"
                        else _POD_STATUS_DEFAULTS[key] == value
                    )
                )
                for key, value in pod_status.items()
            )
        )
        if set(pod) == {"apiVersion", "kind", "metadata", "spec", "status"} and status_is_default:
            pod = {key: value for key, value in pod.items() if key != "status"}
        normalized_spec = _without_apiserver_defaults(pod_spec, expected["spec"])
        normalized_expected = _without_apiserver_defaults(expected["spec"], expected["spec"])
        known_object_fields = {"apiVersion", "kind", "metadata", "spec", "status"}
        known_status_fields = (
            ",".join(sorted(pod_status.keys() & _POD_STATUS_DEFAULTS.keys()))
            if isinstance(pod_status, dict)
            else "not-an-object"
        )
        other_status_fields = (
            ",".join(sorted(pod_status.keys() & _OTHER_POD_STATUS_FIELDS))
            if isinstance(pod_status, dict)
            else ""
        )
        mismatch = (
            "object-shape: "
            f"fields={','.join(sorted(pod.keys() & known_object_fields))};"
            f"status-fields={known_status_fields};other-status-fields={other_status_fields}"
            if set(pod) != {"apiVersion", "kind", "metadata", "spec"}
            else (
                f"metadata-shape: extras={known_metadata_extras};"
                f"other-count={unknown_metadata_extras};"
                f"namespace-ok={metadata.get('namespace') in (None, self.namespace)};"
                f"name-ok={pod_name == expected_metadata['name']}"
            )
            if metadata_extras
            or metadata.get("namespace") not in (None, self.namespace)
            or pod_name != expected_metadata["name"]
            else "labels"
            if labels != expected_metadata["labels"]
            else "annotations"
            if annotations != expected_metadata["annotations"]
            else "owner-reference"
            if owner_refs != expected_metadata["ownerReferences"]
            else "scheduler-placement"
            if pod_spec.get("nodeName") not in (None, "")
            or pod_spec.get("schedulerName") not in (None, "", "default-scheduler")
            else (
                "pod-spec: fields=" + _pod_spec_mismatch_paths(normalized_spec, normalized_expected)
            )
            if normalized_spec != normalized_expected
            else None
        )
        if mismatch:
            return False, f"Pod reservation mismatch: {mismatch}"

        admission_uid = request.get("uid")
        resource_version = workload_metadata.get("resourceVersion")
        if (
            not isinstance(admission_uid, str)
            or not admission_uid
            or not isinstance(resource_version, str)
        ):
            return False, "Admission request identity or workload resourceVersion is missing"
        status = dict(status)
        status["pendingPodCreate"] = {**pending, "admissionUID": admission_uid}
        try:
            claimed = self.api.patch_status(workload_name, resource_version, status)
        except ApiError:
            return False, "Pod CREATE reservation was concurrently claimed"
        claimed_status = claimed.get("status", {}) if isinstance(claimed, dict) else {}
        claimed_pending = (
            claimed_status.get("pendingPodCreate") if isinstance(claimed_status, dict) else None
        )
        if (
            not isinstance(claimed_pending, dict)
            or claimed_pending.get("admissionUID") != admission_uid
        ):
            return False, "Pod CREATE reservation claim was not durable"
        return True, ""

    def _admit_eviction(self, request: dict[str, Any]) -> tuple[bool, str]:
        eviction = request.get("object")
        resource = request.get("resource")
        if (
            request.get("operation") != "CREATE"
            or resource != {"group": "", "version": "v1", "resource": "pods"}
            or request.get("namespace") != self.namespace
            or not isinstance(eviction, dict)
            or eviction.get("apiVersion") != "policy/v1"
            or eviction.get("kind") != "Eviction"
        ):
            return False, "Eviction request type or namespace is invalid"
        metadata = eviction.get("metadata")
        if (
            not isinstance(metadata, dict)
            or not isinstance(metadata.get("name"), str)
            or not metadata["name"]
            or request.get("name") != metadata["name"]
            or metadata.get("namespace") not in (None, self.namespace)
        ):
            return False, "Eviction source name is invalid"
        source = self.api.request(
            "GET", f"/api/v1/namespaces/{self.namespace}/pods/{quote(metadata['name'], safe='')}"
        )
        source_metadata = source.get("metadata", {})
        owners = source_metadata.get("ownerReferences", [])
        owner = next(
            (
                item
                for item in owners
                if isinstance(item, dict) and item.get("kind") == "FleetRightWorkload"
            ),
            None,
        )
        user = request.get("userInfo", {})
        username = user.get("username") if isinstance(user, dict) else None
        if username != self.executor_username:
            labels = source_metadata.get("labels", {})
            if owner is not None or WORKLOAD_UID_LABEL in labels:
                return False, "only the FleetRight executor may evict owned Pods"
            return True, ""
        if not isinstance(owner, dict):
            return False, "FleetRight executor may only evict retired FleetRightWorkload Pods"
        annotations = metadata.get("annotations", {})
        options = eviction.get("deleteOptions", {})
        preconditions = options.get("preconditions", {})
        if (
            not isinstance(annotations, dict)
            or not isinstance(options, dict)
            or not isinstance(preconditions, dict)
            or not isinstance(preconditions.get("uid"), str)
            or not preconditions["uid"]
            or preconditions["uid"] != source_metadata.get("uid")
            or set(options) - {"preconditions", "dryRun"}
            or set(preconditions) - {"uid", "resourceVersion"}
            or not isinstance(preconditions.get("resourceVersion"), str)
            or not preconditions["resourceVersion"]
            or preconditions["resourceVersion"] != source_metadata.get("resourceVersion")
        ):
            return False, "Eviction requires exact source Pod UID and resourceVersion preconditions"
        plan_id = annotations.get("fleetright.io/plan-id")
        operation_id = annotations.get("fleetright.io/operation-id")
        token_text = annotations.get("fleetright.io/fencing-token")
        if (
            not isinstance(plan_id, str)
            or not plan_id
            or not isinstance(operation_id, str)
            or not operation_id
            or not isinstance(token_text, str)
            or not _FENCING_TOKEN.fullmatch(token_text)
            or int(token_text) > 2**63 - 1
        ):
            return False, "Eviction execution authority is invalid"
        token = int(token_text)
        workload = self.api.request(
            "GET",
            f"{GROUP_VERSION_PATH}/namespaces/{self.namespace}/"
            f"fleetrightworkloads/{quote(owner['name'], safe='')}",
        )
        workload_metadata, spec, status = (
            workload["metadata"],
            workload["spec"],
            workload.get("status", {}),
        )
        execution_request, fence, execution = (
            spec.get("executionRequest", {}),
            spec.get("executionFence", {}),
            status.get("execution", {}),
        )
        workload_uid, generation = workload_metadata.get("uid"), workload_metadata.get("generation")
        if (
            owner.get("apiVersion") != API_VERSION
            or owner.get("controller") is not True
            or owner.get("uid") != workload_uid
            or annotations.get("fleetright.io/workload-uid") != workload_uid
            or workload_metadata.get("namespace") != self.namespace
            or not isinstance(workload_metadata.get("resourceVersion"), str)
            or not workload_metadata["resourceVersion"]
            or spec.get("workloadID") != owner.get("name")
            or spec.get("executionProtocol") != "monotonic-scale-up-v1"
            or spec.get("podCreateProtocol") != "reserved-create-v1"
            or spec.get("podCreateAdmissionProtocol") != POD_CREATE_ADMISSION_PROTOCOL
            or type(generation) is not int
            or status.get("observedGeneration") != generation
            or status.get("pendingPodCreate") is not None
            or status.get("podCreateHistoryUncertain", False) is not False
            or status.get("unverifiedPodCreates", 0) != 0
            or type(status.get("unverifiedPodCreates", 0)) is not int
            or execution_request.get("purpose") != "staged-replacement-v1"
            or (execution_request.get("planID"), execution_request.get("operationID"))
            != (plan_id, operation_id)
            or type(execution_request.get("generation")) is not int
            or not 0 < execution_request["generation"] <= token
            or (fence.get("planID"), fence.get("operationID"), fence.get("generation"))
            != (plan_id, operation_id, token)
            or type(fence.get("generation")) is not int
            or fence.get("fencedGeneration") != generation
            or type(fence.get("fencedGeneration")) is not int
            or type(execution_request.get("expectedReplicas")) is not int
            or execution_request["expectedReplicas"] < 1
            or execution_request.get("targetReplicas") != execution_request["expectedReplicas"] + 1
            or execution_request.get("targetReplicas") != spec.get("replicas")
            or (execution.get("planID"), execution.get("operationID"), execution.get("generation"))
            != (plan_id, operation_id, execution_request["generation"])
            or execution.get("targetReplicas") != spec.get("replicas")
            or execution.get("phase") != "APPLIED"
            or not _has_create_authorization(source, workload)
            or source_metadata.get("deletionTimestamp") is not None
        ):
            return False, "Eviction has no current workload execution authority"
        records = [
            record
            for record in status.get("retiredReplicaSlots", [])
            if isinstance(record, dict) and record.get("podUID") == source_metadata["uid"]
        ]
        if len(records) != 1:
            return False, "Eviction has no unique durable source retirement"
        retirement = records[0]
        required_bindings = (
            "sourceNodeName",
            "sourceNodeUID",
            "replacementPodUID",
            "replacementPodName",
            "replacementReplicaID",
            "replacementNodeName",
            "replacementNodeUID",
            "replacementCreateID",
        )
        if (
            not all(
                isinstance(retirement.get(key), str) and retirement[key]
                for key in required_bindings
            )
            or retirement.get("workloadUID") != workload_uid
            or (retirement.get("planID"), retirement.get("operationID")) != (plan_id, operation_id)
            or retirement.get("replicaID") != source_metadata["labels"][REPLICA_ID_LABEL]
            or str(retirement.get("workloadGeneration"))
            != source_metadata["annotations"].get(WORKLOAD_GENERATION_ANNOTATION)
            or retirement["sourceNodeName"] != source["spec"].get("nodeName")
            or annotations.get("fleetright.io/replacement-pod-uid")
            != retirement["replacementPodUID"]
            or annotations.get("fleetright.io/replacement-node-uid")
            != retirement["replacementNodeUID"]
            or retirement["replacementPodUID"] == source_metadata["uid"]
            or retirement["replacementNodeName"] == retirement["sourceNodeName"]
            or retirement["replacementReplicaID"]
            != f"{owner['name']}-{execution_request['expectedReplicas']}"
        ):
            return False, "Eviction retirement source or replacement binding is invalid"
        approved = {node["name"]: node["uid"] for node in spec["placement"]["allowedNodes"]}
        destinations = execution_request.get("destinationNodes", [])
        if (
            approved.get(retirement["sourceNodeName"]) != retirement["sourceNodeUID"]
            or approved.get(retirement["replacementNodeName"]) != retirement["replacementNodeUID"]
            or {"name": retirement["replacementNodeName"], "uid": retirement["replacementNodeUID"]}
            not in destinations
        ):
            return False, "Eviction Node bindings are outside approved placement"
        nodes = [
            self.api.request("GET", f"/api/v1/nodes/{quote(retirement[key], safe='')}")
            for key in ("sourceNodeName", "replacementNodeName")
        ]
        for node, prefix in zip(nodes, ("source", "replacement"), strict=True):
            if (
                node["metadata"].get("name") != retirement[f"{prefix}NodeName"]
                or node["metadata"].get("uid") != retirement[f"{prefix}NodeUID"]
                or node["metadata"].get("deletionTimestamp") is not None
            ):
                return False, "Eviction bound Node identity changed"
        replacement_node_ready = [
            condition.get("status")
            for condition in nodes[1].get("status", {}).get("conditions", [])
            if isinstance(condition, dict) and condition.get("type") == "Ready"
        ]
        if replacement_node_ready != ["True"]:
            return False, "Eviction replacement Node is not Ready"
        replacement = self.api.request(
            "GET",
            f"/api/v1/namespaces/{self.namespace}/pods/"
            f"{quote(retirement['replacementPodName'], safe='')}",
        )
        replacement_metadata = replacement["metadata"]
        replacement_record = {
            "workloadUID": workload_uid,
            "podUID": retirement["replacementPodUID"],
            "podName": retirement["replacementPodName"],
            "replicaID": retirement["replacementReplicaID"],
            "workloadGeneration": retirement.get("replacementWorkloadGeneration"),
            "planID": plan_id,
            "operationID": operation_id,
            "createID": retirement["replacementCreateID"],
        }
        if (
            replacement_metadata.get("uid") != retirement["replacementPodUID"]
            or replacement_metadata.get("name") != retirement["replacementPodName"]
            or replacement_metadata.get("labels", {}).get(REPLICA_ID_LABEL)
            != retirement["replacementReplicaID"]
            or replacement["spec"].get("nodeName") != retirement["replacementNodeName"]
            or replacement_record not in status.get("authorizedPodCreates", [])
            or not _has_create_authorization(replacement, workload)
            or not _pod_ready(replacement)
        ):
            return False, "Eviction replacement Pod identity or readiness changed"
        _validate_scheduled_pod(source, approved, nodes)
        _validate_scheduled_pod(
            replacement, approved, nodes, [node["name"] for node in destinations]
        )
        floor, minimum, replicas = (
            spec.get("availabilityFloor"),
            execution_request.get("minimumReadyReplicas"),
            spec.get("replicas"),
        )
        if (
            any(type(value) is not int for value in (floor, minimum, replicas))
            or not 0 <= floor <= minimum <= replicas
        ):
            return False, "Eviction effective availability floor is invalid"
        expected_pdb = _pdb_object(workload)
        pdb = self.api.request(
            "GET",
            f"/apis/policy/v1/namespaces/{self.namespace}/poddisruptionbudgets/"
            f"{quote(owner['name'], safe='')}",
        )
        pdb_metadata, pdb_status = pdb.get("metadata", {}), pdb.get("status", {})
        effective_floor = replicas - expected_pdb["spec"]["maxUnavailable"]
        source_conditions = [
            condition.get("status")
            for condition in source.get("status", {}).get("conditions", [])
            if isinstance(condition, dict) and condition.get("type") == "Ready"
        ]
        if (
            pdb_metadata.get("name") != owner["name"]
            or pdb_metadata.get("namespace") != self.namespace
            or not isinstance(pdb_metadata.get("uid"), str)
            or not pdb_metadata["uid"]
            or pdb_metadata.get("annotations", {}).get(WORKLOAD_GENERATION_ANNOTATION)
            != str(generation)
            or expected_pdb["metadata"]["ownerReferences"] != pdb_metadata.get("ownerReferences")
            or pdb.get("spec") != expected_pdb["spec"]
            or type(pdb_metadata.get("generation")) is not int
            or pdb_metadata["generation"] < 1
            or pdb_status.get("observedGeneration") != pdb_metadata["generation"]
            or any(
                type(pdb_status.get(key)) is not int
                for key in (
                    "desiredHealthy",
                    "currentHealthy",
                    "expectedPods",
                    "disruptionsAllowed",
                )
            )
            or pdb_status["desiredHealthy"] != effective_floor
            or pdb_status["currentHealthy"] < effective_floor
            or pdb_status["expectedPods"] != replicas
            or pdb_status["disruptionsAllowed"] < 0
            or pdb_status.get("disruptedPods") not in (None, {})
            or len(source_conditions) != 1
            or source_conditions[0] not in {"True", "False"}
            or (
                source_conditions[0] == "True"
                and (
                    pdb_status["disruptionsAllowed"] < 1
                    or pdb_status["currentHealthy"] <= effective_floor
                )
            )
        ):
            return False, "Eviction managed PDB evidence is stale or blocks the effective floor"
        current = self.api.request(
            "GET",
            f"{GROUP_VERSION_PATH}/namespaces/{self.namespace}/fleetrightworkloads/"
            f"{quote(owner['name'], safe='')}",
        )
        if current.get("metadata", {}).get("resourceVersion") != workload_metadata.get(
            "resourceVersion"
        ):
            return False, "Eviction workload authority changed during admission"
        return True, ""


def make_handler(admission: PodCreateAdmission) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200 if self.path == "/healthz" else 404)
            self.end_headers()
            if self.path == "/healthz":
                self.wfile.write(b"ok")

        def do_POST(self) -> None:
            if urlsplit(self.path).path != PATH:
                self.send_error(404)
                return
            request_uid = None
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 < size <= 1_048_576:
                    raise ValueError("AdmissionReview size is invalid")
                review = json.loads(self.rfile.read(size))
                request = review.get("request") if isinstance(review, dict) else None
                if (
                    not isinstance(request, dict)
                    or review.get("apiVersion") != "admission.k8s.io/v1"
                    or review.get("kind") != "AdmissionReview"
                ):
                    raise ValueError("AdmissionReview version or request is invalid")
                request_uid = request.get("uid")
                allowed, message = admission.admit(request)
                LOG.info("Pod admission decision: allowed=%s reason=%s", allowed, message)
                response: dict[str, Any] = {"uid": request.get("uid"), "allowed": allowed}
                if not allowed:
                    response["status"] = {"code": 403, "message": message}
            except (ValueError, TypeError) as error:
                response = {
                    "uid": request_uid,
                    "allowed": False,
                    "status": {"code": 400, "message": str(error)},
                }
            except Exception:
                LOG.exception("pod admission failed closed")
                response = {
                    "uid": request_uid,
                    "allowed": False,
                    "status": {"code": 403, "message": "FleetRight Pod admission check failed"},
                }
            payload = json.dumps(
                {
                    "apiVersion": "admission.k8s.io/v1",
                    "kind": "AdmissionReview",
                    "response": response,
                },
                separators=(",", ":"),
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:
            LOG.info("pod admission: " + format, *args)

    return Handler


def run() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    namespace = os.environ.get("POD_NAMESPACE", "fleetright-v2")
    service_account = os.environ.get(
        "POD_ADMISSION_CONTROLLER_SERVICE_ACCOUNT", "fleetright-workload-controller"
    )
    executor_username = os.environ.get("POD_ADMISSION_EXECUTOR_USERNAME", EXECUTOR_USERNAME)
    cert_path = os.environ.get("POD_ADMISSION_TLS_CERT", "/var/run/fleetright-webhook/tls.crt")
    key_path = os.environ.get("POD_ADMISSION_TLS_KEY", "/var/run/fleetright-webhook/tls.key")
    server = ThreadingHTTPServer(
        ("0.0.0.0", 8443),
        make_handler(
            PodCreateAdmission(
                KubernetesAPI(namespace),
                namespace,
                service_account,
                executor_username=executor_username,
            )
        ),
    )
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert_path, key_path)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    server.serve_forever()


if __name__ == "__main__":
    run()
