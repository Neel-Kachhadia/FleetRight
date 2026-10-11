from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import ssl
import time
from copy import deepcopy
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import (
    HTTPRedirectHandler,
    HTTPSHandler,
    ProxyHandler,
    Request,
    build_opener,
)
from uuid import uuid4

API_VERSION = "fleetright.io/v1alpha1"
GROUP_VERSION_PATH = "/apis/fleetright.io/v1alpha1"
WORKLOAD_UID_LABEL = "fleetright.io/workload-uid"
WORKLOAD_ID_LABEL = "fleetright.io/workload-id"
REPLICA_ID_LABEL = "fleetright.io/replica-id"
TEMPLATE_HASH_ANNOTATION = "fleetright.io/pod-template-sha256"
WORKLOAD_GENERATION_ANNOTATION = "fleetright.io/workload-generation"
POD_CREATE_PLAN_ANNOTATION = "fleetright.io/pod-create-plan"
POD_CREATE_OPERATION_ANNOTATION = "fleetright.io/pod-create-operation"
POD_CREATE_ATTEMPT_ANNOTATION = "fleetright.io/pod-create-attempt"
POD_CREATE_PROTOCOL = "reserved-create-v1"
_DNS_LABEL = re.compile(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?\Z")
_QUANTITY = re.compile(r"([0-9]+(?:\.[0-9]+)?|\.[0-9]+)(n|u|m|Ki|Mi|Gi|Ti|Pi|Ei|K|M|G|T|P|E)?\Z")
_DECIMAL_MULTIPLIERS = {
    None: Decimal(1),
    "n": Decimal("0.000000001"),
    "u": Decimal("0.000001"),
    "m": Decimal("0.001"),
    "K": Decimal(1_000),
    "M": Decimal(1_000_000),
    "G": Decimal(1_000_000_000),
    "T": Decimal(1_000_000_000_000),
    "P": Decimal(1_000_000_000_000_000),
    "E": Decimal(1_000_000_000_000_000_000),
    "Ki": Decimal(1_024),
    "Mi": Decimal(1_048_576),
    "Gi": Decimal(1_073_741_824),
    "Ti": Decimal(1_099_511_627_776),
    "Pi": Decimal(1_125_899_906_842_624),
    "Ei": Decimal(1_152_921_504_606_846_976),
}


class ApiError(RuntimeError):
    def __init__(self, status: int, message: str):
        self.status = status
        super().__init__(message)


class SafetyError(ValueError):
    def __init__(self, reason: str, message: str):
        self.reason = reason
        super().__init__(message)


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req: Request, fp: Any, code: int, msg: str, headers: Any, url: str):
        return None


class KubernetesAPI:
    """Small in-cluster REST client; controller state is reconstructed from the API each pass."""

    def __init__(self, namespace: str):
        host = os.environ.get("KUBERNETES_SERVICE_HOST", "")
        port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        if not host or not port.isdecimal():
            raise RuntimeError("in-cluster Kubernetes service address is unavailable")
        self.base_url = f"https://{host}:{int(port)}"
        service_account = "/var/run/secrets/kubernetes.io/serviceaccount"
        self.token_path = os.path.join(service_account, "token")
        self.context = ssl.create_default_context(cafile=os.path.join(service_account, "ca.crt"))
        self.opener = build_opener(
            ProxyHandler({}), _NoRedirect, HTTPSHandler(context=self.context)
        )
        self.namespace = namespace

    def request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        *,
        content_type: str = "application/json",
    ) -> dict[str, Any]:
        data = None if body is None else json.dumps(body, separators=(",", ":")).encode()
        try:
            with open(self.token_path, encoding="utf-8") as token_file:
                token = token_file.read().strip()
        except OSError as error:
            raise RuntimeError("in-cluster service account token is unavailable") from error
        if not token:
            raise RuntimeError("in-cluster service account token is empty")
        request = Request(
            f"{self.base_url}{path}",
            data=data,
            method=method,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "Content-Type": content_type,
            },
        )
        try:
            with self.opener.open(request, timeout=10) as response:
                content = response.read()
        except HTTPError as error:
            raise ApiError(error.code, f"Kubernetes API returned HTTP {error.code}") from error
        except (TimeoutError, URLError, OSError) as error:
            raise RuntimeError("Kubernetes API request failed") from error
        if not content:
            return {}
        try:
            value = json.loads(content)
        except json.JSONDecodeError as error:
            raise RuntimeError("Kubernetes API returned invalid JSON") from error
        if not isinstance(value, dict):
            raise RuntimeError("Kubernetes API returned an unexpected object")
        return value

    def list_workloads(self) -> list[dict[str, Any]]:
        path = f"{GROUP_VERSION_PATH}/namespaces/{self.namespace}/fleetrightworkloads"
        return self.request("GET", path).get("items", [])

    def list_nodes(self) -> list[dict[str, Any]]:
        return self.request("GET", "/api/v1/nodes").get("items", [])

    def list_pods(self, workload_uid: str) -> list[dict[str, Any]]:
        selector = urlencode({"labelSelector": f"{WORKLOAD_UID_LABEL}={workload_uid}"})
        path = f"/api/v1/namespaces/{self.namespace}/pods?{selector}"
        return self.request("GET", path).get("items", [])

    def get_pod(self, name: str) -> dict[str, Any] | None:
        path = f"/api/v1/namespaces/{self.namespace}/pods/{name}"
        try:
            return self.request("GET", path)
        except ApiError as error:
            if error.status == 404:
                return None
            raise

    def create_pod(self, pod: dict[str, Any]) -> dict[str, Any]:
        path = f"/api/v1/namespaces/{self.namespace}/pods"
        return self.request("POST", path, pod)

    def get_pdb(self, name: str) -> dict[str, Any] | None:
        path = f"/apis/policy/v1/namespaces/{self.namespace}/poddisruptionbudgets/{name}"
        try:
            return self.request("GET", path)
        except ApiError as error:
            if error.status == 404:
                return None
            raise

    def create_pdb(self, pdb: dict[str, Any]) -> dict[str, Any]:
        path = f"/apis/policy/v1/namespaces/{self.namespace}/poddisruptionbudgets"
        return self.request("POST", path, pdb)

    def patch_pdb(
        self,
        name: str,
        resource_version: str,
        spec: dict[str, Any],
        annotations: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        path = f"/apis/policy/v1/namespaces/{self.namespace}/poddisruptionbudgets/{name}"
        metadata: dict[str, Any] = {"resourceVersion": resource_version}
        if annotations is not None:
            metadata["annotations"] = annotations
        return self.request(
            "PATCH",
            path,
            {"metadata": metadata, "spec": spec},
            content_type="application/merge-patch+json",
        )

    def patch_status(
        self, name: str, resource_version: str, status: dict[str, Any]
    ) -> dict[str, Any]:
        path = f"{GROUP_VERSION_PATH}/namespaces/{self.namespace}/fleetrightworkloads/{name}/status"
        return self.request(
            "PATCH",
            path,
            {"metadata": {"resourceVersion": resource_version}, "status": status},
            content_type="application/merge-patch+json",
        )


def _quantity(value: object, resource: str) -> Decimal:
    if not isinstance(value, str):
        raise SafetyError("InvalidResourceRequest", f"{resource} request must be a string")
    match = _QUANTITY.fullmatch(value)
    if not match:
        raise SafetyError("InvalidResourceRequest", f"invalid {resource} request quantity")
    try:
        quantity = Decimal(match.group(1)) * _DECIMAL_MULTIPLIERS[match.group(2)]
    except InvalidOperation as error:
        raise SafetyError(
            "InvalidResourceRequest", f"invalid {resource} request quantity"
        ) from error
    if not quantity.is_finite() or quantity <= 0:
        raise SafetyError("InvalidResourceRequest", f"{resource} request must be positive")
    return quantity


def _validate_template(template: object) -> dict[str, Any]:
    if not isinstance(template, dict):
        raise SafetyError("InvalidPodTemplate", "podTemplate must be an object")
    containers = template.get("containers")
    if not isinstance(containers, list) or not containers:
        raise SafetyError("InvalidPodTemplate", "at least one container is required")
    seen_names: set[str] = set()
    for container in containers:
        if not isinstance(container, dict):
            raise SafetyError("InvalidPodTemplate", "container entries must be objects")
        name, image, resources = (
            container.get("name"),
            container.get("image"),
            container.get("resources"),
        )
        if (
            not isinstance(name, str)
            or not _DNS_LABEL.fullmatch(name)
            or name in seen_names
            or not isinstance(image, str)
            or not image.strip()
        ):
            raise SafetyError("InvalidPodTemplate", "container names and images must be valid")
        if "securityContext" in container:
            raise SafetyError(
                "InvalidPodTemplate", "container securityContext is managed by the controller"
            )
        seen_names.add(name)
        requests = resources.get("requests") if isinstance(resources, dict) else None
        if not isinstance(requests, dict) or set(requests) != {"cpu", "memory"}:
            raise SafetyError(
                "InvalidResourceRequest", "each container must request CPU and memory only"
            )
        parsed_requests = {key: _quantity(value, key) for key, value in requests.items()}
        limits = resources.get("limits", {})
        if not isinstance(limits, dict) or not set(limits).issubset({"cpu", "memory"}):
            raise SafetyError("InvalidResourceLimit", "only CPU and memory limits are supported")
        for key, limit in limits.items():
            if _quantity(limit, key) < parsed_requests[key]:
                raise SafetyError("InvalidResourceLimit", f"{key} limit is below its request")
        for field in ("command", "args", "env", "ports", "readinessProbe"):
            if field in container and not isinstance(container[field], (list, dict)):
                raise SafetyError("InvalidPodTemplate", f"container {field} has the wrong type")
    security_context = template.get("securityContext", {})
    if not isinstance(security_context, dict):
        raise SafetyError("UnsafeSecurityContext", "pod securityContext must be an object")
    if "runAsNonRoot" in security_context:
        raise SafetyError(
            "UnsafeSecurityContext", "runAsNonRoot is fixed by the controller to true"
        )
    unsupported_security_fields = set(security_context) - {"runAsUser", "runAsGroup", "fsGroup"}
    if unsupported_security_fields:
        raise SafetyError(
            "UnsafeSecurityContext", "pod securityContext contains an unsupported override"
        )
    for field in ("runAsUser", "runAsGroup", "fsGroup"):
        if field in security_context and (
            type(security_context[field]) is not int or security_context[field] < 1
        ):
            raise SafetyError(
                "UnsafeSecurityContext", f"{field} must be a positive nonzero integer"
            )
    for field in ("labels", "annotations"):
        values = template.get(field, {})
        if not isinstance(values, dict) or any(
            not isinstance(key, str)
            or key.startswith("fleetright.io/")
            or not isinstance(value, str)
            for key, value in values.items()
        ):
            raise SafetyError(
                "InvalidPodTemplate", f"template {field} contains a reserved or invalid value"
            )
    return template


def _owner_reference(workload: dict[str, Any]) -> dict[str, Any]:
    metadata = workload["metadata"]
    return {
        "apiVersion": API_VERSION,
        "kind": "FleetRightWorkload",
        "name": metadata["name"],
        "uid": metadata["uid"],
        "controller": True,
        "blockOwnerDeletion": False,
    }


def _pod_name(workload_uid: str, ordinal: int) -> str:
    identity = hashlib.sha256(workload_uid.encode()).hexdigest()
    return f"frw-{identity}-r{ordinal}"


def _pod_create_intent(workload: dict[str, Any], ordinal: int) -> dict[str, Any]:
    metadata, spec = workload["metadata"], workload["spec"]
    intent = {
        "workloadUID": metadata["uid"],
        "workloadGeneration": metadata["generation"],
        "podName": _pod_name(metadata["uid"], ordinal),
        "replicaID": f"{spec['workloadID']}-{ordinal}",
        "planID": "bootstrap",
        "operationID": f"bootstrap-{metadata['generation']}-{ordinal}",
        "createID": uuid4().hex,
    }
    request = spec.get("executionRequest")
    if isinstance(request, dict):
        intent.update(planID=request["planID"], operationID=request["operationID"])
    return intent


def _selector(workload_uid: str) -> str:
    return f"{WORKLOAD_UID_LABEL}={workload_uid}"


def _pod_labels(workload: dict[str, Any], ordinal: int) -> dict[str, str]:
    metadata, spec = workload["metadata"], workload["spec"]
    template = spec["podTemplate"]
    return {
        **template.get("labels", {}),
        WORKLOAD_UID_LABEL: metadata["uid"],
        WORKLOAD_ID_LABEL: spec["workloadID"],
        REPLICA_ID_LABEL: f"{spec['workloadID']}-{ordinal}",
    }


def _placement_fingerprint(spec: dict[str, Any], template: dict[str, Any]) -> str:
    normalized_template = deepcopy(template)
    for container in normalized_template["containers"]:
        if "ports" in container:
            container["ports"] = sorted(
                container["ports"],
                key=lambda item: (item["containerPort"], item.get("protocol", "TCP")),
            )
    normalized_template["containers"] = sorted(
        normalized_template["containers"], key=lambda container: container["name"]
    )
    normalized_placement = deepcopy(spec["placement"])
    normalized_placement["allowedNodes"] = sorted(
        normalized_placement["allowedNodes"], key=lambda node: node["name"]
    )
    value = {"podTemplate": normalized_template, "placement": normalized_placement}
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest()


def _pod_object(
    workload: dict[str, Any], ordinal: int, intent: dict[str, Any] | None = None
) -> dict[str, Any]:
    metadata, spec = workload["metadata"], workload["spec"]
    template = _validate_template(spec.get("podTemplate"))
    placement = spec["placement"]
    allowed_nodes = [node["name"] for node in placement["allowedNodes"]]
    labels = _pod_labels(workload, ordinal)
    intent = intent or _pod_create_intent(workload, ordinal)
    topology = placement.get("topology")
    pod_security_context = {
        "runAsNonRoot": True,
        "runAsUser": 10001,
        "runAsGroup": 10001,
        "seccompProfile": {"type": "RuntimeDefault"},
        **template.get("securityContext", {}),
    }
    containers = deepcopy(template["containers"])
    for container in containers:
        container["securityContext"] = {
            "allowPrivilegeEscalation": False,
            "capabilities": {"drop": ["ALL"]},
        }
    pod_spec: dict[str, Any] = {
        "restartPolicy": "Always",
        "automountServiceAccountToken": False,
        "securityContext": pod_security_context,
        "containers": containers,
        "affinity": {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchFields": [
                                {"key": "metadata.name", "operator": "In", "values": [node_name]}
                            ]
                        }
                        for node_name in allowed_nodes
                    ]
                }
            }
        },
    }
    if topology:
        pod_spec["topologySpreadConstraints"] = [
            {
                "maxSkew": topology.get("maxSkew", 1),
                "topologyKey": topology["key"],
                "whenUnsatisfiable": "DoNotSchedule",
                "labelSelector": {"matchLabels": {WORKLOAD_UID_LABEL: metadata["uid"]}},
                **({"minDomains": topology["minDomains"]} if "minDomains" in topology else {}),
            }
        ]
    if placement.get("requirePodAntiAffinity", False):
        topology_key = (
            topology.get("key", "kubernetes.io/hostname") if topology else "kubernetes.io/hostname"
        )
        pod_spec.setdefault("affinity", {})["podAntiAffinity"] = {
            "requiredDuringSchedulingIgnoredDuringExecution": [
                {
                    "topologyKey": topology_key,
                    "labelSelector": {"matchLabels": {WORKLOAD_UID_LABEL: metadata["uid"]}},
                }
            ]
        }
    if "terminationGracePeriodSeconds" in template:
        pod_spec["terminationGracePeriodSeconds"] = template["terminationGracePeriodSeconds"]
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": _pod_name(metadata["uid"], ordinal),
            "labels": labels,
            "annotations": {
                **template.get("annotations", {}),
                TEMPLATE_HASH_ANNOTATION: _placement_fingerprint(spec, template),
                WORKLOAD_GENERATION_ANNOTATION: str(metadata["generation"]),
                POD_CREATE_PLAN_ANNOTATION: intent["planID"],
                POD_CREATE_OPERATION_ANNOTATION: intent["operationID"],
                POD_CREATE_ATTEMPT_ANNOTATION: intent["createID"],
            },
            "ownerReferences": [_owner_reference(workload)],
        },
        "spec": pod_spec,
    }


def _pdb_object(workload: dict[str, Any]) -> dict[str, Any]:
    metadata, spec = workload["metadata"], workload["spec"]
    disruption = spec["disruption"]
    max_unavailable = min(
        disruption["maxConcurrentDisruptions"],
        spec["replicas"] - spec["availabilityFloor"],
    )
    return {
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudget",
        "metadata": {
            "name": metadata["name"],
            "labels": {WORKLOAD_UID_LABEL: metadata["uid"]},
            "annotations": {WORKLOAD_GENERATION_ANNOTATION: str(metadata["generation"])},
            "ownerReferences": [_owner_reference(workload)],
        },
        "spec": {
            "maxUnavailable": max_unavailable,
            "selector": {"matchLabels": {WORKLOAD_UID_LABEL: metadata["uid"]}},
        },
    }


def _is_owned(value: dict[str, Any], workload: dict[str, Any]) -> bool:
    owners = value.get("metadata", {}).get("ownerReferences", [])
    expected = _owner_reference(workload)
    return any(
        isinstance(owner, dict)
        and owner.get("apiVersion") == expected["apiVersion"]
        and owner.get("kind") == expected["kind"]
        and owner.get("name") == expected["name"]
        and owner.get("uid") == expected["uid"]
        and owner.get("controller") is True
        for owner in owners
    )


def _check_existing_pod(pod: dict[str, Any], workload: dict[str, Any]) -> int:
    ordinal = _check_existing_pod_identity(pod, workload)
    metadata = pod["metadata"]
    annotations = metadata["annotations"]
    logical_id = metadata["labels"][REPLICA_ID_LABEL]
    pod_uid = metadata.get("uid")
    records = _authorized_pod_creates(workload)
    if (
        not isinstance(pod_uid, str)
        or not pod_uid
        or not any(
            isinstance(record, dict)
            and record.get("workloadUID") == workload["metadata"]["uid"]
            and record.get("podUID") == pod_uid
            and record.get("podName") == metadata.get("name")
            and record.get("replicaID") == logical_id
            and str(record.get("workloadGeneration"))
            == annotations.get(WORKLOAD_GENERATION_ANNOTATION)
            and record.get("planID") == annotations.get(POD_CREATE_PLAN_ANNOTATION)
            and record.get("operationID") == annotations.get(POD_CREATE_OPERATION_ANNOTATION)
            and record.get("createID") == annotations.get(POD_CREATE_ATTEMPT_ANNOTATION)
            for record in records
        )
    ):
        raise SafetyError(
            "PodCreateIdentityConflict", "Pod UID or create operation lacks durable authorization"
        )
    return ordinal


def _check_pending_pod(
    pod: dict[str, Any], workload: dict[str, Any], intent: dict[str, Any]
) -> int:
    ordinal = _check_existing_pod_identity(pod, workload)
    annotations = pod.get("metadata", {}).get("annotations", {})
    if (
        intent["podName"] != _pod_name(workload["metadata"]["uid"], ordinal)
        or intent["replicaID"] != f"{workload['spec']['workloadID']}-{ordinal}"
        or intent["workloadGeneration"] != workload["metadata"]["generation"]
        or intent["planID"] != annotations.get(POD_CREATE_PLAN_ANNOTATION)
        or intent["operationID"] != annotations.get(POD_CREATE_OPERATION_ANNOTATION)
        or intent["createID"] != annotations.get(POD_CREATE_ATTEMPT_ANNOTATION)
        or str(intent["workloadGeneration"]) != annotations.get(WORKLOAD_GENERATION_ANNOTATION)
    ):
        raise SafetyError(
            "PodCreateIdentityConflict", "observed Pod does not match its durable create intent"
        )
    if not isinstance(pod.get("metadata", {}).get("uid"), str):
        raise SafetyError("PodCreateIdentityConflict", "created Pod UID is missing")
    return ordinal


def _check_existing_pod_identity(pod: dict[str, Any], workload: dict[str, Any]) -> int:
    """Validate API ownership and logical identity before matching a durable UID record."""
    metadata, spec = pod.get("metadata"), workload["spec"]
    if not isinstance(metadata, dict):
        raise SafetyError("PodIdentityConflict", "an existing Pod requires metadata")
    labels = metadata.get("labels", {})
    if not isinstance(labels, dict):
        raise SafetyError("PodIdentityConflict", "an existing Pod requires identity labels")
    logical_id = labels.get(REPLICA_ID_LABEL, "")
    prefix = f"{spec['workloadID']}-"
    if (
        not _is_owned(pod, workload)
        or labels.get(WORKLOAD_UID_LABEL) != workload["metadata"]["uid"]
        or labels.get(WORKLOAD_ID_LABEL) != spec["workloadID"]
        or not logical_id.startswith(prefix)
        or not logical_id[len(prefix) :].isdecimal()
    ):
        raise SafetyError(
            "PodIdentityConflict", "an existing Pod has conflicting owner or replica identity"
        )
    ordinal = int(logical_id[len(prefix) :])
    if metadata.get("name") != _pod_name(workload["metadata"]["uid"], ordinal):
        raise SafetyError(
            "PodIdentityConflict",
            "an existing Pod name conflicts with its logical replica identity",
        )
    if not isinstance(metadata.get("uid"), str) or not metadata["uid"]:
        raise SafetyError("PodIdentityConflict", "an existing Pod UID is required")
    logical_id = labels[REPLICA_ID_LABEL]
    retirement = next(
        (
            record
            for record in workload.get("status", {}).get("retiredReplicaSlots", [])
            if isinstance(record, dict) and record.get("replicaID") == logical_id
        ),
        None,
    )
    if retirement is not None and retirement.get("podUID") != metadata["uid"]:
        raise SafetyError(
            "RetiredReplicaPodObserved", "a Pod appeared in a durably retired logical slot"
        )
    annotations = metadata.get("annotations")
    if (
        not isinstance(annotations, dict)
        or not isinstance(annotations.get(WORKLOAD_GENERATION_ANNOTATION), str)
        or not annotations[WORKLOAD_GENERATION_ANNOTATION].isdecimal()
        or not all(
            isinstance(annotations.get(key), str) and annotations[key]
            for key in (
                POD_CREATE_PLAN_ANNOTATION,
                POD_CREATE_OPERATION_ANNOTATION,
                POD_CREATE_ATTEMPT_ANNOTATION,
            )
        )
    ):
        raise SafetyError(
            "PodCreateIdentityConflict", "Pod generation or create operation identity is invalid"
        )
    if annotations.get(TEMPLATE_HASH_ANNOTATION) != _placement_fingerprint(
        spec, spec["podTemplate"]
    ):
        raise SafetyError(
            "PodTemplateChanged", "existing Pods use a different immutable Pod template"
        )
    return ordinal


def _has_create_authorization(pod: dict[str, Any], workload: dict[str, Any]) -> bool:
    try:
        _check_existing_pod_identity(pod, workload)
    except (SafetyError, AttributeError, TypeError):
        return False
    metadata = pod["metadata"]
    annotations = metadata["annotations"]
    logical_id = metadata["labels"][REPLICA_ID_LABEL]
    generation = int(annotations[WORKLOAD_GENERATION_ANNOTATION])
    records = workload.get("status", {}).get("authorizedPodCreates", [])
    if not isinstance(records, list):
        return False
    return any(
        isinstance(record, dict)
        and record.get("workloadUID") == workload["metadata"]["uid"]
        and record.get("podUID") == metadata["uid"]
        and record.get("podName") == metadata["name"]
        and record.get("replicaID") == logical_id
        and record.get("workloadGeneration") == generation
        and record.get("planID") == annotations[POD_CREATE_PLAN_ANNOTATION]
        and record.get("operationID") == annotations[POD_CREATE_OPERATION_ANNOTATION]
        and record.get("createID") == annotations[POD_CREATE_ATTEMPT_ANNOTATION]
        for record in records
    )


def _allowed_nodes(
    workload: dict[str, Any], nodes: list[dict[str, Any]]
) -> tuple[list[str], dict[str, str]]:
    raw = workload["spec"].get("placement", {}).get("allowedNodes")
    if not isinstance(raw, list) or not raw or len(raw) > 8:
        raise SafetyError(
            "InvalidAllowedNodes", "one to eight allowed Node identities are required"
        )
    node_by_name = {
        node.get("metadata", {}).get("name"): node for node in nodes if isinstance(node, dict)
    }
    names: list[str] = []
    uids: dict[str, str] = {}
    for approved in raw:
        if not isinstance(approved, dict):
            raise SafetyError("InvalidAllowedNodes", "allowed Node identities must be objects")
        name, uid = approved.get("name"), approved.get("uid")
        if not isinstance(name, str) or not name or not isinstance(uid, str) or not uid:
            raise SafetyError("InvalidAllowedNodes", "allowed Node name and UID are required")
        current = node_by_name.get(name)
        current_uid = current.get("metadata", {}).get("uid") if current else None
        if name in uids:
            raise SafetyError("InvalidAllowedNodes", "allowed Node names must be unique")
        if current_uid != uid:
            raise SafetyError(
                "NodeIdentityChanged", f"allowed Node {name} is missing or has a new UID"
            )
        names.append(name)
        uids[name] = uid
    return names, uids


def _retired_replica_slots(workload: dict[str, Any]) -> list[dict[str, Any]]:
    slots = workload.get("status", {}).get("retiredReplicaSlots", [])
    if not isinstance(slots, list) or any(
        not isinstance(record, dict)
        or not all(
            isinstance(record.get(key), str) and record[key]
            for key in (
                "workloadUID",
                "replicaID",
                "podUID",
                "planID",
                "operationID",
            )
        )
        or type(record.get("workloadGeneration")) is not int
        for record in slots
    ):
        raise SafetyError("InvalidRetirementHistory", "retired replica slot history is invalid")
    return slots


def _authorized_pod_creates(workload: dict[str, Any]) -> list[dict[str, Any]]:
    records = workload.get("status", {}).get("authorizedPodCreates", [])
    if not isinstance(records, list) or any(
        not isinstance(record, dict)
        or not all(
            isinstance(record.get(key), str) and record[key]
            for key in (
                "workloadUID",
                "podUID",
                "podName",
                "replicaID",
                "planID",
                "operationID",
                "createID",
            )
        )
        or type(record.get("workloadGeneration")) is not int
        or record.get("workloadGeneration") < 1
        or record.get("workloadUID") != workload.get("metadata", {}).get("uid")
        for record in records
    ):
        raise SafetyError("InvalidCreateHistory", "authorized Pod create history is invalid")
    if len({record["podUID"] for record in records}) != len(records):
        raise SafetyError("InvalidCreateHistory", "authorized Pod UIDs must be unique")
    return records


def _validate_spec(
    workload: dict[str, Any], nodes: list[dict[str, Any]]
) -> tuple[list[str], dict[str, str]]:
    spec = workload.get("spec")
    if not isinstance(spec, dict):
        raise SafetyError("InvalidSpec", "spec must be an object")
    if spec.get("podCreateProtocol") != POD_CREATE_PROTOCOL:
        raise SafetyError(
            "LegacyPodCreateUncertain",
            "legacy workload UID has untracked Pod CREATE history; create a new workload UID",
        )
    if workload.get("status", {}).get("podCreateHistoryUncertain") is True:
        raise SafetyError(
            "PodCreateHistoryUncertain", "workload has durable Pod CREATE history uncertainty"
        )
    metadata = workload.get("metadata")
    if (
        not isinstance(metadata, dict)
        or not all(
            isinstance(metadata.get(key), str) and metadata.get(key)
            for key in ("name", "uid", "resourceVersion")
        )
        or type(metadata.get("generation")) is not int
        or metadata["generation"] < 1
    ):
        raise SafetyError(
            "InvalidIdentity", "workload name, UID, generation, and resourceVersion are required"
        )
    workload_id = spec.get("workloadID")
    if (
        not isinstance(workload_id, str)
        or len(workload_id) > 44
        or not _DNS_LABEL.fullmatch(workload_id)
    ):
        raise SafetyError(
            "InvalidIdentity", "workloadID must be a DNS label of at most 44 characters"
        )
    retired_slots = _retired_replica_slots(workload)
    _authorized_pod_creates(workload)
    retired_ids = [record["replicaID"] for record in retired_slots]
    if len(retired_ids) != len(set(retired_ids)) or any(
        record["workloadUID"] != metadata["uid"]
        or not record["replicaID"].startswith(f"{workload_id}-")
        or not record["replicaID"][len(workload_id) + 1 :].isdecimal()
        or record["workloadGeneration"] < 1
        for record in retired_slots
    ):
        raise SafetyError("InvalidRetirementHistory", "retired replica slot identity is invalid")
    for field, minimum, maximum in (("replicas", 0, 1_000), ("availabilityFloor", 0, 1_000)):
        value = spec.get(field)
        if type(value) is not int or not minimum <= value <= maximum:
            raise SafetyError("InvalidSpec", f"{field} is outside its supported range")
    if spec["availabilityFloor"] > spec["replicas"]:
        raise SafetyError("InvalidSpec", "availabilityFloor cannot exceed replicas")
    execution = spec.get("executionRequest")
    fence = spec.get("executionFence")
    if execution is not None and (
        spec.get("executionProtocol") != "monotonic-scale-up-v1"
        or not isinstance(execution, dict)
        or not all(
            isinstance(execution.get(key), str) and execution[key]
            for key in ("planID", "operationID")
        )
        or type(execution.get("generation")) is not int
        or execution["generation"] < 1
        or type(execution.get("expectedReplicas")) is not int
        or type(execution.get("targetReplicas")) is not int
        or execution["targetReplicas"] != spec["replicas"]
        or execution["expectedReplicas"] > execution["targetReplicas"]
    ):
        raise SafetyError(
            "InvalidExecutionRequest", "execution request identity or target is invalid"
        )
    if fence is not None and (
        not isinstance(fence, dict)
        or not all(
            isinstance(fence.get(key), str) and fence[key] for key in ("planID", "operationID")
        )
        or type(fence.get("generation")) is not int
        or fence["generation"] < 1
        or type(fence.get("expectedGeneration")) is not int
        or fence["expectedGeneration"] < 1
        or type(fence.get("expectedReplicas")) is not int
        or fence["expectedReplicas"] < 0
        or type(fence.get("fencedGeneration")) is not int
        or fence["fencedGeneration"] != metadata["generation"]
        or (
            isinstance(execution, dict)
            and (
                fence["generation"] < execution["generation"]
                or (
                    fence["generation"] == execution["generation"]
                    and (fence["planID"], fence["operationID"])
                    != (execution["planID"], execution["operationID"])
                )
            )
        )
    ):
        raise SafetyError("InvalidExecutionFence", "execution fence is invalid")
    disruption = spec.get("disruption")
    max_unavailable = (
        disruption.get("maxConcurrentDisruptions") if isinstance(disruption, dict) else None
    )
    if type(max_unavailable) is not int or not 0 <= max_unavailable <= 1_000:
        raise SafetyError("InvalidSpec", "maxConcurrentDisruptions is outside its supported range")
    surge_budget = spec.get("surgeBudget", 0)
    if type(surge_budget) is not int or not 0 <= surge_budget <= 100:
        raise SafetyError("InvalidSpec", "surgeBudget is outside its supported range")
    if spec.get("networkMbps", 0) != 0:
        raise SafetyError(
            "UnsupportedNetworkBandwidth", "network bandwidth scheduling is unsupported"
        )
    _validate_template(spec.get("podTemplate"))
    placement = spec.get("placement")
    if not isinstance(placement, dict):
        raise SafetyError("InvalidPlacement", "placement must be an object")
    topology = placement.get("topology")
    if topology is not None:
        if not isinstance(topology, dict) or not isinstance(topology.get("key"), str):
            raise SafetyError("InvalidPlacement", "topology requires a topology key")
        if type(topology.get("maxSkew", 1)) is not int or topology.get("maxSkew", 1) < 1:
            raise SafetyError("InvalidPlacement", "topology maxSkew must be positive")
        if "minDomains" in topology and (
            type(topology["minDomains"]) is not int or topology["minDomains"] < 1
        ):
            raise SafetyError("InvalidPlacement", "topology minDomains must be positive")
    if type(placement.get("requirePodAntiAffinity", False)) is not bool:
        raise SafetyError("InvalidPlacement", "requirePodAntiAffinity must be boolean")
    return _allowed_nodes(workload, nodes)


def _condition(
    condition_type: str,
    reason: str,
    message: str,
    ready: bool,
    old: list[dict[str, Any]],
) -> dict[str, Any]:
    previous = next((item for item in old if item.get("type") == condition_type), {})
    status = "True" if ready else "False"
    return {
        "type": condition_type,
        "status": status,
        "reason": reason,
        "message": message,
        "lastTransitionTime": (
            previous.get("lastTransitionTime")
            if previous.get("status") == status and previous.get("lastTransitionTime")
            else time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        ),
    }


def _pod_ready(pod: dict[str, Any]) -> bool:
    """Use the kubelet Ready condition and configured probes, not separate app health."""
    return not pod.get("metadata", {}).get("deletionTimestamp") and any(
        condition.get("type") == "Ready" and condition.get("status") == "True"
        for condition in pod.get("status", {}).get("conditions", [])
    )


def _status(
    workload: dict[str, Any],
    pods: list[dict[str, Any]],
    reason: str,
    message: str,
    old: dict[str, Any],
) -> dict[str, Any]:
    owned_live = [
        pod
        for pod in pods
        if pod.get("metadata", {}).get("labels", {}).get(WORKLOAD_UID_LABEL)
        == workload.get("metadata", {}).get("uid")
        and pod.get("status", {}).get("phase") not in {"Succeeded", "Failed"}
    ]
    live_pods = [pod for pod in owned_live if _has_create_authorization(pod, workload)]
    unverified_pods = [pod for pod in owned_live if not _has_create_authorization(pod, workload)]
    replicas = sum(1 for pod in live_pods if not pod.get("metadata", {}).get("deletionTimestamp"))
    ready_replicas = sum(1 for pod in live_pods if _pod_ready(pod))
    desired = workload.get("spec", {}).get("replicas", 0)
    floor = workload.get("spec", {}).get("availabilityFloor", 0)
    history_uncertain = (
        old.get("podCreateHistoryUncertain") is True
        or reason == "LegacyPodCreateUncertain"
        or reason == "PodCreateHistoryUncertain"
        or reason
        in {
            "PodCreateIdentityConflict",
            "PodIdentityConflict",
            "PodOutsideAllowedNodes",
            "NodeIdentityChanged",
            "PodTemplateChanged",
            "RetiredReplicaPodObserved",
        }
        or bool(unverified_pods)
    )
    retired_ids = {
        record.get("replicaID")
        for record in old.get("retiredReplicaSlots", [])
        if isinstance(record, dict)
    }
    retired_in_desired_range = any(
        f"{workload.get('spec', {}).get('workloadID')}-{ordinal}" in retired_ids
        for ordinal in range(desired)
    )
    counts_are_trustworthy = (
        reason in {"Reconciled", "ScaleDownRequiresStagedExecution"}
        and not unverified_pods
        and not history_uncertain
        and not retired_in_desired_range
    )
    ready = counts_are_trustworthy and ready_replicas >= desired
    available = counts_are_trustworthy and ready_replicas >= floor
    old_conditions = old.get("conditions", [])
    ready_reason = "AllReplicasReady" if ready else "ReplicasNotReady"
    availability_reason = "MinimumAvailabilityMet" if available else "AvailabilityFloorNotMet"
    ready_message = (
        ("all desired replicas are ready" if ready else "waiting for all desired replicas")
        if counts_are_trustworthy
        else message
    )
    availability_message = (
        (
            "ready replicas meet the availability floor"
            if available
            else "ready replicas are below availabilityFloor"
        )
        if counts_are_trustworthy
        else message
    )
    conditions = [
        _condition(
            "Ready",
            ready_reason if counts_are_trustworthy else reason,
            ready_message,
            ready,
            old_conditions,
        ),
        _condition(
            "Available",
            availability_reason if counts_are_trustworthy else reason,
            availability_message,
            available,
            old_conditions,
        ),
        _condition(
            "Reconciled",
            "Reconciled" if reason == "Reconciled" else reason,
            "workload state was reconciled" if reason == "Reconciled" else message,
            reason == "Reconciled",
            old_conditions,
        ),
    ]
    metadata = workload.get("metadata", {})
    status: dict[str, Any] = {
        "observedGeneration": metadata.get("generation", 0),
        "replicas": replicas,
        "readyReplicas": ready_replicas,
        "availableReplicas": ready_replicas,
        "surgeReplicas": max(0, len(owned_live) - desired),
        "unverifiedPodCreates": len(unverified_pods),
        "labelSelector": _selector(metadata.get("uid", "")),
        "activeOperationRefs": old.get("activeOperationRefs", []),
        "conditions": conditions,
    }
    if isinstance(old.get("execution"), dict):
        status["execution"] = old["execution"]
    if isinstance(old.get("pendingPodCreate"), dict):
        status["pendingPodCreate"] = old["pendingPodCreate"]
    if history_uncertain:
        status["podCreateHistoryUncertain"] = True
    if isinstance(old.get("authorizedPodCreates"), list):
        status["authorizedPodCreates"] = old["authorizedPodCreates"]
    if isinstance(old.get("retiredReplicaSlots"), list):
        status["retiredReplicaSlots"] = old["retiredReplicaSlots"]
    return status


def _validate_execution_request(workload: dict[str, Any]) -> None:
    spec = workload["spec"]
    request = spec.get("executionRequest")
    if request is None:
        return
    metadata, status = workload["metadata"], workload.get("status", {})
    previous = status.get("execution")
    observed_generation = status.get("observedGeneration")
    expected = request["expectedReplicas"]
    fence = spec.get("executionFence")
    same_operation = isinstance(previous, dict) and all(
        previous.get(key) == request[key] for key in ("planID", "operationID")
    )
    if same_operation and (
        type(previous.get("generation")) is int
        and previous["generation"] <= request["generation"]
        and previous.get("targetReplicas") == request["targetReplicas"]
    ):
        return
    reconciled_previous_generation = (
        type(observed_generation) is int
        and observed_generation == metadata["generation"] - 1
        and status.get("replicas") == expected
    )
    fenced_previous_generation = (
        isinstance(fence, dict)
        and fence.get("planID") == request["planID"]
        and fence.get("operationID") == request["operationID"]
        and type(fence.get("generation")) is int
        and fence["generation"] >= request["generation"]
        and type(fence.get("expectedGeneration")) is int
        and observed_generation == fence["expectedGeneration"]
        and status.get("replicas") == fence.get("expectedReplicas") == expected
    )
    if isinstance(previous, dict):
        if (
            reconciled_previous_generation
            and previous.get("phase") == "APPLIED"
            and type(previous.get("generation")) is int
            and request["generation"] > previous["generation"]
        ):
            return
        if (
            fenced_previous_generation
            and previous.get("phase") == "APPLIED"
            and previous.get("targetReplicas") == expected
        ):
            return
    else:
        conditions = status.get("conditions", [])
        reconciled = any(
            isinstance(condition, dict)
            and condition.get("type") == "Reconciled"
            and condition.get("status") == "True"
            for condition in conditions
        )
        if (
            reconciled_previous_generation
            and reconciled
            and status.get("readyReplicas", 0) >= spec["availabilityFloor"]
        ):
            return
        if (
            fenced_previous_generation
            and reconciled
            and status.get("readyReplicas", 0) >= spec["availabilityFloor"]
        ):
            return
    raise SafetyError(
        "WorkloadOperationUnreconciled",
        "execution request does not follow a durable reconciled workload state",
    )


def _write_status(
    api: Any,
    workload: dict[str, Any],
    pods: list[dict[str, Any]],
    reason: str,
    message: str,
    execution_phase: str | None = None,
) -> None:
    metadata = workload["metadata"]
    old = workload.get("status", {})
    desired = _status(workload, pods, reason, message, old)
    request = workload.get("spec", {}).get("executionRequest")
    if execution_phase is not None and isinstance(request, dict):
        desired["execution"] = {
            **{
                key: request[key]
                for key in ("planID", "operationID", "generation", "targetReplicas")
            },
            "phase": execution_phase,
        }
    if desired != old:
        result = api.patch_status(metadata["name"], metadata["resourceVersion"], desired)
        result_metadata = result.get("metadata", {}) if isinstance(result, dict) else {}
        result_status = result.get("status") if isinstance(result, dict) else None
        if isinstance(result_metadata.get("resourceVersion"), str):
            metadata["resourceVersion"] = result_metadata["resourceVersion"]
        workload["status"] = result_status if isinstance(result_status, dict) else desired


def _write_execution_phase(api: Any, workload: dict[str, Any], phase: str) -> None:
    request = workload.get("spec", {}).get("executionRequest")
    if not isinstance(request, dict):
        return
    identity = {
        key: request[key] for key in ("planID", "operationID", "generation", "targetReplicas")
    }
    execution = {**identity, "phase": phase}
    old_status = workload.get("status", {})
    if old_status.get("execution") == execution:
        return
    status = {**old_status, "execution": execution}
    result = api.patch_status(
        workload["metadata"]["name"], workload["metadata"]["resourceVersion"], status
    )
    metadata = result.get("metadata", {}) if isinstance(result, dict) else {}
    if isinstance(metadata.get("resourceVersion"), str):
        workload["metadata"]["resourceVersion"] = metadata["resourceVersion"]
    workload["status"] = status


def _write_pending_pod_create(api: Any, workload: dict[str, Any], intent: dict[str, Any]) -> None:
    # shortcut: reservation covers participating writers; add API-server fencing before retirement.
    old_status = workload.get("status", {})
    if old_status.get("pendingPodCreate") is not None:
        raise SafetyError("PodCreateOutcomeUnknown", "another Pod create outcome is unresolved")
    # shortcut: cap lifetime authorized history at 1,000; use a new workload UID beyond this cap.
    if len(_authorized_pod_creates(workload)) >= 1_000:
        raise SafetyError("PodCreateHistoryFull", "authorized Pod create history is at capacity")
    status = {**old_status, "pendingPodCreate": intent}
    result = api.patch_status(
        workload["metadata"]["name"], workload["metadata"]["resourceVersion"], status
    )
    metadata = result.get("metadata", {}) if isinstance(result, dict) else {}
    returned_status = result.get("status") if isinstance(result, dict) else None
    if (
        not isinstance(metadata.get("resourceVersion"), str)
        or not isinstance(returned_status, dict)
        or returned_status.get("pendingPodCreate") != intent
    ):
        raise SafetyError(
            "PodCreateOutcomeUnknown", "Kubernetes did not confirm the durable Pod create intent"
        )
    workload["metadata"]["resourceVersion"] = metadata["resourceVersion"]
    workload["status"] = status


def _clear_pending_pod_create(api: Any, workload: dict[str, Any]) -> bool:
    status = dict(workload.get("status", {}))
    status["pendingPodCreate"] = None
    try:
        result = api.patch_status(
            workload["metadata"]["name"], workload["metadata"]["resourceVersion"], status
        )
    except ApiError as error:
        if error.status == 409:
            return False
        raise
    metadata = result.get("metadata", {}) if isinstance(result, dict) else {}
    returned_status = result.get("status") if isinstance(result, dict) else None
    if (
        not isinstance(metadata.get("resourceVersion"), str)
        or not isinstance(returned_status, dict)
        or returned_status.get("pendingPodCreate") is not None
    ):
        raise SafetyError(
            "PodCreateOutcomeUnknown", "Kubernetes did not confirm Pod create intent resolution"
        )
    workload["metadata"]["resourceVersion"] = metadata["resourceVersion"]
    status.pop("pendingPodCreate")
    workload["status"] = status
    return True


def _authorize_pending_pod_create(
    api: Any, workload: dict[str, Any], intent: dict[str, Any], pod: dict[str, Any]
) -> None:
    _check_pending_pod(pod, workload, intent)
    annotations = pod["metadata"]["annotations"]
    record = {
        "workloadUID": intent["workloadUID"],
        "podUID": pod["metadata"]["uid"],
        "podName": intent["podName"],
        "replicaID": intent["replicaID"],
        "workloadGeneration": intent["workloadGeneration"],
        "planID": annotations[POD_CREATE_PLAN_ANNOTATION],
        "operationID": annotations[POD_CREATE_OPERATION_ANNOTATION],
        "createID": annotations[POD_CREATE_ATTEMPT_ANNOTATION],
    }
    old_status = workload.get("status", {})
    authorized = list(_authorized_pod_creates(workload))
    if record not in authorized:
        if len(authorized) >= 1_000:
            raise SafetyError(
                "PodCreateHistoryFull", "authorized Pod create history is at capacity"
            )
        if any(
            item.get("podUID") == record["podUID"] for item in authorized if isinstance(item, dict)
        ):
            raise SafetyError(
                "PodCreateIdentityConflict", "Pod UID is bound to another create intent"
            )
        authorized.append(record)
    status = {**old_status, "authorizedPodCreates": authorized, "pendingPodCreate": None}
    result = api.patch_status(
        workload["metadata"]["name"], workload["metadata"]["resourceVersion"], status
    )
    metadata = result.get("metadata", {}) if isinstance(result, dict) else {}
    returned_status = result.get("status") if isinstance(result, dict) else None
    if (
        not isinstance(metadata.get("resourceVersion"), str)
        or not isinstance(returned_status, dict)
        or returned_status.get("pendingPodCreate") is not None
        or record not in returned_status.get("authorizedPodCreates", [])
    ):
        raise SafetyError(
            "PodCreateOutcomeUnknown", "Kubernetes did not durably authorize the observed Pod UID"
        )
    workload["metadata"]["resourceVersion"] = metadata["resourceVersion"]
    workload["status"] = dict(returned_status)
    workload["status"].pop("pendingPodCreate", None)


def _write_retired_slot_tombstone(
    api: Any,
    workload: dict[str, Any],
    pod: dict[str, Any],
    plan_id: str,
    operation_id: str,
    nodes: list[dict[str, Any]],
) -> None:
    # shortcut: CR status cannot fence admitted CREATE; upgrade before enabling retirement.
    if not all(isinstance(value, str) and value for value in (plan_id, operation_id)):
        raise SafetyError("InvalidRetirementIntent", "retirement operation identity is required")
    _, approved_uids = _validate_spec(workload, nodes)
    old_status = workload.get("status", {})
    if old_status.get("pendingPodCreate") is not None or old_status.get(
        "podCreateHistoryUncertain"
    ):
        raise SafetyError(
            "PodCreateHistoryUncertain", "unresolved Pod create history blocks retirement"
        )
    _check_existing_pod(pod, workload)
    supplied_metadata = pod["metadata"]
    supplied_record = {
        "workloadUID": workload["metadata"]["uid"],
        "replicaID": supplied_metadata["labels"][REPLICA_ID_LABEL],
        "podUID": supplied_metadata["uid"],
        "workloadGeneration": int(supplied_metadata["annotations"][WORKLOAD_GENERATION_ANNOTATION]),
        "planID": plan_id,
        "operationID": operation_id,
    }
    retired = list(_retired_replica_slots(workload))
    existing = next(
        (item for item in retired if item["replicaID"] == supplied_record["replicaID"]), None
    )
    if existing is not None and existing != supplied_record:
        raise SafetyError(
            "RetirementIdentityConflict", "logical slot already has another tombstone"
        )
    current = api.get_pod(supplied_metadata["name"])
    if current is None:
        if existing is not None:
            return
        raise SafetyError("RetirementIdentityConflict", "source Pod disappeared before tombstoning")
    if current.get("metadata", {}).get("uid") != supplied_metadata.get("uid"):
        raise SafetyError("RetirementIdentityConflict", "source Pod UID changed before tombstoning")
    _check_existing_pod(current, workload)
    _validate_scheduled_pod(current, approved_uids, nodes)
    metadata = current["metadata"]
    annotations = metadata["annotations"]
    record = {
        "workloadUID": workload["metadata"]["uid"],
        "replicaID": metadata["labels"][REPLICA_ID_LABEL],
        "podUID": metadata["uid"],
        "workloadGeneration": int(annotations[WORKLOAD_GENERATION_ANNOTATION]),
        "planID": plan_id,
        "operationID": operation_id,
    }
    if existing is not None and existing != record:
        raise SafetyError(
            "RetirementIdentityConflict", "logical slot already has another tombstone"
        )
    if existing is not None:
        return
    retired.append(record)
    status = {**old_status, "retiredReplicaSlots": retired}
    result = api.patch_status(
        workload["metadata"]["name"], workload["metadata"]["resourceVersion"], status
    )
    result_metadata = result.get("metadata", {}) if isinstance(result, dict) else {}
    returned_status = result.get("status") if isinstance(result, dict) else None
    if (
        not isinstance(result_metadata.get("resourceVersion"), str)
        or not isinstance(returned_status, dict)
        or record not in returned_status.get("retiredReplicaSlots", [])
    ):
        raise SafetyError(
            "RetirementIntentUnknown", "Kubernetes did not confirm the slot tombstone"
        )
    workload["metadata"]["resourceVersion"] = result_metadata["resourceVersion"]
    workload["status"] = dict(returned_status)


def _ensure_pdb(api: Any, workload: dict[str, Any]) -> None:
    desired = _pdb_object(workload)
    name = desired["metadata"]["name"]
    existing = api.get_pdb(name)
    if existing is None:
        api.create_pdb(desired)
        return
    if not isinstance(existing, dict):
        raise SafetyError("PDBIdentityConflict", "existing PodDisruptionBudget is invalid")
    existing_metadata = existing.get("metadata", {})
    existing_spec = existing.get("spec", {})
    if not isinstance(existing_metadata, dict) or not isinstance(existing_spec, dict):
        raise SafetyError("PDBIdentityConflict", "existing PodDisruptionBudget is invalid")
    if (
        not _is_owned(existing, workload)
        or existing_spec.get("selector") != desired["spec"]["selector"]
    ):
        raise SafetyError(
            "PDBIdentityConflict", "a PodDisruptionBudget with a conflicting identity exists"
        )
    annotations = existing_metadata.get("annotations", {})
    if not isinstance(annotations, dict):
        raise SafetyError("PDBIdentityConflict", "managed PDB annotations are invalid")
    raw_generation = annotations.get(WORKLOAD_GENERATION_ANNOTATION)
    version = existing_metadata.get("resourceVersion")
    if not isinstance(version, str):
        raise SafetyError(
            "PDBIdentityConflict", "existing PodDisruptionBudget has no resourceVersion"
        )
    workload_generation = workload["metadata"]["generation"]
    if raw_generation is None:
        raise SafetyError(
            "PDBGenerationUnknown",
            "managed PodDisruptionBudget has no trusted workload generation",
        )
    if not isinstance(raw_generation, str) or not raw_generation.isdecimal():
        raise SafetyError("PDBIdentityConflict", "managed PDB workload generation is invalid")
    if workload_generation < int(raw_generation):
        raise SafetyError(
            "StaleWorkloadGeneration",
            "stale workload snapshot cannot mutate Pods or its managed PDB",
        )
    if (
        existing_spec.get("selector") != desired["spec"]["selector"]
        or existing_spec.get("maxUnavailable") != desired["spec"]["maxUnavailable"]
        or workload_generation > int(raw_generation)
    ):
        api.patch_pdb(name, version, desired["spec"], desired["metadata"]["annotations"])


def reconcile_workload(api: Any, workload: dict[str, Any], nodes: list[dict[str, Any]]) -> None:
    metadata = workload.get("metadata", {})
    uid = metadata.get("uid")
    if metadata.get("deletionTimestamp"):
        return
    try:
        if not isinstance(uid, str) or not uid:
            raise SafetyError("InvalidIdentity", "workload UID is required")
        pods = api.list_pods(uid)
        _, approved_uids = _validate_spec(workload, nodes)

        pending = workload.get("status", {}).get("pendingPodCreate")
        if pending is not None:
            if (
                not isinstance(pending, dict)
                or not all(
                    isinstance(pending.get(key), str) and pending[key]
                    for key in (
                        "workloadUID",
                        "podName",
                        "replicaID",
                        "planID",
                        "operationID",
                        "createID",
                    )
                )
                or type(pending.get("workloadGeneration")) is not int
            ):
                raise SafetyError(
                    "PodCreateIdentityConflict", "pending Pod create identity is invalid"
                )
            if pending["workloadUID"] != uid:
                raise SafetyError(
                    "PodCreateIdentityConflict", "pending Pod create workload changed"
                )
            pending_pod = api.get_pod(pending["podName"])
            if pending_pod is None:
                return
            _check_pending_pod(pending_pod, workload, pending)
            _validate_scheduled_pod(pending_pod, approved_uids, nodes)
            if all(pod.get("metadata", {}).get("name") != pending["podName"] for pod in pods):
                pods.append(pending_pod)
            _authorize_pending_pod_create(api, workload, pending, pending_pod)

        _validate_execution_request(workload)
        _ensure_pdb(api, workload)
        pods_by_ordinal: dict[int, dict[str, Any]] = {}
        for pod in pods:
            ordinal = _check_existing_pod(pod, workload)
            if ordinal in pods_by_ordinal:
                raise SafetyError(
                    "PodIdentityConflict", "duplicate logical replica identity exists"
                )
            _validate_scheduled_pod(pod, approved_uids, nodes)
            pods_by_ordinal[ordinal] = pod
            if pod.get("status", {}).get("phase") in {"Succeeded", "Failed"} and not pod.get(
                "metadata", {}
            ).get("deletionTimestamp"):
                raise SafetyError(
                    "TerminalPodRequiresStagedExecution",
                    "terminal logical replica requires an explicit replacement protocol",
                )

        _write_execution_phase(api, workload, "RECONCILING")

        desired = workload["spec"]["replicas"]
        retired_ids = {record["replicaID"] for record in _retired_replica_slots(workload)}
        if any(
            f"{workload['spec']['workloadID']}-{ordinal}" in retired_ids
            for ordinal in range(desired)
        ):
            raise SafetyError(
                "RetiredReplicaRequiresNewOperation",
                "ordinary reconciliation cannot complete while a desired slot is retired",
            )
        live_pods = [
            pod for pod in pods if pod.get("status", {}).get("phase") not in {"Succeeded", "Failed"}
        ]
        live_count = len(live_pods)
        ready_replicas = sum(1 for pod in live_pods if _pod_ready(pod))
        if live_count > desired:
            if any(pod.get("metadata", {}).get("deletionTimestamp") for pod in live_pods):
                _write_status(
                    api,
                    workload,
                    pods,
                    "ScaleDownRequiresStagedExecution",
                    "waiting for an in-progress Pod deletion before staged retirement",
                )
                return
            can_preserve_floor = any(
                pod
                for pod in pods_by_ordinal.values()
                if pod.get("status", {}).get("phase") not in {"Succeeded", "Failed"}
                and ready_replicas - int(_pod_ready(pod)) >= workload["spec"]["availabilityFloor"]
            )
            if not can_preserve_floor:
                raise SafetyError(
                    "AvailabilityFloorWouldBeViolated",
                    "no single Pod can be removed while preserving availabilityFloor",
                )
            raise SafetyError(
                "ScaleDownRequiresStagedExecution",
                "nonterminal Pod retirement requires staged PDB-aware execution",
            )

        # Phase 1 ordinary reconciliation never consumes surgeBudget. It is
        # reserved for a future explicit staged-replacement protocol.
        maximum_live = desired
        for ordinal in range(desired):
            if ordinal in pods_by_ordinal:
                continue
            replica_id = f"{workload['spec']['workloadID']}-{ordinal}"
            if any(
                record["replicaID"] == replica_id for record in _retired_replica_slots(workload)
            ):
                raise SafetyError(
                    "RetiredReplicaRequiresNewOperation",
                    "ordinary reconciliation cannot restore a durably retired logical slot",
                )
            if live_count >= maximum_live:
                break
            name = _pod_name(uid, ordinal)
            pod = api.get_pod(name)
            if pod is not None:
                _check_existing_pod(pod, workload)
                _validate_scheduled_pod(pod, approved_uids, nodes)
                pods_by_ordinal[ordinal] = pod
                pods.append(pod)
                live_count += 1
                continue
            intent = _pod_create_intent(workload, ordinal)
            _write_pending_pod_create(api, workload, intent)
            try:
                created = api.create_pod(_pod_object(workload, ordinal, intent))
            except ApiError as error:
                if error.status == 409:
                    existing = api.get_pod(name)
                    if existing is None:
                        raise SafetyError(
                            "PodCreateOutcomeUnknown",
                            "Pod create conflicted but the deterministic name is not observable",
                        ) from error
                    _check_pending_pod(existing, workload, intent)
                    _validate_scheduled_pod(existing, approved_uids, nodes)
                    _authorize_pending_pod_create(api, workload, intent, existing)
                    pods_by_ordinal[ordinal] = existing
                    pods.append(existing)
                    live_count += 1
                    continue
                if error.status in {400, 403, 422, 429}:
                    if not _clear_pending_pod_create(api, workload):
                        return
                    raise SafetyError(
                        "PodCreateRejectedByAPIServer",
                        f"Kubernetes API rejected Pod creation (HTTP {error.status})",
                    ) from error
                raise
            _check_pending_pod(created, workload, intent)
            _authorize_pending_pod_create(api, workload, intent, created)
            pods_by_ordinal[ordinal] = created
            pods.append(created)
            live_count += 1
        if all(ordinal in pods_by_ordinal for ordinal in range(desired)):
            _write_execution_phase(api, workload, "APPLIED")
        _write_status(api, workload, pods, "Reconciled", "workload reconciled from API state")
    except SafetyError as error:
        current_pods = api.list_pods(uid) if isinstance(uid, str) and uid else []
        _write_status(
            api,
            workload,
            current_pods,
            error.reason,
            str(error),
            execution_phase="BLOCKED",
        )


def _validate_scheduled_pod(
    pod: dict[str, Any], approved_uids: dict[str, str], nodes: list[dict[str, Any]]
) -> None:
    terms = (
        pod.get("spec", {})
        .get("affinity", {})
        .get("nodeAffinity", {})
        .get("requiredDuringSchedulingIgnoredDuringExecution", {})
        .get("nodeSelectorTerms")
    )
    actual_allowed: set[str] = set()
    if isinstance(terms, list):
        for term in terms:
            fields = term.get("matchFields", []) if isinstance(term, dict) else []
            if (
                not isinstance(fields, list)
                or len(fields) != 1
                or not isinstance(fields[0], dict)
                or fields[0].get("key") != "metadata.name"
                or fields[0].get("operator") != "In"
                or not isinstance(fields[0].get("values"), list)
                or len(fields[0]["values"]) != 1
                or not isinstance(fields[0]["values"][0], str)
            ):
                raise SafetyError("PodOutsideAllowedNodes", "Pod allowed placement is invalid")
            actual_allowed.add(fields[0]["values"][0])
    if actual_allowed != set(approved_uids):
        raise SafetyError("PodOutsideAllowedNodes", "Pod allowed placement differs from workload")
    scheduled_name = pod.get("spec", {}).get("nodeName")
    if not scheduled_name:
        return
    if scheduled_name not in approved_uids:
        raise SafetyError("PodOutsideAllowedNodes", "Pod is scheduled outside the allowed Node set")
    current_node = next(
        (node for node in nodes if node.get("metadata", {}).get("name") == scheduled_name),
        None,
    )
    if (
        current_node is None
        or current_node.get("metadata", {}).get("uid") != approved_uids[scheduled_name]
    ):
        raise SafetyError(
            "NodeIdentityChanged", "scheduled Pod Node UID differs from approved identity"
        )


def run() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    namespace = os.environ.get("POD_NAMESPACE", "fleetright-v2")
    if not _DNS_LABEL.fullmatch(namespace):
        raise RuntimeError("POD_NAMESPACE must be a DNS label")
    api = KubernetesAPI(namespace)
    while True:
        try:
            workloads = api.list_workloads()
            nodes = api.list_nodes()
            for workload in workloads:
                try:
                    reconcile_workload(api, workload, nodes)
                except Exception:
                    logging.exception("FleetRightWorkload reconciliation failed")
        except Exception:
            logging.exception("FleetRightWorkload list failed")
        time.sleep(2)


if __name__ == "__main__":
    run()
