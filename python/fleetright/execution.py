from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import re
import stat
import subprocess
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from .workload_controller import (
    POD_CREATE_ATTEMPT_ANNOTATION,
    POD_CREATE_OPERATION_ANNOTATION,
    POD_CREATE_PLAN_ANNOTATION,
    REPLICA_ID_LABEL,
    WORKLOAD_GENERATION_ANNOTATION,
    WORKLOAD_UID_LABEL,
    SafetyError,
    _pod_ready_stable,
    _write_retired_slot_tombstone,
)

_DNS_LABEL = re.compile(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?\Z")
_DNS_SUBDOMAIN = re.compile(
    r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?(?:\.[a-z0-9](?:[-a-z0-9]*[a-z0-9])?)*\Z"
)
_CONTAINER_ID = re.compile(r"[0-9a-f]{12,64}\Z")
_WORKLOAD_SERVICES = {
    "cpu-api": "cpu-service",
    "memory-api": "memory-service",
    "queued-api": "queued-service",
}
_WORKLOAD_EXECUTION_RULES = {
    "self.workloadID == oldSelf.workloadID",
    "self.podTemplate == oldSelf.podTemplate",
    "self.placement == oldSelf.placement",
    "self.availabilityFloor == oldSelf.availabilityFloor",
    "self.disruption == oldSelf.disruption",
    "self.surgeBudget == oldSelf.surgeBudget",
    "self.replicas >= oldSelf.replicas",
    "!has(self.executionRequest) || self.executionProtocol == 'monotonic-scale-up-v1'",
    "!has(oldSelf.executionProtocol) || self.executionProtocol == oldSelf.executionProtocol",
    "has(oldSelf.executionProtocol) || !has(self.executionProtocol)",
    "!has(oldSelf.executionRequest) || has(self.executionRequest)",
    (
        "!has(oldSelf.executionRequest) || !has(self.executionRequest) || "
        "self.executionRequest.generation >= oldSelf.executionRequest.generation"
    ),
    (
        "!has(oldSelf.executionRequest) || !has(self.executionRequest) || "
        "(self.executionRequest.planID == oldSelf.executionRequest.planID && "
        "self.executionRequest.operationID == oldSelf.executionRequest.operationID) || "
        "self.executionRequest.generation > oldSelf.executionRequest.generation"
    ),
    (
        "!has(oldSelf.executionRequest) || !has(self.executionRequest) || "
        "self.executionRequest.generation > oldSelf.executionRequest.generation || "
        "self.executionRequest == oldSelf.executionRequest"
    ),
    (
        "!has(oldSelf.executionRequest) || !has(self.executionRequest) || "
        "oldSelf.executionRequest.planID != self.executionRequest.planID || "
        "oldSelf.executionRequest.operationID != self.executionRequest.operationID || "
        "(has(oldSelf.executionRequest.minimumReadyReplicas) == "
        "has(self.executionRequest.minimumReadyReplicas) && "
        "(!has(oldSelf.executionRequest.minimumReadyReplicas) || "
        "self.executionRequest.minimumReadyReplicas == "
        "oldSelf.executionRequest.minimumReadyReplicas))"
    ),
    "!has(self.executionRequest) || self.executionRequest.targetReplicas == self.replicas",
    (
        "!has(self.executionRequest) || self.executionRequest.purpose != "
        "'staged-replacement-v1' || "
        "(has(self.executionRequest.minimumReadyReplicas) && "
        "self.executionRequest.minimumReadyReplicas >= self.availabilityFloor && "
        "self.executionRequest.minimumReadyReplicas <= self.executionRequest.targetReplicas)"
    ),
    (
        "!has(self.executionRequest) || self.executionRequest.purpose != "
        "'staged-replacement-v1' || "
        "(has(self.placement.requirePodAntiAffinity) && "
        "self.placement.requirePodAntiAffinity)"
    ),
    (
        "self.replicas <= oldSelf.replicas || (self.executionProtocol == "
        "'monotonic-scale-up-v1' && has(self.executionRequest) && "
        "self.executionRequest.expectedReplicas == oldSelf.replicas && "
        "self.executionRequest.targetReplicas == self.replicas && "
        "(!has(oldSelf.executionRequest) || self.executionRequest.generation > "
        "oldSelf.executionRequest.generation))"
    ),
    "self.availabilityFloor <= self.replicas",
    "!has(oldSelf.executionFence) || has(self.executionFence)",
    (
        "!has(oldSelf.executionFence) || !has(self.executionFence) || "
        "self.executionFence.generation >= oldSelf.executionFence.generation"
    ),
    (
        "!has(oldSelf.executionFence) || !has(self.executionFence) || "
        "self.executionFence.generation > oldSelf.executionFence.generation || "
        "(self.executionFence.generation == oldSelf.executionFence.generation && "
        "self.executionFence.planID == oldSelf.executionFence.planID && "
        "self.executionFence.operationID == oldSelf.executionFence.operationID && "
        "self.executionFence.expectedGeneration == oldSelf.executionFence.expectedGeneration && "
        "self.executionFence.expectedReplicas == oldSelf.executionFence.expectedReplicas && "
        "self.executionFence.fencedGeneration == oldSelf.executionFence.fencedGeneration)"
    ),
    (
        "!has(oldSelf.executionFence) || !has(self.executionFence) || "
        "self.executionFence.generation == oldSelf.executionFence.generation || "
        "self.executionFence.fencedGeneration > oldSelf.executionFence.fencedGeneration"
    ),
    (
        "!has(self.executionRequest) || "
        "(has(oldSelf.executionRequest) && self.executionRequest == oldSelf.executionRequest) || "
        "(has(self.executionFence) && "
        "self.executionRequest.generation == self.executionFence.generation && "
        "self.executionRequest.planID == self.executionFence.planID && "
        "self.executionRequest.operationID == self.executionFence.operationID)"
    ),
    (
        "!has(oldSelf.executionFence) || !has(self.executionRequest) || "
        "(has(oldSelf.executionRequest) && self.executionRequest == oldSelf.executionRequest) || "
        "self.executionRequest.generation > oldSelf.executionFence.generation"
    ),
    (
        "!has(self.executionFence) || !has(self.executionRequest) || "
        "self.executionFence.generation >= self.executionRequest.generation"
    ),
    (
        "!has(self.executionFence) || !has(self.executionRequest) || "
        "self.executionFence.generation != self.executionRequest.generation || "
        "(self.executionFence.planID == self.executionRequest.planID && "
        "self.executionFence.operationID == self.executionRequest.operationID)"
    ),
}
_WORKLOAD_PENDING_CREATE_RULE = (
    "!has(oldSelf.status) || !has(oldSelf.status.pendingPodCreate) || self.spec == oldSelf.spec"
)
_WORKLOAD_POD_CREATE_PROTOCOL_RULE = (
    "oldSelf.hasValue() ? (!has(oldSelf.value().podCreateProtocol) ? "
    "!has(self.podCreateProtocol) : "
    "self.podCreateProtocol == oldSelf.value().podCreateProtocol) : "
    "self.podCreateProtocol == 'reserved-create-v1'"
)
_WORKLOAD_AUTHORIZED_POD_CREATES_RULE = (
    "!has(oldSelf.status) || !has(oldSelf.status.authorizedPodCreates) || "
    "(has(self.status) && has(self.status.authorizedPodCreates) && "
    "self.status.authorizedPodCreates + oldSelf.status.authorizedPodCreates == "
    "self.status.authorizedPodCreates)"
)
_WORKLOAD_RETIRED_SLOTS_RULE = (
    "!has(oldSelf.status) || !has(oldSelf.status.retiredReplicaSlots) || "
    "(has(self.status) && has(self.status.retiredReplicaSlots) && "
    "self.status.retiredReplicaSlots + oldSelf.status.retiredReplicaSlots == "
    "self.status.retiredReplicaSlots)"
)
_WORKLOAD_CREATE_HISTORY_UNCERTAIN_RULE = (
    "!has(oldSelf.status) || !has(oldSelf.status.podCreateHistoryUncertain) || "
    "!oldSelf.status.podCreateHistoryUncertain || "
    "(has(self.status) && has(self.status.podCreateHistoryUncertain) && "
    "self.status.podCreateHistoryUncertain)"
)

DEFAULT_EXECUTION_HEALTH_CEILING = (120, 1_000)
EXECUTION_HEALTH_CEILINGS = {
    "cpu-api": (120, 1_000),
    "memory-api": (150, 1_000),
    "queued-api": (80, 1_000),
}


def valid_execution_health_policy(workload_id: str, policy: object) -> bool:
    ceiling = EXECUTION_HEALTH_CEILINGS.get(workload_id, DEFAULT_EXECUTION_HEALTH_CEILING)
    if not isinstance(policy, dict):
        return False
    latency, errors = policy.get("p99_limit_ms"), policy.get("error_slo_ppm")
    return (
        type(latency) is int
        and 0 < latency <= ceiling[0]
        and type(errors) is int
        and 0 <= errors <= ceiling[1]
        and type(policy.get("max_queue_growth_milli")) is int
        and policy.get("max_queue_growth_milli") == 0
    )


def bounded_execution_health_policy(
    workload_id: str, p99_limit_ms: object, error_slo_ppm: object
) -> dict[str, int] | None:
    ceiling = EXECUTION_HEALTH_CEILINGS.get(workload_id, DEFAULT_EXECUTION_HEALTH_CEILING)
    if type(p99_limit_ms) is not int or p99_limit_ms <= 0:
        return None
    if type(error_slo_ppm) is not int or not 0 <= error_slo_ppm <= 1_000_000:
        return None
    return {
        "p99_limit_ms": min(p99_limit_ms, ceiling[0]),
        "error_slo_ppm": min(error_slo_ppm, ceiling[1]),
        "max_queue_growth_milli": 0,
    }


class TargetError(RuntimeError):
    pass


class TargetUnavailable(TargetError):
    pass


class TargetPreconditionFailed(TargetError):
    pass


class TargetNotFound(TargetError):
    pass


class OutcomeUnknown(TargetError):
    pass


class ActionBlocked(TargetError):
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True)
class CommandResult:
    stdout: str
    stderr: str


def _checked_name(value: object, label: str) -> str:
    if not isinstance(value, str) or len(value) > 63 or not _DNS_LABEL.fullmatch(value):
        raise TargetPreconditionFailed(f"invalid {label}")
    return value


def _checked_pod_name(value: object) -> str:
    if not isinstance(value, str) or len(value) > 253 or not _DNS_SUBDOMAIN.fullmatch(value):
        raise TargetPreconditionFailed("invalid source Pod name")
    return value


def _run(command: list[str], *, timeout: float = 10, stdin: str | None = None) -> CommandResult:
    try:
        result = subprocess.run(
            command,
            input=stdin,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError as error:
        raise TargetUnavailable("target client is not installed") from error
    except subprocess.TimeoutExpired as error:
        raise OutcomeUnknown("target request timed out; observe before retry") from error
    if result.returncode:
        message = result.stderr.lower()
        if (
            "disruption budget" in message
            or "eviction managed pdb evidence is stale or blocks the effective floor" in message
        ):
            raise ActionBlocked("PDB_BLOCKED")
        if "too many requests" in message or "toomanyrequests" in message:
            raise TargetUnavailable("Kubernetes API throttled the request")
        if (
            "conflict" in message
            or "the object has been modified" in message
            or "testing value" in message
            or "test failed" in message
        ):
            raise TargetPreconditionFailed("target resource version changed")
        if "not found" in message:
            raise TargetNotFound("target resource no longer exists")
        raise TargetError("target rejected the operation")
    return CommandResult(result.stdout, result.stderr)


class KubernetesTarget:
    """Deployment replica-scale adapter; V2 Pod and Node mutations are unsupported."""

    def __init__(
        self,
        *,
        kubectl: str | None = None,
        namespace: str | None = None,
        kubeconfig: str | None = None,
    ):
        self.kubectl = kubectl or os.getenv("FLEETRIGHT_KUBECTL", "kubectl")
        self.kubeconfig = kubeconfig or os.getenv("FLEETRIGHT_KUBERNETES_KUBECONFIG")
        self.namespace = _checked_name(
            namespace or os.getenv("FLEETRIGHT_KUBERNETES_NAMESPACE", "default"), "namespace"
        )

    def _kubectl(self, *args: str, stdin: str | None = None) -> CommandResult:
        context = os.getenv("FLEETRIGHT_KUBERNETES_CONTEXT", "")
        command = [self.kubectl, "--request-timeout=10s"]
        if self.kubeconfig:
            command.extend(("--kubeconfig", self.kubeconfig))
        if context:
            if len(context) > 128 or not re.fullmatch(r"[A-Za-z0-9._:/-]+", context):
                raise TargetPreconditionFailed("invalid Kubernetes context")
            command.extend(("--context", context))
        return _run([*command, *args], stdin=stdin)

    def _json(self, *args: str) -> dict[str, Any]:
        try:
            value = json.loads(self._kubectl(*args).stdout)
        except json.JSONDecodeError as error:
            raise TargetError("target returned invalid JSON") from error
        if not isinstance(value, dict):
            raise TargetError("target returned an unexpected object")
        return value

    def get_pod(self, name: str) -> dict[str, Any] | None:
        try:
            return self._json("get", "pod", name, "-n", self.namespace, "-o", "json")
        except TargetNotFound:
            return None

    def patch_status(
        self, name: str, resource_version: str, status: dict[str, Any]
    ) -> dict[str, Any]:
        return self._json(
            "patch",
            "fleetrightworkload",
            _checked_name(name, "workload identifier"),
            "-n",
            self.namespace,
            "--subresource=status",
            "--type=merge",
            "-p",
            json.dumps(
                {"metadata": {"resourceVersion": resource_version}, "status": status},
                separators=(",", ":"),
            ),
            "-o",
            "json",
        )

    def deployment(self, workload_id: str) -> dict[str, Any]:
        name = _checked_name(workload_id, "workload identifier")
        return self._json("get", "deployment", name, "-n", self.namespace, "-o", "json")

    def observe_scale(self, workload_id: str) -> dict[str, Any]:
        return self._scale_observation(self.deployment(workload_id))

    @staticmethod
    def _scale_observation(value: dict[str, Any]) -> dict[str, Any]:
        metadata, spec, status = (
            value.get("metadata", {}),
            value.get("spec", {}),
            value.get("status", {}),
        )
        if (
            not isinstance(metadata, dict)
            or not isinstance(spec, dict)
            or not isinstance(status, dict)
        ):
            raise TargetError("Deployment response is incomplete")
        replicas = spec.get("replicas", 1)
        ready = status.get("readyReplicas", 0)
        available = status.get("availableReplicas", 0)
        observed_generation = status.get("observedGeneration", 0)
        if not all(isinstance(item, int) and item >= 0 for item in (replicas, ready, available)):
            raise TargetError("Deployment replica status is invalid")
        if not isinstance(observed_generation, int) or observed_generation < 0:
            raise TargetError("Deployment observed generation is invalid")
        uid, generation, version = (
            metadata.get("uid"),
            metadata.get("generation"),
            metadata.get("resourceVersion"),
        )
        annotations = metadata.get("annotations", {})
        if (
            not isinstance(uid, str)
            or not isinstance(generation, int)
            or not isinstance(version, str)
            or not isinstance(annotations, dict)
        ):
            raise TargetError("Deployment identity/version is missing")
        raw_fence = annotations.get("fleetright.io/fencing-token", "0")
        if not isinstance(raw_fence, str) or not raw_fence.isdecimal():
            raise TargetError("Deployment fencing token is invalid")
        return {
            "uid": uid,
            "generation": generation,
            "resource_version": version,
            "replicas": replicas,
            "ready_replicas": ready,
            "available_replicas": available,
            "observed_generation": observed_generation,
            "fencing_token": int(raw_fence),
            "generation_tracks_spec": True,
            "annotations_present": "annotations" in metadata,
        }

    def fence_scale(
        self,
        workload_id: str,
        fencing_token: int,
        *,
        expected: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        current = self.observe_scale(workload_id)
        if expected is not None:
            identity_matches = current.get("uid") == expected.get("uid") and current.get(
                "replicas"
            ) == expected.get("replicas")
            generation_matches = current.get("generation") == expected.get("generation")
            prior_fence_applied = (
                current["fencing_token"] == fencing_token
                and isinstance(expected.get("generation"), int)
                and current.get("generation") == expected["generation"] + 1
            )
            if not identity_matches or not (generation_matches or prior_fence_applied):
                raise TargetPreconditionFailed("Deployment changed before fencing")
        if fencing_token <= 0 or current["fencing_token"] > fencing_token:
            raise TargetPreconditionFailed("a newer controller already fenced this Deployment")
        precondition = (
            {key: expected[key] for key in ("uid", "generation", "replicas")}
            if expected is not None and current["fencing_token"] == fencing_token
            else {key: current[key] for key in ("uid", "generation", "replicas")}
        )
        if current["fencing_token"] == fencing_token:
            return {**current, "fence_precondition": precondition}
        patch = json.dumps(
            [
                {
                    "op": "test",
                    "path": "/metadata/resourceVersion",
                    "value": current["resource_version"],
                },
                {"op": "test", "path": "/metadata/uid", "value": current["uid"]},
                {"op": "test", "path": "/metadata/generation", "value": current["generation"]},
                self._fence_annotation_op(current, fencing_token),
            ],
            separators=(",", ":"),
        )
        patched = self._json(
            "patch",
            "deployment",
            _checked_name(workload_id, "workload identifier"),
            "-n",
            self.namespace,
            "-o",
            "json",
            "--type=json",
            "-p",
            patch,
        )
        fenced = self._scale_observation(patched)
        if (
            fenced["uid"] != current["uid"]
            or fenced["replicas"] != current["replicas"]
            or fenced["fencing_token"] != fencing_token
        ):
            raise TargetPreconditionFailed("Deployment changed while publishing fence")
        return {**fenced, "fence_precondition": precondition}

    def set_replicas(
        self,
        workload_id: str,
        replicas: int,
        observed: dict[str, Any],
        *,
        expected_replicas: int,
        fencing_token: int,
    ) -> dict[str, Any]:
        if not isinstance(replicas, int) or replicas < 0 or replicas > 1000:
            raise TargetPreconditionFailed("replica target is outside the supported range")
        if not isinstance(fencing_token, int) or fencing_token <= 0:
            raise TargetPreconditionFailed("fencing token must be positive")
        current = self.observe_scale(workload_id)
        if current["uid"] != observed["uid"] or current["generation"] != observed["generation"]:
            raise TargetPreconditionFailed("Deployment identity changed")
        if current["fencing_token"] > fencing_token:
            raise TargetPreconditionFailed("a newer controller fenced this Deployment")
        if current["replicas"] == replicas and current["fencing_token"] == fencing_token:
            return {**current, "already_applied": True}
        if current["replicas"] != expected_replicas:
            raise TargetPreconditionFailed("Deployment replica precondition changed")
        # JSON Patch tests make this update conditional on the exact object version and UID.
        patch = json.dumps(
            [
                {
                    "op": "test",
                    "path": "/metadata/resourceVersion",
                    "value": current["resource_version"],
                },
                {"op": "test", "path": "/metadata/uid", "value": current["uid"]},
                {"op": "test", "path": "/metadata/generation", "value": current["generation"]},
                *(
                    [
                        {
                            "op": "test",
                            "path": "/metadata/annotations/fleetright.io~1fencing-token",
                            "value": str(fencing_token),
                        }
                    ]
                    if current["fencing_token"] == fencing_token
                    else [self._fence_annotation_op(current, fencing_token)]
                ),
                {"op": "replace", "path": "/spec/replicas", "value": replicas},
            ],
            separators=(",", ":"),
        )
        value = self._json(
            "patch",
            "deployment",
            _checked_name(workload_id, "workload identifier"),
            "-n",
            self.namespace,
            "-o",
            "json",
            "--type=json",
            "-p",
            patch,
        )
        metadata = value.get("metadata", {})
        if not isinstance(metadata, dict):
            raise OutcomeUnknown("Deployment patch response omitted its target metadata")
        generation = metadata.get("generation")
        resource_version = metadata.get("resourceVersion")
        if (
            not isinstance(metadata.get("uid"), str)
            or not isinstance(generation, int)
            or not isinstance(resource_version, str)
        ):
            raise OutcomeUnknown("Deployment patch response omitted its target version")
        return {
            "uid": metadata["uid"],
            "generation": generation,
            "resource_version": resource_version,
            "fencing_token": fencing_token,
            "replicas": replicas,
            "applied": True,
        }

    @staticmethod
    def _fence_annotation_op(observed: dict[str, Any], fencing_token: int) -> dict[str, Any]:
        if fencing_token <= 0:
            raise TargetPreconditionFailed("fencing token must be positive")
        if observed.get("annotations_present"):
            return {
                "op": "add",
                "path": "/metadata/annotations/fleetright.io~1fencing-token",
                "value": str(fencing_token),
            }
        return {
            "op": "add",
            "path": "/metadata/annotations",
            "value": {"fleetright.io/fencing-token": str(fencing_token)},
        }


class FleetRightWorkloadTarget(KubernetesTarget):
    """Publish monotonic requests; the workload controller alone creates Pods."""

    supports_workload_execution = True
    source_retirement_preparation_enabled = True

    def _observe_source_node(self, plan_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        name = _checked_name(payload.get("source_node_id"), "source Node name")
        if (
            not isinstance(plan_id, str)
            or not plan_id
            or not isinstance(payload.get("operation_id"), str)
            or not payload["operation_id"]
            or not isinstance(payload.get("source_node_uid"), str)
            or not payload["source_node_uid"]
        ):
            raise TargetPreconditionFailed("Node drain identity is incomplete")
        node = self._json("get", "node", name, "-o", "json")
        metadata, spec = node.get("metadata"), node.get("spec")
        if not isinstance(metadata, dict) or not isinstance(spec, dict):
            raise TargetError("Node observation is incomplete")
        if metadata.get("name") != name or metadata.get("uid") != payload["source_node_uid"]:
            raise ActionBlocked("DRAIN_NODE_UID_CHANGED")
        version = metadata.get("resourceVersion")
        annotations = metadata.get("annotations", {})
        if not isinstance(version, str) or not version or not isinstance(annotations, dict):
            raise TargetError("Node observation is incomplete")
        token = annotations.get("fleetright.io/fencing-token", "0")
        if not isinstance(token, str) or not token.isdecimal():
            raise ActionBlocked("DRAIN_NODE_FENCE_INVALID")
        owner = (
            annotations.get("fleetright.io/plan-id"),
            annotations.get("fleetright.io/operation-id"),
        )
        if owner != (None, None) and owner != (plan_id, payload["operation_id"]):
            raise ActionBlocked("DRAIN_NODE_OPERATION_CONFLICT")
        if int(token) and owner == (None, None):
            raise ActionBlocked("DRAIN_NODE_FENCE_INVALID")
        return {
            "source_node_id": name,
            "source_node_uid": metadata["uid"],
            "resource_version": version,
            "fencing_token": int(token),
            "plan_id": owner[0],
            "operation_id": owner[1],
            "unschedulable": spec.get("unschedulable") is True,
            "annotations": annotations,
            "annotations_present": "annotations" in metadata,
        }

    def cordon_source_node(
        self, plan_id: str, payload: dict[str, Any], fencing_token: int
    ) -> dict[str, Any]:
        if type(fencing_token) is not int or fencing_token <= 0:
            raise TargetPreconditionFailed("Node drain fencing token must be positive")
        current = self._observe_source_node(plan_id, payload)
        if current["fencing_token"] > fencing_token:
            raise TargetPreconditionFailed("a newer controller fenced this Node drain")
        if current["fencing_token"] == fencing_token and current["unschedulable"]:
            return {**current, "stage": "NODE_CORDONED", "already_applied": True}
        annotations = {
            **current["annotations"],
            "fleetright.io/plan-id": plan_id,
            "fleetright.io/operation-id": payload["operation_id"],
            "fleetright.io/fencing-token": str(fencing_token),
        }
        patch = [
            {"op": "test", "path": "/metadata/uid", "value": current["source_node_uid"]},
            {
                "op": "test",
                "path": "/metadata/resourceVersion",
                "value": current["resource_version"],
            },
            {
                "op": "replace" if current["annotations_present"] else "add",
                "path": "/metadata/annotations",
                "value": annotations,
            },
            {"op": "add", "path": "/spec/unschedulable", "value": True},
        ]
        self._json(
            "patch",
            "node",
            current["source_node_id"],
            "-o",
            "json",
            "--type=json",
            "-p",
            json.dumps(patch),
        )
        observed = self._observe_source_node(plan_id, payload)
        if not observed["unschedulable"] or observed["fencing_token"] != fencing_token:
            raise TargetPreconditionFailed("Node drain authority changed after cordon")
        return {**observed, "stage": "NODE_CORDONED", "already_applied": False}

    def observe_source_node_drain(
        self, plan_id: str, payload: dict[str, Any], fencing_token: int
    ) -> dict[str, Any]:
        if type(fencing_token) is not int or fencing_token <= 0:
            raise TargetPreconditionFailed("Node drain fencing token must be positive")
        node_payload = {
            **payload,
            "operation_id": payload.get("cordon_operation_id", payload.get("operation_id")),
        }
        current = self._observe_source_node(plan_id, node_payload)
        if (
            not current["unschedulable"]
            or current["plan_id"] != plan_id
            or current["fencing_token"] != fencing_token
        ):
            raise TargetPreconditionFailed("Node drain authority is not held by this controller")
        sources = payload.get("source_pods")
        if not isinstance(sources, list) or not sources:
            raise TargetPreconditionFailed("Node drain selected Pod inventory is empty")
        selected: dict[str, tuple[str, str]] = {}
        for source in sources:
            if not isinstance(source, dict):
                raise TargetPreconditionFailed("Node drain selected Pod identity is invalid")
            name = _checked_pod_name(source.get("pod_name"))
            workload = _checked_name(source.get("workload_id"), "workload name")
            uid = source.get("pod_uid")
            if not isinstance(uid, str) or not uid or uid in selected:
                raise TargetPreconditionFailed("Node drain selected Pod identity is invalid")
            selected[uid] = (name, workload)
        inventory = self._json("get", "pods", "--all-namespaces", "-o", "json")
        items, metadata = inventory.get("items"), inventory.get("metadata", {})
        if (
            not isinstance(items, list)
            or not isinstance(metadata, dict)
            or metadata.get("continue")
        ):
            raise TargetError("Node drain Pod inventory is incomplete")
        daemonset_inventory = self._json("get", "daemonsets", "--all-namespaces", "-o", "json")
        daemonsets, daemonset_metadata = (
            daemonset_inventory.get("items"),
            daemonset_inventory.get("metadata", {}),
        )
        if (
            not isinstance(daemonsets, list)
            or not isinstance(daemonset_metadata, dict)
            or daemonset_metadata.get("continue")
        ):
            raise TargetError("Node drain DaemonSet inventory is incomplete")
        daemonset_by_owner: dict[tuple[str, str, str], dict[str, Any]] = {}
        for daemonset in daemonsets:
            if not isinstance(daemonset, dict):
                raise TargetError("Node drain DaemonSet inventory is invalid")
            ds_metadata = daemonset.get("metadata")
            if not isinstance(ds_metadata, dict):
                raise TargetError("Node drain DaemonSet inventory is invalid")
            if all(
                isinstance(ds_metadata.get(key), str) and ds_metadata[key]
                for key in ("name", "namespace", "uid")
            ):
                daemonset_by_owner[
                    (ds_metadata["namespace"], ds_metadata["name"], ds_metadata["uid"])
                ] = daemonset
        remaining, exempt = [], []
        for pod in items:
            if not isinstance(pod, dict):
                raise TargetError("Node drain Pod inventory is invalid")
            meta, spec = pod.get("metadata"), pod.get("spec")
            if not isinstance(meta, dict) or not isinstance(spec, dict):
                raise TargetError("Node drain Pod inventory is invalid")
            if meta.get("uid") in selected:
                expected_name, _ = selected[meta["uid"]]
                if meta.get("name") != expected_name or meta.get("namespace") != self.namespace:
                    raise ActionBlocked("DRAIN_SELECTED_POD_IDENTITY_CHANGED")
                remaining.append(meta["uid"])
                continue
            annotations, owners = meta.get("annotations", {}), meta.get("ownerReferences", [])
            labels = meta.get("labels", {})
            status = pod.get("status", {})
            if (
                not isinstance(annotations, dict)
                or not isinstance(labels, dict)
                or not isinstance(owners, list)
                or not isinstance(status, dict)
            ):
                raise TargetError("Node drain Pod classification is invalid")
            node_name = spec.get("nodeName", "")
            if not isinstance(node_name, str):
                raise TargetError("Node drain Pod placement is invalid")
            phase = status.get("phase")
            if phase in {"Succeeded", "Failed"}:
                continue
            classification = None
            if annotations.get("kubernetes.io/config.mirror"):
                if node_name == current["source_node_id"] and any(
                    isinstance(owner, dict)
                    and owner.get("apiVersion") == "v1"
                    and owner.get("kind") == "Node"
                    and owner.get("name") == current["source_node_id"]
                    and owner.get("uid") == current["source_node_uid"]
                    for owner in owners
                ):
                    classification = "STATIC_MIRROR"
            else:
                for owner in owners:
                    if not isinstance(owner, dict) or (
                        owner.get("apiVersion") != "apps/v1"
                        or owner.get("kind") != "DaemonSet"
                        or owner.get("controller") is not True
                    ):
                        continue
                    daemonset = daemonset_by_owner.get(
                        (meta.get("namespace"), owner.get("name"), owner.get("uid"))
                    )
                    ds_spec = daemonset.get("spec") if isinstance(daemonset, dict) else None
                    selector = ds_spec.get("selector") if isinstance(ds_spec, dict) else None
                    match_labels = (
                        selector.get("matchLabels") if isinstance(selector, dict) else None
                    )
                    expressions = (
                        selector.get("matchExpressions", []) if isinstance(selector, dict) else None
                    )
                    selector_matches = isinstance(match_labels, dict) and all(
                        isinstance(key, str) and isinstance(value, str) and labels.get(key) == value
                        for key, value in match_labels.items()
                    )
                    if selector_matches and isinstance(expressions, list):
                        for requirement in expressions:
                            if not isinstance(requirement, dict):
                                selector_matches = False
                                break
                            key, operator, values = (
                                requirement.get("key"),
                                requirement.get("operator"),
                                requirement.get("values", []),
                            )
                            value_present = key in labels if isinstance(key, str) else False
                            value = labels.get(key) if value_present else None
                            if operator == "In":
                                selector_matches &= value_present and value in values
                            elif operator == "NotIn":
                                selector_matches &= not value_present or value not in values
                            elif operator == "Exists":
                                selector_matches &= value_present
                            elif operator == "DoesNotExist":
                                selector_matches &= not value_present
                            else:
                                selector_matches = False
                                break
                    if selector_matches:
                        classification = "DAEMONSET"
                        break
            if not node_name:
                if classification is None:
                    raise ActionBlocked("DRAIN_UNBOUND_POD_MAY_BIND_TO_SOURCE_NODE")
                continue
            if node_name != current["source_node_id"]:
                continue
            if classification is None:
                raise ActionBlocked("DRAIN_UNSUPPORTED_POD_ON_SOURCE_NODE")
            exempt.append(
                {
                    "namespace": meta.get("namespace"),
                    "pod_name": meta.get("name"),
                    "pod_uid": meta.get("uid"),
                    "classification": classification,
                }
            )
        after = self._observe_source_node(plan_id, node_payload)
        if not after["unschedulable"] or after["fencing_token"] != fencing_token:
            raise TargetPreconditionFailed("Node drain authority changed during inventory")
        return {
            **after,
            "stage": "NODE_DRAIN_WAITING" if remaining else "NODE_DRAIN_COMPLETE",
            "remaining_source_pod_uids": remaining,
            "exempt_pods": exempt,
            "inventory_pod_count": len(items),
        }

    @staticmethod
    def _check_retired_source(
        current: dict[str, Any],
        replica_id: str | None,
        pod_uid: str | None,
        plan_id: str,
        operation_id: str,
    ) -> None:
        if replica_id is None:
            return
        retired = [
            item for item in current["retired_replica_slots"] if item.get("replicaID") == replica_id
        ]
        if retired and (
            len(retired) != 1
            or retired[0].get("workloadUID") != current["uid"]
            or retired[0].get("podUID") != pod_uid
            or retired[0].get("planID") != plan_id
            or retired[0].get("operationID") != operation_id
        ):
            raise ActionBlocked("SOURCE_REPLICA_ALREADY_RETIRED")

    def _require_pdb_eviction_ready(
        self,
        workload_id: str,
        workload_uid: str,
        workload_generation: int,
        minimum_ready: int,
        desired_replicas: int,
        source_ready: bool,
    ) -> str | None:
        try:
            pdb = self._json("get", "pdb", workload_id, "-n", self.namespace, "-o", "json")
        except TargetNotFound:
            return "PDB_STATUS_PENDING"
        metadata = pdb.get("metadata", {}) if isinstance(pdb, dict) else {}
        spec = pdb.get("spec", {}) if isinstance(pdb, dict) else {}
        status = pdb.get("status", {}) if isinstance(pdb, dict) else {}
        annotations = metadata.get("annotations", {}) if isinstance(metadata, dict) else {}
        owners = metadata.get("ownerReferences", []) if isinstance(metadata, dict) else []
        owner_matches = any(
            isinstance(owner, dict)
            and owner.get("apiVersion") == "fleetright.io/v1alpha1"
            and owner.get("kind") == "FleetRightWorkload"
            and owner.get("name") == workload_id
            and owner.get("uid") == workload_uid
            and owner.get("controller") is True
            for owner in owners
        )
        selector = spec.get("selector", {}) if isinstance(spec, dict) else {}
        match_labels = selector.get("matchLabels", {}) if isinstance(selector, dict) else {}
        observed = status.get("observedGeneration") if isinstance(status, dict) else None
        desired = status.get("desiredHealthy") if isinstance(status, dict) else None
        healthy = status.get("currentHealthy") if isinstance(status, dict) else None
        allowed = status.get("disruptionsAllowed") if isinstance(status, dict) else None
        expected = status.get("expectedPods") if isinstance(status, dict) else None
        generation = metadata.get("generation") if isinstance(metadata, dict) else None
        policy = spec.get("unhealthyPodEvictionPolicy") if isinstance(spec, dict) else None
        if (
            not isinstance(metadata, dict)
            or metadata.get("name") != workload_id
            or type(generation) is not int
            or observed != generation
            or annotations.get(WORKLOAD_GENERATION_ANNOTATION) != str(workload_generation)
            or not owner_matches
            or match_labels != {"fleetright.io/workload-uid": workload_uid}
            or policy != "IfHealthyBudget"
            or type(desired) is not int
            or type(healthy) is not int
            or type(allowed) is not int
            or type(expected) is not int
            or expected < desired_replicas
            or status.get("disruptedPods")
        ):
            return "PDB_STATUS_PENDING"
        if desired < minimum_ready or healthy < desired:
            return "PDB_BLOCKED"
        if source_ready and (allowed < 1 or healthy <= desired):
            return "PDB_BLOCKED"
        return None

    def prepare_source_retirement(
        self,
        workload_id: str,
        plan_id: str,
        payload: dict[str, Any],
        *,
        fencing_token: int | None = None,
    ) -> dict[str, Any]:
        """Request one approved surge Pod and return only observed replacement evidence."""
        current = self.observe_scale(workload_id)
        if current["uid"] != payload.get("source_owner_uid"):
            raise ActionBlocked("SOURCE_WORKLOAD_UID_CHANGED")
        if current["pod_create_protocol"] != "reserved-create-v1":
            raise ActionBlocked("UNSUPPORTED_POD_CREATE_PROTOCOL")
        if current["pod_create_admission_protocol"] != "claimed-create-v1":
            raise ActionBlocked("UNSUPPORTED_POD_CREATE_ADMISSION_PROTOCOL")
        if current["require_hard_pod_anti_affinity"] is not True:
            raise ActionBlocked("UNSUPPORTED_REPLACEMENT_PLACEMENT_CONTRACT")
        if current["pod_create_history_uncertain"] or current["unverified_pod_creates"]:
            raise ActionBlocked("POD_CREATE_HISTORY_UNCERTAIN")
        slot_id = payload.get("source_logical_slot_id")
        if current["retired_replica_slots"] and not isinstance(slot_id, str):
            raise ActionBlocked("RETIRED_SLOT_HISTORY_PRESENT")
        existing_retirement = [
            item for item in current["retired_replica_slots"] if item.get("replicaID") == slot_id
        ]
        if existing_retirement and (
            len(existing_retirement) != 1
            or any(
                item.get("workloadUID") != current["uid"]
                or item.get("podUID") != payload.get("source_pod_uid")
                or item.get("planID") != plan_id
                or item.get("operationID") != payload.get("operation_id")
                for item in existing_retirement
            )
        ):
            raise ActionBlocked("RETIRED_SLOT_HISTORY_PRESENT")
        pending = current["pending_pod_create"]
        if pending is not None:
            if (
                pending.get("workloadUID") == current["uid"]
                and pending.get("planID") == plan_id
                and pending.get("operationID") == payload.get("operation_id")
                and pending.get("replicaID") != slot_id
            ):
                return {
                    "stage": "REPLACEMENT_CREATE_PENDING",
                    "replica_id": pending["replicaID"],
                }
            raise ActionBlocked("POD_CREATE_OUTCOME_UNKNOWN")
        if not plan_id or not payload.get("operation_id"):
            raise TargetPreconditionFailed("retirement operation identity is invalid")
        baseline, surge, final = (
            payload.get("baseline_replicas"),
            payload.get("surge_desired_replicas"),
            payload.get("final_desired_replicas"),
        )
        max_live, max_surge = (
            payload.get("maximum_live_replicas"),
            payload.get("maximum_temporary_surge_replicas"),
        )
        destinations = payload.get("destination_nodes")
        if (
            type(baseline) is not int
            or type(surge) is not int
            or type(final) is not int
            or type(max_live) is not int
            or type(max_surge) is not int
            or type(payload.get("profile_required_replicas")) is not int
            or not isinstance(destinations, list)
            or type(payload.get("readiness_stabilization_ms")) is not int
            or payload["readiness_stabilization_ms"] < 0
        ):
            raise TargetPreconditionFailed("verified replacement bounds are incomplete")
        if (
            baseline < 1
            or final != surge
            or surge != baseline + 1
            or max_live < final
            or max_surge < 1
            or max_live > baseline + max_surge
            or max_surge > current["surge_budget"]
            or surge - baseline > max_surge
            or not 0 <= payload["profile_required_replicas"] <= baseline
            or not destinations
        ):
            raise ActionBlocked("APPROVED_REPLACEMENT_SURGE_UNAVAILABLE")
        if not any(
            isinstance(node, dict)
            and node.get("name") == payload.get("source_node_id")
            and node.get("uid") == payload.get("source_node_uid")
            for node in current["approved_nodes"]
        ):
            raise ActionBlocked("SOURCE_NODE_UID_CHANGED")
        self._verify_retirement_source(current["uid"], workload_id, payload)
        request = current["execution_request"]
        same_request = (
            isinstance(request, dict)
            and request.get("planID") == plan_id
            and request.get("operationID") == payload["operation_id"]
            and request.get("expectedReplicas") == baseline
            and request.get("targetReplicas") == surge
            and request.get("destinationNodes") == destinations
            and request.get("purpose") == "staged-replacement-v1"
            and request.get("minimumReadyReplicas")
            == max(current["availability_floor"], payload.get("profile_required_replicas", 0))
        )
        if not same_request:
            if (
                current["generation"] != payload.get("workload_generation")
                or current["replicas"] != baseline
            ):
                raise ActionBlocked("SOURCE_WORKLOAD_GENERATION_CHANGED")
            if current["pending_pod_create"] is not None:
                raise ActionBlocked("POD_CREATE_OUTCOME_UNKNOWN")
            if current["ready_replicas"] < baseline or current["available_replicas"] < baseline:
                raise ActionBlocked("SOURCE_WORKLOAD_NOT_READY")
            if type(fencing_token) is not int or fencing_token <= 0:
                raise TargetPreconditionFailed("replacement fencing token is invalid")
            current = self.authorize_scale(
                workload_id,
                fencing_token,
                expected=current,
                plan_id=plan_id,
                operation_id=payload["operation_id"],
                target_replicas=surge,
                destination_nodes=destinations,
                purpose="staged-replacement-v1",
                minimum_ready_replicas=max(
                    current["availability_floor"], payload["profile_required_replicas"]
                ),
                source_replica_id=payload["source_logical_slot_id"],
                source_pod_uid=payload["source_pod_uid"],
            )
            same_request = True
        elif type(fencing_token) is int and current["fencing_token"] != fencing_token:
            current = self.authorize_scale(
                workload_id,
                fencing_token,
                expected=current,
                plan_id=plan_id,
                operation_id=payload["operation_id"],
                target_replicas=surge,
                destination_nodes=destinations,
                purpose="staged-replacement-v1",
                minimum_ready_replicas=max(
                    current["availability_floor"], payload["profile_required_replicas"]
                ),
                source_replica_id=payload["source_logical_slot_id"],
                source_pod_uid=payload["source_pod_uid"],
            )
        if not same_request:
            raise ActionBlocked("REPLACEMENT_INTENT_UNRESOLVED")
        if (
            current["observed_generation"] != current["generation"]
            or current["observed_replicas"] != surge
            or not isinstance(current["execution"], dict)
            or current["execution"].get("phase") != "APPLIED"
        ):
            return {"stage": "REPLACEMENT_REQUESTED", "surge_replicas": current["replicas"]}
        records = [
            record
            for record in current["authorized_pod_creates"]
            if record.get("planID") == plan_id
            and record.get("operationID") == payload["operation_id"]
        ]
        if len(records) != 1:
            if len(records) > 1:
                raise ActionBlocked("REPLACEMENT_SLOT_DUPLICATED")
            return {"stage": "REPLACEMENT_REQUESTED"}
        replacement = records[0]
        slot = replacement["replicaID"]
        if slot == slot_id:
            raise ActionBlocked("REPLACEMENT_SLOT_DUPLICATED")
        try:
            pod = self._json(
                "get", "pod", replacement["podName"], "-n", self.namespace, "-o", "json"
            )
        except TargetNotFound as error:
            raise ActionBlocked("REPLACEMENT_POD_DISAPPEARED") from error
        metadata, spec, status = pod.get("metadata", {}), pod.get("spec", {}), pod.get("status", {})
        labels = metadata.get("labels", {}) if isinstance(metadata, dict) else {}
        annotations = metadata.get("annotations", {}) if isinstance(metadata, dict) else {}
        owners = metadata.get("ownerReferences", []) if isinstance(metadata, dict) else []
        if (
            not isinstance(metadata, dict)
            or metadata.get("uid") != replacement["podUID"]
            or metadata.get("name") != replacement["podName"]
            or not isinstance(labels, dict)
            or labels.get("fleetright.io/workload-uid") != current["uid"]
            or labels.get("fleetright.io/replica-id") != slot
            or not isinstance(annotations, dict)
            or annotations.get(POD_CREATE_PLAN_ANNOTATION) != plan_id
            or annotations.get(POD_CREATE_OPERATION_ANNOTATION) != payload["operation_id"]
            or annotations.get(POD_CREATE_ATTEMPT_ANNOTATION) != replacement["createID"]
            or annotations.get(WORKLOAD_GENERATION_ANNOTATION)
            != str(replacement["workloadGeneration"])
            or not isinstance(owners, list)
            or not any(
                isinstance(owner, dict)
                and owner.get("kind") == "FleetRightWorkload"
                and owner.get("name") == workload_id
                and owner.get("uid") == current["uid"]
                and owner.get("controller") is True
                for owner in owners
            )
        ):
            raise ActionBlocked("REPLACEMENT_POD_IDENTITY_CHANGED")
        if metadata.get("deletionTimestamp") is not None:
            return {"stage": "REPLACEMENT_WAITING_READY", "replica_id": slot}
        node_name = spec.get("nodeName") if isinstance(spec, dict) else None
        if not isinstance(node_name, str) or not node_name:
            return {"stage": "REPLACEMENT_SCHEDULING", "replica_id": slot}
        destination = next(
            (
                node
                for node in destinations
                if isinstance(node, dict) and node.get("name") == node_name
            ),
            None,
        )
        if destination is None:
            raise ActionBlocked("REPLACEMENT_DESTINATION_FORBIDDEN")
        try:
            node = self._json("get", "node", node_name, "-o", "json")
        except TargetNotFound as error:
            raise ActionBlocked("REPLACEMENT_NODE_NOT_FOUND") from error
        node_metadata = node.get("metadata", {}) if isinstance(node, dict) else {}
        if not isinstance(node_metadata, dict) or node_metadata.get("uid") != destination.get(
            "uid"
        ):
            raise ActionBlocked("REPLACEMENT_NODE_UID_CHANGED")
        ready_since = next(
            (
                condition.get("lastTransitionTime")
                for condition in status.get("conditions", [])
                if isinstance(condition, dict)
                and condition.get("type") == "Ready"
                and condition.get("status") == "True"
            ),
            None,
        )
        if status.get("phase") != "Running" or not isinstance(ready_since, str):
            return {"stage": "REPLACEMENT_WAITING_READY", "replica_id": slot}
        try:
            ready_at = datetime.fromisoformat(ready_since.replace("Z", "+00:00"))
        except ValueError as error:
            raise ActionBlocked("REPLACEMENT_READINESS_TIME_INVALID") from error
        if ready_at.utcoffset() is None:
            raise ActionBlocked("REPLACEMENT_READINESS_TIME_INVALID")
        warm_ms = (datetime.now(UTC) - ready_at).total_seconds() * 1000
        if warm_ms < payload["readiness_stabilization_ms"]:
            return {"stage": "REPLACEMENT_WARMING", "replica_id": slot}
        required_ready = max(
            current["availability_floor"], payload.get("profile_required_replicas", 0)
        )
        if current["ready_replicas"] < surge or current["available_replicas"] < required_ready:
            return {"stage": "REPLACEMENT_CAPACITY_PENDING", "replica_id": slot}
        result = {
            "stage": "READY_FOR_SAFE_EVICTION",
            "workload_uid": current["uid"],
            "workload_generation": current["generation"],
            "source_pod_name": payload["source_pod_name"],
            "source_pod_uid": payload["source_pod_uid"],
            "source_node_id": payload["source_node_id"],
            "source_node_uid": payload["source_node_uid"],
            "replacement_pod_name": replacement["podName"],
            "replacement_pod_uid": replacement["podUID"],
            "replacement_replica_id": slot,
            "replacement_node_id": node_name,
            "replacement_node_uid": destination["uid"],
            "replacement_workload_generation": replacement["workloadGeneration"],
            "replacement_create_id": replacement["createID"],
            "ready_since": ready_since,
            "readiness_stabilization_ms": payload["readiness_stabilization_ms"],
        }
        if existing_retirement and any(
            existing_retirement[0].get(key) != value
            for key, value in {
                "replacementPodUID": result["replacement_pod_uid"],
                "replacementPodName": result["replacement_pod_name"],
                "replacementReplicaID": result["replacement_replica_id"],
                "replacementNodeName": result["replacement_node_id"],
                "replacementNodeUID": result["replacement_node_uid"],
                "replacementCreateID": result["replacement_create_id"],
                "replacementWorkloadGeneration": result["replacement_workload_generation"],
            }.items()
        ):
            raise ActionBlocked("RETIREMENT_REPLACEMENT_IDENTITY_UNBOUND")
        workload = self._json(
            "get",
            "fleetrightworkload",
            _checked_name(workload_id, "workload identifier"),
            "-n",
            self.namespace,
            "-o",
            "json",
        )
        fresh = self._observe(workload, workload_id)
        fresh_request, fresh_execution = fresh["execution_request"], fresh["execution"]
        if (
            fresh["uid"] != current["uid"]
            or fresh_request != request
            or fresh["pending_pod_create"] is not None
            or fresh["pod_create_history_uncertain"]
            or fresh["unverified_pod_creates"]
        ):
            raise ActionBlocked("RETIREMENT_INTENT_PRECONDITION_CHANGED")
        execution_matches = isinstance(fresh_execution, dict) and all(
            fresh_execution.get(key) == request.get(key)
            for key in ("planID", "operationID", "generation", "targetReplicas", "purpose")
        )
        if not execution_matches:
            raise ActionBlocked("RETIREMENT_INTENT_PRECONDITION_CHANGED")
        if isinstance(fresh_execution, dict) and fresh_execution.get("phase") == "RECONCILING":
            return {"stage": "REPLACEMENT_REQUESTED", "surge_replicas": surge}
        if not isinstance(fresh_execution, dict) or fresh_execution.get("phase") != "APPLIED":
            raise ActionBlocked("RETIREMENT_INTENT_PRECONDITION_CHANGED")
        if fresh["ready_replicas"] < surge or fresh["available_replicas"] < required_ready:
            return {"stage": "REPLACEMENT_CAPACITY_PENDING", "surge_replicas": surge}
        source = self.get_pod(payload["source_pod_name"])
        if source is None:
            raise ActionBlocked("SOURCE_POD_NOT_FOUND")
        source_metadata = source.get("metadata", {})
        source_spec = source.get("spec", {})
        if (
            not isinstance(source_metadata, dict)
            or source_metadata.get("uid") != payload.get("source_pod_uid")
            or source_metadata.get("name") != payload.get("source_pod_name")
            or source_metadata.get("deletionTimestamp") is not None
            or not isinstance(source_spec, dict)
            or source_spec.get("nodeName") != payload.get("source_node_id")
        ):
            raise ActionBlocked("SOURCE_POD_UID_CHANGED")
        ready_condition = next(
            (
                condition.get("status")
                for condition in source.get("status", {}).get("conditions", [])
                if isinstance(condition, dict) and condition.get("type") == "Ready"
            ),
            None,
        )
        if ready_condition not in {"True", "False"}:
            raise ActionBlocked("SOURCE_READINESS_UNKNOWN")
        pdb_stage = self._require_pdb_eviction_ready(
            workload_id,
            fresh["uid"],
            fresh["generation"],
            required_ready,
            surge,
            ready_condition == "True",
        )
        if pdb_stage is not None:
            return {"stage": pdb_stage, "minimum_ready_replicas": required_ready}
        result.update(
            workload_uid=fresh["uid"],
            workload_generation=fresh["generation"],
            source_pod_resource_version=source_metadata.get("resourceVersion"),
        )
        approved_nodes = fresh["approved_nodes"]
        if not isinstance(approved_nodes, list) or not approved_nodes:
            raise ActionBlocked("RETIREMENT_NODE_INVENTORY_UNAVAILABLE")
        nodes = []
        for approved in approved_nodes:
            if (
                not isinstance(approved, dict)
                or not isinstance(approved.get("name"), str)
                or not isinstance(approved.get("uid"), str)
            ):
                raise ActionBlocked("RETIREMENT_NODE_INVENTORY_UNAVAILABLE")
            try:
                node = self._json(
                    "get", "node", _checked_name(approved["name"], "Node name"), "-o", "json"
                )
            except TargetNotFound as error:
                raise ActionBlocked("RETIREMENT_NODE_UID_CHANGED") from error
            metadata = node.get("metadata", {}) if isinstance(node, dict) else {}
            if (
                not isinstance(metadata, dict)
                or metadata.get("name") != approved["name"]
                or metadata.get("uid") != approved["uid"]
            ):
                raise ActionBlocked("RETIREMENT_NODE_UID_CHANGED")
            nodes.append(node)
        try:
            self._require_fencing_schema()
            _write_retired_slot_tombstone(
                self,
                workload,
                source,
                plan_id,
                payload["operation_id"],
                nodes,
                result,
            )
        except SafetyError as error:
            raise ActionBlocked(error.reason) from error
        result["source_retirement_intent_recorded"] = True
        return result

    def evict_source(
        self,
        workload_id: str,
        plan_id: str,
        payload: dict[str, Any],
        prepared: dict[str, Any],
        *,
        fencing_token: int,
        before_dispatch: Callable[[], bool],
    ) -> dict[str, Any]:
        """Send one UID- and resourceVersion-preconditioned policy/v1 Eviction."""
        current = self.observe_scale(workload_id)
        request, fence, execution = (
            current["execution_request"],
            current["execution_fence"],
            current["execution"],
        )
        if (
            not prepared.get("source_retirement_intent_recorded")
            or prepared.get("stage") != "READY_FOR_SAFE_EVICTION"
            or prepared.get("workload_uid") != current["uid"]
            or prepared.get("source_pod_uid") != payload.get("source_pod_uid")
            or prepared.get("source_pod_name") != payload.get("source_pod_name")
            or prepared.get("replacement_pod_uid") is None
            or prepared.get("replacement_node_uid") is None
            or type(fencing_token) is not int
            or current["fencing_token"] != fencing_token
            or type(payload.get("readiness_stabilization_ms")) is not int
            or payload["readiness_stabilization_ms"] < 0
            or not isinstance(request, dict)
            or request.get("planID") != plan_id
            or request.get("operationID") != payload.get("operation_id")
            or request.get("purpose") != "staged-replacement-v1"
            or not isinstance(fence, dict)
            or fence.get("generation") != fencing_token
            or fence.get("planID") != plan_id
            or fence.get("operationID") != payload.get("operation_id")
            or not isinstance(execution, dict)
            or execution.get("phase") != "APPLIED"
            or current["pending_pod_create"] is not None
            or current["pod_create_history_uncertain"]
            or current["unverified_pod_creates"]
            or current["require_hard_pod_anti_affinity"] is not True
        ):
            raise ActionBlocked("RETIREMENT_AUTHORITY_OR_IDENTITY_CHANGED")
        tombstones = [
            item
            for item in current["retired_replica_slots"]
            if item.get("replicaID") == payload.get("source_logical_slot_id")
        ]
        if len(tombstones) != 1 or any(
            tombstones[0].get(key) != value
            for key, value in {
                "workloadUID": current["uid"],
                "podUID": payload["source_pod_uid"],
                "planID": plan_id,
                "operationID": payload["operation_id"],
                "sourceNodeName": payload["source_node_id"],
                "sourceNodeUID": payload["source_node_uid"],
                "replacementPodUID": prepared["replacement_pod_uid"],
                "replacementPodName": prepared["replacement_pod_name"],
                "replacementReplicaID": prepared["replacement_replica_id"],
                "replacementNodeName": prepared["replacement_node_id"],
                "replacementNodeUID": prepared["replacement_node_uid"],
                "replacementCreateID": prepared["replacement_create_id"],
                "replacementWorkloadGeneration": prepared["replacement_workload_generation"],
                "readinessStabilizationMs": payload["readiness_stabilization_ms"],
            }.items()
        ):
            raise ActionBlocked("RETIREMENT_TOMBSTONE_IDENTITY_CHANGED")
        source = self.get_pod(payload["source_pod_name"])
        if source is None:
            return {**prepared, "stage": "EVICTION_SOURCE_REMOVED"}
        source_metadata = source.get("metadata", {})
        source_spec = source.get("spec", {})
        if (
            not isinstance(source_metadata, dict)
            or source_metadata.get("uid") != payload.get("source_pod_uid")
            or source_metadata.get("name") != payload.get("source_pod_name")
            or not isinstance(source_spec, dict)
            or source_spec.get("nodeName") != payload.get("source_node_id")
        ):
            raise ActionBlocked("SOURCE_POD_UID_CHANGED")
        if source_metadata.get("deletionTimestamp") is not None:
            return {**prepared, "stage": "EVICTION_IN_PROGRESS"}
        resource_version = source_metadata.get("resourceVersion")
        if not isinstance(resource_version, str) or resource_version != prepared.get(
            "source_pod_resource_version"
        ):
            raise TargetPreconditionFailed("source Pod resourceVersion changed before eviction")
        replacement = self.get_pod(prepared["replacement_pod_name"])
        if replacement is None:
            raise ActionBlocked("REPLACEMENT_POD_DISAPPEARED")
        replacement_metadata = replacement.get("metadata", {})
        replacement_spec = replacement.get("spec", {})
        if (
            not isinstance(replacement_metadata, dict)
            or replacement_metadata.get("uid") != prepared.get("replacement_pod_uid")
            or not isinstance(replacement_spec, dict)
            or replacement_spec.get("nodeName") != prepared.get("replacement_node_id")
        ):
            raise ActionBlocked("REPLACEMENT_READINESS_OR_IDENTITY_CHANGED")
        if not _pod_ready_stable(replacement, payload["readiness_stabilization_ms"]):
            return {**prepared, "stage": "REPLACEMENT_WARMING"}
        self._verify_node_uid(payload["source_node_id"], payload["source_node_uid"])
        self._verify_node_uid(prepared["replacement_node_id"], prepared["replacement_node_uid"])
        ready_condition = next(
            (
                condition.get("status")
                for condition in source.get("status", {}).get("conditions", [])
                if isinstance(condition, dict) and condition.get("type") == "Ready"
            ),
            None,
        )
        if ready_condition not in {"True", "False"}:
            raise ActionBlocked("SOURCE_READINESS_UNKNOWN")
        minimum_ready = max(current["availability_floor"], payload["profile_required_replicas"])
        pdb_stage = self._require_pdb_eviction_ready(
            workload_id,
            current["uid"],
            current["generation"],
            minimum_ready,
            current["replicas"],
            ready_condition == "True",
        )
        if pdb_stage is not None:
            return {
                **prepared,
                "stage": pdb_stage,
                "minimum_ready_replicas": minimum_ready,
            }
        eviction = {
            "apiVersion": "policy/v1",
            "kind": "Eviction",
            "metadata": {
                "name": payload["source_pod_name"],
                "namespace": self.namespace,
                "annotations": {
                    "fleetright.io/plan-id": plan_id,
                    "fleetright.io/operation-id": payload["operation_id"],
                    "fleetright.io/fencing-token": str(fencing_token),
                    "fleetright.io/workload-uid": current["uid"],
                    "fleetright.io/replacement-pod-uid": prepared["replacement_pod_uid"],
                    "fleetright.io/replacement-node-uid": prepared["replacement_node_uid"],
                },
            },
            "deleteOptions": {
                "preconditions": {
                    "uid": payload["source_pod_uid"],
                    "resourceVersion": resource_version,
                }
            },
        }
        if not before_dispatch():
            return {**prepared, "stage": "READY_FOR_SAFE_EVICTION"}
        try:
            response = self._kubectl(
                "create",
                "--raw",
                f"/api/v1/namespaces/{self.namespace}/pods/{payload['source_pod_name']}/eviction",
                "-f",
                "-",
                stdin=json.dumps(eviction, separators=(",", ":")),
            )
        except ActionBlocked as error:
            if error.reason == "PDB_BLOCKED":
                return {
                    **prepared,
                    "stage": "PDB_BLOCKED",
                    "minimum_ready_replicas": minimum_ready,
                }
            raise
        try:
            accepted = json.loads(response.stdout)
        except json.JSONDecodeError as error:
            raise OutcomeUnknown("Eviction response was not valid JSON") from error
        if not isinstance(accepted, dict) or accepted.get("kind") != "Status":
            raise OutcomeUnknown("Eviction response was not a Kubernetes Status")
        return {
            "stage": "EVICTION_ACCEPTED",
            "source_pod_uid": payload["source_pod_uid"],
            "source_pod_resource_version": resource_version,
        }

    def observe_source_retirement(
        self,
        workload_id: str,
        plan_id: str,
        payload: dict[str, Any],
        prepared: dict[str, Any],
        *,
        fencing_token: int,
    ) -> dict[str, Any]:
        """Reconcile an uncertain Eviction by UID and verify the final managed workload."""
        current = self.observe_scale(workload_id)
        request, fence = current["execution_request"], current["execution_fence"]
        if (
            current["uid"] != prepared.get("workload_uid")
            or current["fencing_token"] != fencing_token
            or not isinstance(request, dict)
            or request.get("planID") != plan_id
            or request.get("operationID") != payload.get("operation_id")
            or not isinstance(fence, dict)
            or fence.get("generation") != fencing_token
            or fence.get("planID") != plan_id
            or fence.get("operationID") != payload.get("operation_id")
        ):
            raise ActionBlocked("RETIREMENT_AUTHORITY_OR_IDENTITY_CHANGED")
        source = self.get_pod(payload["source_pod_name"])
        if source is not None:
            metadata = source.get("metadata", {})
            spec = source.get("spec", {})
            if (
                not isinstance(metadata, dict)
                or metadata.get("uid") != payload.get("source_pod_uid")
                or not isinstance(spec, dict)
                or spec.get("nodeName") != payload.get("source_node_id")
            ):
                raise ActionBlocked("SOURCE_POD_UID_CHANGED")
            if metadata.get("deletionTimestamp") is not None:
                return {**prepared, "stage": "EVICTION_IN_PROGRESS"}
            if prepared.get("stage") in {
                "EVICTION_INTENT",
                "EVICTION_ACCEPTED",
                "EVICTION_IN_PROGRESS",
                "EVICTION_OUTCOME_UNKNOWN",
                "EVICTION_SOURCE_REMOVED",
                "RETIREMENT_WAITING_FINAL_HEALTH",
            }:
                # An admitted request can still delete this exact Pod after takeover.
                return {**prepared, "stage": "EVICTION_OUTCOME_UNKNOWN"}
            if prepared.get("stage") not in {
                "READY_FOR_SAFE_EVICTION",
                "PDB_BLOCKED",
                "PDB_STATUS_PENDING",
            }:
                return {**prepared, "stage": "EVICTION_OUTCOME_UNKNOWN"}
            return {
                **prepared,
                "stage": "EVICTION_RETRY_SAFE",
                "source_pod_resource_version": metadata.get("resourceVersion"),
                "fencing_token": fencing_token,
            }
        if (
            current["pending_pod_create"] is not None
            or current["pod_create_history_uncertain"]
            or current["unverified_pod_creates"] != 0
        ):
            return {
                **prepared,
                "stage": "RETIREMENT_WAITING_FINAL_HEALTH",
                "source_removed_observed": True,
                "observation": current,
            }
        final_replicas = payload["final_desired_replicas"]
        required_ready = max(current["availability_floor"], payload["profile_required_replicas"])
        replacement = self.get_pod(prepared["replacement_pod_name"])
        if replacement is None:
            return {
                **prepared,
                "stage": "RETIREMENT_WAITING_FINAL_HEALTH",
                "source_removed_observed": True,
                "observation": current,
            }
        metadata = replacement.get("metadata", {})
        spec = replacement.get("spec", {})
        if (
            not isinstance(metadata, dict)
            or metadata.get("uid") != prepared.get("replacement_pod_uid")
            or not isinstance(spec, dict)
            or spec.get("nodeName") != prepared.get("replacement_node_id")
            or not any(
                isinstance(condition, dict)
                and condition.get("type") == "Ready"
                and condition.get("status") == "True"
                for condition in replacement.get("status", {}).get("conditions", [])
            )
        ):
            return {
                **prepared,
                "stage": "RETIREMENT_WAITING_FINAL_HEALTH",
                "source_removed_observed": True,
                "observation": current,
            }
        self._verify_node_uid(prepared["replacement_node_id"], prepared["replacement_node_uid"])
        if (
            current["replicas"] != final_replicas
            or current["observed_replicas"] != final_replicas
            or current["ready_replicas"] < final_replicas
            or current["available_replicas"] < required_ready
            or not current["reconciled"]
            or current["observed_generation"] != current["generation"]
            or not isinstance(current["execution"], dict)
            or current["execution"].get("phase") != "APPLIED"
        ):
            return {
                **prepared,
                "stage": "RETIREMENT_WAITING_FINAL_HEALTH",
                "observation": current,
                "source_removed_observed": True,
            }
        if not self._verify_final_retirement_pods(current, payload, final_replicas):
            return {
                **prepared,
                "stage": "RETIREMENT_WAITING_FINAL_HEALTH",
                "observation": current,
                "source_removed_observed": True,
            }
        return {
            **prepared,
            "stage": "RETIREMENT_COMPLETE",
            "observation": current,
            "source_pod_uid": payload["source_pod_uid"],
            "replacement_pod_uid": prepared["replacement_pod_uid"],
            "replacement_node_id": prepared["replacement_node_id"],
            "replacement_node_uid": prepared["replacement_node_uid"],
            "final_desired_replicas": final_replicas,
        }

    def _verify_node_uid(self, name: str, uid: str) -> None:
        node = self._json("get", "node", _checked_name(name, "Node name"), "-o", "json")
        metadata = node.get("metadata", {})
        if not isinstance(metadata, dict) or metadata.get("uid") != uid:
            raise ActionBlocked("RETIREMENT_NODE_UID_CHANGED")

    def _verify_final_retirement_pods(
        self, current: dict[str, Any], payload: dict[str, Any], final_replicas: int
    ) -> bool:
        items = self._json(
            "get",
            "pods",
            "-n",
            self.namespace,
            "-l",
            f"fleetright.io/workload-uid={current['uid']}",
            "-o",
            "json",
        ).get("items")
        approved = {
            node["name"]: node["uid"]
            for node in current["approved_nodes"]
            if isinstance(node, dict)
            and isinstance(node.get("name"), str)
            and isinstance(node.get("uid"), str)
        }
        records = current["authorized_pod_creates"]
        if not isinstance(items, list) or not isinstance(records, list):
            return False
        if len(items) > final_replicas:
            raise ActionBlocked("FINAL_POD_COUNT_EXCEEDS_APPROVED_PLAN")
        if len(approved) != len(current["approved_nodes"]):
            raise ActionBlocked("FINAL_POD_APPROVED_NODE_SET_INVALID")
        seen_slots: set[str] = set()
        domains: dict[str, int] = {}
        topology_key = payload.get("topology_key")
        for pod in items:
            if not isinstance(pod, dict):
                raise ActionBlocked("FINAL_POD_IDENTITY_INVALID")
            metadata, spec, status = (
                pod.get("metadata", {}),
                pod.get("spec", {}),
                pod.get("status", {}),
            )
            if not all(isinstance(value, dict) for value in (metadata, spec, status)):
                raise ActionBlocked("FINAL_POD_IDENTITY_INVALID")
            labels = metadata.get("labels", {})
            if not isinstance(labels, dict):
                raise ActionBlocked("FINAL_POD_IDENTITY_INVALID")
            slot, uid, name, node_name = (
                labels.get("fleetright.io/replica-id"),
                metadata.get("uid"),
                metadata.get("name"),
                spec.get("nodeName"),
            )
            owners = metadata.get("ownerReferences", [])
            if (
                labels.get("fleetright.io/workload-uid") != current["uid"]
                or not isinstance(slot, str)
                or slot in seen_slots
                or not isinstance(uid, str)
                or not isinstance(name, str)
                or not any(
                    isinstance(owner, dict)
                    and owner.get("kind") == "FleetRightWorkload"
                    and owner.get("name") == payload.get("source_owner_name")
                    and owner.get("uid") == current["uid"]
                    and owner.get("controller") is True
                    for owner in owners
                )
            ):
                raise ActionBlocked("FINAL_POD_OWNER_OR_SLOT_IDENTITY_INVALID")
            seen_slots.add(slot)
            record = next(
                (
                    item
                    for item in records
                    if isinstance(item, dict)
                    and item.get("workloadUID") == current["uid"]
                    and item.get("podUID") == uid
                    and item.get("podName") == name
                    and item.get("replicaID") == slot
                ),
                None,
            )
            if record is None:
                raise ActionBlocked("FINAL_POD_CREATE_IDENTITY_UNAUTHORIZED")
            if (
                metadata.get("deletionTimestamp") is not None
                or status.get("phase") != "Running"
                or not any(
                    isinstance(condition, dict)
                    and condition.get("type") == "Ready"
                    and condition.get("status") == "True"
                    for condition in status.get("conditions", [])
                )
            ):
                return False
            if not isinstance(node_name, str) or node_name not in approved:
                raise ActionBlocked("FINAL_POD_DESTINATION_FORBIDDEN")
            node = self._json("get", "node", node_name, "-o", "json")
            node_metadata = node.get("metadata", {})
            node_status = node.get("status", {})
            if (
                not isinstance(node_metadata, dict)
                or not isinstance(node_status, dict)
                or node_metadata.get("uid") != approved[node_name]
                or not any(
                    isinstance(condition, dict)
                    and condition.get("type") == "Ready"
                    and condition.get("status") == "True"
                    for condition in node_status.get("conditions", [])
                )
            ):
                raise ActionBlocked("FINAL_POD_NODE_IDENTITY_OR_READINESS_CHANGED")
            labels_on_node = node_metadata.get("labels", {})
            if isinstance(topology_key, str) and topology_key:
                domain = labels_on_node.get(topology_key)
                if not isinstance(domain, str) or not domain:
                    raise ActionBlocked("FINAL_POD_TOPOLOGY_LABEL_MISSING")
                domains[domain] = domains.get(domain, 0) + 1
        if len(items) != final_replicas:
            return False
        minimum_domains = payload.get("minimum_topology_domains", 0)
        maximum_skew = payload.get("maximum_topology_skew")
        if minimum_domains:
            if (
                not isinstance(topology_key, str)
                or not topology_key
                or type(minimum_domains) is not int
                or len(domains) < minimum_domains
                or type(maximum_skew) is not int
                or max(domains.values()) - min(domains.values()) > maximum_skew
            ):
                return False
        return True

    def verify_recovery(
        self, plan_id: str, action: dict[str, Any], observed: dict[str, Any]
    ) -> None:
        payload = action.get("payload")
        precondition = payload.get("target_precondition") if isinstance(payload, dict) else None
        desired = payload.get("proposed_replicas") if isinstance(payload, dict) else None
        original = payload.get("current_replicas") if isinstance(payload, dict) else None
        intent_id = action.get("intent_id")
        destinations = payload.get("destination_nodes") if isinstance(payload, dict) else None
        request, fence, execution = (
            observed.get("execution_request"),
            observed.get("execution_fence"),
            observed.get("execution"),
        )
        if (
            not isinstance(precondition, dict)
            or observed.get("uid") != precondition.get("uid")
            or precondition.get("replicas") != original
            or not isinstance(intent_id, str)
            or type(desired) is not int
            or type(original) is not int
            or desired <= original
            or not isinstance(destinations, list)
            or not destinations
            or not isinstance(request, dict)
            or (request.get("planID"), request.get("operationID")) != (plan_id, intent_id)
            or request.get("targetReplicas") != desired
            or request.get("expectedReplicas") != original
            or request.get("destinationNodes") != destinations
            or type(request.get("generation")) is not int
            or observed.get("pending_pod_create") is not None
            or observed.get("pod_create_history_uncertain") is True
            or observed.get("unverified_pod_creates") != 0
            or not isinstance(fence, dict)
            or (fence.get("planID"), fence.get("operationID")) != (plan_id, intent_id)
            or observed.get("fencing_token") != fence.get("generation")
            or fence.get("generation", 0) < request.get("generation")
            or fence.get("expectedGeneration") != precondition.get("generation")
            or fence.get("expectedReplicas") != original
            or fence.get("fencedGeneration") != observed.get("generation")
            or not isinstance(execution, dict)
            or execution.get("phase") != "APPLIED"
            or any(
                execution.get(key) != request.get(key)
                for key in ("planID", "operationID", "generation", "targetReplicas")
            )
        ):
            raise ActionBlocked("RECOVERY_WORKLOAD_OR_OPERATION_IDENTITY_CHANGED")

        approved = {
            node.get("name"): node.get("uid")
            for node in observed.get("approved_nodes", [])
            if isinstance(node, dict)
            and isinstance(node.get("name"), str)
            and isinstance(node.get("uid"), str)
        }
        recovery = {
            node.get("name"): node.get("uid")
            for node in destinations
            if isinstance(node, dict)
            and isinstance(node.get("name"), str)
            and isinstance(node.get("uid"), str)
        }
        if (
            len(approved) != len(observed.get("approved_nodes", []))
            or len(recovery) != len(destinations)
            or any(approved.get(name) != uid for name, uid in recovery.items())
        ):
            raise ActionBlocked("RECOVERY_NODE_AUTHORIZATION_INVALID")

        records = observed.get("authorized_pod_creates")
        if not isinstance(records, list):
            raise ActionBlocked("RECOVERY_CREATE_HISTORY_INVALID")
        created = [
            record
            for record in records
            if isinstance(record, dict)
            and record.get("planID") == plan_id
            and record.get("operationID") == intent_id
        ]
        if len(created) != desired - original:
            raise ActionBlocked("RECOVERY_CREATE_EVIDENCE_INCOMPLETE")
        created_uids = {record.get("podUID") for record in created}
        if len(created_uids) != len(created) or not all(
            isinstance(uid, str) for uid in created_uids
        ):
            raise ActionBlocked("RECOVERY_CREATE_IDENTITY_DUPLICATED")

        result = self._json(
            "get",
            "pods",
            "-n",
            self.namespace,
            "-l",
            f"{WORKLOAD_UID_LABEL}={observed['uid']}",
            "-o",
            "json",
        )
        items = result.get("items")
        if not isinstance(items, list):
            raise TargetUnavailable("recovery Pod inventory is unavailable")
        if any(not isinstance(pod, dict) for pod in items):
            raise ActionBlocked("RECOVERY_POD_INVENTORY_INVALID")
        live = []
        for pod in items:
            status = pod.get("status", {})
            if not isinstance(status, dict):
                raise ActionBlocked("RECOVERY_POD_IDENTITY_INVALID")
            if status.get("phase") not in {"Succeeded", "Failed"}:
                live.append(pod)
        if len(live) != desired:
            raise ActionBlocked("RECOVERY_LIVE_POD_COUNT_CHANGED")

        topology = observed.get("placement_topology")
        topology_key = payload.get("topology_key")
        minimum_domains = payload.get("minimum_topology_domains", 0)
        maximum_skew = payload.get("maximum_topology_skew")
        recovery_node_objects: dict[str, dict[str, Any]] = {}
        if topology_key:
            if (
                not isinstance(topology, dict)
                or topology.get("key") != topology_key
                or type(minimum_domains) is not int
                or type(maximum_skew) is not int
                or topology.get("maxSkew", 1) != maximum_skew
                or topology.get("minDomains", 0) != minimum_domains
            ):
                raise ActionBlocked("RECOVERY_TOPOLOGY_POLICY_CHANGED")
            for name, uid in recovery.items():
                node = self._json("get", "node", _checked_name(name, "Node name"), "-o", "json")
                metadata, status = node.get("metadata", {}), node.get("status", {})
                if (
                    not isinstance(metadata, dict)
                    or not isinstance(status, dict)
                    or metadata.get("uid") != uid
                    or not any(
                        isinstance(condition, dict)
                        and condition.get("type") == "Ready"
                        and condition.get("status") == "True"
                        for condition in status.get("conditions", [])
                    )
                ):
                    raise ActionBlocked("RECOVERY_NODE_IDENTITY_OR_READINESS_CHANGED")
                recovery_node_objects[name] = node
        node_cache: dict[str, dict[str, Any]] = dict(recovery_node_objects)
        domains: dict[str, int] = {}
        if topology_key:
            for node in recovery_node_objects.values():
                metadata = node["metadata"]
                labels_on_node = metadata.get("labels", {})
                domain = (
                    labels_on_node.get(topology_key) if isinstance(labels_on_node, dict) else None
                )
                if not isinstance(domain, str) or not domain:
                    raise ActionBlocked("RECOVERY_TOPOLOGY_LABEL_MISSING")
                domains.setdefault(domain, 0)
        seen_slots: set[str] = set()
        seen_pod_uids: set[str] = set()
        retired = {
            slot.get("replicaID")
            for slot in observed.get("retired_replica_slots", [])
            if isinstance(slot, dict)
        }
        for pod in live:
            metadata, spec, status = (
                pod.get("metadata", {}),
                pod.get("spec", {}),
                pod.get("status", {}),
            )
            labels = metadata.get("labels", {}) if isinstance(metadata, dict) else {}
            annotations = metadata.get("annotations", {}) if isinstance(metadata, dict) else {}
            if not all(
                isinstance(value, dict) for value in (metadata, spec, status, labels, annotations)
            ):
                raise ActionBlocked("RECOVERY_POD_IDENTITY_INVALID")
            slot, uid, name, node_name = (
                labels.get(REPLICA_ID_LABEL),
                metadata.get("uid"),
                metadata.get("name"),
                spec.get("nodeName"),
            )
            record = next(
                (
                    item
                    for item in records
                    if isinstance(item, dict)
                    and item.get("podUID") == uid
                    and item.get("podName") == name
                    and item.get("replicaID") == slot
                ),
                None,
            )
            owners = metadata.get("ownerReferences", [])
            if (
                not isinstance(slot, str)
                or slot in seen_slots
                or slot in retired
                or not isinstance(uid, str)
                or not isinstance(name, str)
                or not isinstance(node_name, str)
                or approved.get(node_name) is None
                or not isinstance(record, dict)
                or record.get("workloadUID") != observed["uid"]
                or type(record.get("workloadGeneration")) is not int
                or annotations.get(WORKLOAD_GENERATION_ANNOTATION)
                != str(record.get("workloadGeneration"))
                or annotations.get(POD_CREATE_PLAN_ANNOTATION) != record.get("planID")
                or annotations.get(POD_CREATE_OPERATION_ANNOTATION) != record.get("operationID")
                or annotations.get(POD_CREATE_ATTEMPT_ANNOTATION) != record.get("createID")
                or labels.get(WORKLOAD_UID_LABEL) != observed["uid"]
                or not isinstance(owners, list)
                or not any(
                    isinstance(owner, dict)
                    and owner.get("apiVersion") == "fleetright.io/v1alpha1"
                    and owner.get("kind") == "FleetRightWorkload"
                    and owner.get("name") == action.get("workload_id")
                    and owner.get("uid") == observed["uid"]
                    and owner.get("controller") is True
                    for owner in owners
                )
                or metadata.get("deletionTimestamp") is not None
                or status.get("phase") != "Running"
                or not any(
                    isinstance(condition, dict)
                    and condition.get("type") == "Ready"
                    and condition.get("status") == "True"
                    for condition in status.get("conditions", [])
                )
            ):
                raise ActionBlocked("RECOVERY_POD_IDENTITY_OR_READINESS_CHANGED")
            seen_slots.add(slot)
            seen_pod_uids.add(uid)
            if uid in created_uids and recovery.get(node_name) != approved.get(node_name):
                raise ActionBlocked("RECOVERY_POD_DESTINATION_CHANGED")
            if node_name not in node_cache:
                node = self._json(
                    "get", "node", _checked_name(node_name, "Node name"), "-o", "json"
                )
                node_metadata, node_status = node.get("metadata", {}), node.get("status", {})
                if (
                    not isinstance(node_metadata, dict)
                    or not isinstance(node_status, dict)
                    or node_metadata.get("uid") != approved[node_name]
                    or not any(
                        isinstance(condition, dict)
                        and condition.get("type") == "Ready"
                        and condition.get("status") == "True"
                        for condition in node_status.get("conditions", [])
                    )
                ):
                    raise ActionBlocked("RECOVERY_NODE_IDENTITY_OR_READINESS_CHANGED")
                node_cache[node_name] = node
            if topology_key:
                labels_on_node = node_cache[node_name]["metadata"].get("labels", {})
                domain = (
                    labels_on_node.get(topology_key) if isinstance(labels_on_node, dict) else None
                )
                if not isinstance(domain, str) or not domain:
                    raise ActionBlocked("RECOVERY_TOPOLOGY_LABEL_MISSING")
                if domain in domains:
                    domains[domain] += 1

        if topology_key:
            if (
                type(minimum_domains) is not int
                or type(maximum_skew) is not int
                or len(domains) < minimum_domains
                or max(domains.values(), default=0) - min(domains.values(), default=0)
                > maximum_skew
            ):
                raise ActionBlocked("RECOVERY_TOPOLOGY_REQUIREMENT_CHANGED")
        if created_uids - seen_pod_uids:
            raise ActionBlocked("RECOVERY_CREATED_POD_NOT_LIVE")

    def _verify_retirement_source(
        self, workload_uid: str, workload_id: str, payload: dict[str, Any]
    ) -> None:
        pod_name = payload.get("source_pod_name")
        pod_uid = payload.get("source_pod_uid")
        node_name = payload.get("source_node_id")
        node_uid = payload.get("source_node_uid")
        slot_id = payload.get("source_logical_slot_id")
        if not all(
            isinstance(value, str) and value
            for value in (pod_name, pod_uid, node_name, node_uid, slot_id)
        ):
            raise TargetPreconditionFailed("retirement source identity is incomplete")
        try:
            pod = self._json("get", "pod", pod_name, "-n", self.namespace, "-o", "json")
        except TargetNotFound as error:
            raise ActionBlocked("SOURCE_POD_NOT_FOUND") from error
        metadata, spec = pod.get("metadata", {}), pod.get("spec", {})
        labels = metadata.get("labels", {}) if isinstance(metadata, dict) else {}
        owners = metadata.get("ownerReferences", []) if isinstance(metadata, dict) else []
        if not isinstance(metadata, dict) or metadata.get("uid") != pod_uid:
            raise ActionBlocked("SOURCE_POD_UID_CHANGED")
        if (
            metadata.get("name") != pod_name
            or not isinstance(labels, dict)
            or labels.get("fleetright.io/workload-uid") != workload_uid
            or labels.get("fleetright.io/replica-id") != slot_id
            or not isinstance(owners, list)
            or not any(
                isinstance(owner, dict)
                and owner.get("kind") == "FleetRightWorkload"
                and owner.get("name") == workload_id
                and owner.get("uid") == workload_uid
                and owner.get("controller") is True
                for owner in owners
            )
        ):
            raise ActionBlocked("SOURCE_POD_OWNER_OR_SLOT_CHANGED")
        if not isinstance(spec, dict) or spec.get("nodeName") != node_name:
            raise ActionBlocked("SOURCE_NODE_CHANGED")
        try:
            node = self._json("get", "node", node_name, "-o", "json")
        except TargetNotFound as error:
            raise ActionBlocked("SOURCE_NODE_NOT_FOUND") from error
        node_metadata = node.get("metadata", {})
        if not isinstance(node_metadata, dict) or node_metadata.get("uid") != node_uid:
            raise ActionBlocked("SOURCE_NODE_UID_CHANGED")

    def _require_fencing_schema(self) -> None:
        crd = self._json("get", "crd", "fleetrightworkloads.fleetright.io", "-o", "json")
        spec_root = crd.get("spec") if isinstance(crd, dict) else None
        versions = spec_root.get("versions", []) if isinstance(spec_root, dict) else []
        if not isinstance(versions, list):
            versions = []
        version = next(
            (
                item
                for item in versions
                if isinstance(item, dict) and item.get("name") == "v1alpha1"
            ),
            None,
        )
        version_schema = version.get("schema") if isinstance(version, dict) else None
        schema = version_schema.get("openAPIV3Schema") if isinstance(version_schema, dict) else None
        if not isinstance(schema, dict):
            schema = {}
        root_properties = schema.get("properties")
        root_rules = schema.get("x-kubernetes-validations", [])
        spec = root_properties.get("spec", {}) if isinstance(root_properties, dict) else {}
        status = root_properties.get("status", {}) if isinstance(root_properties, dict) else {}
        rules = spec.get("x-kubernetes-validations", []) if isinstance(spec, dict) else []
        properties = spec.get("properties", {}) if isinstance(spec, dict) else {}
        status_properties = status.get("properties", {}) if isinstance(status, dict) else {}
        fence = properties.get("executionFence", {}) if isinstance(properties, dict) else {}
        request = properties.get("executionRequest", {}) if isinstance(properties, dict) else {}
        protocol = properties.get("executionProtocol", {}) if isinstance(properties, dict) else {}
        pending = (
            status_properties.get("pendingPodCreate", {})
            if isinstance(status_properties, dict)
            else {}
        )
        authorized_pods = (
            status_properties.get("authorizedPodCreates", {})
            if isinstance(status_properties, dict)
            else {}
        )
        retired_slots = (
            status_properties.get("retiredReplicaSlots", {})
            if isinstance(status_properties, dict)
            else {}
        )
        if not isinstance(rules, list):
            rules = []
        if not isinstance(root_rules, list):
            root_rules = []
        required_fence_fields = fence.get("required", []) if isinstance(fence, dict) else []
        required_request_fields = request.get("required", []) if isinstance(request, dict) else []
        required_pending_fields = pending.get("required", []) if isinstance(pending, dict) else []
        request_properties = request.get("properties", {}) if isinstance(request, dict) else {}
        purpose = (
            request_properties.get("purpose", {}) if isinstance(request_properties, dict) else {}
        )
        minimum_ready = (
            request_properties.get("minimumReadyReplicas", {})
            if isinstance(request_properties, dict)
            else {}
        )
        destinations = (
            request_properties.get("destinationNodes", {})
            if isinstance(request_properties, dict)
            else {}
        )
        destination_items = destinations.get("items", {}) if isinstance(destinations, dict) else {}
        destination_required = (
            destination_items.get("required", []) if isinstance(destination_items, dict) else []
        )
        if not isinstance(destination_required, list):
            destination_required = []
        if not isinstance(required_fence_fields, list):
            required_fence_fields = []
        if not isinstance(required_request_fields, list):
            required_request_fields = []
        if not isinstance(required_pending_fields, list):
            required_pending_fields = []
        authorized_pod_properties = (
            authorized_pods.get("items", {}) if isinstance(authorized_pods, dict) else {}
        )
        retired_slot_properties = (
            retired_slots.get("items", {}) if isinstance(retired_slots, dict) else {}
        )
        authorized_pod_required = (
            authorized_pod_properties.get("required", [])
            if isinstance(authorized_pod_properties, dict)
            else []
        )
        retired_slot_required = (
            retired_slot_properties.get("required", [])
            if isinstance(retired_slot_properties, dict)
            else []
        )
        retired_slot_schema_properties = (
            retired_slot_properties.get("properties", {})
            if isinstance(retired_slot_properties, dict)
            else {}
        )
        readiness_stabilization = (
            retired_slot_schema_properties.get("readinessStabilizationMs", {})
            if isinstance(retired_slot_schema_properties, dict)
            else {}
        )
        installed_rules = {
            rule.get("rule")
            for rule in rules
            if isinstance(rule, dict) and isinstance(rule.get("rule"), str)
        }
        protocol_rule = next(
            (
                rule
                for rule in rules
                if isinstance(rule, dict) and rule.get("rule") == _WORKLOAD_POD_CREATE_PROTOCOL_RULE
            ),
            {},
        )
        pod_create_protocol = (
            properties.get("podCreateProtocol", {}) if isinstance(properties, dict) else {}
        )
        installed_root_rules = {
            rule.get("rule")
            for rule in root_rules
            if isinstance(rule, dict) and isinstance(rule.get("rule"), str)
        }
        if not (
            _WORKLOAD_EXECUTION_RULES <= installed_rules
            and _WORKLOAD_POD_CREATE_PROTOCOL_RULE in installed_rules
            and protocol_rule.get("optionalOldSelf") is True
            and {
                _WORKLOAD_PENDING_CREATE_RULE,
                _WORKLOAD_AUTHORIZED_POD_CREATES_RULE,
                _WORKLOAD_RETIRED_SLOTS_RULE,
                _WORKLOAD_CREATE_HISTORY_UNCERTAIN_RULE,
            }
            <= installed_root_rules
            and isinstance(fence, dict)
            and set(required_fence_fields)
            >= {
                "planID",
                "operationID",
                "generation",
                "expectedGeneration",
                "expectedReplicas",
                "fencedGeneration",
            }
            and isinstance(request, dict)
            and set(required_request_fields)
            >= {"planID", "operationID", "generation", "expectedReplicas", "targetReplicas"}
            and isinstance(purpose, dict)
            and purpose.get("enum") == ["staged-replacement-v1"]
            and isinstance(minimum_ready, dict)
            and minimum_ready.get("type") == "integer"
            and isinstance(destinations, dict)
            and destinations.get("type") == "array"
            and destinations.get("maxItems") == 8
            and destinations.get("x-kubernetes-list-type") == "map"
            and destinations.get("x-kubernetes-list-map-keys") == ["name"]
            and isinstance(destination_items, dict)
            and {"name", "uid"} <= set(destination_required)
            and isinstance(protocol, dict)
            and protocol.get("enum") == ["monotonic-scale-up-v1"]
            and isinstance(pending, dict)
            and set(required_pending_fields)
            >= {
                "workloadUID",
                "workloadGeneration",
                "podName",
                "replicaID",
                "planID",
                "operationID",
                "createID",
            }
            and isinstance(pod_create_protocol, dict)
            and pod_create_protocol.get("enum") == ["reserved-create-v1"]
            and isinstance(authorized_pods, dict)
            and authorized_pods.get("x-kubernetes-list-type") == "map"
            and authorized_pods.get("x-kubernetes-list-map-keys") == ["podUID"]
            and authorized_pods.get("maxItems") == 1_000
            and set(authorized_pod_required)
            >= {
                "workloadUID",
                "podUID",
                "podName",
                "replicaID",
                "workloadGeneration",
                "planID",
                "operationID",
                "createID",
            }
            and isinstance(retired_slots, dict)
            and retired_slots.get("x-kubernetes-list-type") == "map"
            and retired_slots.get("x-kubernetes-list-map-keys") == ["replicaID"]
            and retired_slots.get("maxItems") == 1_000
            and set(retired_slot_required)
            >= {"workloadUID", "replicaID", "podUID", "workloadGeneration", "planID", "operationID"}
            and isinstance(retired_slot_schema_properties, dict)
            and {
                "sourceNodeName",
                "sourceNodeUID",
                "replacementPodUID",
                "replacementPodName",
                "replacementReplicaID",
                "replacementNodeName",
                "replacementNodeUID",
                "replacementCreateID",
                "replacementWorkloadGeneration",
            }
            <= set(retired_slot_schema_properties)
            and isinstance(readiness_stabilization, dict)
            and readiness_stabilization.get("type") == "integer"
            and readiness_stabilization.get("minimum") == 0
        ):
            raise TargetPreconditionFailed("FleetRightWorkload execution CRD is not installed")

    def observe_scale(self, workload_id: str) -> dict[str, Any]:
        name = _checked_name(workload_id, "workload identifier")
        value = self._json("get", "fleetrightworkload", name, "-n", self.namespace, "-o", "json")
        return self._observe(value, name)

    @staticmethod
    def _observe(value: dict[str, Any], name: str) -> dict[str, Any]:
        metadata, spec, status = (
            value.get("metadata", {}),
            value.get("spec", {}),
            value.get("status", {}),
        )
        request = spec.get("executionRequest")
        fence = spec.get("executionFence")
        execution = status.get("execution")
        replicas, ready, available = (
            spec.get("replicas"),
            status.get("readyReplicas", 0),
            status.get("availableReplicas", 0),
        )
        observed_replicas = status.get("replicas", 0)
        conditions = status.get("conditions", [])
        generation = metadata.get("generation")
        history_uncertain = status.get("podCreateHistoryUncertain", False)
        unverified_pod_creates = status.get("unverifiedPodCreates", 0)
        authorized_pod_creates = status.get("authorizedPodCreates", [])
        retired_slots = status.get("retiredReplicaSlots", [])
        pending_create = status.get("pendingPodCreate")
        placement = spec.get("placement", {})
        approved_nodes = placement.get("allowedNodes", []) if isinstance(placement, dict) else []
        if (
            spec.get("workloadID") != name
            or not isinstance(metadata.get("uid"), str)
            or type(generation) is not int
            or not isinstance(metadata.get("resourceVersion"), str)
            or not all(
                type(item) is int and item >= 0
                for item in (replicas, ready, available, observed_replicas)
            )
            or type(spec.get("availabilityFloor")) is not int
            or type(status.get("observedGeneration", 0)) is not int
            or not isinstance(conditions, list)
            or (request is not None and not isinstance(request, dict))
            or (fence is not None and not isinstance(fence, dict))
            or (execution is not None and not isinstance(execution, dict))
            or type(history_uncertain) is not bool
            or type(unverified_pod_creates) is not int
            or unverified_pod_creates < 0
            or not isinstance(authorized_pod_creates, list)
            or any(
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
                or record.get("workloadUID") != metadata.get("uid")
                for record in authorized_pod_creates
            )
            or len(
                {record["podUID"] for record in authorized_pod_creates if isinstance(record, dict)}
            )
            != len(authorized_pod_creates)
            or not isinstance(retired_slots, list)
            or any(
                any(key in record for key in ("sourceNodeName", "sourceNodeUID"))
                and not all(
                    isinstance(record.get(key), str) and record[key]
                    for key in ("sourceNodeName", "sourceNodeUID")
                )
                for record in retired_slots
                if isinstance(record, dict)
            )
            or any(
                any(
                    key in record
                    for key in (
                        "replacementPodUID",
                        "replacementPodName",
                        "replacementReplicaID",
                        "replacementNodeName",
                        "replacementNodeUID",
                        "replacementCreateID",
                        "replacementWorkloadGeneration",
                    )
                )
                and (
                    not all(
                        isinstance(record.get(key), str) and record[key]
                        for key in (
                            "replacementPodUID",
                            "replacementPodName",
                            "replacementReplicaID",
                            "replacementNodeName",
                            "replacementNodeUID",
                            "replacementCreateID",
                        )
                    )
                    or type(record.get("replacementWorkloadGeneration")) is not int
                    or record.get("replacementWorkloadGeneration") < 1
                )
                for record in retired_slots
                if isinstance(record, dict)
            )
            or any(
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
                or record.get("workloadGeneration") < 1
                or record.get("workloadUID") != metadata.get("uid")
                for record in retired_slots
            )
            or len({record["replicaID"] for record in retired_slots if isinstance(record, dict)})
            != len(retired_slots)
            or (
                pending_create is not None
                and (
                    not isinstance(pending_create, dict)
                    or not all(
                        isinstance(pending_create.get(key), str) and pending_create[key]
                        for key in (
                            "workloadUID",
                            "podName",
                            "replicaID",
                            "planID",
                            "operationID",
                            "createID",
                        )
                    )
                    or type(pending_create.get("workloadGeneration")) is not int
                    or pending_create.get("workloadUID") != metadata.get("uid")
                )
            )
        ):
            raise TargetError("FleetRightWorkload identity or status is invalid")
        if metadata.get("name") != name:
            raise TargetPreconditionFailed("FleetRightWorkload name changed")
        if spec.get("executionProtocol") != "monotonic-scale-up-v1":
            raise TargetPreconditionFailed(
                "existing FleetRightWorkload was not created for monotonic scale-up"
            )
        if spec.get("podCreateProtocol") != "reserved-create-v1":
            raise TargetPreconditionFailed("FleetRightWorkload Pod CREATE history is uncertain")
        token = request.get("generation", 0) if isinstance(request, dict) else 0
        if type(token) is not int or token < 0:
            raise TargetError("FleetRightWorkload execution generation is invalid")
        if isinstance(request, dict) and (
            not all(
                isinstance(request.get(key), str) and request[key]
                for key in ("planID", "operationID")
            )
            or token < 1
            or type(request.get("expectedReplicas")) is not int
            or type(request.get("targetReplicas")) is not int
            or request["targetReplicas"] != replicas
            or request["expectedReplicas"] > request["targetReplicas"]
            or request.get("purpose") not in (None, "staged-replacement-v1")
            or (
                request.get("purpose") == "staged-replacement-v1"
                and (
                    type(request.get("minimumReadyReplicas")) is not int
                    or request["minimumReadyReplicas"] < spec["availabilityFloor"]
                    or request["minimumReadyReplicas"] > request["targetReplicas"]
                )
            )
        ):
            raise TargetError("FleetRightWorkload durable operation identity is invalid")
        fence_token = fence.get("generation", 0) if isinstance(fence, dict) else 0
        if isinstance(fence, dict) and (
            not all(
                isinstance(fence.get(key), str) and fence[key] for key in ("planID", "operationID")
            )
            or type(fence_token) is not int
            or fence_token < 1
            or type(fence.get("expectedGeneration")) is not int
            or type(fence.get("expectedReplicas")) is not int
            or type(fence.get("fencedGeneration")) is not int
            or fence["expectedGeneration"] < 1
            or fence["expectedReplicas"] < 0
            or fence["fencedGeneration"] < 1
        ):
            raise TargetError("FleetRightWorkload execution fence is invalid")
        if isinstance(fence, dict) and fence["fencedGeneration"] != generation:
            raise TargetPreconditionFailed("workload changed after the authority fence")
        if isinstance(fence, dict) and (
            fence_token < token
            or (
                fence_token == token
                and isinstance(request, dict)
                and (fence.get("planID"), fence.get("operationID"))
                != (request.get("planID"), request.get("operationID"))
            )
        ):
            raise TargetError("FleetRightWorkload execution fence is behind its request")
        return {
            "uid": metadata["uid"],
            "generation": generation,
            "resource_version": metadata["resourceVersion"],
            "replicas": replicas,
            "observed_replicas": observed_replicas,
            "ready_replicas": ready,
            "available_replicas": available,
            "reconciled": any(
                isinstance(condition, dict)
                and condition.get("type") == "Reconciled"
                and condition.get("status") == "True"
                for condition in conditions
            ),
            "availability_floor": spec["availabilityFloor"],
            "observed_generation": status.get("observedGeneration", 0),
            "fencing_token": max(token, fence_token),
            "generation_tracks_spec": True,
            "workload_execution": True,
            "pod_create_protocol": spec.get("podCreateProtocol"),
            "execution_request": request,
            "execution_fence": fence,
            "execution": execution,
            "pending_pod_create": pending_create,
            "pod_create_history_uncertain": history_uncertain,
            "unverified_pod_creates": unverified_pod_creates,
            "authorized_pod_creates": authorized_pod_creates,
            "retired_replica_slots": retired_slots,
            "approved_nodes": approved_nodes,
            "placement_topology": placement.get("topology"),
            "require_hard_pod_anti_affinity": (
                isinstance(placement, dict) and placement.get("requirePodAntiAffinity") is True
            ),
            "surge_budget": spec.get("surgeBudget", 0),
            "pod_create_admission_protocol": spec.get("podCreateAdmissionProtocol"),
        }

    def fence_scale(
        self,
        workload_id: str,
        fencing_token: int,
        *,
        expected: dict[str, Any],
        plan_id: str,
        operation_id: str,
        target_replicas: int,
        destination_nodes: list[dict[str, str]] | None = None,
        purpose: str | None = None,
        minimum_ready_replicas: int | None = None,
        source_replica_id: str | None = None,
        source_pod_uid: str | None = None,
    ) -> dict[str, Any]:
        if (
            type(fencing_token) is not int
            or fencing_token <= 0
            or not plan_id
            or not operation_id
            or type(target_replicas) is not int
            or (
                purpose == "staged-replacement-v1"
                and (
                    type(minimum_ready_replicas) is not int
                    or minimum_ready_replicas < 0
                    or minimum_ready_replicas > target_replicas
                )
            )
        ):
            raise TargetPreconditionFailed("invalid workload execution fence")
        current = self.observe_scale(workload_id)
        if target_replicas > current["replicas"] and (
            current["pod_create_protocol"] != "reserved-create-v1"
            or current["pod_create_admission_protocol"] != "claimed-create-v1"
        ):
            raise ActionBlocked("UNSUPPORTED_POD_CREATE_PROTOCOL")
        if purpose == "staged-replacement-v1":
            minimum_ready_replicas = max(current["availability_floor"], minimum_ready_replicas)
            if minimum_ready_replicas > target_replicas:
                raise ActionBlocked("APPROVED_READY_FLOOR_UNAVAILABLE")
        if current["pod_create_history_uncertain"] or current["unverified_pod_creates"]:
            raise ActionBlocked("POD_CREATE_HISTORY_UNCERTAIN")
        self._check_retired_source(
            current, source_replica_id, source_pod_uid, plan_id, operation_id
        )
        if current["pending_pod_create"] is not None:
            raise ActionBlocked("POD_CREATE_OUTCOME_UNKNOWN")
        if current["uid"] != expected.get("uid"):
            raise TargetPreconditionFailed("FleetRightWorkload identity changed")
        request = current["execution_request"]
        fence = current["execution_fence"]
        same_request = (
            isinstance(request, dict)
            and request.get("planID") == plan_id
            and request.get("operationID") == operation_id
            and request.get("targetReplicas") == target_replicas
            and request.get("destinationNodes") == destination_nodes
            and request.get("purpose") == purpose
            and request.get("minimumReadyReplicas") == minimum_ready_replicas
            and current["replicas"] == target_replicas
            and request.get("generation", fencing_token) <= fencing_token
        )
        same_fence = (
            isinstance(fence, dict)
            and fence.get("generation") == fencing_token
            and fence.get("planID") == plan_id
            and fence.get("operationID") == operation_id
        )
        applied_before_fence = bool(same_request)
        expected_unchanged = all(
            current.get(key) == expected.get(key) for key in ("generation", "replicas")
        )
        if not expected_unchanged and not (same_fence or same_request):
            raise TargetPreconditionFailed("FleetRightWorkload changed before fencing")
        if current["fencing_token"] > fencing_token:
            raise TargetPreconditionFailed("a newer workload execution generation exists")
        self._require_fencing_schema()
        if (
            isinstance(request, dict)
            and not same_request
            and not (
                isinstance(current["execution"], dict)
                and current["execution"].get("planID") == request.get("planID")
                and current["execution"].get("operationID") == request.get("operationID")
                and current["execution"].get("generation") == request.get("generation")
                and current["execution"].get("phase") == "APPLIED"
                and current["observed_generation"] == current["generation"]
            )
        ):
            raise ActionBlocked("PRIOR_WORKLOAD_OPERATION_UNRESOLVED")
        if same_fence:
            same_request = (
                isinstance(request, dict)
                and (request.get("planID"), request.get("operationID")) == (plan_id, operation_id)
                and current["replicas"] == target_replicas
            )
            fence_precondition = {
                "uid": current["uid"],
                "generation": fence["fencedGeneration"] - 1,
                "replicas": current["replicas"] if same_request else fence["expectedReplicas"],
            }
            return {
                **current,
                "fence_precondition": fence_precondition,
                "fence_applied": True,
                "fence_replayed": True,
                "operation_applied_before_fence": applied_before_fence,
            }

        if same_request and not expected_unchanged:
            raise TargetPreconditionFailed("workload changed before fencing")

        if isinstance(fence, dict) and (fence.get("planID"), fence.get("operationID")) == (
            plan_id,
            operation_id,
        ):
            base_generation = fence["expectedGeneration"]
            base_replicas = fence["expectedReplicas"]
        elif same_request:
            base_generation = current["generation"] - 1
            base_replicas = request["expectedReplicas"]
        else:
            base_generation = expected["generation"]
            base_replicas = expected["replicas"]
        fence_precondition = {key: current[key] for key in ("uid", "generation", "replicas")}
        new_fence = {
            "planID": plan_id,
            "operationID": operation_id,
            "generation": fencing_token,
            "expectedGeneration": base_generation,
            "expectedReplicas": base_replicas,
            "fencedGeneration": current["generation"] + 1,
        }
        value = self._json(
            "patch",
            "fleetrightworkload",
            _checked_name(workload_id, "workload identifier"),
            "-n",
            self.namespace,
            "--type=merge",
            "-p",
            json.dumps(
                {
                    "metadata": {"resourceVersion": current["resource_version"]},
                    "spec": {"executionFence": new_fence},
                },
                separators=(",", ":"),
            ),
            "-o",
            "json",
        )
        fenced = self._observe(value, workload_id)
        if (
            fenced["uid"] != current["uid"]
            or fenced["replicas"] != current["replicas"]
            or fenced["generation"] != current["generation"] + 1
            or fenced["fencing_token"] != fencing_token
            or fenced["execution_fence"] != new_fence
        ):
            raise OutcomeUnknown("Kubernetes did not confirm the workload authority fence")
        return {
            **fenced,
            "fence_precondition": fence_precondition,
            "fence_applied": True,
            "fence_replayed": False,
            "prior_execution_fence": fence if isinstance(fence, dict) else None,
            "operation_applied_before_fence": applied_before_fence,
        }

    def authorize_scale(
        self,
        workload_id: str,
        fencing_token: int,
        *,
        expected: dict[str, Any],
        plan_id: str,
        operation_id: str,
        target_replicas: int,
        destination_nodes: list[dict[str, str]] | None = None,
        purpose: str | None = None,
        minimum_ready_replicas: int | None = None,
        source_replica_id: str | None = None,
        source_pod_uid: str | None = None,
    ) -> dict[str, Any]:
        if (
            type(fencing_token) is not int
            or fencing_token <= 0
            or not plan_id
            or not operation_id
            or type(target_replicas) is not int
            or not 0 <= target_replicas <= 1000
            or purpose not in (None, "staged-replacement-v1")
            or (
                purpose == "staged-replacement-v1"
                and (
                    type(minimum_ready_replicas) is not int
                    or minimum_ready_replicas < 0
                    or minimum_ready_replicas > target_replicas
                )
            )
            or (
                destination_nodes is not None
                and (
                    not isinstance(destination_nodes, list)
                    or not 1 <= len(destination_nodes) <= 8
                    or any(
                        not isinstance(node, dict)
                        or not isinstance(node.get("name"), str)
                        or not node["name"]
                        or not isinstance(node.get("uid"), str)
                        or not node["uid"]
                        for node in destination_nodes
                    )
                    or len({node["name"] for node in destination_nodes}) != len(destination_nodes)
                )
            )
        ):
            raise TargetPreconditionFailed("invalid durable workload authorization")
        current = self.observe_scale(workload_id)
        if target_replicas > current["replicas"] and (
            current["pod_create_protocol"] != "reserved-create-v1"
            or current["pod_create_admission_protocol"] != "claimed-create-v1"
        ):
            raise ActionBlocked("UNSUPPORTED_POD_CREATE_PROTOCOL")
        if current["pod_create_history_uncertain"] or current["unverified_pod_creates"]:
            raise ActionBlocked("POD_CREATE_HISTORY_UNCERTAIN")
        self._check_retired_source(
            current, source_replica_id, source_pod_uid, plan_id, operation_id
        )
        if current["pending_pod_create"] is not None:
            raise ActionBlocked("POD_CREATE_OUTCOME_UNKNOWN")
        request = current["execution_request"]
        same = (
            isinstance(request, dict)
            and request.get("planID") == plan_id
            and request.get("operationID") == operation_id
            and request.get("targetReplicas") == target_replicas
            and request.get("destinationNodes") == destination_nodes
            and request.get("purpose") == purpose
            and request.get("minimumReadyReplicas") == minimum_ready_replicas
        )
        recovered = (
            same
            and current["fencing_token"] == fencing_token
            and current["uid"] == expected.get("uid")
            and current["generation"] == expected.get("generation", -2) + 1
            and current["replicas"] == target_replicas
        )
        if current["uid"] != expected.get("uid"):
            raise TargetPreconditionFailed("FleetRightWorkload identity changed")
        if destination_nodes is not None:
            approved = current.get("approved_nodes")
            if not isinstance(approved, list) or any(
                not isinstance(node, dict)
                or not isinstance(node.get("name"), str)
                or not isinstance(node.get("uid"), str)
                for node in approved
            ):
                raise ActionBlocked("WORKLOAD_PLACEMENT_IDENTITY_INVALID")
            if any(
                not any(
                    item["name"] == node["name"] and item["uid"] == node["uid"] for item in approved
                )
                for node in destination_nodes
            ):
                raise ActionBlocked("DESTINATION_NODE_OUTSIDE_WORKLOAD_PLACEMENT")
        if purpose == "staged-replacement-v1" and (
            current["require_hard_pod_anti_affinity"] is not True
            or current["surge_budget"] < 1
            or (
                same
                and (
                    not isinstance(request, dict)
                    or request.get("expectedReplicas") != target_replicas - 1
                    or current["replicas"] != target_replicas
                )
            )
            or (not same and target_replicas != current["replicas"] + 1)
            or minimum_ready_replicas < current["availability_floor"]
        ):
            raise ActionBlocked("APPROVED_REPLACEMENT_SURGE_UNAVAILABLE")
        if not recovered and (
            current["generation"] != expected.get("generation")
            or current["replicas"] != expected.get("replicas")
        ):
            raise TargetPreconditionFailed("FleetRightWorkload precondition changed")
        if same and current["fencing_token"] == fencing_token:
            return {**current, "fence_precondition": expected, "already_authorized": True}
        if current["fencing_token"] > fencing_token:
            raise TargetPreconditionFailed("a newer workload execution generation exists")
        if target_replicas < current["replicas"]:
            raise ActionBlocked("WORKLOAD_SCALE_DOWN_DISABLED")
        self._require_fencing_schema()
        fence = current["execution_fence"]
        fenced_base_ready = (
            isinstance(fence, dict)
            and fence.get("planID") == plan_id
            and fence.get("operationID") == operation_id
            and fence.get("expectedGeneration") == current["observed_generation"]
            and fence.get("expectedReplicas") == current["replicas"]
            and current["observed_replicas"] == current["replicas"]
            and current["reconciled"]
        )
        if (
            current["observed_generation"] != current["generation"]
            and not same
            and not fenced_base_ready
        ):
            raise ActionBlocked("WORKLOAD_RECONCILIATION_PENDING")
        if (
            min(current["ready_replicas"], current["available_replicas"])
            < current["availability_floor"]
        ):
            raise ActionBlocked("AVAILABILITY_FLOOR_NOT_MET")
        if isinstance(request, dict) and not same:
            status = current["execution"]
            if not (
                isinstance(status, dict)
                and status.get("planID") == request.get("planID")
                and status.get("operationID") == request.get("operationID")
                and status.get("generation") == request.get("generation")
                and status.get("phase") == "APPLIED"
            ):
                raise ActionBlocked("PRIOR_WORKLOAD_OPERATION_UNRESOLVED")
        elif (
            request is None
            and current["replicas"]
            and current["ready_replicas"] < current["replicas"]
        ):
            raise ActionBlocked("WORKLOAD_NOT_READY")
        if not same and not current["reconciled"]:
            raise ActionBlocked("WORKLOAD_NOT_RECONCILED")
        new_request = {
            "planID": plan_id,
            "operationID": operation_id,
            "generation": fencing_token,
            "expectedReplicas": (
                request["expectedReplicas"]
                if same and isinstance(request, dict)
                else current["replicas"]
            ),
            "targetReplicas": target_replicas,
        }
        if destination_nodes is not None:
            new_request["destinationNodes"] = destination_nodes
        if purpose is not None:
            new_request["purpose"] = purpose
        if minimum_ready_replicas is not None:
            new_request["minimumReadyReplicas"] = minimum_ready_replicas
        current_fence = current["execution_fence"]
        base_generation = (
            current_fence["expectedGeneration"]
            if isinstance(current_fence, dict)
            and (current_fence.get("planID"), current_fence.get("operationID"))
            == (plan_id, operation_id)
            else current["generation"]
        )
        base_replicas = (
            current_fence["expectedReplicas"]
            if isinstance(current_fence, dict)
            and (current_fence.get("planID"), current_fence.get("operationID"))
            == (plan_id, operation_id)
            else current["replicas"]
        )
        new_fence = {
            "planID": plan_id,
            "operationID": operation_id,
            "generation": fencing_token,
            "expectedGeneration": base_generation,
            "expectedReplicas": base_replicas,
            "fencedGeneration": current["generation"] + 1,
        }
        patch: dict[str, Any] = {
            "metadata": {"resourceVersion": current["resource_version"]},
            "spec": {"executionRequest": new_request, "executionFence": new_fence},
        }
        if target_replicas != current["replicas"]:
            patch["spec"]["replicas"] = target_replicas
        value = self._json(
            "patch",
            "fleetrightworkload",
            _checked_name(workload_id, "workload identifier"),
            "-n",
            self.namespace,
            "--type=merge",
            "-p",
            json.dumps(patch, separators=(",", ":")),
            "-o",
            "json",
        )
        published = self._observe(value, workload_id)
        if (
            published["uid"] != current["uid"]
            or published["replicas"] != target_replicas
            or published["fencing_token"] != fencing_token
            or published["execution_request"].get("planID") != plan_id
            or published["execution_request"].get("operationID") != operation_id
        ):
            raise OutcomeUnknown("Kubernetes did not confirm the durable workload authorization")
        return {**published, "fence_precondition": expected, "authorized": True}

    def set_replicas(
        self,
        workload_id: str,
        replicas: int,
        observed: dict[str, Any],
        *,
        expected_replicas: int,
        fencing_token: int,
        plan_id: str,
        operation_id: str,
    ) -> dict[str, Any]:
        current = self.observe_scale(workload_id)
        request = current["execution_request"]
        if (
            not isinstance(request, dict)
            or current["uid"] != observed["uid"]
            or current["replicas"] != replicas
            or current["replicas"] < expected_replicas
            or current["fencing_token"] != fencing_token
            or request.get("planID") != plan_id
            or request.get("operationID") != operation_id
            or request.get("targetReplicas") != replicas
        ):
            raise TargetPreconditionFailed("durable workload authorization changed")
        return {**current, "applied": True}


class DockerComposeTarget:
    """Compose adapter with a host-local process fence."""

    def __init__(self, *, compose_file: Path | None = None, project: str | None = None):
        self.compose_file = compose_file or Path(
            os.getenv(
                "FLEETRIGHT_DOCKER_COMPOSE_FILE",
                Path(__file__).resolve().parents[2] / "docker-compose.yml",
            )
        )
        self.project = project or os.getenv("FLEETRIGHT_DOCKER_PROJECT", "fleetright")
        _checked_name(self.project, "Docker Compose project")

    def _compose(self, *args: str, timeout: float = 15) -> CommandResult:
        command = [
            "docker",
            "compose",
            "--project-name",
            self.project,
            "--file",
            str(self.compose_file),
            *args,
        ]
        try:
            return _run(command, timeout=timeout)
        except TargetError as error:
            if isinstance(error, OutcomeUnknown):
                raise
            raise

    def _service(self, workload_id: str) -> str:
        name = _checked_name(workload_id, "workload identifier")
        service = _WORKLOAD_SERVICES.get(name)
        if service is None:
            raise TargetPreconditionFailed("workload has no Docker Compose mapping")
        return service

    @contextmanager
    def _fence(self, token: int | None = None):
        # ponytail: local Docker uses a host lock; multi-host Docker needs an API CAS.
        directory = Path(
            os.getenv("FLEETRIGHT_DOCKER_FENCE_DIR", "/tmp/fleetright-execution-fences")
        )
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        if directory.stat().st_uid != os.getuid():
            raise TargetPreconditionFailed("Docker fence directory has a different owner")
        os.chmod(directory, 0o700)
        path = directory / f"{hashlib.sha256(self.project.encode()).hexdigest()}.lock"
        flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(path, flags, 0o600)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
                raise TargetPreconditionFailed("Docker fence file is not a private regular file")
            os.fchmod(fd, 0o600)
            fcntl.flock(fd, fcntl.LOCK_EX)
            raw = os.read(fd, 64).decode("ascii", "strict").strip()
            current = int(raw) if raw else 0
            if token is not None:
                if token < current:
                    raise TargetPreconditionFailed("a newer controller fenced this Docker project")
                if token > current:
                    os.lseek(fd, 0, os.SEEK_SET)
                    os.ftruncate(fd, 0)
                    os.write(fd, str(token).encode("ascii"))
                    os.fsync(fd)
                    current = token
            yield current
        except (UnicodeDecodeError, ValueError) as error:
            raise TargetError("Docker fencing state is corrupt") from error
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    def fence_scale(
        self,
        workload_id: str,
        fencing_token: int,
        *,
        expected: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not isinstance(fencing_token, int) or fencing_token <= 0:
            raise TargetPreconditionFailed("fencing token must be positive")
        with self._fence(fencing_token):
            current = self.observe_scale(workload_id)
            if expected is not None and any(
                current.get(key) != expected.get(key) for key in ("uid", "generation", "replicas")
            ):
                raise TargetPreconditionFailed("Compose service changed before fencing")
            return {
                **current,
                "fencing_token": fencing_token,
                "fence_precondition": {
                    key: current[key] for key in ("uid", "generation", "replicas")
                },
            }

    def observe_scale(self, workload_id: str) -> dict[str, Any]:
        service = self._service(workload_id)
        result = self._compose("ps", "--all", "--quiet", service)
        ids = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        if any(not _CONTAINER_ID.fullmatch(item) for item in ids):
            raise TargetError("Docker returned an invalid container ID")
        ids.sort()
        identity = f"{self.project}/{service}"
        generation = int(hashlib.sha256("\n".join(ids).encode()).hexdigest()[:12], 16)
        if not ids:
            return {
                "uid": identity,
                "generation": generation,
                "observed_generation": generation,
                "replicas": 0,
                "container_ids": [],
                "ready_replicas": 0,
            }
        details = _run(["docker", "inspect", *ids]).stdout
        try:
            containers = json.loads(details)
        except json.JSONDecodeError as error:
            raise TargetError("Docker returned invalid container state") from error
        if not isinstance(containers, list) or len(containers) != len(ids):
            raise TargetError("Docker container state is incomplete")
        ready = 0
        for item in containers:
            state = item.get("State") if isinstance(item, dict) else None
            if not isinstance(state, dict) or state.get("Running") is not True:
                continue
            health = state.get("Health")
            if health is None or (isinstance(health, dict) and health.get("Status") == "healthy"):
                ready += 1
        return {
            "uid": identity,
            "generation": generation,
            "observed_generation": generation,
            "replicas": len(ids),
            "container_ids": ids,
            "ready_replicas": ready,
        }

    def set_replicas(
        self,
        workload_id: str,
        replicas: int,
        observed: dict[str, Any],
        *,
        expected_replicas: int,
        fencing_token: int,
    ) -> dict[str, Any]:
        if not isinstance(replicas, int) or replicas < 0 or replicas > 100:
            raise TargetPreconditionFailed("replica target is outside the supported range")
        service = self._service(workload_id)
        if replicas > 1:
            try:
                output = self._compose("config", "--no-interpolate", "--format", "json").stdout
                config = json.loads(output)
                ports = config["services"][service].get("ports", [])
            except (AttributeError, json.JSONDecodeError, KeyError, TypeError) as error:
                raise TargetPreconditionFailed(
                    "cannot verify Compose service ports for multiple replicas"
                ) from error
            if not isinstance(ports, list) or ports:
                raise TargetPreconditionFailed(
                    "multiple Compose replicas require a service without published host ports"
                )
        with self._fence(fencing_token) as active_token:
            if active_token != fencing_token:
                raise TargetPreconditionFailed("Docker fencing token changed before mutation")
            current = self.observe_scale(workload_id)
            if current["uid"] != observed.get("uid") or current["generation"] != observed.get(
                "generation"
            ):
                raise TargetPreconditionFailed("Compose service identity changed")
            if current["replicas"] == replicas:
                return {**current, "fencing_token": fencing_token, "already_applied": True}
            if current["replicas"] != expected_replicas:
                raise TargetPreconditionFailed("Compose service replica precondition changed")
            try:
                self._compose(
                    "up",
                    "--detach",
                    "--no-deps",
                    "--scale",
                    f"{service}={replicas}",
                    service,
                    timeout=60,
                )
            except OutcomeUnknown:
                raise
            except TargetError as error:
                # Compose may have reconciled some containers before a later failure.
                raise OutcomeUnknown(
                    "Compose response was inconclusive; inspect container state"
                ) from error
            return {
                **self.observe_scale(workload_id),
                "fencing_token": fencing_token,
                "applied": True,
            }


class WorkloadHealthObserver:
    """Reads the existing synthetic workload's readiness and Prometheus exposition."""

    def __init__(self, targets: dict[str, str] | None = None, *, timeout: float = 2):
        if targets is None:
            raw = os.getenv("FLEETRIGHT_HEALTH_TARGETS_JSON")
            if raw:
                try:
                    targets = json.loads(raw)
                except json.JSONDecodeError as error:
                    raise ValueError("FLEETRIGHT_HEALTH_TARGETS_JSON must be valid JSON") from error
            else:
                targets = {
                    "cpu-api": "http://127.0.0.1:8081",
                    "memory-api": "http://127.0.0.1:8082",
                    "queued-api": "http://127.0.0.1:8083",
                }
        if not isinstance(targets, dict) or timeout <= 0:
            raise ValueError("health targets and positive timeout are required")
        self.targets: dict[str, str] = {}
        for workload, url in targets.items():
            _checked_name(workload, "workload identifier")
            parsed = urlsplit(url) if isinstance(url, str) else None
            if (
                parsed is None
                or parsed.scheme not in {"http", "https"}
                or not parsed.hostname
                or parsed.username is not None
                or parsed.password is not None
                or parsed.query
                or parsed.fragment
                or parsed.path not in {"", "/"}
            ):
                raise ValueError("health target URL must be an unauthenticated HTTP(S) origin")
            try:
                if parsed.port is not None and not 1 <= parsed.port <= 65535:
                    raise ValueError("health target port is invalid")
            except ValueError as error:
                raise ValueError("health target URL is invalid") from error
            self.targets[workload] = url.rstrip("/")
        self.timeout = timeout
        self._sample_lock = threading.Lock()
        self._samples: dict[str, tuple[float, dict[float, float], dict[str, float]]] = {}

    def _get(self, url: str) -> tuple[int, str]:
        request = urllib.request.Request(url, headers={"Accept": "text/plain"})
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return response.status, response.read(1024 * 1024).decode("utf-8", "replace")
        except urllib.error.HTTPError as error:
            return error.code, ""
        except (TimeoutError, OSError) as error:
            raise TargetUnavailable("health target unavailable or stale") from error

    def observe(
        self,
        workload_id: str,
        *,
        p99_limit_ms: int,
        error_slo_ppm: int,
        max_queue_growth_milli: int = 0,
        baseline: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if p99_limit_ms <= 0 or not 0 <= error_slo_ppm <= 1_000_000 or max_queue_growth_milli < 0:
            raise ValueError("health thresholds are invalid")
        url = self.targets.get(_checked_name(workload_id, "workload identifier"))
        if url is None:
            raise TargetUnavailable("health target is not configured")
        ready_status, _ = self._get(f"{url}/ready")
        status, body = self._get(f"{url}/metrics")
        if status != 200:
            raise TargetUnavailable("health metrics endpoint is not ready")
        buckets: dict[float, float] = {}
        scalar: dict[str, float] = {}
        request_results = {"success": 0.0, "error": 0.0, "rejected": 0.0}
        for line in body.splitlines():
            match = re.fullmatch(
                r'fleetright_request_duration_seconds_bucket\{le="([^\"]+)"\}\s+([0-9.eE+-]+)',
                line,
            )
            if match:
                boundary = float("inf") if match.group(1) == "+Inf" else float(match.group(1))
                buckets[boundary] = float(match.group(2))
                continue
            match = re.fullmatch(
                r'fleetright_requests_total\{result="(success|error|rejected)"\}\s+([0-9.eE+-]+)',
                line,
            )
            if match:
                request_results[match.group(1)] = float(match.group(2))
                continue
            match = re.fullmatch(
                r"(fleetright_request_duration_seconds_count|fleetright_queue_growth_milli_requests_per_second)\s+([0-9.eE+-]+)",
                line,
            )
            if match:
                scalar[match.group(1)] = float(match.group(2))
        count = scalar.get("fleetright_request_duration_seconds_count")
        queue_growth = scalar.get("fleetright_queue_growth_milli_requests_per_second")
        values = [count, queue_growth, *request_results.values(), *buckets.values()]
        if any(value is not None and not math.isfinite(value) for value in values):
            raise TargetError("health metrics contain non-finite values")
        if (count is not None and count < 0) or any(
            value < 0 for value in request_results.values()
        ):
            raise TargetError("health metrics contain negative counts")
        if count is None or sum(request_results.values()) > count:
            raise TargetError("health request counters are incomplete or inconsistent")

        now_ms = time.time_ns() // 1_000_000
        with self._sample_lock:
            previous = self._samples.get(workload_id)
            self._samples[workload_id] = (count or 0, buckets, request_results)
        if baseline is not None:
            try:
                baseline_buckets = {
                    float("inf") if key == "+Inf" else float(key): float(value)
                    for key, value in baseline["buckets"].items()
                }
                baseline_results = {key: float(value) for key, value in baseline["results"].items()}
                previous = (
                    float(baseline["request_count"]),
                    baseline_buckets,
                    baseline_results,
                )
            except (KeyError, TypeError, ValueError) as error:
                raise TargetError("health baseline is invalid") from error
        fresh_sample = previous is not None
        if previous is None:
            window_count = count or 0
            window_buckets = buckets
            window_results = request_results
        else:
            previous_count, previous_buckets, previous_results = previous
            counters_monotonic = (
                (count or 0) >= previous_count
                and all(
                    buckets.get(boundary, 0) >= previous_buckets.get(boundary, 0)
                    for boundary in previous_buckets
                )
                and all(request_results[key] >= previous_results[key] for key in request_results)
            )
            if counters_monotonic:
                window_count = (count or 0) - previous_count
                window_buckets = {
                    boundary: value - previous_buckets.get(boundary, 0)
                    for boundary, value in buckets.items()
                }
                window_results = {
                    key: request_results[key] - previous_results[key] for key in request_results
                }
            else:
                fresh_sample = False
                window_count = 0
                window_buckets = {}
                window_results = {"success": 0.0, "error": 0.0, "rejected": 0.0}
        finite_slo_bucket = max(
            (
                value
                for boundary, value in window_buckets.items()
                if boundary <= p99_limit_ms / 1000
            ),
            default=0.0,
        )
        errors = window_results["error"] + window_results["rejected"]
        error_ppm = 0 if window_count == 0 else math.ceil(errors * 1_000_000 / window_count)
        p99_ok = window_count > 0 and finite_slo_bucket >= window_count * 0.99
        error_ok = window_count > 0 and error_ppm <= error_slo_ppm
        queue_ok = queue_growth is not None and queue_growth <= max_queue_growth_milli
        return {
            "ready": ready_status == 200,
            "fresh_sample": fresh_sample and window_count > 0,
            "window_requests": int(window_count),
            "p99_within_slo": p99_ok,
            "error_rate_ppm": error_ppm,
            "error_rate_within_slo": error_ok,
            "queue_growth_milli": queue_growth,
            "queue_growth_within_limit": queue_ok,
            "observed_at_ms": now_ms,
            "counters": {
                "request_count": count or 0,
                "buckets": {
                    "+Inf" if boundary == float("inf") else str(boundary): value
                    for boundary, value in buckets.items()
                },
                "results": request_results,
            },
            "healthy": ready_status == 200 and p99_ok and error_ok and queue_ok,
        }
