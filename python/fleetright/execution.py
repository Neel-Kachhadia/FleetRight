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
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

_DNS_LABEL = re.compile(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?\Z")
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
    "!has(self.executionRequest) || self.executionRequest.targetReplicas == self.replicas",
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

# ponytail: ceilings cover three checked-in workloads; add one with each new target.
EXECUTION_HEALTH_CEILINGS = {
    "cpu-api": (120, 1_000),
    "memory-api": (150, 1_000),
    "queued-api": (80, 1_000),
}


def valid_execution_health_policy(workload_id: str, policy: object) -> bool:
    ceiling = EXECUTION_HEALTH_CEILINGS.get(workload_id)
    if not isinstance(policy, dict) or ceiling is None:
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
    ceiling = EXECUTION_HEALTH_CEILINGS.get(workload_id)
    if type(p99_limit_ms) is not int or p99_limit_ms <= 0:
        return None
    if type(error_slo_ppm) is not int or not 0 <= error_slo_ppm <= 1_000_000:
        return None
    return (
        {
            "p99_limit_ms": min(p99_limit_ms, ceiling[0]),
            "error_slo_ppm": min(error_slo_ppm, ceiling[1]),
            "max_queue_growth_milli": 0,
        }
        if ceiling is not None
        else None
    )


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
        if "disruption budget" in message:
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
        spec = root_properties.get("spec", {}) if isinstance(root_properties, dict) else {}
        rules = spec.get("x-kubernetes-validations", []) if isinstance(spec, dict) else []
        properties = spec.get("properties", {}) if isinstance(spec, dict) else {}
        fence = properties.get("executionFence", {}) if isinstance(properties, dict) else {}
        request = properties.get("executionRequest", {}) if isinstance(properties, dict) else {}
        protocol = properties.get("executionProtocol", {}) if isinstance(properties, dict) else {}
        if not isinstance(rules, list):
            rules = []
        required_fence_fields = fence.get("required", []) if isinstance(fence, dict) else []
        required_request_fields = request.get("required", []) if isinstance(request, dict) else []
        if not isinstance(required_fence_fields, list):
            required_fence_fields = []
        if not isinstance(required_request_fields, list):
            required_request_fields = []
        installed_rules = {
            rule.get("rule")
            for rule in rules
            if isinstance(rule, dict) and isinstance(rule.get("rule"), str)
        }
        if not (
            _WORKLOAD_EXECUTION_RULES <= installed_rules
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
            and isinstance(protocol, dict)
            and protocol.get("enum") == ["monotonic-scale-up-v1"]
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
        ):
            raise TargetError("FleetRightWorkload identity or status is invalid")
        if metadata.get("name") != name:
            raise TargetPreconditionFailed("FleetRightWorkload name changed")
        if spec.get("executionProtocol") != "monotonic-scale-up-v1":
            raise TargetPreconditionFailed(
                "existing FleetRightWorkload was not created for monotonic scale-up"
            )
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
            "execution_request": request,
            "execution_fence": fence,
            "execution": execution,
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
    ) -> dict[str, Any]:
        if (
            type(fencing_token) is not int
            or fencing_token <= 0
            or not plan_id
            or not operation_id
            or type(target_replicas) is not int
        ):
            raise TargetPreconditionFailed("invalid workload execution fence")
        current = self.observe_scale(workload_id)
        if current["uid"] != expected.get("uid"):
            raise TargetPreconditionFailed("FleetRightWorkload identity changed")
        request = current["execution_request"]
        fence = current["execution_fence"]
        same_request = (
            isinstance(request, dict)
            and request.get("planID") == plan_id
            and request.get("operationID") == operation_id
            and request.get("targetReplicas") == target_replicas
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
    ) -> dict[str, Any]:
        if (
            type(fencing_token) is not int
            or fencing_token <= 0
            or not plan_id
            or not operation_id
            or type(target_replicas) is not int
            or not 0 <= target_replicas <= 1000
        ):
            raise TargetPreconditionFailed("invalid durable workload authorization")
        current = self.observe_scale(workload_id)
        request = current["execution_request"]
        same = (
            isinstance(request, dict)
            and request.get("planID") == plan_id
            and request.get("operationID") == operation_id
            and request.get("targetReplicas") == target_replicas
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
            "expectedReplicas": current["replicas"],
            "targetReplicas": target_replicas,
        }
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
    """Single-replica Compose adapter with a host-local process fence."""

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
        running = sum(
            1
            for item in containers
            if isinstance(item, dict)
            and isinstance(item.get("State"), dict)
            and item["State"].get("Running") is True
        )
        return {
            "uid": identity,
            "generation": generation,
            "observed_generation": generation,
            "replicas": len(ids),
            "container_ids": ids,
            "ready_replicas": running,
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
        if replicas > 1:
            raise TargetPreconditionFailed(
                "the checked-in Compose service has a fixed published port and supports one replica"
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
            service = self._service(workload_id)
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
