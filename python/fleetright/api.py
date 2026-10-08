from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import subprocess
import tempfile
import threading
import time
import uuid
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import psycopg
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from psycopg.types.json import Jsonb
from pydantic import BaseModel, ConfigDict, Field, field_validator
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from fleetright.contracts import decode_text, encode_snapshot
from fleetright.control import (
    ArtifactIntegrityError,
    ControlError,
    ExecutionConflict,
    IdempotencyConflict,
    LeaseConflict,
    Repository,
    StaleState,
    canonical_json,
    transition_allowed,
)
from fleetright.execution import (
    DockerComposeTarget,
    KubernetesTarget,
    TargetError,
    TargetNotFound,
    WorkloadHealthObserver,
    bounded_execution_health_policy,
    valid_execution_health_policy,
)
from fleetright.forecast import (
    DemandForecast,
    EwmaTrendModel,
    Observation,
    RollingQuantileModel,
    ScaleInEvidence,
    SeasonalTimeOfDayModel,
    forecast_decision,
    scale_in_eligibility,
)
from fleetright.reconciler import ExecutionController

ROOT = Path(__file__).resolve().parents[2]
MAX_REQUEST_BYTES = 2 * 1024 * 1024
MAX_ARTIFACT_BYTES = 64 * 1024 * 1024
PLANNER_TIMEOUT_SECONDS = 15
ROLES = {"viewer", "recommender", "operator", "admin"}
READ_ROLES = ROLES
OPTIMIZE_ROLES = {"recommender", "operator", "admin"}
OPERATOR_ROLES = {"operator", "admin"}
EXECUTION_WORKLOADS = {"cpu-api", "memory-api", "queued-api"}


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class ObservationInput(StrictModel):
    observed_at_ms: int = Field(ge=0)
    qps_milli: int = Field(ge=0)
    concurrency: int = Field(ge=0)


class ScaleInEvidenceInput(StrictModel):
    samples: list[ObservationInput] = Field(max_length=1000)
    reference_qps_milli: int = Field(gt=0)
    reference_concurrency: int = Field(gt=0)
    telemetry_coverage: float = Field(ge=0, le=1)
    required_stabilization_ms: int = Field(default=300000, ge=0, le=86400000)
    minimum_sample_count: int = Field(default=5, ge=2, le=1000)
    minimum_coverage: float = Field(default=0.95, gt=0, le=1)
    telemetry_max_age_ms: int = Field(default=30000, ge=1, le=3600000)
    recovery_reserve_replicas: int = Field(default=0, ge=0)
    required_recovery_reserve: int = Field(default=1, ge=0)
    recent_bad_health: bool = False


class OptimizeRequest(StrictModel):
    snapshot: dict[str, Any]
    history: dict[str, list[ObservationInput]] = Field(default_factory=dict, max_length=100)
    model: Literal["rolling_quantile", "ewma_trend", "seasonal_time_of_day"] = "ewma_trend"
    scale_in_evidence: dict[str, ScaleInEvidenceInput] = Field(default_factory=dict)
    scheduling_buffer_ms: int = Field(default=30000, ge=0, le=300000)
    mode: Literal["OBSERVE", "RECOMMEND", "SHADOW"] = "RECOMMEND"

    @field_validator("history")
    @classmethod
    def bound_history(
        cls, value: dict[str, list[ObservationInput]]
    ) -> dict[str, list[ObservationInput]]:
        if any(len(samples) > 1000 for samples in value.values()):
            raise ValueError("each forecast history is limited to 1000 samples")
        return value


class OperationRequest(StrictModel):
    reason: str = Field(default="operator-request", min_length=1, max_length=256)

    @field_validator("reason")
    @classmethod
    def no_secret_text(cls, value: str) -> str:
        if re.search(r"(?i)\b(bearer|password|secret|api[_-]?key|token)\s*[:=]", value):
            raise ValueError("reason must not contain credential-like text")
        return value


@dataclass(frozen=True)
class Principal:
    name: str
    role: str


class BodyTooLarge(Exception):
    pass


class RequestSizeLimit:
    def __init__(self, app: ASGIApp, maximum: int):
        self.app = app
        self.maximum = maximum

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = dict(scope.get("headers", []))
        content_length = headers.get(b"content-length")
        if content_length is not None:
            try:
                if int(content_length) > self.maximum:
                    await self._too_large(send)
                    return
            except ValueError:
                await self._too_large(send)
                return
        received = 0

        async def limited_receive() -> Message:
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.maximum:
                    raise BodyTooLarge
            return message

        try:
            await self.app(scope, limited_receive, send)
        except BodyTooLarge:
            await self._too_large(send)

    @staticmethod
    async def _too_large(send: Send) -> None:
        body = b'{"detail":"request body exceeds 2 MiB"}'
        await send(
            {
                "type": "http.response.start",
                "status": 413,
                "headers": [(b"content-type", b"application/json")],
            }
        )
        await send({"type": "http.response.body", "body": body})


def _request_id(request: Request) -> str:
    value = getattr(request.state, "request_id", None)
    return value if isinstance(value, str) else str(uuid.uuid4())


def _token_index(tokens: dict[str, Principal] | None = None) -> dict[str, Principal]:
    if tokens is None:
        raw = os.getenv("FLEETRIGHT_TOKENS_JSON", "{}")
        try:
            values = json.loads(raw)
        except json.JSONDecodeError as error:
            raise RuntimeError("FLEETRIGHT_TOKENS_JSON must be valid JSON") from error
        if not isinstance(values, dict):
            raise RuntimeError("FLEETRIGHT_TOKENS_JSON must be an object")
        tokens = {}
        for token, identity in values.items():
            if not isinstance(token, str) or not isinstance(identity, dict):
                raise RuntimeError("invalid development token entry")
            name, role = identity.get("principal"), identity.get("role")
            if not isinstance(name, str) or not name or role not in ROLES:
                raise RuntimeError("invalid development token identity")
            tokens[token] = Principal(name, role)
    index: dict[str, Principal] = {}
    for token, principal in tokens.items():
        if len(token) < 24 or principal.role not in ROLES or not principal.name:
            raise RuntimeError(
                "development tokens must be long and map to a valid server-side role"
            )
        index[hashlib.sha256(token.encode()).hexdigest()] = principal
    return index


def _authenticate(authorization: str | None, index: dict[str, Principal]) -> Principal | None:
    if not authorization or not authorization.startswith("Bearer "):
        return None
    token = authorization[7:]
    if not token or len(token) > 4096:
        return None
    supplied = hashlib.sha256(token.encode()).hexdigest()
    for digest, principal in index.items():
        if hmac.compare_digest(supplied, digest):
            return principal
    return None


def _audit_denial(
    repository: Repository | None,
    request: Request,
    principal: Principal | None,
    operation: str,
    result: str,
    reason: str,
) -> None:
    if repository is None:
        return
    try:
        with repository.transaction() as conn:
            Repository.audit(
                conn,
                actor="anonymous" if principal is None else principal.name,
                role="anonymous" if principal is None else principal.role,
                request_id=_request_id(request),
                operation=operation,
                result=result,
                reason=reason,
            )
    except psycopg.Error:
        pass


def _require(*allowed: str):
    def dependency(request: Request, authorization: str | None = Header(default=None)) -> Principal:
        principal = _authenticate(authorization, request.app.state.tokens)
        operation = f"{request.method.lower()} {request.url.path}"
        if principal is None:
            _audit_denial(
                request.app.state.repository,
                request,
                None,
                operation,
                "DENIED",
                "authentication-required",
            )
            raise HTTPException(
                status_code=401,
                detail="authentication-required",
                headers={"WWW-Authenticate": "Bearer"},
            )
        if principal.role not in allowed:
            _audit_denial(
                request.app.state.repository,
                request,
                principal,
                operation,
                "DENIED",
                "insufficient-role",
            )
            raise HTTPException(status_code=403, detail="insufficient-role")
        return principal

    return dependency


def _required_idempotency_key(value: str | None) -> str:
    if not value or len(value) > 128 or not re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", value):
        raise HTTPException(status_code=400, detail="valid Idempotency-Key header is required")
    return value


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _sha256(value: object) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def _proto_blocks(text: str, field: str) -> list[str]:
    pattern = re.compile(rf"(?m)^\s*{re.escape(field)}\s*\{{")
    blocks: list[str] = []
    position = 0
    while match := pattern.search(text, position):
        opening = text.find("{", match.start(), match.end())
        depth = 0
        quoted = False
        escaped = False
        end = opening
        while end < len(text):
            char = text[end]
            if quoted:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == '"':
                    quoted = False
            elif char == '"':
                quoted = True
            elif char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    blocks.append(text[opening + 1 : end])
                    position = end + 1
                    break
            end += 1
        else:
            raise ValueError(f"unterminated protobuf block {field}")
    return blocks


def _proto_scalars(text: str, field: str) -> list[object]:
    pattern = re.compile(rf"(?m)^\s*{re.escape(field)}\s*:\s*(.*?)\s*$")
    values: list[object] = []
    for match in pattern.finditer(text):
        raw = match.group(1)
        if raw.startswith('"'):
            values.append(json.loads(raw))
        elif raw in {"true", "false"}:
            values.append(raw == "true")
        else:
            try:
                values.append(int(raw))
            except ValueError:
                values.append(raw)
    return values


def _proto_scalar(text: str, field: str, default: object = None) -> object:
    values = _proto_scalars(text, field)
    return values[0] if values else default


def _plan_summary(plan_text: str) -> dict[str, object]:
    decisions: list[dict[str, object]] = []
    evacuations: list[dict[str, object]] = []
    for block in _proto_blocks(plan_text, "workload_decisions"):
        resources = _proto_blocks(block, "proposed_resources")
        decisions.append(
            {
                "workload_id": _proto_scalar(block, "workload_id", ""),
                "current_replicas": _proto_scalar(block, "current_replicas", 0),
                "proposed_replicas": _proto_scalar(block, "proposed_replicas", 0),
                "proposed_resources": {
                    key: _proto_scalar(resources[0], key, 0) if resources else 0
                    for key in ("cpu_millicores", "memory_mib", "network_mbps")
                },
                "destructive": _proto_scalar(block, "destructive", False),
            }
        )
    for block in _proto_blocks(plan_text, "evacuation_steps"):
        evacuations.append(
            {
                "sequence": _proto_scalar(block, "sequence", 0),
                "workload_id": _proto_scalar(block, "workload_id", ""),
                "replica_id": _proto_scalar(block, "replica_id", ""),
                "source_node_id": _proto_scalar(block, "source_node_id", ""),
                "destination_node_id": _proto_scalar(block, "destination_node_id", ""),
            }
        )
    return {
        "plan_id": _proto_scalar(plan_text, "plan_id", ""),
        "created_at_ms": _proto_scalar(plan_text, "created_at_ms", 0),
        "expires_at_ms": _proto_scalar(plan_text, "expires_at_ms", 0),
        "snapshot_id": _proto_scalar(plan_text, "snapshot_id", ""),
        "base_generation": _proto_scalar(plan_text, "base_generation", 0),
        "feasible": _proto_scalar(plan_text, "feasible", False),
        "planner_build_id": _proto_scalar(plan_text, "optimizer_build_id", "unknown"),
        "decisions": decisions,
        "deactivated_node_ids": _proto_scalars(plan_text, "deactivated_node_ids"),
        "evacuation_count": len(evacuations),
        "evacuations": evacuations,
    }


def _run_planner(snapshot_bytes: bytes) -> tuple[bytes, bytes, str, str, dict[str, object]]:
    planner = Path(os.getenv("FLEETRIGHT_PLANNER", ROOT / "build/fleetright-plan"))
    verifier = Path(os.getenv("FLEETRIGHT_VERIFIER", ROOT / "build/fleetright-verify"))
    if not planner.is_file() or not verifier.is_file():
        raise HTTPException(status_code=503, detail="planner-or-verifier-unavailable")
    with tempfile.TemporaryDirectory(prefix="fleetright-plan-") as temp:
        directory = Path(temp)
        snapshot_path, plan_path, result_path = (
            directory / "snapshot.pb",
            directory / "plan.pb",
            directory / "verification.pb",
        )
        snapshot_path.write_bytes(snapshot_bytes)
        try:
            plan_run = subprocess.run(
                [str(planner), str(snapshot_path), str(plan_path)],
                capture_output=True,
                timeout=PLANNER_TIMEOUT_SECONDS,
                check=False,
            )
            if plan_run.returncode or not plan_path.is_file():
                raise HTTPException(status_code=503, detail="planner-failed")
            plan_bytes = plan_path.read_bytes()
            if len(plan_bytes) > MAX_ARTIFACT_BYTES:
                raise HTTPException(status_code=413, detail="planner-output-too-large")
            verify_run = subprocess.run(
                [str(verifier), str(snapshot_path), str(plan_path), str(result_path)],
                capture_output=True,
                timeout=PLANNER_TIMEOUT_SECONDS,
                check=False,
            )
            if verify_run.returncode or not result_path.is_file():
                raise HTTPException(status_code=503, detail="verifier-failed")
            result_bytes = result_path.read_bytes()
            if len(result_bytes) > MAX_ARTIFACT_BYTES:
                raise HTTPException(status_code=413, detail="verifier-output-too-large")
        except subprocess.TimeoutExpired as error:
            raise HTTPException(status_code=503, detail="planner-or-verifier-timeout") from error
    plan_text = decode_text(
        "fleetright.v1.OptimizationPlan", "fleetright/v1/optimizer.proto", plan_bytes
    )
    result_text = decode_text(
        "fleetright.v1.VerificationResult", "fleetright/v1/optimizer.proto", result_bytes
    )
    summary = _plan_summary(plan_text)
    if not summary["plan_id"]:
        raise HTTPException(status_code=503, detail="planner-returned-malformed-plan")
    accepted = _proto_scalar(result_text, "accepted", False)
    verification_hash = _proto_scalar(result_text, "canonical_result_sha256", "")
    verifier_build_id = _proto_scalar(result_text, "verifier_build_id", "unknown")
    return (
        plan_bytes,
        result_bytes,
        plan_text,
        result_text,
        {
            **summary,
            "verification_accepted": accepted is True,
            "verification_hash": verification_hash,
            "verifier_build_id": verifier_build_id,
        },
    )


def _model(name: str):
    return {
        "rolling_quantile": RollingQuantileModel,
        "ewma_trend": EwmaTrendModel,
        "seasonal_time_of_day": SeasonalTimeOfDayModel,
    }[name]()


def _binding(snapshot: dict[str, Any], normalized: dict[str, object]) -> dict[str, object]:
    policy = normalized["policy"]
    profiles = normalized["capacity_profiles"]
    catalog = normalized["machine_catalog"]
    placements: dict[str, list[dict[str, str]]] = {}
    for item in snapshot["current_placements"]:
        placements.setdefault(item["workload_id"], []).append(
            {"replica_id": item["replica_id"], "node_id": item["node_id"]}
        )
    workload_hashes = {
        item["workload_id"]: _sha256(
            {
                "workload": item,
                "placements": sorted(
                    placements.get(item["workload_id"], []), key=lambda row: row["replica_id"]
                ),
            }
        )
        for item in snapshot["workloads"]
    }
    stable_snapshot = dict(snapshot)
    stable_snapshot.pop("evaluation_time_ms", None)
    stable_snapshot.pop("content_sha256", None)
    # ponytail: one cluster-wide lease set; per-scope planning only when parallelism matters.
    return {
        "snapshot_id": snapshot["snapshot_id"],
        "snapshot_generation": snapshot["generation"],
        "material_snapshot_sha256": _sha256(stable_snapshot),
        "policy_hash": _sha256(policy),
        "profile_hashes": {item["profile_id"]: item["profile_sha256"] for item in profiles},
        "machine_catalog_hash": catalog["catalog_sha256"],
        "workload_hashes": workload_hashes,
        "workload_ids": sorted(workload_hashes),
        "conflict_keys": sorted(
            [f"workload:{workload_id}" for workload_id in workload_hashes]
            + [f"node:{node['node_id']}" for node in snapshot["nodes"]]
        ),
    }


def _scale_in_evidence(
    workload: dict[str, Any],
    profiles: list[dict[str, Any]],
    evidence: ScaleInEvidenceInput | None,
    now_ms: int,
) -> tuple[bool, tuple[str, ...]]:
    if evidence is None:
        return False, ("OBSERVED_DOWNSCALE_STABILIZATION_NOT_MET",)
    profile = next(
        (
            item
            for item in profiles
            if item["workload_id"] == workload["workload_id"]
            and item["allocation"] == workload["current_resources"]
        ),
        None,
    )
    if profile is None:
        return False, ("CAPACITY_PROFILE_INVALID",)
    raw_health = evidence.recent_bad_health or bool(workload.get("recent_oom_or_reclaim", False))
    item = ScaleInEvidence(
        samples=[Observation(**entry.model_dump()) for entry in evidence.samples],
        reference_qps_milli=evidence.reference_qps_milli,
        reference_concurrency=evidence.reference_concurrency,
        telemetry_coverage=evidence.telemetry_coverage,
        required_stabilization_ms=evidence.required_stabilization_ms,
        minimum_sample_count=evidence.minimum_sample_count,
        minimum_coverage=evidence.minimum_coverage,
        telemetry_max_age_ms=evidence.telemetry_max_age_ms,
        profile_valid_until_ms=profile["valid_until_ms"],
        recent_bad_health=raw_health,
        recovery_reserve_replicas=evidence.recovery_reserve_replicas,
        required_recovery_reserve=evidence.required_recovery_reserve,
    )
    return scale_in_eligibility(item, now_ms)


def _reduction_workloads(summary: dict[str, object], snapshot: dict[str, Any]) -> set[str]:
    source = {item["workload_id"]: item for item in snapshot["workloads"]}
    reduced: set[str] = set()
    for decision in summary["decisions"]:
        workload_id = decision["workload_id"]
        current = source.get(workload_id)
        if current is None:
            reduced.add(str(workload_id))
            continue
        if decision["proposed_replicas"] < decision["current_replicas"]:
            reduced.add(workload_id)
        if any(
            decision["proposed_resources"][resource] < current["current_resources"][resource]
            for resource in ("cpu_millicores", "memory_mib", "network_mbps")
        ):
            reduced.add(workload_id)
    if summary["deactivated_node_ids"] or summary["evacuation_count"]:
        reduced.update(source)
    return reduced


_SNAPSHOT_KEYS = {
    "schema_version",
    "snapshot_id",
    "generation",
    "observed_at_ms",
    "expires_at_ms",
    "evaluation_time_ms",
    "workloads",
    "nodes",
    "current_placements",
    "capacity_profiles",
    "machine_catalog",
    "policy",
    "content_sha256",
}
_WORKLOAD_KEYS = {
    "workload_id",
    "priority",
    "min_replicas",
    "max_replicas",
    "availability_floor",
    "demand_qps_milli",
    "concurrency_bound",
    "latency_slo_ms",
    "error_slo_ppm",
    "startup_ms",
    "readiness_stabilization_ms",
    "failure_tolerance",
    "min_topology_domains",
    "max_topology_skew",
    "topology_key",
    "current_resources",
    "image_digest",
    "request_mix_id",
    "runtime_config_class",
    "platform_class",
    "profile_confidence_factor_ppm",
    "model_accuracy_factor_ppm",
    "degraded_cluster_factor_ppm",
    "max_throttle_ppm",
    "max_memory_pressure_ppm",
    "max_scale_down_fraction_ppm",
    "max_moves",
    "recent_oom_or_reclaim",
    "tolerated_taint_keys",
}
_POLICY_KEYS = {
    "schema_version",
    "headroom_ppm",
    "minimum_profile_confidence_ppm",
    "maximum_profile_age_ms",
    "maximum_snapshot_age_ms",
    "maximum_plan_actions",
    "movement_cost_micros",
    "minimum_improvement_ppm",
    "maximum_local_search_iterations",
    "allow_synthetic_profiles",
    "planning_horizon_seconds",
    "action_churn_cost_micros",
    "uncertainty_cost_micros_per_replica",
    "boundary_cost_micros_per_workload",
}
_PROFILE_KEYS = {
    "schema_version",
    "profile_id",
    "profile_version",
    "workload_id",
    "image_digest",
    "request_mix_id",
    "runtime_config_class",
    "platform_class",
    "allocation",
    "generated_at_ms",
    "valid_until_ms",
    "confidence_ppm",
    "conservative_haircut_ppm",
    "safe_points",
    "measurement_kind",
    "enforceable_resource_limits",
    "profile_sha256",
}
_SAFE_POINT_KEYS = {
    "safe_qps_milli",
    "safe_concurrency",
    "p99_upper_ms",
    "error_rate_upper_ppm",
    "queue_growth_upper_milli",
    "memory_pressure_ppm",
    "cpu_throttling_ppm",
    "request_count",
    "repeated_run_count",
    "threshold_successes",
    "threshold_trials",
    "latency_success_lower_ppm",
    "error_rate_wilson_upper_ppm",
    "run_dispersion_ppm",
    "variance_ppm",
    "histogram_config_id",
    "stable_across_runs",
    "error_count",
}
_NODE_KEYS = {
    "node_id",
    "machine_type",
    "allocatable",
    "reserved",
    "hourly_cost_micros",
    "labels",
    "taints",
    "health",
    "active",
    "generation",
}
_RESOURCE_KEYS = {"cpu_millicores", "memory_mib", "network_mbps"}


def _check_keys(value: object, allowed: set[str], name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise HTTPException(status_code=422, detail=f"{name}-must-be-an-object")
    unknown = set(value) - allowed
    if unknown:
        raise HTTPException(status_code=422, detail=f"unknown-fields-in-{name}")
    return value


def _check_resource(value: object, name: str) -> None:
    resource = _check_keys(value, _RESOURCE_KEYS, name)
    if set(resource) != _RESOURCE_KEYS:
        raise HTTPException(status_code=422, detail=f"{name}-missing-resource-units")


def _check_label_pairs(value: object, name: str) -> None:
    if not isinstance(value, list):
        raise HTTPException(status_code=422, detail=f"{name}-must-be-a-list")
    for pair in value:
        if (
            not isinstance(pair, (list, tuple))
            or len(pair) != 2
            or not all(isinstance(item, str) for item in pair)
        ):
            raise HTTPException(status_code=422, detail=f"{name}-contains-invalid-pair")


def _validate_snapshot(
    snapshot: dict[str, Any], now_ms: int
) -> tuple[dict[str, Any], bytes, dict[str, Any]]:
    fields = _check_keys(snapshot, _SNAPSHOT_KEYS, "snapshot")
    if fields.get("schema_version", 1) != 1:
        raise HTTPException(status_code=422, detail="unsupported-snapshot-schema-version")
    required = _SNAPSHOT_KEYS - {"schema_version", "content_sha256"}
    if not required.issubset(fields):
        raise HTTPException(status_code=422, detail="snapshot-missing-required-fields")
    if (
        not isinstance(fields["snapshot_id"], str)
        or not fields["snapshot_id"]
        or len(fields["snapshot_id"]) > 255
    ):
        raise HTTPException(status_code=422, detail="invalid-snapshot-id")
    if (
        isinstance(fields["generation"], bool)
        or not isinstance(fields["generation"], int)
        or fields["generation"] < 0
    ):
        raise HTTPException(status_code=422, detail="invalid-snapshot-generation")
    policy = _check_keys(fields["policy"], _POLICY_KEYS, "policy")
    if policy.get("schema_version") != 1:
        raise HTTPException(status_code=422, detail="unsupported-policy-schema-version")
    for key in ("observed_at_ms", "expires_at_ms"):
        if isinstance(fields[key], bool) or not isinstance(fields[key], int) or fields[key] < 0:
            raise HTTPException(status_code=422, detail=f"invalid-{key}")
    if fields["observed_at_ms"] > now_ms or fields["expires_at_ms"] <= now_ms:
        raise HTTPException(status_code=409, detail="snapshot-not-current")
    maximum_age = policy.get("maximum_snapshot_age_ms")
    if (
        not isinstance(maximum_age, int)
        or maximum_age < 0
        or now_ms - fields["observed_at_ms"] > maximum_age
    ):
        raise HTTPException(status_code=409, detail="snapshot-stale")
    for key in ("workloads", "nodes", "current_placements", "capacity_profiles"):
        if not isinstance(fields[key], list) or len(fields[key]) > 10000:
            raise HTTPException(status_code=422, detail=f"invalid-{key}")
    for workload in fields["workloads"]:
        _check_keys(workload, _WORKLOAD_KEYS, "workload")
        _check_resource(workload.get("current_resources"), "workload-current-resources")
    for profile in fields["capacity_profiles"]:
        _check_keys(profile, _PROFILE_KEYS, "capacity-profile")
        if profile.get("schema_version", 1) != 1:
            raise HTTPException(status_code=422, detail="unsupported-profile-schema-version")
        if not isinstance(profile.get("safe_points"), list):
            raise HTTPException(status_code=422, detail="invalid-capacity-profile-safe-points")
        _check_resource(profile.get("allocation"), "capacity-profile-allocation")
        for point in profile["safe_points"]:
            _check_keys(point, _SAFE_POINT_KEYS, "capacity-profile-safe-point")
    for node in fields["nodes"]:
        _check_keys(node, _NODE_KEYS, "node")
        _check_resource(node.get("allocatable"), "node-allocatable")
        _check_resource(node.get("reserved"), "node-reserved")
        _check_label_pairs(node.get("labels"), "node-labels")
        _check_label_pairs(node.get("taints", []), "node-taints")
    for placement in fields["current_placements"]:
        _check_keys(placement, {"workload_id", "replica_id", "node_id"}, "placement")
    catalog = _check_keys(
        fields["machine_catalog"],
        {"schema_version", "catalog_version", "machines", "catalog_sha256"},
        "machine-catalog",
    )
    if catalog.get("schema_version", 1) != 1 or not isinstance(catalog.get("machines"), list):
        raise HTTPException(status_code=422, detail="unsupported-or-malformed-machine-catalog")
    for machine in catalog["machines"]:
        _check_keys(machine, {"machine_type", "allocatable", "hourly_cost_micros"}, "machine")
    result = deepcopy(fields)
    result.pop("schema_version", None)
    result.pop("content_sha256", None)
    result["evaluation_time_ms"] = now_ms
    try:
        canonical_json(fields)
        encoded, normalized = encode_snapshot(result)
    except (
        KeyError,
        TypeError,
        ValueError,
        OSError,
        AttributeError,
        IndexError,
        OverflowError,
    ) as error:
        raise HTTPException(status_code=422, detail="snapshot-contract-invalid") from error
    if len(encoded) > MAX_ARTIFACT_BYTES:
        raise HTTPException(status_code=413, detail="snapshot-too-large")
    return result, encoded, normalized


def _database_now_ms(conn: psycopg.Connection) -> int:
    row = conn.execute(
        "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms"
    ).fetchone()
    return int(row["now_ms"])


def _response_plan(conn: psycopg.Connection, plan_id: str) -> dict[str, object]:
    row = conn.execute(
        "SELECT plan_id, created_at_ms, expires_at_ms, snapshot_id, snapshot_generation, state, "
        "state_version, reason_codes, planner_build_id, verifier_build_id, verification_accepted, "
        "admission_accepted, verification_hash, forecast_metadata FROM plans WHERE plan_id = %s",
        (plan_id,),
    ).fetchone()
    if row is None:
        raise ControlError("persisted plan disappeared")
    artifact_rows = conn.execute(
        "SELECT role, artifact_sha256 FROM plan_artifacts WHERE plan_id = %s ORDER BY role",
        (plan_id,),
    ).fetchall()
    row["artifact_hashes"] = {
        item["role"]: item["artifact_sha256"].strip() for item in artifact_rows
    }
    row["candidate_actions"] = conn.execute(
        "SELECT intent_id, action_type, workload_id, payload, status "
        "FROM action_intents WHERE plan_id = %s ORDER BY intent_id",
        (plan_id,),
    ).fetchall()
    return row


def _verify_lease_tokens(conn: psycopg.Connection, owner: str, leases: dict[str, int]) -> bool:
    for conflict_key, token in leases.items():
        row = conn.execute(
            "SELECT 1 FROM conflict_leases WHERE conflict_key = %s AND owner_id = %s "
            "AND fencing_token = %s AND expires_at > clock_timestamp() FOR UPDATE",
            (conflict_key, owner, token),
        ).fetchone()
        if row is None:
            return False
    return True


def _run_verifier(snapshot_bytes: bytes, plan_bytes: bytes) -> tuple[bool, str, str]:
    verifier = Path(os.getenv("FLEETRIGHT_VERIFIER", ROOT / "build/fleetright-verify"))
    if not verifier.is_file():
        raise HTTPException(status_code=503, detail="verifier-unavailable")
    with tempfile.TemporaryDirectory(prefix="fleetright-verify-") as temp:
        directory = Path(temp)
        snapshot_path, plan_path, result_path = (
            directory / "snapshot.pb",
            directory / "plan.pb",
            directory / "result.pb",
        )
        snapshot_path.write_bytes(snapshot_bytes)
        plan_path.write_bytes(plan_bytes)
        try:
            result = subprocess.run(
                [str(verifier), str(snapshot_path), str(plan_path), str(result_path)],
                capture_output=True,
                timeout=PLANNER_TIMEOUT_SECONDS,
                check=False,
            )
        except subprocess.TimeoutExpired as error:
            raise HTTPException(status_code=503, detail="verifier-timeout") from error
        if result.returncode or not result_path.is_file():
            return False, "", "unknown"
        text = decode_text(
            "fleetright.v1.VerificationResult",
            "fleetright/v1/optimizer.proto",
            result_path.read_bytes(),
        )
    return (
        _proto_scalar(text, "accepted", False) is True,
        str(_proto_scalar(text, "canonical_result_sha256", "")),
        str(_proto_scalar(text, "verifier_build_id", "unknown")),
    )


def _public_plan(row: dict[str, object]) -> dict[str, object]:
    result = dict(row)
    result.pop("binding", None)
    return result


def _supported_execution_workloads(target: str | None) -> set[str]:
    return {"cpu-api"} if target == "kubernetes" else EXECUTION_WORKLOADS


def _execution_admission_reasons(
    summary: dict[str, object], source_workloads: list[dict[str, Any]], target: str | None
) -> list[str]:
    reasons: list[str] = []
    if summary["deactivated_node_ids"]:
        reasons.append("UNSUPPORTED_EXECUTION_ACTION_DEACTIVATE_NODE")
    if summary["evacuation_count"]:
        reasons.append("UNSUPPORTED_EXECUTION_ACTION_MOVE_REPLICA")

    current = {workload["workload_id"]: workload for workload in source_workloads}
    supported = _supported_execution_workloads(target)
    for decision in summary["decisions"]:
        workload_id = decision["workload_id"]
        source = current.get(workload_id)
        if source is None:
            reasons.append("UNSUPPORTED_EXECUTION_ACTION_INVALID")
            continue
        if decision["proposed_resources"] != source["current_resources"]:
            reasons.append("UNSUPPORTED_EXECUTION_ACTION_RESIZE_WORKLOAD")
        if (
            decision["proposed_replicas"] != decision["current_replicas"]
            and workload_id not in supported
        ):
            reasons.append(f"UNSUPPORTED_EXECUTION_WORKLOAD:{workload_id}")
    return list(dict.fromkeys(reasons))


def create_app(
    repository: Repository | None = None,
    *,
    tokens: dict[str, Principal] | None = None,
    mode: str | None = None,
    controller_id: str | None = None,
    execution_target: str | None = None,
    target_adapter: Any | None = None,
    health_observer: Any | None = None,
    start_execution_worker: bool | None = None,
) -> FastAPI:
    if repository is None:
        database_url = os.getenv("FLEETRIGHT_DATABASE_URL", "")
        repository = Repository(database_url) if database_url else None
    token_hashes = _token_index(tokens)
    configured_mode = mode or os.getenv("FLEETRIGHT_MODE", "RECOMMEND")
    if configured_mode not in {"OBSERVE", "RECOMMEND", "SHADOW", "EXECUTE"}:
        raise RuntimeError("FLEETRIGHT_MODE must be OBSERVE, RECOMMEND, SHADOW or EXECUTE")
    target_name = execution_target or os.getenv("FLEETRIGHT_EXECUTION_TARGET", "")
    if (
        configured_mode == "EXECUTE"
        and not target_adapter
        and target_name
        not in {
            "docker",
            "kubernetes",
        }
    ):
        raise RuntimeError("EXECUTE mode requires FLEETRIGHT_EXECUTION_TARGET=docker or kubernetes")
    instance_id = controller_id or f"controller-{uuid.uuid4()}"
    app = FastAPI(title="FleetRight Control API", version="2.0b")
    app.state.repository = repository
    app.state.tokens = token_hashes
    app.state.mode = configured_mode
    app.state.controller_id = instance_id
    app.state.execution_enabled = configured_mode == "EXECUTE"
    app.state.execution_target = target_name
    if app.state.execution_enabled:
        if target_adapter:
            target = target_adapter
        elif target_name == "docker":
            target = DockerComposeTarget()
        else:
            kubeconfig = os.getenv("FLEETRIGHT_KUBERNETES_KUBECONFIG", "")
            namespace = os.getenv("FLEETRIGHT_KUBERNETES_NAMESPACE", "")
            if not kubeconfig:
                raise RuntimeError(
                    "Kubernetes EXECUTE requires an explicit least-privilege kubeconfig"
                )
            if namespace != "fleetright":
                raise RuntimeError("Kubernetes EXECUTE requires namespace fleetright")
            target = KubernetesTarget(kubeconfig=kubeconfig, namespace=namespace)
        health = health_observer or WorkloadHealthObserver()
        app.state.execution_controller = ExecutionController(
            repository,
            target,
            health,
            instance_id,
            lease_ttl_ms=int(os.getenv("FLEETRIGHT_EXECUTION_LEASE_TTL_MS", "120000")),
            max_attempts=2,
            readiness_timeout_seconds=float(
                os.getenv("FLEETRIGHT_READINESS_TIMEOUT_SECONDS", "60")
            ),
            poll_seconds=float(os.getenv("FLEETRIGHT_RECONCILE_POLL_SECONDS", "1")),
        )
    else:
        app.state.execution_controller = None
    app.state.execution_stop = None
    app.state.execution_thread = None
    run_worker = (
        configured_mode == "EXECUTE" if start_execution_worker is None else start_execution_worker
    )

    def start_execution_worker_fn() -> None:
        if not run_worker or app.state.execution_controller is None:
            return
        stop = threading.Event()
        worker = threading.Thread(
            target=app.state.execution_controller.run_forever,
            args=(stop,),
            name="fleetright-execution-reconciler",
            daemon=True,
        )
        app.state.execution_stop = stop
        app.state.execution_thread = worker
        worker.start()

    def stop_execution_worker_fn() -> None:
        stop, worker = app.state.execution_stop, app.state.execution_thread
        if stop is not None:
            stop.set()
        if worker is not None:
            worker.join(timeout=70)

    app.router.on_startup.append(start_execution_worker_fn)
    app.router.on_shutdown.append(stop_execution_worker_fn)
    app.add_middleware(RequestSizeLimit, maximum=MAX_REQUEST_BYTES)

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        request_id = request.headers.get("X-Request-ID", "")
        request.state.request_id = (
            request_id if re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", request_id) else str(uuid.uuid4())
        )
        response = await call_next(request)
        response.headers["X-Request-ID"] = request.state.request_id
        return response

    def require_repository() -> Repository:
        if app.state.repository is None:
            raise HTTPException(status_code=503, detail="database-not-configured")
        return app.state.repository

    @app.get("/healthz")
    def healthz():
        return {"status": "ok", "execution_enabled": app.state.execution_enabled}

    @app.get("/readyz")
    def readyz():
        repo = require_repository()
        ready, reason = repo.readiness()
        if not ready:
            raise HTTPException(status_code=503, detail=reason)
        return {"status": "ready", "database": "postgresql", "mode": app.state.mode}

    @app.get("/v1/state")
    def get_state(principal: Principal = Depends(_require(*READ_ROLES))):
        repo = require_repository()
        state = repo.current_state()
        return {
            "mode": app.state.mode,
            "execution_enabled": app.state.execution_enabled,
            "snapshot_id": None if state is None else state["snapshot_id"],
            "snapshot_generation": None if state is None else state["snapshot_generation"],
        }

    @app.get("/v1/plans")
    def list_plans(
        limit: int = Query(default=100, ge=1, le=200),
        principal: Principal = Depends(_require(*READ_ROLES)),
    ):
        return require_repository().list_plans(limit)

    @app.get("/v1/plans/{plan_id}")
    def get_plan(plan_id: str, principal: Principal = Depends(_require(*READ_ROLES))):
        row = require_repository().get_plan(plan_id)
        if row is None:
            raise HTTPException(status_code=404, detail="plan-not-found")
        return _public_plan(row)

    @app.get("/v1/audit")
    def list_audit(
        limit: int = Query(default=100, ge=1, le=200),
        principal: Principal = Depends(_require(*READ_ROLES)),
    ):
        return require_repository().list_audit(limit)

    @app.post("/v1/optimize")
    def optimize(
        body: OptimizeRequest,
        request: Request,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
        principal: Principal = Depends(_require(*OPTIMIZE_ROLES)),
    ):
        repo = require_repository()
        key = _required_idempotency_key(idempotency_key)
        request_id = _request_id(request)
        operation = "optimize"
        request_content = body.model_dump(mode="json")
        try:
            with repo.transaction() as conn:
                prior = Repository.begin_idempotent(
                    conn, principal.name, operation, key, request_content
                )
                if prior is not None:
                    return JSONResponse(status_code=prior[0], content=prior[1])
                now_ms = _database_now_ms(conn)
                source_fields, source_pb, normalized = _validate_snapshot(body.snapshot, now_ms)
                source_snapshot = body.snapshot
                source_content = canonical_json(source_snapshot)
                source_digest = Repository.store_artifact(
                    conn, "source_snapshot_json", source_content
                )
                state_binding = _binding(source_fields, normalized)
                current_state_valid = Repository.bind_current_state(
                    conn,
                    source_fields["snapshot_id"],
                    source_fields["generation"],
                    state_binding,
                    source_digest,
                )
                if current_state_valid:
                    Repository.stale_prior_plans(
                        conn, source_fields["generation"], "system", request_id
                    )
                    Repository.stale_incompatible_plans(conn, state_binding, "system", request_id)
                else:
                    Repository.stale_incompatible_plans(conn, state_binding, "system", request_id)
                    response = {
                        "detail": "snapshot-content-changed-without-generation-increment",
                        "snapshot_id": source_fields["snapshot_id"],
                        "snapshot_generation": source_fields["generation"],
                    }
                    Repository.audit(
                        conn,
                        actor=principal.name,
                        role=principal.role,
                        request_id=request_id,
                        operation=operation,
                        snapshot_id=source_fields["snapshot_id"],
                        idempotency_key=key,
                        result="DENIED",
                        reason="snapshot-generation-not-incremented",
                    )
                    Repository.finish_idempotent(
                        conn, principal.name, operation, key, 409, response
                    )
                    return JSONResponse(status_code=409, content=response)
                if app.state.mode == "OBSERVE" or body.mode == "OBSERVE":
                    observation_response = {
                        "mode": "OBSERVE",
                        "snapshot_id": source_fields["snapshot_id"],
                        "snapshot_generation": source_fields["generation"],
                        "snapshot_sha256": source_digest,
                        "planning": "disabled",
                        "execution_enabled": False,
                    }
                    Repository.audit(
                        conn,
                        actor=principal.name,
                        role=principal.role,
                        request_id=request_id,
                        operation=operation,
                        snapshot_id=source_fields["snapshot_id"],
                        idempotency_key=key,
                        result="OBSERVED",
                        reason="observe-mode",
                    )
                    Repository.finish_idempotent(
                        conn, principal.name, operation, key, 200, observation_response
                    )
                    return observation_response
                leases: dict[str, int] = {}
                for conflict_key in state_binding["conflict_keys"]:
                    token = Repository.acquire_lease(
                        conn, conflict_key, app.state.controller_id, principal.name, 60000
                    )
                    if token is None:
                        raise LeaseConflict(conflict_key)
                    leases[conflict_key] = token
                forecast_outputs: dict[str, DemandForecast] = {}
                forecast_metadata: dict[str, dict[str, object]] = {}
                eligibility: dict[str, tuple[bool, tuple[str, ...]]] = {}
                effective = deepcopy(source_fields)
                model = _model(body.model)
                for workload in effective["workloads"]:
                    workload_id = workload["workload_id"]
                    evidence = body.scale_in_evidence.get(workload_id)
                    eligible, scale_reasons = _scale_in_evidence(
                        workload, normalized["capacity_profiles"], evidence, now_ms
                    )
                    eligibility[workload_id] = (eligible, scale_reasons)
                    history = [
                        Observation(**item.model_dump())
                        for item in body.history.get(workload_id, [])
                    ]
                    horizon_ms = max(
                        1,
                        workload["startup_ms"]
                        + workload["readiness_stabilization_ms"]
                        + body.scheduling_buffer_ms,
                    )
                    forecast, forecast_status, fallback_reason = forecast_decision(
                        workload_id, history, model, now_ms, horizon_ms
                    )
                    metadata: dict[str, object] = {
                        "forecast_status": forecast_status,
                        "fallback_reason": fallback_reason,
                        "model_name": model.name,
                        "scale_in_eligible": eligible,
                        "scale_in_reason_codes": list(scale_reasons),
                    }
                    if forecast is not None:
                        forecast_outputs[workload_id] = forecast
                        effective_workload = next(
                            item
                            for item in effective["workloads"]
                            if item["workload_id"] == workload_id
                        )
                        effective_workload["demand_qps_milli"] = max(
                            effective_workload["demand_qps_milli"], forecast.qps_upper_bound_milli
                        )
                        effective_workload["concurrency_bound"] = max(
                            effective_workload["concurrency_bound"],
                            forecast.concurrency_upper_bound,
                        )
                        metadata.update(
                            {
                                "forecast_sha256": _sha256(forecast.to_dict()),
                                "training_window_sha256": forecast.training_window_sha256,
                                "model_version": forecast.model_version,
                                "horizon_ms": forecast.horizon_ms,
                                "forecast_generated_at_ms": forecast.forecast_generated_at_ms,
                                "forecast_valid_until_ms": forecast.forecast_valid_until_ms,
                                "expected_qps_milli": forecast.expected_qps_milli,
                                "qps_upper_bound_milli": forecast.qps_upper_bound_milli,
                                "expected_concurrency": forecast.expected_concurrency,
                                "concurrency_upper_bound": forecast.concurrency_upper_bound,
                                "confidence": forecast.confidence,
                                "empirical_upper_bound_coverage": (
                                    forecast.empirical_upper_bound_coverage
                                ),
                            }
                        )
                    forecast_metadata[workload_id] = metadata
                effective["evaluation_time_ms"] = now_ms
                effective_id_suffix = _sha256(forecast_metadata)[:16]
                effective["snapshot_id"] = (
                    f"{source_fields['snapshot_id']}:effective:{effective_id_suffix}"
                )
                effective_pb, normalized_effective = encode_snapshot(effective)
                if len(effective_pb) > MAX_ARTIFACT_BYTES:
                    raise HTTPException(status_code=413, detail="effective-snapshot-too-large")
                plan_bytes, verification_bytes, plan_text, verification_text, summary = (
                    _run_planner(effective_pb)
                )
                if summary["snapshot_id"] != effective["snapshot_id"]:
                    raise HTTPException(status_code=503, detail="planner-snapshot-binding-mismatch")
                reason_codes = [
                    f"{item['fallback_reason']}:{workload_id}"
                    for workload_id, item in forecast_metadata.items()
                    if item["fallback_reason"]
                ]
                for workload_id, (_, reasons) in eligibility.items():
                    if reasons:
                        reason_codes.extend(f"{reason}:{workload_id}" for reason in reasons)
                execution_rejections = _execution_admission_reasons(
                    summary, source_fields["workloads"], app.state.execution_target
                )
                reason_codes.extend(execution_rejections)
                reductions = _reduction_workloads(summary, source_fields)
                disallowed = sorted(
                    workload_id
                    for workload_id in reductions
                    if not eligibility.get(workload_id, (False, ()))[0]
                )
                if disallowed:
                    reason_codes.extend(
                        f"OBSERVED_DOWNSCALE_STABILIZATION_NOT_MET:{workload_id}"
                        for workload_id in disallowed
                    )
                    if any(
                        forecast_outputs.get(workload_id)
                        and forecast_outputs[workload_id].expected_qps_milli
                        < next(
                            item["demand_qps_milli"]
                            for item in source_fields["workloads"]
                            if item["workload_id"] == workload_id
                        )
                        for workload_id in disallowed
                    ):
                        reason_codes.append("PREDICTION_SUGGESTS_LOWER_DEMAND")
                verifier_ok = bool(summary["verification_accepted"])
                admission_ok = (
                    bool(summary["feasible"]) and not disallowed and not execution_rejections
                )
                if not verifier_ok:
                    reason_codes.append("INDEPENDENT_VERIFIER_REJECTED")
                if not summary["feasible"]:
                    reason_codes.append("PLANNER_INFEASIBLE")
                reason_codes = list(dict.fromkeys(reason_codes))
                native_id = str(summary["plan_id"])
                source_binding = state_binding
                plan_binding = {
                    "source": source_binding,
                    "effective_snapshot_id": effective["snapshot_id"],
                    "effective_snapshot_sha256": normalized_effective["content_sha256"],
                    "forecast_hash": _sha256(forecast_metadata),
                    "workload_ids": source_binding["workload_ids"],
                    "conflict_keys": source_binding["conflict_keys"],
                }
                artifacts: dict[str, tuple[str, bytes]] = {
                    "source_snapshot.json": ("source_snapshot_json", source_content),
                    "request.json": ("optimization_request_json", canonical_json(request_content)),
                    "effective_snapshot.pb": ("effective_snapshot_protobuf", effective_pb),
                    "plan.pb": ("candidate_plan_protobuf", plan_bytes),
                    "verification.pb": ("verifier_result_protobuf", verification_bytes),
                    "plan.txt": ("candidate_plan_text", plan_text.encode()),
                    "verification.txt": ("verifier_result_text", verification_text.encode()),
                    "policy.json": ("policy_json", canonical_json(normalized["policy"])),
                    "machine_catalog.json": (
                        "machine_catalog_json",
                        canonical_json(normalized["machine_catalog"]),
                    ),
                    "decision_manifest.json": (
                        "decision_manifest_json",
                        canonical_json(
                            {
                                "mode": body.mode,
                                "forecast_metadata": forecast_metadata,
                                "scale_in_eligibility": {
                                    key: {"eligible": value[0], "reason_codes": list(value[1])}
                                    for key, value in eligibility.items()
                                },
                                "plan_summary": summary,
                                "reason_codes": reason_codes,
                            }
                        ),
                    ),
                }
                for workload_id, forecast in forecast_outputs.items():
                    artifacts[f"forecast:{workload_id}.json"] = (
                        "demand_forecast_json",
                        canonical_json(forecast.to_dict()),
                    )
                    artifacts[f"training:{workload_id}.json"] = (
                        "forecast_training_json",
                        canonical_json(
                            [item.model_dump(mode="json") for item in body.history[workload_id]]
                        ),
                    )
                for profile in normalized["capacity_profiles"]:
                    artifacts[f"profile:{profile['profile_id']}.json"] = (
                        "capacity_profile_json",
                        canonical_json(profile),
                    )
                plan_expiries = [int(summary["expires_at_ms"])]
                plan_expiries.extend(
                    int(profile["valid_until_ms"]) for profile in normalized["capacity_profiles"]
                )
                plan_expiries.extend(
                    int(item["forecast_valid_until_ms"])
                    for item in forecast_metadata.values()
                    if item.get("forecast_valid_until_ms")
                )
                plan_metadata = {
                    "plan_id": native_id,
                    "created_at_ms": int(summary["created_at_ms"]),
                    "expires_at_ms": min(plan_expiries),
                    "snapshot_id": source_fields["snapshot_id"],
                    "snapshot_generation": source_fields["generation"],
                    "policy_version": str(normalized["policy"]["schema_version"]),
                    "policy_hash": source_binding["policy_hash"],
                    "profile_hashes": source_binding["profile_hashes"],
                    "machine_catalog_hash": source_binding["machine_catalog_hash"],
                    "forecast_metadata": forecast_metadata,
                    "planner_build_id": str(summary["planner_build_id"]),
                    "verifier_build_id": str(summary["verifier_build_id"]),
                    "verification_hash": str(summary["verification_hash"]),
                    "verification_accepted": verifier_ok,
                    "admission_accepted": admission_ok,
                    "reason_codes": reason_codes,
                    "binding": plan_binding,
                    "created_by": principal.name,
                }
                intents: list[dict[str, object]] = []
                current_workloads = {
                    item["workload_id"]: item for item in source_fields["workloads"]
                }
                for index, decision in enumerate(summary["decisions"]):
                    current = current_workloads[decision["workload_id"]]
                    if (
                        decision["proposed_replicas"] != decision["current_replicas"]
                        or decision["proposed_resources"] != current["current_resources"]
                    ):
                        payload = {
                            "current_replicas": decision["current_replicas"],
                            "proposed_replicas": decision["proposed_replicas"],
                            "current_resources": current["current_resources"],
                            "proposed_resources": decision["proposed_resources"],
                            "health": bounded_execution_health_policy(
                                decision["workload_id"],
                                current["latency_slo_ms"],
                                current["error_slo_ppm"],
                            ),
                        }
                        action_type = (
                            "SET_REPLICA_COUNT"
                            if decision["proposed_replicas"] != decision["current_replicas"]
                            else "RESIZE_WORKLOAD"
                        )
                        intents.append(
                            {
                                "intent_id": hashlib.sha256(
                                    f"{native_id}:{index}".encode()
                                ).hexdigest(),
                                "action_type": action_type,
                                "workload_id": decision["workload_id"],
                                "payload": payload,
                            }
                        )
                for index, node_id in enumerate(summary["deactivated_node_ids"]):
                    intents.append(
                        {
                            "intent_id": hashlib.sha256(
                                f"{native_id}:node:{index}".encode()
                            ).hexdigest(),
                            "action_type": "DEACTIVATE_NODE",
                            "payload": {"node_id": node_id},
                        }
                    )
                for evacuation in summary["evacuations"]:
                    sequence = int(evacuation["sequence"])
                    intents.append(
                        {
                            "intent_id": hashlib.sha256(
                                f"{native_id}:move:{sequence}".encode()
                            ).hexdigest(),
                            "action_type": "MOVE_REPLICA",
                            "workload_id": evacuation["workload_id"],
                            "payload": {
                                "sequence": sequence,
                                "replica_id": evacuation["replica_id"],
                                "source_node_id": evacuation["source_node_id"],
                                "destination_node_id": evacuation["destination_node_id"],
                            },
                        }
                    )
                if not _verify_lease_tokens(conn, app.state.controller_id, leases):
                    raise LeaseConflict("planning lease expired or lost its fencing token")
                repo.create_plan(
                    conn,
                    plan_metadata,
                    artifacts,
                    intents,
                    principal.name,
                    principal.role,
                    request_id,
                    key,
                )
                for conflict_key, token in leases.items():
                    Repository.release_lease(conn, conflict_key, app.state.controller_id, token)
                response_payload = _public_plan(_response_plan(conn, native_id))
                status = 201 if admission_ok and verifier_ok else 200
                Repository.finish_idempotent(
                    conn, principal.name, operation, key, status, response_payload
                )
                return JSONResponse(status_code=status, content=response_payload)
        except IdempotencyConflict as error:
            _audit_denial(repo, request, principal, operation, "CONFLICT", "idempotency-key-reused")
            raise HTTPException(
                status_code=409, detail="idempotency-key-reused-with-different-request"
            ) from error
        except StaleState as error:
            _audit_denial(repo, request, principal, operation, "STALE", "snapshot-state-conflict")
            raise HTTPException(status_code=409, detail="snapshot-state-conflict") from error
        except LeaseConflict as error:
            _audit_denial(repo, request, principal, operation, "CONFLICT", "conflict-lease-held")
            raise HTTPException(status_code=409, detail="conflict-lease-held") from error
        except ArtifactIntegrityError as error:
            raise HTTPException(status_code=409, detail="artifact-integrity-failure") from error
        except HTTPException:
            raise
        except psycopg.Error as error:
            raise HTTPException(status_code=503, detail="control-database-error") from error

    @app.post("/v1/plans/{plan_id}/approve")
    def approve_plan(
        plan_id: str,
        body: OperationRequest,
        request: Request,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
        principal: Principal = Depends(_require(*OPERATOR_ROLES)),
    ):
        return _mutate_plan(plan_id, body, request, principal, idempotency_key, "approve", app)

    @app.post("/v1/plans/{plan_id}/pause")
    def pause_plan(
        plan_id: str,
        body: OperationRequest,
        request: Request,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
        principal: Principal = Depends(_require(*OPERATOR_ROLES)),
    ):
        return _mutate_plan(plan_id, body, request, principal, idempotency_key, "pause", app)

    @app.post("/v1/plans/{plan_id}/cancel")
    def cancel_plan(
        plan_id: str,
        body: OperationRequest,
        request: Request,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
        principal: Principal = Depends(_require(*OPERATOR_ROLES)),
    ):
        return _mutate_plan(plan_id, body, request, principal, idempotency_key, "cancel", app)

    @app.post("/v1/plans/{plan_id}/execute")
    def execute_plan(
        plan_id: str,
        body: OperationRequest,
        request: Request,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
        principal: Principal = Depends(_require(*OPERATOR_ROLES)),
    ):
        repo = require_repository()
        if not app.state.execution_enabled or app.state.execution_controller is None:
            raise HTTPException(status_code=409, detail="execution-mode-not-enabled")
        key = _required_idempotency_key(idempotency_key)
        operation = f"execute:{plan_id}"
        request_id = _request_id(request)
        request_body = body.model_dump(mode="json")
        try:
            with repo.transaction() as conn:
                claim = Repository.claim_execution(
                    conn,
                    plan_id,
                    principal.name,
                    key,
                    request_body,
                    app.state.controller_id,
                    int(os.getenv("FLEETRIGHT_EXECUTION_LEASE_TTL_MS", "120000")),
                )
                if claim.get("response_status") is not None:
                    return JSONResponse(
                        status_code=claim["response_status"], content=claim["response"]
                    )
                if not claim.get("claimed"):
                    execution = claim.get("execution", {})
                    result = {
                        "plan_id": plan_id,
                        "state": execution.get("status", "EXECUTING"),
                        "execution_id": plan_id,
                    }
                    Repository.finish_idempotent(conn, principal.name, operation, key, 202, result)
                    return JSONResponse(status_code=202, content=result)

                row = conn.execute(
                    "SELECT * FROM plans WHERE plan_id = %s FOR UPDATE", (plan_id,)
                ).fetchone()
                if row is None:
                    raise KeyError(plan_id)
                state = conn.execute(
                    "SELECT binding FROM control_state WHERE singleton FOR SHARE"
                ).fetchone()
                source_binding = row["binding"].get("source", {})
                stale_reason = None
                if state is None or state["binding"] != source_binding:
                    stale_reason = "base-snapshot-policy-profile-or-catalog-changed"
                elif _database_now_ms(conn) >= row["expires_at_ms"]:
                    stale_reason = "plan-expired"
                if stale_reason:
                    conn.execute("DELETE FROM plan_executions WHERE plan_id = %s", (plan_id,))
                    Repository.release_plan_leases(conn, plan_id)
                    Repository.transition_plan(
                        conn,
                        plan_id,
                        "STALE",
                        principal.name,
                        principal.role,
                        stale_reason,
                        request_id,
                        key,
                    )
                    result = {"plan_id": plan_id, "state": "STALE", "detail": stale_reason}
                    Repository.audit(
                        conn,
                        actor=principal.name,
                        role=principal.role,
                        request_id=request_id,
                        operation=operation,
                        plan_id=plan_id,
                        snapshot_id=row["snapshot_id"],
                        idempotency_key=key,
                        before_state="APPROVED",
                        after_state="STALE",
                        result="DENIED",
                        reason=stale_reason,
                    )
                    Repository.finish_idempotent(conn, principal.name, operation, key, 409, result)
                    return JSONResponse(status_code=409, content=result)

                artifacts = Repository.load_artifacts(conn, plan_id)
                if not {"effective_snapshot.pb", "plan.pb", "verification.pb"}.issubset(artifacts):
                    raise ArtifactIntegrityError("replay artifacts missing")
                verifier_ok, verification_hash, verifier_build_id = _run_verifier(
                    artifacts["effective_snapshot.pb"], artifacts["plan.pb"]
                )
                if (
                    not verifier_ok
                    or verification_hash != row["verification_hash"].strip()
                    or verifier_build_id != row["verifier_build_id"]
                    or not row["verification_accepted"]
                    or not row["admission_accepted"]
                ):
                    raise ArtifactIntegrityError("independent verification no longer passes")
                actions = conn.execute(
                    "SELECT action_type, workload_id, payload FROM action_intents "
                    "WHERE plan_id = %s ORDER BY action_sequence",
                    (plan_id,),
                ).fetchall()
                if not actions:
                    raise ControlError("plan has no executable action")
                for action in actions:
                    payload = action["payload"]
                    if (
                        action["action_type"] != "SET_REPLICA_COUNT"
                        or not isinstance(action["workload_id"], str)
                        or not re.fullmatch(
                            r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?", action["workload_id"]
                        )
                        or not isinstance(payload, dict)
                        or not isinstance(payload.get("current_replicas"), int)
                        or not isinstance(payload.get("proposed_replicas"), int)
                        or not isinstance(payload.get("target_precondition"), dict)
                        or not valid_execution_health_policy(
                            action["workload_id"], payload.get("health")
                        )
                        or payload.get("current_resources") != payload.get("proposed_resources")
                    ):
                        raise ControlError("plan contains an unsupported execution action")
                    if action["workload_id"] not in _supported_execution_workloads(
                        app.state.execution_target
                    ):
                        raise ControlError("plan target is outside the supported workload set")

                Repository.transition_plan(
                    conn,
                    plan_id,
                    "EXECUTING",
                    principal.name,
                    principal.role,
                    body.reason,
                    request_id,
                    key,
                )
                result = {
                    "plan_id": plan_id,
                    "execution_id": plan_id,
                    "state": "EXECUTING",
                    "target": app.state.execution_target or "injected",
                }
                Repository.audit(
                    conn,
                    actor=principal.name,
                    role=principal.role,
                    request_id=request_id,
                    operation=operation,
                    plan_id=plan_id,
                    snapshot_id=row["snapshot_id"],
                    idempotency_key=key,
                    before_state="APPROVED",
                    after_state="EXECUTING",
                    result="ACCEPTED",
                    reason=body.reason,
                )
                Repository.finish_idempotent(conn, principal.name, operation, key, 202, result)
        except KeyError as error:
            raise HTTPException(status_code=404, detail="plan-not-found") from error
        except IdempotencyConflict as error:
            raise HTTPException(
                status_code=409, detail="idempotency-key-reused-with-different-request"
            ) from error
        except ExecutionConflict as error:
            raise HTTPException(status_code=409, detail="execution-already-claimed") from error
        except ArtifactIntegrityError as error:
            raise HTTPException(
                status_code=409, detail="independent-verification-failed"
            ) from error
        except (ControlError, LeaseConflict, StaleState) as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        except psycopg.Error as error:
            raise HTTPException(status_code=503, detail="control-database-error") from error
        return JSONResponse(status_code=202, content=result)

    return app


def _mutate_plan(
    plan_id: str,
    body: OperationRequest,
    request: Request,
    principal: Principal,
    raw_key: str | None,
    operation: str,
    app: FastAPI,
) -> JSONResponse:
    repo: Repository = app.state.repository
    if repo is None:
        raise HTTPException(status_code=503, detail="database-not-configured")
    key = _required_idempotency_key(raw_key)
    request_id = _request_id(request)
    op_key = f"{operation}:{plan_id}"
    request_body = body.model_dump(mode="json")
    try:
        with repo.transaction() as conn:
            prior = Repository.begin_idempotent(conn, principal.name, op_key, key, request_body)
            if prior is not None:
                return JSONResponse(status_code=prior[0], content=prior[1])
            if operation == "approve":
                current_state = conn.execute(
                    "SELECT snapshot_id, snapshot_generation, binding FROM control_state "
                    "WHERE singleton FOR UPDATE"
                ).fetchone()
            else:
                current_state = None
            row = conn.execute(
                "SELECT * FROM plans WHERE plan_id = %s FOR UPDATE", (plan_id,)
            ).fetchone()
            if row is None:
                result = {"detail": "plan-not-found"}
                Repository.audit(
                    conn,
                    actor=principal.name,
                    role=principal.role,
                    request_id=request_id,
                    operation=operation,
                    plan_id=plan_id,
                    idempotency_key=key,
                    result="NOT_FOUND",
                    reason="plan-not-found",
                )
                Repository.finish_idempotent(conn, principal.name, op_key, key, 404, result)
                return JSONResponse(status_code=404, content=result)
            old_state = row["state"]
            target_state = {"approve": "APPROVED", "pause": "PAUSED", "cancel": "CANCELLED"}[
                operation
            ]
            has_execution = (
                conn.execute(
                    "SELECT 1 FROM plan_executions WHERE plan_id = %s", (plan_id,)
                ).fetchone()
                is not None
            )
            if (
                operation in {"pause", "cancel"}
                and old_state
                in {
                    "EXECUTING",
                    "VERIFYING",
                    "PAUSED",
                }
                and has_execution
            ):
                control = Repository.request_execution_control(
                    conn, plan_id, operation.upper(), body.reason
                )
                control_request = "CANCEL" if control["cancel_requested"] else "PAUSE"
                response = {
                    "plan_id": plan_id,
                    "state": old_state,
                    "control_request": f"{control_request}_REQUESTED",
                }
                Repository.audit(
                    conn,
                    actor=principal.name,
                    role=principal.role,
                    request_id=request_id,
                    operation=operation,
                    plan_id=plan_id,
                    snapshot_id=row["snapshot_id"],
                    idempotency_key=key,
                    before_state=old_state,
                    after_state=old_state,
                    result="REQUESTED",
                    reason=body.reason,
                )
                Repository.finish_idempotent(conn, principal.name, op_key, key, 202, response)
                return JSONResponse(status_code=202, content=response)
            if not transition_allowed(old_state, target_state):
                result = {
                    "plan_id": plan_id,
                    "state": old_state,
                    "detail": "illegal-state-transition",
                }
                Repository.audit(
                    conn,
                    actor=principal.name,
                    role=principal.role,
                    request_id=request_id,
                    operation=operation,
                    plan_id=plan_id,
                    snapshot_id=row["snapshot_id"],
                    idempotency_key=key,
                    before_state=old_state,
                    after_state=old_state,
                    result="DENIED",
                    reason="illegal-state-transition",
                )
                Repository.finish_idempotent(conn, principal.name, op_key, key, 409, result)
                return JSONResponse(status_code=409, content=result)
            if operation == "approve":
                db_now = _database_now_ms(conn)
                source_binding = row["binding"].get("source", {})
                current_binding = None if current_state is None else current_state["binding"]
                stale_reason = None
                if db_now >= row["expires_at_ms"]:
                    stale_reason = "plan-expired"
                elif current_binding != source_binding:
                    stale_reason = "base-snapshot-policy-profile-or-catalog-changed"
                if stale_reason:
                    Repository.transition_plan(
                        conn,
                        plan_id,
                        "STALE",
                        principal.name,
                        principal.role,
                        stale_reason,
                        request_id,
                        key,
                    )
                    response = {"plan_id": plan_id, "state": "STALE", "detail": stale_reason}
                    Repository.audit(
                        conn,
                        actor=principal.name,
                        role=principal.role,
                        request_id=request_id,
                        operation=operation,
                        plan_id=plan_id,
                        snapshot_id=row["snapshot_id"],
                        idempotency_key=key,
                        before_state=old_state,
                        after_state="STALE",
                        result="DENIED",
                        reason=stale_reason,
                    )
                    Repository.finish_idempotent(conn, principal.name, op_key, key, 409, response)
                    return JSONResponse(status_code=409, content=response)
                try:
                    artifacts = Repository.load_artifacts(conn, plan_id)
                    required = {"effective_snapshot.pb", "plan.pb", "verification.pb"}
                    if not required.issubset(artifacts):
                        raise ArtifactIntegrityError("replay artifacts missing")
                except ArtifactIntegrityError:
                    Repository.transition_plan(
                        conn,
                        plan_id,
                        "REJECTED",
                        principal.name,
                        principal.role,
                        "artifact-integrity-failure",
                        request_id,
                        key,
                    )
                    response = {
                        "plan_id": plan_id,
                        "state": "REJECTED",
                        "detail": "artifact-integrity-failure",
                    }
                    Repository.audit(
                        conn,
                        actor=principal.name,
                        role=principal.role,
                        request_id=request_id,
                        operation=operation,
                        plan_id=plan_id,
                        snapshot_id=row["snapshot_id"],
                        idempotency_key=key,
                        before_state=old_state,
                        after_state="REJECTED",
                        result="DENIED",
                        reason="artifact-integrity-failure",
                    )
                    Repository.finish_idempotent(conn, principal.name, op_key, key, 409, response)
                    return JSONResponse(status_code=409, content=response)
                verifier_ok, verification_hash, verifier_build_id = _run_verifier(
                    artifacts["effective_snapshot.pb"], artifacts["plan.pb"]
                )
                if (
                    not verifier_ok
                    or verification_hash != row["verification_hash"].strip()
                    or verifier_build_id != row["verifier_build_id"]
                    or not row["verification_accepted"]
                    or not row["admission_accepted"]
                ):
                    reason = "approval-reverification-failed"
                    Repository.transition_plan(
                        conn,
                        plan_id,
                        "REJECTED",
                        principal.name,
                        principal.role,
                        reason,
                        request_id,
                        key,
                    )
                    response = {"plan_id": plan_id, "state": "REJECTED", "detail": reason}
                    Repository.audit(
                        conn,
                        actor=principal.name,
                        role=principal.role,
                        request_id=request_id,
                        operation=operation,
                        plan_id=plan_id,
                        snapshot_id=row["snapshot_id"],
                        idempotency_key=key,
                        before_state=old_state,
                        after_state="REJECTED",
                        result="DENIED",
                        reason=reason,
                    )
                    Repository.finish_idempotent(conn, principal.name, op_key, key, 409, response)
                    return JSONResponse(status_code=409, content=response)
                supported_workloads = _supported_execution_workloads(app.state.execution_target)
                approval_actions = conn.execute(
                    "SELECT intent_id, action_type, workload_id, payload FROM action_intents "
                    "WHERE plan_id = %s ORDER BY action_sequence",
                    (plan_id,),
                ).fetchall()
                for action in approval_actions:
                    payload = action["payload"]
                    if (
                        action["action_type"] != "SET_REPLICA_COUNT"
                        or action["workload_id"] not in supported_workloads
                        or not isinstance(payload, dict)
                        or not valid_execution_health_policy(
                            action["workload_id"], payload.get("health")
                        )
                        or payload.get("current_resources") != payload.get("proposed_resources")
                    ):
                        raise ControlError("plan contains an unsupported execution action")
                if app.state.execution_enabled:
                    for action in approval_actions:
                        payload = action["payload"]
                        try:
                            observed = app.state.execution_controller.target.observe_scale(
                                action["workload_id"]
                            )
                        except TargetNotFound:
                            reason = "target-workload-not-found-at-approval"
                            Repository.transition_plan(
                                conn,
                                plan_id,
                                "STALE",
                                principal.name,
                                principal.role,
                                reason,
                                request_id,
                                key,
                            )
                            response = {"plan_id": plan_id, "state": "STALE", "detail": reason}
                            Repository.audit(
                                conn,
                                actor=principal.name,
                                role=principal.role,
                                request_id=request_id,
                                operation=operation,
                                plan_id=plan_id,
                                snapshot_id=row["snapshot_id"],
                                idempotency_key=key,
                                before_state=old_state,
                                after_state="STALE",
                                result="DENIED",
                                reason=reason,
                            )
                            Repository.finish_idempotent(
                                conn, principal.name, op_key, key, 409, response
                            )
                            return JSONResponse(status_code=409, content=response)
                        except TargetError as error:
                            raise HTTPException(
                                status_code=503, detail="execution-target-unavailable"
                            ) from error
                        if observed.get("replicas") != payload.get("current_replicas"):
                            reason = "target-replica-precondition-changed"
                            Repository.transition_plan(
                                conn,
                                plan_id,
                                "STALE",
                                principal.name,
                                principal.role,
                                reason,
                                request_id,
                                key,
                            )
                            response = {"plan_id": plan_id, "state": "STALE", "detail": reason}
                            Repository.audit(
                                conn,
                                actor=principal.name,
                                role=principal.role,
                                request_id=request_id,
                                operation=operation,
                                plan_id=plan_id,
                                snapshot_id=row["snapshot_id"],
                                idempotency_key=key,
                                before_state=old_state,
                                after_state="STALE",
                                result="DENIED",
                                reason=reason,
                            )
                            Repository.finish_idempotent(
                                conn, principal.name, op_key, key, 409, response
                            )
                            return JSONResponse(status_code=409, content=response)
                        payload["target_precondition"] = {
                            key: observed[key]
                            for key in ("uid", "generation", "replicas")
                            if key in observed
                        }
                        conn.execute(
                            "UPDATE action_intents SET payload = %s "
                            "WHERE plan_id = %s AND intent_id = %s",
                            (Jsonb(payload), plan_id, action["intent_id"]),
                        )
                leases: dict[str, int] = {}
                conflict_keys = row["binding"].get("conflict_keys", [])
                ttl_ms = row["expires_at_ms"] - db_now
                for conflict_key in conflict_keys:
                    token = Repository.acquire_lease(
                        conn, conflict_key, plan_id, principal.name, ttl_ms
                    )
                    if token is None:
                        for old_key, old_token in leases.items():
                            Repository.release_lease(conn, old_key, plan_id, old_token)
                        result = {
                            "plan_id": plan_id,
                            "state": old_state,
                            "detail": "conflict-lease-held",
                        }
                        Repository.audit(
                            conn,
                            actor=principal.name,
                            role=principal.role,
                            request_id=request_id,
                            operation=operation,
                            plan_id=plan_id,
                            snapshot_id=row["snapshot_id"],
                            idempotency_key=key,
                            result="CONFLICT",
                            reason="conflict-lease-held",
                        )
                        Repository.finish_idempotent(conn, principal.name, op_key, key, 409, result)
                        return JSONResponse(status_code=409, content=result)
                    leases[conflict_key] = token
                new_state = "APPROVED"
            elif operation == "pause":
                leases = {}
                new_state = "PAUSED"
            else:
                leases = {}
                new_state = "CANCELLED"
            old_state, version = Repository.transition_plan(
                conn,
                plan_id,
                new_state,
                principal.name,
                principal.role,
                body.reason,
                request_id,
                key,
            )
            if new_state == "APPROVED":
                for conflict_key, token in leases.items():
                    conn.execute(
                        "INSERT INTO plan_leases (plan_id, conflict_key, owner_id, fencing_token) "
                        "VALUES (%s, %s, %s, %s)",
                        (plan_id, conflict_key, plan_id, token),
                    )
                conn.execute(
                    "UPDATE action_intents SET status = 'APPROVED' WHERE plan_id = %s", (plan_id,)
                )
            else:
                held = conn.execute(
                    "SELECT conflict_key, owner_id, fencing_token FROM plan_leases "
                    "WHERE plan_id = %s",
                    (plan_id,),
                ).fetchall()
                for item in held:
                    Repository.release_lease(
                        conn, item["conflict_key"], item["owner_id"], item["fencing_token"]
                    )
                conn.execute("DELETE FROM plan_leases WHERE plan_id = %s", (plan_id,))
                if new_state == "CANCELLED":
                    conn.execute(
                        "UPDATE action_intents SET status = 'CANCELLED' WHERE plan_id = %s",
                        (plan_id,),
                    )
            Repository.audit(
                conn,
                actor=principal.name,
                role=principal.role,
                request_id=request_id,
                operation=operation,
                plan_id=plan_id,
                snapshot_id=row["snapshot_id"],
                idempotency_key=key,
                before_state=old_state,
                after_state=new_state,
                result="ACCEPTED",
                reason=body.reason,
            )
            result = {"plan_id": plan_id, "state": new_state, "state_version": version}
            Repository.finish_idempotent(conn, principal.name, op_key, key, 200, result)
            return JSONResponse(status_code=200, content=result)
    except IdempotencyConflict as error:
        _audit_denial(repo, request, principal, op_key, "CONFLICT", "idempotency-key-reused")
        raise HTTPException(
            status_code=409, detail="idempotency-key-reused-with-different-request"
        ) from error
    except ControlError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    except psycopg.Error as error:
        raise HTTPException(status_code=503, detail="control-database-error") from error


app = create_app()
