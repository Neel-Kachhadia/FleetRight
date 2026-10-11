from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import TypeAlias

REPO_ROOT = Path(__file__).resolve().parents[2]
PROTO_ROOT = REPO_ROOT / "proto"
_MAX_PROTO_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True)
class Block:
    fields: tuple[tuple[str, object], ...]


@dataclass(frozen=True)
class EnumToken:
    value: str


ProtoValue: TypeAlias = str | int | bool | Block | EnumToken | list[object]


def _quote(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def _render(fields: tuple[tuple[str, object], ...]) -> str:
    lines: list[str] = []
    for name, value in fields:
        values = value if isinstance(value, list) else [value]
        for item in values:
            if isinstance(item, Block):
                inner = _render(item.fields)
                body = "\n".join(f"  {line}" for line in inner.splitlines())
                lines.append(f"{name} {{\n{body}\n}}")
            elif isinstance(item, bool):
                lines.append(f"{name}: {'true' if item else 'false'}")
            elif isinstance(item, str):
                lines.append(f"{name}: {_quote(item)}")
            elif isinstance(item, EnumToken):
                if not item.value.isidentifier():
                    raise ValueError(f"invalid protobuf enum token for {name}")
                lines.append(f"{name}: {item.value}")
            elif isinstance(item, int):
                lines.append(f"{name}: {item}")
            else:
                raise TypeError(
                    f"unsupported protobuf text value for {name}: {type(item).__name__}"
                )
    return "\n".join(lines) + "\n"


def encode_text(message_type: str, source_file: str, textproto: str) -> bytes:
    if len(textproto.encode("utf-8")) > _MAX_PROTO_BYTES:
        raise ValueError("protobuf text input exceeds 64 MiB")
    command = [
        "protoc",
        f"--proto_path={PROTO_ROOT}",
        f"--encode={message_type}",
        source_file,
    ]
    completed = subprocess.run(
        command, input=textproto.encode("utf-8"), cwd=REPO_ROOT, capture_output=True, check=False
    )
    if completed.returncode:
        raise ValueError(
            f"protoc rejected {message_type}: {completed.stderr.decode(errors='replace').strip()}"
        )
    if len(completed.stdout) > _MAX_PROTO_BYTES:
        raise ValueError("encoded protobuf exceeds 64 MiB")
    return completed.stdout


def decode_text(message_type: str, source_file: str, payload: bytes) -> str:
    if len(payload) > _MAX_PROTO_BYTES:
        raise ValueError("protobuf artifact exceeds 64 MiB")
    command = [
        "protoc",
        f"--proto_path={PROTO_ROOT}",
        f"--decode={message_type}",
        source_file,
    ]
    completed = subprocess.run(
        command, input=payload, cwd=REPO_ROOT, capture_output=True, check=False
    )
    if completed.returncode:
        raise ValueError(
            f"protoc rejected {message_type} artifact: "
            f"{completed.stderr.decode(errors='replace').strip()}"
        )
    return completed.stdout.decode("utf-8")


def _resource(value: dict[str, int]) -> Block:
    return Block(
        (
            ("cpu_millicores", value["cpu_millicores"]),
            ("memory_mib", value["memory_mib"]),
            ("network_mbps", value["network_mbps"]),
        )
    )


def _profile_fields(
    profile: dict[str, object], include_hash: bool = True
) -> tuple[tuple[str, object], ...]:
    fields: list[tuple[str, object]] = [
        ("schema_version", 1),
        ("profile_id", str(profile["profile_id"])),
        ("profile_version", str(profile["profile_version"])),
        ("workload_id", str(profile["workload_id"])),
        ("image_digest", str(profile["image_digest"])),
        ("request_mix_id", str(profile["request_mix_id"])),
        ("runtime_config_class", str(profile["runtime_config_class"])),
        ("platform_class", str(profile["platform_class"])),
        ("allocation", _resource(profile["allocation"])),
        ("generated_at_ms", int(profile["generated_at_ms"])),
        ("valid_until_ms", int(profile["valid_until_ms"])),
        ("confidence_ppm", int(profile["confidence_ppm"])),
        ("conservative_haircut_ppm", int(profile["conservative_haircut_ppm"])),
        (
            "safe_points",
            [
                Block(tuple((key, value) for key, value in point.items()))
                for point in profile["safe_points"]
            ],
        ),
        ("measurement_kind", str(profile["measurement_kind"])),
        ("enforceable_resource_limits", bool(profile["enforceable_resource_limits"])),
    ]
    if include_hash:
        fields.append(("profile_sha256", str(profile["profile_sha256"])))
    return tuple(fields)


def finalize_profile(profile: dict[str, object]) -> dict[str, object]:
    result = dict(profile)
    base = encode_text(
        "fleetright.v1.CapacityProfile",
        "fleetright/v1/model.proto",
        _render(_profile_fields(result, False)),
    )
    result["profile_sha256"] = hashlib.sha256(base).hexdigest()
    return result


def encode_profile(profile: dict[str, object]) -> tuple[bytes, dict[str, object]]:
    result = finalize_profile(profile)
    payload = encode_text(
        "fleetright.v1.CapacityProfile",
        "fleetright/v1/model.proto",
        _render(_profile_fields(result)),
    )
    return payload, result


def _catalog_fields(
    catalog: dict[str, object], include_hash: bool = True
) -> tuple[tuple[str, object], ...]:
    fields: list[tuple[str, object]] = [
        ("schema_version", 1),
        ("catalog_version", str(catalog["catalog_version"])),
        (
            "machines",
            [
                Block(
                    (
                        ("machine_type", item["machine_type"]),
                        ("allocatable", _resource(item["allocatable"])),
                        ("hourly_cost_micros", item["hourly_cost_micros"]),
                    )
                )
                for item in catalog["machines"]
            ],
        ),
    ]
    if include_hash:
        fields.append(("catalog_sha256", str(catalog["catalog_sha256"])))
    return tuple(fields)


def finalize_catalog(catalog: dict[str, object]) -> dict[str, object]:
    result = dict(catalog)
    base = encode_text(
        "fleetright.v1.MachineCatalog",
        "fleetright/v1/model.proto",
        _render(_catalog_fields(result, False)),
    )
    result["catalog_sha256"] = hashlib.sha256(base).hexdigest()
    return result


def _workload(value: dict[str, object]) -> Block:
    fields: list[tuple[str, object]] = []
    scalar_names = (
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
    )
    for name in scalar_names:
        scalar = value[name]
        fields.append((name, EnumToken(str(scalar)) if name == "failure_tolerance" else scalar))
    fields.append(("current_resources", _resource(value["current_resources"])))
    for name in (
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
    ):
        fields.append((name, value[name]))
    fields.append(("tolerated_taint_keys", list(value.get("tolerated_taint_keys", []))))
    return Block(tuple(fields))


def _node(value: dict[str, object]) -> Block:
    labels = [Block((("key", key), ("value", label))) for key, label in value["labels"]]
    taints = [Block((("key", key), ("value", taint))) for key, taint in value.get("taints", [])]
    return Block(
        (
            ("node_id", value["node_id"]),
            ("machine_type", value["machine_type"]),
            ("allocatable", _resource(value["allocatable"])),
            ("reserved", _resource(value["reserved"])),
            ("hourly_cost_micros", value["hourly_cost_micros"]),
            ("labels", labels),
            ("taints", taints),
            ("health", EnumToken(str(value["health"]))),
            ("active", value["active"]),
            ("generation", value["generation"]),
        )
    )


def _policy(value: dict[str, object]) -> Block:
    return Block(
        tuple(
            (name, value[name])
            for name in (
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
            )
        )
    )


def snapshot_text(fields: dict[str, object], include_hash: bool = True) -> str:
    values: list[tuple[str, object]] = [
        ("schema_version", 1),
        ("snapshot_id", fields["snapshot_id"]),
        ("generation", fields["generation"]),
        ("observed_at_ms", fields["observed_at_ms"]),
        ("expires_at_ms", fields["expires_at_ms"]),
        ("evaluation_time_ms", fields["evaluation_time_ms"]),
        ("workloads", [_workload(item) for item in fields["workloads"]]),
        ("nodes", [_node(item) for item in fields["nodes"]]),
        (
            "current_placements",
            [
                Block(
                    (
                        ("workload_id", item["workload_id"]),
                        ("replica_id", item["replica_id"]),
                        ("node_id", item["node_id"]),
                    )
                )
                for item in fields["current_placements"]
            ],
        ),
        (
            "capacity_profiles",
            [Block(_profile_fields(item)) for item in fields["capacity_profiles"]],
        ),
        ("machine_catalog", Block(_catalog_fields(fields["machine_catalog"]))),
        ("policy", _policy(fields["policy"])),
    ]
    if include_hash:
        values.append(("content_sha256", fields["content_sha256"]))
    return _render(tuple(values))


def encode_snapshot(fields: dict[str, object]) -> tuple[bytes, dict[str, object]]:
    result = dict(fields)
    result["machine_catalog"] = finalize_catalog(fields["machine_catalog"])
    result["capacity_profiles"] = [
        finalize_profile(profile) for profile in fields["capacity_profiles"]
    ]
    base = encode_text(
        "fleetright.v1.OptimizationSnapshot",
        "fleetright/v1/model.proto",
        snapshot_text(result, False),
    )
    result["content_sha256"] = hashlib.sha256(base).hexdigest()
    final = encode_text(
        "fleetright.v1.OptimizationSnapshot",
        "fleetright/v1/model.proto",
        snapshot_text(result, True),
    )
    return final, result


def encode_evacuation_snapshot(
    model_fields: dict[str, object], inventory: dict[str, object]
) -> tuple[bytes, dict[str, object]]:
    model_payload, normalized_model = encode_snapshot(model_fields)
    model_text = decode_text(
        "fleetright.v1.OptimizationSnapshot", "fleetright/v1/model.proto", model_payload
    )
    lines = [
        "schema_version: 2",
        f"owner_contract_version: {int(inventory['owner_contract_version'])}",
        f"cluster_uid: {_quote(str(inventory['cluster_uid']))}",
        f"namespace: {_quote(str(inventory['namespace']))}",
        f"inventory_observed_at_ms: {int(inventory['inventory_observed_at_ms'])}",
        "model_snapshot {",
        *(f"  {line}" for line in model_text.rstrip().splitlines()),
        "}",
    ]
    for controller in inventory["controllers"]:
        lines.extend(
            _render(
                (
                    (
                        "controllers",
                        Block(
                            tuple(
                                (name, controller[name])
                                for name in (
                                    "workload_id",
                                    "namespace",
                                    "owner_kind",
                                    "owner_name",
                                    "owner_uid",
                                    "owner_resource_version",
                                    "owner_generation",
                                    "controller_type",
                                    "execution_protocol",
                                    "pod_template_sha256",
                                    "desired_replicas",
                                    "pdb_allowed_disruptions",
                                    "pdb_resource_version",
                                    "observed_at_ms",
                                )
                            )
                        ),
                    ),
                )
            )
            .rstrip()
            .splitlines()
        )
    for pod in inventory["pods"]:
        lines.extend(
            _render(
                (
                    (
                        "pods",
                        Block(
                            (
                                ("workload_id", pod["workload_id"]),
                                ("replica_id", pod["replica_id"]),
                                ("node_id", pod["node_id"]),
                                ("pod_name", pod["pod_name"]),
                                ("pod_uid", pod["pod_uid"]),
                                ("owner_kind", pod["owner_kind"]),
                                ("owner_name", pod["owner_name"]),
                                ("owner_uid", pod["owner_uid"]),
                                ("logical_slot_id", pod["logical_slot_id"]),
                                ("ready", pod["ready"]),
                                ("terminating", pod["terminating"]),
                            )
                        ),
                    ),
                )
            )
            .rstrip()
            .splitlines()
        )
    for node in inventory["nodes"]:
        lines.extend(
            _render(
                (
                    (
                        "nodes",
                        Block(
                            (
                                ("node_id", node["node_id"]),
                                ("node_uid", node["node_uid"]),
                                ("resource_version", node["resource_version"]),
                            )
                        ),
                    ),
                )
            )
            .rstrip()
            .splitlines()
        )
    base = encode_text(
        "fleetright.v2.EvacuationSnapshot",
        "fleetright/v2/evacuation.proto",
        "\n".join(lines) + "\n",
    )
    digest = hashlib.sha256(b"fleetright.v2.EvacuationSnapshot:2:" + base).hexdigest()
    final = encode_text(
        "fleetright.v2.EvacuationSnapshot",
        "fleetright/v2/evacuation.proto",
        "\n".join([*lines, f"content_sha256: {_quote(digest)}"]) + "\n",
    )
    return final, {
        **normalized_model,
        "plan_schema_version": 2,
        "evacuation_v2": inventory,
        "evacuation_snapshot_sha256": digest,
    }
