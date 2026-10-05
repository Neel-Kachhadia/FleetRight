from __future__ import annotations

from copy import deepcopy

from fleetright.contracts import encode_snapshot


def _capacity_point(qps_milli: int, concurrency: int, latency: int) -> dict[str, int | str | bool]:
    return {
        "safe_qps_milli": qps_milli,
        "safe_concurrency": concurrency,
        "p99_upper_ms": latency,
        "error_rate_upper_ppm": 20,
        "queue_growth_upper_milli": 0,
        "memory_pressure_ppm": 50_000,
        "cpu_throttling_ppm": 10_000,
        "request_count": 50_000,
        "repeated_run_count": 3,
        "threshold_successes": 49_960,
        "threshold_trials": 50_000,
        "latency_success_lower_ppm": 995_000,
        "error_rate_wilson_upper_ppm": 300,
        "run_dispersion_ppm": 20_000,
        "variance_ppm": 300,
        "histogram_config_id": "classic-ms-slo-v1",
        "stable_across_runs": True,
        "error_count": 0,
    }


def _profile(
    workload: dict[str, object],
    profile_id: str,
    cpu: int,
    memory: int,
    network: int,
    qps: int,
    concurrency: int,
    latency: int,
    generated_at_ms: int,
    valid_until_ms: int,
) -> dict[str, object]:
    return {
        "profile_id": profile_id,
        "profile_version": "1",
        "workload_id": workload["workload_id"],
        "image_digest": workload["image_digest"],
        "request_mix_id": workload["request_mix_id"],
        "runtime_config_class": workload["runtime_config_class"],
        "platform_class": workload["platform_class"],
        "allocation": {"cpu_millicores": cpu, "memory_mib": memory, "network_mbps": network},
        "generated_at_ms": generated_at_ms,
        "valid_until_ms": valid_until_ms,
        "confidence_ppm": 990_000,
        "conservative_haircut_ppm": 100_000,
        "safe_points": [_capacity_point(qps, concurrency, latency)],
        "measurement_kind": "synthetic_benchmark_fixture",
        "enforceable_resource_limits": False,
    }


def demo_snapshot_fields(base_time_ms: int = 1_800_000_000_000) -> dict[str, object]:
    epoch = base_time_ms
    workloads: list[dict[str, object]] = [
        {
            "workload_id": "cpu-api",
            "priority": 100,
            "min_replicas": 2,
            "max_replicas": 12,
            "availability_floor": 2,
            "demand_qps_milli": 700_000,
            "concurrency_bound": 200,
            "latency_slo_ms": 120,
            "error_slo_ppm": 1_000,
            "startup_ms": 20_000,
            "readiness_stabilization_ms": 5_000,
            "failure_tolerance": "FAILURE_TOLERANCE_N_MINUS_1",
            "min_topology_domains": 2,
            "max_topology_skew": 1,
            "topology_key": "zone",
            "current_resources": {
                "cpu_millicores": 1_000,
                "memory_mib": 1_024,
                "network_mbps": 100,
            },
            "image_digest": "sha256:cpu-build-v1",
            "request_mix_id": "cpu-read-v1",
            "runtime_config_class": "python-default",
            "platform_class": "x86_64-linux",
            "profile_confidence_factor_ppm": 950_000,
            "model_accuracy_factor_ppm": 980_000,
            "degraded_cluster_factor_ppm": 1_000_000,
            "max_throttle_ppm": 50_000,
            "max_memory_pressure_ppm": 200_000,
            "max_scale_down_fraction_ppm": 100_000,
            "max_moves": 2,
            "recent_oom_or_reclaim": False,
        },
        {
            "workload_id": "memory-api",
            "priority": 70,
            "min_replicas": 2,
            "max_replicas": 12,
            "availability_floor": 2,
            "demand_qps_milli": 600_000,
            "concurrency_bound": 160,
            "latency_slo_ms": 150,
            "error_slo_ppm": 1_000,
            "startup_ms": 15_000,
            "readiness_stabilization_ms": 4_000,
            "failure_tolerance": "FAILURE_TOLERANCE_NONE",
            "min_topology_domains": 0,
            "max_topology_skew": 1,
            "topology_key": "zone",
            "current_resources": {"cpu_millicores": 700, "memory_mib": 1_536, "network_mbps": 100},
            "image_digest": "sha256:memory-build-v1",
            "request_mix_id": "memory-read-v1",
            "runtime_config_class": "32m-working-set",
            "platform_class": "x86_64-linux",
            "profile_confidence_factor_ppm": 950_000,
            "model_accuracy_factor_ppm": 980_000,
            "degraded_cluster_factor_ppm": 1_000_000,
            "max_throttle_ppm": 50_000,
            "max_memory_pressure_ppm": 200_000,
            "max_scale_down_fraction_ppm": 100_000,
            "max_moves": 2,
            "recent_oom_or_reclaim": False,
        },
        {
            "workload_id": "queued-api",
            "priority": 90,
            "min_replicas": 2,
            "max_replicas": 12,
            "availability_floor": 2,
            "demand_qps_milli": 500_000,
            "concurrency_bound": 150,
            "latency_slo_ms": 80,
            "error_slo_ppm": 1_000,
            "startup_ms": 30_000,
            "readiness_stabilization_ms": 8_000,
            "failure_tolerance": "FAILURE_TOLERANCE_NONE",
            "min_topology_domains": 0,
            "max_topology_skew": 1,
            "topology_key": "zone",
            "current_resources": {"cpu_millicores": 1_000, "memory_mib": 768, "network_mbps": 100},
            "image_digest": "sha256:queue-build-v1",
            "request_mix_id": "queue-mix-v1",
            "runtime_config_class": "concurrency-32",
            "platform_class": "x86_64-linux",
            "profile_confidence_factor_ppm": 950_000,
            "model_accuracy_factor_ppm": 980_000,
            "degraded_cluster_factor_ppm": 1_000_000,
            "max_throttle_ppm": 50_000,
            "max_memory_pressure_ppm": 200_000,
            "max_scale_down_fraction_ppm": 100_000,
            "max_moves": 2,
            "recent_oom_or_reclaim": False,
        },
    ]
    profiles = [
        _profile(
            workloads[0],
            "cpu-api-500m-512-v1",
            500,
            512,
            50,
            500_000,
            150,
            90,
            epoch - 60_000,
            epoch + 86_400_000,
        ),
        _profile(
            workloads[1],
            "memory-api-500m-768-v1",
            500,
            768,
            50,
            400_000,
            120,
            110,
            epoch - 60_000,
            epoch + 86_400_000,
        ),
        _profile(
            workloads[2],
            "queued-api-750m-512-v1",
            750,
            512,
            50,
            300_000,
            100,
            60,
            epoch - 60_000,
            epoch + 86_400_000,
        ),
    ]
    machines = [
        ("small", 2_000, 4_096, 1_000, 50_000),
        ("medium", 4_000, 8_192, 2_000, 90_000),
        ("large", 8_000, 16_384, 4_000, 170_000),
        ("xlarge", 16_000, 32_768, 8_000, 310_000),
    ]
    nodes = []
    for index in range(10):
        nodes.append(
            {
                "node_id": f"node-{index:02d}",
                "machine_type": "small",
                "allocatable": {
                    "cpu_millicores": 2_000,
                    "memory_mib": 4_096,
                    "network_mbps": 1_000,
                },
                "reserved": {"cpu_millicores": 100, "memory_mib": 256, "network_mbps": 50},
                "hourly_cost_micros": 50_000,
                "labels": [("zone", f"zone-{index % 3}"), ("rack", f"rack-{index % 5}")],
                "taints": [],
                "health": "NODE_HEALTH_HEALTHY",
                "active": True,
                "generation": 2,
            }
        )
    placements = []
    ranges = (("cpu-api", 0, 4), ("memory-api", 4, 7), ("queued-api", 7, 10))
    for workload_id, begin, end in ranges:
        for index, node_index in enumerate(range(begin, end)):
            placements.append(
                {
                    "workload_id": workload_id,
                    "replica_id": f"{workload_id}-{index}",
                    "node_id": f"node-{node_index:02d}",
                }
            )
    return {
        "snapshot_id": "snapshot-demo-seed-42",
        "generation": 17,
        "observed_at_ms": epoch - 30_000,
        "expires_at_ms": epoch + 300_000,
        "evaluation_time_ms": epoch,
        "workloads": workloads,
        "nodes": nodes,
        "current_placements": placements,
        "capacity_profiles": profiles,
        "machine_catalog": {
            "catalog_version": "synthetic-2026-01",
            "machines": [
                {
                    "machine_type": name,
                    "allocatable": {
                        "cpu_millicores": cpu,
                        "memory_mib": memory,
                        "network_mbps": network,
                    },
                    "hourly_cost_micros": price,
                }
                for name, cpu, memory, network, price in machines
            ],
        },
        "policy": {
            "schema_version": 1,
            "headroom_ppm": 50_000,
            "minimum_profile_confidence_ppm": 950_000,
            "maximum_profile_age_ms": 3_600_000,
            "maximum_snapshot_age_ms": 60_000,
            "maximum_plan_actions": 100,
            "movement_cost_micros": 10_000,
            "minimum_improvement_ppm": 10_000,
            "maximum_local_search_iterations": 100,
            "allow_synthetic_profiles": True,
            "planning_horizon_seconds": 3600,
            "action_churn_cost_micros": 250,
            "uncertainty_cost_micros_per_replica": 100_000,
            "boundary_cost_micros_per_workload": 10_000,
        },
    }


def demo_snapshot() -> tuple[bytes, dict[str, object]]:
    return encode_snapshot(demo_snapshot_fields())


def oracle_snapshot_fields() -> dict[str, object]:
    fields = deepcopy(demo_snapshot_fields())
    workload = fields["workloads"][0]
    workload["failure_tolerance"] = "FAILURE_TOLERANCE_NONE"
    workload["min_topology_domains"] = 0
    workload["max_scale_down_fraction_ppm"] = 500_000
    fields["workloads"] = [workload]
    fields["capacity_profiles"] = [fields["capacity_profiles"][0]]
    fields["nodes"] = fields["nodes"][:3]
    fields["current_placements"] = [
        placement
        for placement in fields["current_placements"]
        if placement["workload_id"] == workload["workload_id"]
    ][:3]
    fields["snapshot_id"] = "snapshot-oracle-small-001"
    fields["policy"]["movement_cost_micros"] = 0
    fields["policy"]["action_churn_cost_micros"] = 0
    fields["policy"]["uncertainty_cost_micros_per_replica"] = 0
    fields["policy"]["boundary_cost_micros_per_workload"] = 0
    fields["policy"]["planning_horizon_seconds"] = 3600
    return fields


def oracle_snapshot() -> tuple[bytes, dict[str, object]]:
    return encode_snapshot(oracle_snapshot_fields())
