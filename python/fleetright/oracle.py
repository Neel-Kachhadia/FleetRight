from __future__ import annotations

import itertools
import time
from dataclasses import dataclass
from typing import Any

from fleetright.units import multiply_ppm_floor


@dataclass(frozen=True)
class ExactResult:
    objective_micros_per_hour: int
    replicas: int
    node_ids: tuple[str, ...]
    duration_seconds: float
    scenarios_examined: int


def _ceil_div(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        raise ValueError("capacity must be positive")
    return numerator // denominator + (numerator % denominator != 0)


def exact_single_workload(snapshot: dict[str, Any]) -> ExactResult:
    """Exhaustive placement oracle for one profiled, non-N-1 workload and <=12 nodes."""
    if len(snapshot["workloads"]) != 1 or len(snapshot["nodes"]) > 12:
        raise ValueError("exact oracle bounds are one workload and at most twelve nodes")
    workload = snapshot["workloads"][0]
    if workload["failure_tolerance"] != "FAILURE_TOLERANCE_NONE":
        raise ValueError("exact oracle does not model N-1 recovery")
    if workload["min_topology_domains"]:
        raise ValueError("exact oracle fixture must not require topology labels")
    profiles = [
        p for p in snapshot["capacity_profiles"] if p["workload_id"] == workload["workload_id"]
    ]
    candidates: list[tuple[int, str, int, dict[str, int]]] = []
    for profile in profiles:
        if (
            profile["image_digest"] != workload["image_digest"]
            or profile["request_mix_id"] != workload["request_mix_id"]
            or profile["runtime_config_class"] != workload["runtime_config_class"]
            or profile["platform_class"] != workload["platform_class"]
            or profile["generated_at_ms"] > snapshot["evaluation_time_ms"]
            or profile["valid_until_ms"] <= snapshot["evaluation_time_ms"]
        ):
            continue
        for point in profile["safe_points"]:
            if (
                point["repeated_run_count"] < 2
                or point["latency_success_lower_ppm"] < 990_000
                or point["error_rate_wilson_upper_ppm"] > workload["error_slo_ppm"]
                or point["p99_upper_ms"] > workload["latency_slo_ms"]
                or point["queue_growth_upper_milli"] > 0
                or point["memory_pressure_ppm"] > workload["max_memory_pressure_ppm"]
                or point["cpu_throttling_ppm"] > workload["max_throttle_ppm"]
                or not point["stable_across_runs"]
            ):
                continue
            factors = (
                workload["profile_confidence_factor_ppm"],
                workload["model_accuracy_factor_ppm"],
                workload["degraded_cluster_factor_ppm"],
            )
            if any(not 0 < factor <= 1_000_000 for factor in factors):
                continue
            qps = multiply_ppm_floor(point["safe_qps_milli"], *factors)
            concurrency = multiply_ppm_floor(point["safe_concurrency"], *factors)
            if not qps or not concurrency:
                continue
            replicas = max(
                _ceil_div(workload["demand_qps_milli"], qps),
                _ceil_div(workload["concurrency_bound"], concurrency),
                workload["min_replicas"],
                workload["availability_floor"],
            )
            if replicas > workload["max_replicas"]:
                continue
            resources = profile["allocation"]
            score = (
                resources["cpu_millicores"] * 1_000_000 // 16_000
                + resources["memory_mib"] * 1_000_000 // 32_768
                + resources["network_mbps"] * 1_000_000 // 10_000
            )
            candidates.append((score, profile["profile_id"], replicas, resources))
    if not candidates:
        raise ValueError("no compatible safe profile point")
    _, _, replicas, shape = min(candidates, key=lambda c: (c[0], c[1]))
    policy = snapshot["policy"]
    headroom = policy["headroom_ppm"]
    eligible_nodes = []
    for node in snapshot["nodes"]:
        if not node["active"] or node["health"] != "NODE_HEALTH_HEALTHY":
            continue
        available = {}
        for dimension in ("cpu_millicores", "memory_mib", "network_mbps"):
            safe = node["allocatable"][dimension] * (1_000_000 - headroom) // 1_000_000
            available[dimension] = safe - node["reserved"][dimension]
        if all(shape[dimension] <= available[dimension] for dimension in shape):
            eligible_nodes.append((node, available))
    best_cost: int | None = None
    best_nodes: tuple[str, ...] = ()
    scenarios = 0
    started = time.perf_counter()
    for selected in itertools.combinations(eligible_nodes, replicas):
        scenarios += 1
        if any(
            sum(shape[dimension] for _ in selected)
            > sum(capacity[dimension] for _, capacity in selected)
            for dimension in shape
        ):
            continue
        costs = sum(node["hourly_cost_micros"] for node, _ in selected)
        ids = tuple(sorted(node["node_id"] for node, _ in selected))
        if best_cost is None or (costs, ids) < (best_cost, best_nodes):
            best_cost, best_nodes = costs, ids
    duration = time.perf_counter() - started
    if best_cost is None:
        raise ValueError("bounded scenario has no feasible exact placement")
    return ExactResult(best_cost, replicas, best_nodes, duration, scenarios)
