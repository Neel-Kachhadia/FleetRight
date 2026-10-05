from __future__ import annotations

import heapq
from dataclasses import dataclass

from fleetright.baselines import Controller, MetricWindow
from fleetright.traces import Trace, validate_trace


@dataclass(frozen=True)
class SimulationConfig:
    workload_id: str
    safe_qps_per_replica_milli: int
    safe_concurrency_per_replica: int
    base_latency_ms: int
    latency_slo_ms: int
    initial_replicas: int
    min_replicas: int
    max_replicas: int
    actuation_delay_ms: int = 5_000
    startup_ms: int = 20_000
    readiness_stabilization_ms: int = 5_000
    eviction_restart_ms: int = 3_000
    node_activation_ms: int = 10_000
    queue_limit_milli_requests: int = 50_000
    replica_cost_micros_per_hour: int = 1_000
    injected_error_ppm: int = 0


@dataclass(frozen=True)
class SimulationWindow:
    time_ms: int
    demand_qps_milli: int
    ready_replicas: int
    desired_replicas: int
    queue_milli_requests: int
    queue_growth_milli: int
    latency_p99_ms: int
    error_rate_ppm: int
    cpu_ppm: int
    memory_pressure_ppm: int


@dataclass(frozen=True)
class SimulationResult:
    windows: tuple[SimulationWindow, ...]
    replica_cost_micros: int
    request_count: int
    failed_requests: int
    max_queue_milli_requests: int


def _ceil_div(numerator: int, denominator: int) -> int:
    return numerator // denominator + (numerator % denominator != 0)


def _latency_near_capacity(
    base_ms: int, offered_qps: int, capacity_qps: int, queue_milli: int, queue_limit_milli: int
) -> int:
    if capacity_qps <= 0:
        return (1 << 63) - 1
    ratio_ppm = offered_qps * 1_000_000 // capacity_qps
    if ratio_ppm >= 1_000_000:
        overload = ratio_ppm - 1_000_000
        queue_term = queue_milli * max(base_ms, 1) // max(queue_limit_milli, 1)
        return base_ms + base_ms * (1 + overload // 10_000) + queue_term
    # M/M/1-inspired convex growth: approaches a steep boundary without implying CPU->p99.
    denominator = max(1, 1_000_000 - ratio_ppm)
    penalty = base_ms * ratio_ppm * ratio_ppm // (1_000_000 * denominator)
    return base_ms + penalty


def simulate(
    trace: Trace,
    config: SimulationConfig,
    controller: Controller,
    *,
    missing_metric_every: int = 0,
) -> SimulationResult:
    if not validate_trace(trace):
        raise ValueError("trace content hash or schema is invalid")
    workload = next(
        (item for item in trace.workloads if item.workload_id == config.workload_id), None
    )
    if workload is None:
        raise ValueError(f"trace has no workload {config.workload_id}")
    if (
        min(
            config.safe_qps_per_replica_milli,
            config.safe_concurrency_per_replica,
            config.base_latency_ms,
            config.latency_slo_ms,
            config.min_replicas,
        )
        <= 0
    ):
        raise ValueError("simulator capacity and latency values must be positive")

    # Event order is (time, sequence); stable sequence numbers resolve same-time transitions.
    events: list[tuple[int, int, str, object]] = []
    sequence = 0
    for index, point in enumerate(workload.points):
        heapq.heappush(events, (point.time_ms, sequence, "demand", (index, point)))
        sequence += 1
    desired = min(config.max_replicas, max(config.min_replicas, config.initial_replicas))
    ready = desired
    queue = 0
    windows: list[SimulationWindow] = []
    requests = 0
    failures = 0
    cost = 0
    max_queue = 0

    while events:
        time_ms, _, kind, payload = heapq.heappop(events)
        if kind == "ready":
            additions = min(int(payload), max(0, desired - ready))
            ready = min(desired, ready + additions)
            continue
        if kind == "remove":
            ready = min(ready, desired)
            continue

        index, point = payload  # type: ignore[misc]
        arrivals = point.qps_milli * trace.window_ms // 1_000
        capacity = config.safe_qps_per_replica_milli * ready
        served = min(queue + arrivals, capacity * trace.window_ms // 1_000)
        next_queue = max(0, queue + arrivals - served)
        growth = (next_queue - queue) * 1_000 // max(1, trace.window_ms)
        latency = _latency_near_capacity(
            config.base_latency_ms,
            point.qps_milli,
            capacity,
            next_queue,
            config.queue_limit_milli_requests,
        )
        if point.memory_pressure_ppm >= 900_000:
            latency += config.base_latency_ms
        overload_ppm = (
            0 if capacity == 0 else max(0, point.qps_milli - capacity) * 1_000_000 // capacity
        )
        queue_overflow = next_queue > config.queue_limit_milli_requests
        error_rate = min(1_000_000, config.injected_error_ppm + overload_ppm // 5)
        if queue_overflow:
            error_rate = max(error_rate, 100_000)
        failed = arrivals * error_rate // 1_000_000
        requests += arrivals // 1_000
        failures += failed // 1_000
        max_queue = max(max_queue, next_queue)
        cpu = 0 if capacity == 0 else min(1_000_000, point.qps_milli * 1_000_000 // capacity)
        metrics_missing = missing_metric_every > 0 and (index + 1) % missing_metric_every == 0
        recommendation = controller.recommend(
            desired,
            MetricWindow(
                None if metrics_missing else cpu,
                None if metrics_missing else point.qps_milli,
                None if metrics_missing else point.concurrency,
                ready,
                max(0, desired - ready),
            ),
            index,
        )
        target = min(config.max_replicas, max(config.min_replicas, recommendation))
        if target > desired:
            count = target - desired
            desired = target
            ready_at = (
                time_ms
                + config.actuation_delay_ms
                + config.node_activation_ms
                + config.startup_ms
                + config.readiness_stabilization_ms
            )
            heapq.heappush(events, (ready_at, sequence, "ready", count))
            sequence += 1
        elif target < desired:
            desired = target
            remove_at = time_ms + config.actuation_delay_ms + config.eviction_restart_ms
            heapq.heappush(events, (remove_at, sequence, "remove", 0))
            sequence += 1
        cost += desired * config.replica_cost_micros_per_hour * trace.window_ms // 3_600_000
        windows.append(
            SimulationWindow(
                time_ms,
                point.qps_milli,
                ready,
                desired,
                next_queue,
                growth,
                latency,
                error_rate,
                cpu,
                point.memory_pressure_ppm,
            )
        )
        queue = next_queue

    return SimulationResult(tuple(windows), cost, requests, failures, max_queue)
