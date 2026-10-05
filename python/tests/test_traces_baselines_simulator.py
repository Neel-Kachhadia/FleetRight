from __future__ import annotations

from fleetright.baselines import (
    FixedTarget,
    HpaLikeBaseline,
    MetricWindow,
    ThresholdBaseline,
    lookahead_schedule,
    static_peak_replicas,
)
from fleetright.simulator import SimulationConfig, simulate
from fleetright.traces import generate_trace, validate_trace

FAMILIES = (
    "steady",
    "ramp",
    "diurnal",
    "bursty",
    "flash_crowd",
    "correlated_spike",
    "noisy_tenant",
    "memory_heavy",
    "slow_start",
)


def test_trace_families_have_seeded_content_hashes() -> None:
    for family in FAMILIES:
        first = generate_trace(family, 42)
        second = generate_trace(family, 42)
        different = generate_trace(family, 43)
        assert validate_trace(first)
        assert first.as_dict() == second.as_dict()
        assert first.content_sha256 != different.content_sha256


def test_hpa_like_uses_total_qps_per_replica_once() -> None:
    hpa = HpaLikeBaseline(
        target_cpu_ppm=650_000,
        target_qps_per_replica_milli=300_000,
        target_concurrency_per_replica=100,
        min_replicas=1,
        max_replicas=20,
    )
    at_target = MetricWindow(None, 1_200_000, None, 4, 0)
    assert hpa.recommend(4, at_target, 1) == 4
    scale_out = MetricWindow(None, 1_500_000, None, 4, 0)
    assert hpa.recommend(4, scale_out, 2) == 5


def test_hpa_missing_metric_blocks_scale_in_and_stabilizes_downscale() -> None:
    hpa = HpaLikeBaseline(
        target_cpu_ppm=650_000,
        target_qps_per_replica_milli=300_000,
        target_concurrency_per_replica=100,
        min_replicas=1,
        max_replicas=20,
        downscale_stabilization_windows=3,
    )
    missing = MetricWindow(None, 100_000, 30, 4, 0)
    assert hpa.recommend(4, missing, 1) == 4
    complete = MetricWindow(300_000, 100_000, 30, 4, 0)
    assert hpa.recommend(4, complete, 2) == 3
    assert hpa.recommend(4, complete, 3) == 3


def test_hpa_starting_or_unready_replicas_block_scale_in() -> None:
    hpa = HpaLikeBaseline(
        target_cpu_ppm=650_000,
        target_qps_per_replica_milli=300_000,
        target_concurrency_per_replica=100,
        min_replicas=1,
        max_replicas=20,
    )
    starting = MetricWindow(100_000, 100_000, 30, 3, 1)
    unready = MetricWindow(100_000, 100_000, 30, 2, 0)
    assert hpa.recommend(4, starting, 1) == 4
    assert hpa.recommend(4, unready, 2) == 4


def test_threshold_baseline_honors_separate_up_and_down_windows() -> None:
    policy = ThresholdBaseline(
        scale_up_windows=2, scale_down_windows=2, min_replicas=1, max_replicas=8
    )
    high = MetricWindow(900_000, None, None, 2, 0)
    assert policy.recommend(2, high, 0) == 2
    assert policy.recommend(2, high, 1) == 3
    low = MetricWindow(100_000, None, None, 2, 0)
    assert policy.recommend(3, low, 2) == 3
    assert policy.recommend(3, low, 3) == 2


def test_static_reserve_and_future_lookahead_use_ceiling_capacity() -> None:
    assert static_peak_replicas(1_000_000, 100, 500_000, 50, 1, 200_000) == 3
    assert lookahead_schedule([100, 900], [10, 90], 500, 50, 1, 8, 1, 0) == [2, 2]


def test_simulator_models_nonlinear_saturation_and_queue_growth() -> None:
    trace = generate_trace(
        "steady", 7, windows=2, workload_ids=("cpu-api",), base_qps_milli=990_000
    )
    from dataclasses import replace

    second = replace(trace.workloads[0].points[1], qps_milli=1_100_000)
    altered_workload = replace(trace.workloads[0], points=(trace.workloads[0].points[0], second))
    altered = replace(trace, workloads=(altered_workload,), content_sha256="")
    import hashlib
    import json

    body = altered.as_dict(include_hash=False)
    digest = hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()
    altered = replace(altered, content_sha256=digest)
    result = simulate(
        altered,
        SimulationConfig(
            workload_id="cpu-api",
            safe_qps_per_replica_milli=1_000_000,
            safe_concurrency_per_replica=100,
            base_latency_ms=10,
            latency_slo_ms=120,
            initial_replicas=1,
            min_replicas=1,
            max_replicas=4,
        ),
        FixedTarget(target=1),
    )
    assert result.windows[1].queue_growth_milli > 0
    assert result.windows[1].latency_p99_ms > result.windows[0].latency_p99_ms
    assert result.max_queue_milli_requests > 0


def test_simulator_applies_startup_delay_before_replica_becomes_ready() -> None:
    trace = generate_trace("steady", 2, windows=3, workload_ids=("cpu-api",), base_qps_milli=10_000)
    result = simulate(
        trace,
        SimulationConfig(
            workload_id="cpu-api",
            safe_qps_per_replica_milli=1_000_000,
            safe_concurrency_per_replica=100,
            base_latency_ms=5,
            latency_slo_ms=120,
            initial_replicas=1,
            min_replicas=1,
            max_replicas=4,
            actuation_delay_ms=30_000,
            node_activation_ms=10_000,
            startup_ms=40_000,
            readiness_stabilization_ms=10_000,
        ),
        type("ScaleOut", (), {"recommend": lambda self, current, metrics, window: 2})(),
    )
    assert [window.ready_replicas for window in result.windows] == [1, 1, 2]
