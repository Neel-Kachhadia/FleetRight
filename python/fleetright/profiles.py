from __future__ import annotations

import math
from dataclasses import dataclass

from fleetright.telemetry import Histogram, pooled_histogram


@dataclass(frozen=True)
class RunMetrics:
    histogram: Histogram
    request_count: int
    errors: int
    sustainable_qps_milli: int
    sustainable_concurrency: int
    queue_growth_upper_milli: int
    memory_pressure_ppm: int
    cpu_throttling_ppm: int


def wilson_lower_ppm(successes: int, trials: int, z: float = 1.96) -> int:
    if trials <= 0 or successes < 0 or successes > trials:
        raise ValueError("Wilson interval requires 0 <= successes <= trials and trials > 0")
    n = float(trials)
    p = successes / n
    z2 = z * z
    center = p + z2 / (2 * n)
    radius = z * math.sqrt((p * (1 - p) + z2 / (4 * n)) / n)
    return max(0, min(1_000_000, math.floor((center - radius) / (1 + z2 / n) * 1_000_000)))


def wilson_upper_ppm(successes: int, trials: int, z: float = 1.96) -> int:
    if trials <= 0 or successes < 0 or successes > trials:
        raise ValueError("Wilson interval requires 0 <= successes <= trials and trials > 0")
    n = float(trials)
    p = successes / n
    z2 = z * z
    center = p + z2 / (2 * n)
    radius = z * math.sqrt((p * (1 - p) + z2 / (4 * n)) / n)
    return max(0, min(1_000_000, math.ceil((center + radius) / (1 + z2 / n) * 1_000_000)))


def characterize_point(
    runs: list[RunMetrics],
    latency_slo_ms: int,
    error_slo_ppm: int,
    haircut_ppm: int = 100_000,
    histogram_config_id: str = "classic-ms-v1",
    max_dispersion_ppm: int = 250_000,
) -> dict[str, int | str | bool]:
    if not runs or latency_slo_ms <= 0 or not 0 <= error_slo_ppm <= 1_000_000:
        raise ValueError("profile point requires runs and valid SLO thresholds")
    if not 0 <= haircut_ppm < 1_000_000:
        raise ValueError("capacity haircut must be in [0, 1_000_000)")
    if any(
        run.request_count <= 0 or run.errors < 0 or run.errors > run.request_count for run in runs
    ):
        raise ValueError("run counts are invalid")
    if any(run.request_count > (1 << 64) - 1 for run in runs):
        raise ValueError("run request count exceeds protobuf uint64")
    histograms = pooled_histogram([run.histogram for run in runs])
    requests = sum(run.request_count for run in runs)
    if requests > (1 << 64) - 1:
        raise ValueError("aggregate request count exceeds protobuf uint64")
    failures = sum(run.errors for run in runs)
    successes = sum(run.histogram.count_leq(latency_slo_ms) for run in runs)
    trials = sum(run.histogram.count for run in runs)
    if trials != requests:
        raise ValueError("histogram request counts must match raw request counts")
    qps_values = [run.sustainable_qps_milli for run in runs]
    concurrency_values = [run.sustainable_concurrency for run in runs]
    if min(qps_values) < 0 or min(concurrency_values) < 0:
        raise ValueError("measured capacities must be nonnegative")
    if max(qps_values) > (1 << 64) - 1 or max(concurrency_values) > (1 << 64) - 1:
        raise ValueError("measured capacity exceeds protobuf uint64")
    mean_qps = sum(qps_values) / len(qps_values)
    dispersion = (
        0
        if mean_qps == 0
        else math.ceil((max(qps_values) - min(qps_values)) / mean_qps * 1_000_000)
    )
    mean_qps_sq = sum(value * value for value in qps_values) / len(qps_values)
    variance_ppm = (
        0
        if mean_qps_sq == 0
        else math.ceil(
            (sum((value - mean_qps) ** 2 for value in qps_values) / len(qps_values))
            / mean_qps_sq
            * 1_000_000
        )
    )
    p99_upper = histograms.quantile_upper_ms(0.99)
    return {
        "safe_qps_milli": min(qps_values) * (1_000_000 - haircut_ppm) // 1_000_000,
        "safe_concurrency": min(concurrency_values) * (1_000_000 - haircut_ppm) // 1_000_000,
        "p99_upper_ms": p99_upper if p99_upper is not None else (1 << 63) - 1,
        "error_rate_upper_ppm": math.ceil(failures * 1_000_000 / requests),
        "error_rate_wilson_upper_ppm": wilson_upper_ppm(failures, requests),
        "error_count": failures,
        "queue_growth_upper_milli": max(run.queue_growth_upper_milli for run in runs),
        "memory_pressure_ppm": max(run.memory_pressure_ppm for run in runs),
        "cpu_throttling_ppm": max(run.cpu_throttling_ppm for run in runs),
        "request_count": requests,
        "repeated_run_count": len(runs),
        "threshold_successes": successes,
        "threshold_trials": trials,
        "latency_success_lower_ppm": wilson_lower_ppm(successes, trials),
        "run_dispersion_ppm": dispersion,
        "variance_ppm": variance_ppm,
        "histogram_config_id": histogram_config_id,
        "stable_across_runs": dispersion <= max_dispersion_ppm
        and all(run.queue_growth_upper_milli <= 0 for run in runs)
        and all(run.errors <= run.request_count * error_slo_ppm // 1_000_000 for run in runs),
    }
