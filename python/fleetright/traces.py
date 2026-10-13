from __future__ import annotations

import hashlib
import json
import random
from dataclasses import asdict, dataclass

TRACE_SCHEMA_VERSION = 1
_DIURNAL = (350, 400, 500, 650, 800, 950, 1_000, 900, 750, 600, 450, 380)


@dataclass(frozen=True)
class DemandPoint:
    time_ms: int
    qps_milli: int
    concurrency: int
    memory_pressure_ppm: int = 0


@dataclass(frozen=True)
class WorkloadTrace:
    workload_id: str
    points: tuple[DemandPoint, ...]


@dataclass(frozen=True)
class Trace:
    schema_version: int
    family: str
    seed: int
    window_ms: int
    workloads: tuple[WorkloadTrace, ...]
    metadata: tuple[tuple[str, str], ...]
    content_sha256: str

    def as_dict(self, include_hash: bool = True) -> dict[str, object]:
        value: dict[str, object] = {
            "schema_version": self.schema_version,
            "family": self.family,
            "seed": self.seed,
            "window_ms": self.window_ms,
            "workloads": [
                {
                    "workload_id": item.workload_id,
                    "points": [asdict(point) for point in item.points],
                }
                for item in self.workloads
            ],
            "metadata": dict(self.metadata),
        }
        if include_hash:
            value["content_sha256"] = self.content_sha256
        return value


def _content_hash(value: dict[str, object]) -> str:
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def generate_trace(
    family: str,
    seed: int,
    windows: int = 48,
    window_ms: int = 60_000,
    workload_ids: tuple[str, ...] = ("cpu-api", "memory-api", "queued-api"),
    base_qps_milli: int = 400_000,
    jitter_permille: int = 0,
) -> Trace:
    known = {
        "steady",
        "ramp",
        "diurnal",
        "bursty",
        "flash_crowd",
        "correlated_spike",
        "noisy_tenant",
        "memory_heavy",
        "slow_start",
    }
    if family not in known or windows < 1 or window_ms < 1 or base_qps_milli < 0:
        raise ValueError("invalid trace family or dimensions")
    rng = random.Random(seed)
    result: list[WorkloadTrace] = []
    for workload_index, workload_id in enumerate(workload_ids):
        points: list[DemandPoint] = []
        previous_noise = 0
        for index in range(windows):
            factor = 1_000
            memory = 0
            if family == "ramp":
                factor = 250 + 1_500 * index // max(windows - 1, 1)
            elif family == "diurnal":
                factor = _DIURNAL[index % len(_DIURNAL)]
            elif family == "bursty":
                factor = 1_000 + (rng.randint(0, 1_500) if index % 5 == 0 else 0)
            elif family == "flash_crowd":
                factor = 5_000 if index == windows // 3 else 1_000
            elif family == "correlated_spike":
                factor = 3_000 if windows // 3 <= index < windows // 3 + 3 else 1_000
            elif family == "noisy_tenant":
                previous_noise = max(-400, min(1_500, previous_noise + rng.randint(-250, 250)))
                factor += previous_noise if workload_index == 0 else 0
            elif family == "memory_heavy":
                memory = min(950_000, 300_000 + 20_000 * (index % 8))
                factor = 850 + 100 * (index % 4)
            elif family == "slow_start":
                factor = 1_000
            jitter = rng.randint(-20, 20) if family in {"bursty", "noisy_tenant"} else 0
            if jitter_permille:  # seeded multiplicative noise for every family
                jitter += rng.randint(-jitter_permille, jitter_permille)
            qps = max(0, base_qps_milli * max(0, factor + jitter) // 1_000)
            concurrency = max(1, qps * (25 + workload_index * 10) // 1_000_000)
            points.append(DemandPoint(index * window_ms, qps, concurrency, memory))
        result.append(WorkloadTrace(workload_id, tuple(points)))
    metadata = (
        ("shared-spike", "true" if family == "correlated_spike" else "false"),
        ("startup-delay-modeled", "true" if family == "slow_start" else "false"),
    )
    trace = Trace(TRACE_SCHEMA_VERSION, family, seed, window_ms, tuple(result), metadata, "")
    return Trace(**{**trace.__dict__, "content_sha256": _content_hash(trace.as_dict(False))})


def transform_trace(trace: Trace, family: str, change) -> Trace:
    """Rebuild a hashed trace with change(index, point) -> DemandPoint applied to every point."""
    workloads = tuple(
        WorkloadTrace(item.workload_id, tuple(change(i, p) for i, p in enumerate(item.points)))
        for item in trace.workloads
    )
    result = Trace(**{**trace.__dict__, "family": family, "workloads": workloads})
    return Trace(**{**result.__dict__, "content_sha256": _content_hash(result.as_dict(False))})


def validate_trace(trace: Trace) -> bool:
    return trace.schema_version == TRACE_SCHEMA_VERSION and trace.content_sha256 == _content_hash(
        trace.as_dict(False)
    )
