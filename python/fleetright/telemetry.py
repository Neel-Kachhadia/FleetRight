from __future__ import annotations

import bisect
import math
import re
from dataclasses import dataclass

DEFAULT_BUCKETS_MS = (1, 2, 5, 10, 20, 40, 80, 120, 160, 250, 500, 1_000)


@dataclass(frozen=True)
class Histogram:
    """Classic histogram with non-cumulative finite buckets and an implicit +Inf bucket."""

    buckets_ms: tuple[int, ...] = DEFAULT_BUCKETS_MS
    counts: tuple[int, ...] = (0,) * (len(DEFAULT_BUCKETS_MS) + 1)
    sum_ms: int = 0

    def __post_init__(self) -> None:
        if tuple(sorted(set(self.buckets_ms))) != self.buckets_ms or any(
            b < 0 for b in self.buckets_ms
        ):
            raise ValueError("histogram buckets must be unique, sorted, and nonnegative")
        if len(self.counts) != len(self.buckets_ms) + 1 or any(c < 0 for c in self.counts):
            raise ValueError("histogram count vector does not match buckets")
        if self.sum_ms < 0:
            raise ValueError("histogram latency sum must be nonnegative")

    @property
    def count(self) -> int:
        return sum(self.counts)

    def observe(self, latency_ms: float) -> Histogram:
        if not math.isfinite(latency_ms) or latency_ms < 0:
            raise ValueError("latency must be finite and nonnegative")
        value = math.ceil(latency_ms)
        index = bisect.bisect_left(self.buckets_ms, value)
        counts = list(self.counts)
        counts[index] += 1
        return Histogram(self.buckets_ms, tuple(counts), self.sum_ms + value)

    def merge(self, other: Histogram) -> Histogram:
        if self.buckets_ms != other.buckets_ms:
            raise ValueError("histogram buckets differ")
        return Histogram(
            self.buckets_ms,
            tuple(left + right for left, right in zip(self.counts, other.counts, strict=True)),
            self.sum_ms + other.sum_ms,
        )

    def count_leq(self, threshold_ms: int) -> int:
        # Characterization configs insert the SLO threshold as a bucket boundary.
        try:
            index = self.buckets_ms.index(threshold_ms)
        except ValueError as error:
            raise ValueError("SLO threshold must be an explicit histogram bucket") from error
        return sum(self.counts[: index + 1])

    def quantile_upper_ms(self, quantile: float) -> int | None:
        if not 0 <= quantile <= 1:
            raise ValueError("quantile must be in [0, 1]")
        if self.count == 0:
            return None
        rank = max(1, math.ceil(quantile * self.count))
        cumulative = 0
        for index, count in enumerate(self.counts):
            cumulative += count
            if cumulative >= rank:
                return self.buckets_ms[index] if index < len(self.buckets_ms) else None
        return None

    def prometheus(self, name: str = "fleetright_request_latency_ms") -> str:
        if not re.fullmatch(r"[a-zA-Z_:][a-zA-Z0-9_:]*", name):
            raise ValueError("invalid Prometheus metric name")
        lines = [f"# TYPE {name} histogram"]
        cumulative = 0
        for bound, count in zip(self.buckets_ms, self.counts, strict=False):
            cumulative += count
            lines.append(f'{name}_bucket{{le="{bound}"}} {cumulative}')
        cumulative += self.counts[-1]
        lines.extend(
            (
                f'{name}_bucket{{le="+Inf"}} {cumulative}',
                f"{name}_sum {self.sum_ms}",
                f"{name}_count {self.count}",
            )
        )
        return "\n".join(lines) + "\n"


def pooled_histogram(histograms: list[Histogram]) -> Histogram:
    if not histograms:
        return Histogram()
    buckets = histograms[0].buckets_ms
    result = Histogram(buckets, (0,) * (len(buckets) + 1))
    for histogram in histograms:
        result = result.merge(histogram)
    return result


def tail_confidence(sample_count: int, minimum_samples: int = 10_000) -> str:
    return "low" if sample_count < minimum_samples else "adequate"
