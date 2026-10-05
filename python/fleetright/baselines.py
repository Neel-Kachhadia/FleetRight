from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Protocol


def _ceil_div(numerator: int, denominator: int) -> int:
    if numerator < 0 or denominator <= 0:
        raise ValueError("ceiling division requires nonnegative numerator and positive denominator")
    return numerator // denominator + (numerator % denominator != 0)


@dataclass(frozen=True)
class MetricWindow:
    cpu_ppm: int | None
    qps_milli: int | None
    concurrency: int | None
    ready_replicas: int
    startup_replicas: int


class Controller(Protocol):
    def recommend(self, current: int, metrics: MetricWindow, window: int) -> int: ...


@dataclass(frozen=True)
class FixedTarget:
    target: int

    def recommend(self, current: int, metrics: MetricWindow, window: int) -> int:
        del current, metrics, window
        return self.target


@dataclass(frozen=True)
class LookaheadTarget:
    schedule: tuple[int, ...]

    def recommend(self, current: int, metrics: MetricWindow, window: int) -> int:
        del current, metrics
        return self.schedule[min(window, len(self.schedule) - 1)]


@dataclass
class ThresholdBaseline:
    scale_up_threshold_ppm: int = 800_000
    scale_down_threshold_ppm: int = 300_000
    scale_up_windows: int = 2
    scale_down_windows: int = 5
    max_replicas: int = 100
    min_replicas: int = 1
    high_windows: int = 0
    low_windows: int = 0

    def recommend(self, current: int, metrics: MetricWindow, window: int) -> int:
        del window
        if metrics.cpu_ppm is None:
            self.high_windows = self.low_windows = 0
            return current
        if metrics.cpu_ppm > self.scale_up_threshold_ppm:
            self.high_windows += 1
            self.low_windows = 0
            return (
                min(self.max_replicas, current + 1)
                if self.high_windows >= self.scale_up_windows
                else current
            )
        if metrics.cpu_ppm < self.scale_down_threshold_ppm and metrics.startup_replicas == 0:
            self.low_windows += 1
            self.high_windows = 0
            return (
                max(self.min_replicas, current - 1)
                if self.low_windows >= self.scale_down_windows
                else current
            )
        self.high_windows = self.low_windows = 0
        return current


@dataclass
class HpaLikeBaseline:
    """Deterministic target tracker; representative semantics, not Kubernetes byte equivalence."""

    target_cpu_ppm: int = 650_000
    target_qps_per_replica_milli: int = 300_000
    target_concurrency_per_replica: int = 100
    tolerance_ppm: int = 100_000
    max_scale_up_percent: int = 100
    max_scale_up_pods: int = 4
    max_scale_down_percent: int = 25
    downscale_stabilization_windows: int = 5
    min_replicas: int = 1
    max_replicas: int = 100
    _down_recommendations: deque[tuple[int, int]] | None = None

    def __post_init__(self) -> None:
        if (
            not 0 <= self.target_cpu_ppm <= 1_000_000
            or self.target_qps_per_replica_milli <= 0
            or self.target_concurrency_per_replica <= 0
            or not 0 <= self.tolerance_ppm < 1_000_000
            or self.max_scale_up_percent < 0
            or self.max_scale_up_pods < 0
            or not 0 <= self.max_scale_down_percent <= 100
            or self.downscale_stabilization_windows < 1
            or self.min_replicas < 0
            or self.max_replicas < self.min_replicas
        ):
            raise ValueError("invalid HPA-like target or policy bounds")
        self._down_recommendations = deque()

    def _metric_recommendation(
        self, current: int, observed: int | None, target: int, *, total_metric: bool = False
    ) -> int | None:
        if observed is None or target <= 0:
            return None
        expected = target * current if total_metric else target
        if observed * 1_000_000 <= expected * (
            1_000_000 + self.tolerance_ppm
        ) and observed * 1_000_000 >= expected * (1_000_000 - self.tolerance_ppm):
            return current
        numerator = observed if total_metric else current * observed
        return max(1, _ceil_div(numerator, target))

    def recommend(self, current: int, metrics: MetricWindow, window: int) -> int:
        if (
            current < 0
            or window < 0
            or metrics.ready_replicas < 0
            or metrics.startup_replicas < 0
            or any(
                value is not None and value < 0
                for value in (metrics.cpu_ppm, metrics.qps_milli, metrics.concurrency)
            )
        ):
            raise ValueError("HPA-like metrics and replica counts must be nonnegative")
        ready = max(0, min(current, metrics.ready_replicas))
        recommendations = [
            self._metric_recommendation(ready, metrics.cpu_ppm, self.target_cpu_ppm),
            self._metric_recommendation(
                ready, metrics.qps_milli, self.target_qps_per_replica_milli, total_metric=True
            ),
            self._metric_recommendation(
                ready, metrics.concurrency, self.target_concurrency_per_replica, total_metric=True
            ),
        ]
        # Missing metrics block scale-in; available metrics may still request scale-out.
        if any(value is None for value in recommendations):
            known = [value for value in recommendations if value is not None]
            desired = max([current, *known]) if known else current
        else:
            desired = max(int(value) for value in recommendations if value is not None)
        if desired > current:
            self._down_recommendations.clear()
            velocity = max(
                self.max_scale_up_pods, _ceil_div(current * self.max_scale_up_percent, 100)
            )
            return min(self.max_replicas, current + velocity, desired)
        if desired < current:
            if metrics.startup_replicas > 0 or ready < current:
                self._down_recommendations.clear()
                return current
            assert self._down_recommendations is not None
            self._down_recommendations.append((window, desired))
            while (
                self._down_recommendations
                and window - self._down_recommendations[0][0]
                >= self.downscale_stabilization_windows
            ):
                self._down_recommendations.popleft()
            stabilized = max(value for _, value in self._down_recommendations)
            velocity = max(1, _ceil_div(current * self.max_scale_down_percent, 100))
            return max(self.min_replicas, current - velocity, stabilized)
        self._down_recommendations.clear()
        return min(self.max_replicas, max(self.min_replicas, desired))


def static_peak_replicas(
    peak_qps_milli: int,
    peak_concurrency: int,
    capacity_qps_milli: int,
    capacity_concurrency: int,
    min_replicas: int,
    reserve_ppm: int = 200_000,
) -> int:
    if capacity_qps_milli <= 0 or capacity_concurrency <= 0 or not 0 <= reserve_ppm < 1_000_000:
        raise ValueError("invalid static baseline capacity")
    demand = max(
        _ceil_div(peak_qps_milli, capacity_qps_milli),
        _ceil_div(peak_concurrency, capacity_concurrency),
        min_replicas,
    )
    return _ceil_div(demand * 1_000_000, 1_000_000 - reserve_ppm)


def lookahead_schedule(
    qps_series: list[int],
    concurrency_series: list[int],
    capacity_qps_milli: int,
    capacity_concurrency: int,
    min_replicas: int,
    max_replicas: int,
    action_delay_windows: int,
    startup_delay_windows: int,
) -> list[int]:
    """Offline reference: sees future demand, but capacity arrives only after modeled delay."""
    if (
        len(qps_series) != len(concurrency_series)
        or action_delay_windows < 0
        or startup_delay_windows < 0
    ):
        raise ValueError("invalid lookahead inputs")
    delay = action_delay_windows + startup_delay_windows
    schedule: list[int] = []
    for index in range(len(qps_series)):
        future = min(len(qps_series) - 1, index + delay)
        required = max(
            min_replicas,
            _ceil_div(qps_series[future], capacity_qps_milli),
            _ceil_div(concurrency_series[future], capacity_concurrency),
        )
        schedule.append(min(max_replicas, required))
    return schedule
