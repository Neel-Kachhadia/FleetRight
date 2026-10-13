"""FleetRight sizing policies for the paired simulator.

The sizing rule mirrors the C++ planner's `CandidateFor`/`RequiredReplicas` (profile-derived
effective capacity, availability floor, +1 for N-1); a test checks it against the real planner.
Scale-in is gated by the production `scale_in_eligibility` rule. Only information the baselines
also receive (qps, concurrency, cpu, ready/starting replicas) is used.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from fleetright.baselines import MetricWindow
from fleetright.forecast import (
    EwmaTrendModel,
    Observation,
    ScaleInEvidence,
    forecast_decision,
    scale_in_eligibility,
)

EPOCH_MS = 1_800_000_000_000  # hour-aligned, matches scenario.demo_snapshot_fields


def _ceil_div(numerator: int, denominator: int) -> int:
    return numerator // denominator + (numerator % denominator != 0)


def required_replicas(
    qps_milli: int,
    concurrency: int,
    effective_qps_milli: int,
    effective_concurrency: int,
    min_replicas: int,
    availability_floor: int,
    max_replicas: int,
    n_minus_1: bool,
) -> int:
    """Planner sizing rule for one workload; the result is clamped to max_replicas."""
    needed = max(
        _ceil_div(qps_milli, effective_qps_milli),
        _ceil_div(concurrency, effective_concurrency),
        min_replicas,
        availability_floor,
    )
    return min(max_replicas, needed + (1 if n_minus_1 else 0))


@dataclass
class FleetRightPolicy:
    effective_qps_milli: int
    effective_concurrency: int
    min_replicas: int
    max_replicas: int
    availability_floor: int
    n_minus_1: bool
    max_scale_down_ppm: int
    window_ms: int
    lead_ms: int  # actuation + node activation + startup + readiness
    target_utilization_ppm: int = 1_000_000
    stabilization_windows: int = 5
    predictive: bool = False
    history_limit: int = 120
    model: EwmaTrendModel = field(default_factory=EwmaTrendModel)
    history: list[Observation] = field(default_factory=list)
    saturated: deque[bool] = field(default_factory=deque)
    present: deque[bool] = field(default_factory=deque)
    forecast_modes: dict[str, int] = field(default_factory=dict)

    def _target(self, qps: int, concurrency: int) -> int:
        scale = self.target_utilization_ppm
        return required_replicas(
            qps,
            concurrency,
            max(1, self.effective_qps_milli * scale // 1_000_000),
            max(1, self.effective_concurrency * scale // 1_000_000),
            self.min_replicas,
            self.availability_floor,
            self.max_replicas,
            self.n_minus_1,
        )

    def recommend(self, current: int, metrics: MetricWindow, window: int) -> int:
        self.present.append(metrics.qps_milli is not None)
        self.saturated.append(metrics.cpu_ppm is not None and metrics.cpu_ppm >= 1_000_000)
        for series in (self.present, self.saturated):
            while len(series) > self.stabilization_windows:
                series.popleft()
        if metrics.qps_milli is None or metrics.concurrency is None:
            return current  # no evidence: hold, never act blind
        now_ms = EPOCH_MS + window * self.window_ms
        self.history.append(Observation(now_ms, metrics.qps_milli, metrics.concurrency))
        del self.history[: -self.history_limit]
        qps, concurrency = metrics.qps_milli, metrics.concurrency
        if self.predictive:
            forecast, mode, reason = forecast_decision(
                "workload", self.history, self.model, now_ms, self.lead_ms
            )
            key = mode if forecast else f"{mode}:{reason}"
            self.forecast_modes[key] = self.forecast_modes.get(key, 0) + 1
            if forecast:  # conservative: the upper bound can only add capacity
                qps = max(qps, forecast.qps_upper_bound_milli)
                concurrency = max(concurrency, forecast.concurrency_upper_bound)
        target = self._target(qps, concurrency)
        if target >= current:
            return target
        evidence = ScaleInEvidence(
            samples=self.history[-self.stabilization_windows :],
            reference_qps_milli=max(1, current * self.effective_qps_milli),
            reference_concurrency=max(1, current * self.effective_concurrency),
            telemetry_coverage=sum(self.present) / max(1, len(self.present)),
            required_stabilization_ms=(self.stabilization_windows - 1) * self.window_ms,
            minimum_sample_count=self.stabilization_windows,
            minimum_coverage=0.8,
            telemetry_max_age_ms=2 * self.window_ms,
            profile_valid_until_ms=EPOCH_MS * 2,
            recent_bad_health=any(self.saturated),
            recovery_reserve_replicas=0,
            required_recovery_reserve=0,
        )
        eligible, _ = scale_in_eligibility(evidence, now_ms)
        if not eligible:
            return current
        step = max(1, _ceil_div(current * self.max_scale_down_ppm, 1_000_000))
        return max(target, current - step)
