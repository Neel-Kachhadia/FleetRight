from __future__ import annotations

import hashlib
import json
import math
import statistics
from bisect import bisect_right
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from typing import Protocol

SCHEMA_VERSION = 1
MODEL_VERSION = "1"
_HOUR_MS = 60 * 60 * 1000
_MAX_I64 = (1 << 63) - 1


class ForecastError(ValueError):
    def __init__(self, reason: str, detail: str):
        super().__init__(detail)
        self.reason = reason


@dataclass(frozen=True)
class Observation:
    observed_at_ms: int
    qps_milli: int
    concurrency: int


@dataclass(frozen=True)
class DemandForecast:
    schema_version: int
    workload_id: str
    forecast_generated_at_ms: int
    forecast_valid_until_ms: int
    horizon_ms: int
    expected_qps_milli: int
    qps_upper_bound_milli: int
    expected_concurrency: int
    concurrency_upper_bound: int
    model_name: str
    model_version: str
    training_window_start_ms: int
    training_window_end_ms: int
    training_window_sha256: str
    confidence: float
    target_upper_bound_coverage: float
    calibration_sample_count: int
    qps_mae_milli: float
    qps_wape: float | None
    concurrency_mae: float
    concurrency_wape: float | None
    qps_upper_bound_coverage: float
    concurrency_upper_bound_coverage: float
    empirical_upper_bound_coverage: float
    qps_underprediction_p95_milli: float
    concurrency_underprediction_p95: float
    qps_overprediction_cost_milli: float
    concurrency_overprediction_cost: float
    calibration_bucket: str

    def validate(self, now_ms: int | None = None) -> None:
        _require_int("schema_version", self.schema_version, 1)
        if self.schema_version != SCHEMA_VERSION:
            raise ForecastError("FORECAST_MALFORMED", "unsupported forecast schema version")
        if not self.workload_id or not self.model_name or not self.model_version:
            raise ForecastError("FORECAST_MALFORMED", "forecast identity fields are required")
        for name in (
            "forecast_generated_at_ms",
            "forecast_valid_until_ms",
            "horizon_ms",
            "training_window_start_ms",
            "training_window_end_ms",
            "calibration_sample_count",
        ):
            value = _require_int(name, getattr(self, name), 0)
            if value > _MAX_I64:
                raise ForecastError(
                    "FORECAST_MALFORMED", f"{name} exceeds the supported timestamp/count range"
                )
        if self.horizon_ms == 0 or self.forecast_valid_until_ms <= self.forecast_generated_at_ms:
            raise ForecastError(
                "FORECAST_MALFORMED", "forecast timestamps or horizon are impossible"
            )
        if self.forecast_generated_at_ms + self.horizon_ms > self.forecast_valid_until_ms:
            raise ForecastError("FORECAST_MALFORMED", "forecast expires before its horizon")
        if (
            self.training_window_start_ms > self.training_window_end_ms
            or self.training_window_end_ms > self.forecast_generated_at_ms
        ):
            raise ForecastError("FORECAST_MALFORMED", "training window is not chronological")
        if len(self.training_window_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in self.training_window_sha256
        ):
            raise ForecastError("FORECAST_MALFORMED", "training window hash is malformed")
        for name in (
            "expected_qps_milli",
            "qps_upper_bound_milli",
            "expected_concurrency",
            "concurrency_upper_bound",
        ):
            value = _require_int(name, getattr(self, name), 0)
            if value > _MAX_I64:
                raise ForecastError("FORECAST_MALFORMED", f"{name} exceeds signed 64-bit range")
        if (
            self.qps_upper_bound_milli < self.expected_qps_milli
            or self.concurrency_upper_bound < self.expected_concurrency
        ):
            raise ForecastError("FORECAST_MALFORMED", "upper bounds are below expected demand")
        for name in (
            "confidence",
            "target_upper_bound_coverage",
            "qps_mae_milli",
            "concurrency_mae",
            "qps_upper_bound_coverage",
            "concurrency_upper_bound_coverage",
            "empirical_upper_bound_coverage",
            "qps_underprediction_p95_milli",
            "concurrency_underprediction_p95",
            "qps_overprediction_cost_milli",
            "concurrency_overprediction_cost",
        ):
            _require_finite(name, getattr(self, name))
        for name in (
            "confidence",
            "target_upper_bound_coverage",
            "qps_upper_bound_coverage",
            "concurrency_upper_bound_coverage",
            "empirical_upper_bound_coverage",
        ):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ForecastError("FORECAST_MALFORMED", f"{name} must be between 0 and 1")
        for name in (
            "qps_mae_milli",
            "concurrency_mae",
            "qps_underprediction_p95_milli",
            "concurrency_underprediction_p95",
            "qps_overprediction_cost_milli",
            "concurrency_overprediction_cost",
        ):
            if getattr(self, name) < 0:
                raise ForecastError("FORECAST_MALFORMED", f"{name} cannot be negative")
        for name in ("qps_wape", "concurrency_wape"):
            value = getattr(self, name)
            if value is not None and (not math.isfinite(value) or value < 0):
                raise ForecastError("FORECAST_MALFORMED", f"{name} is invalid")
        if not self.calibration_bucket:
            raise ForecastError("FORECAST_MALFORMED", "calibration bucket is required")
        if now_ms is not None:
            _require_int("now_ms", now_ms, 0)
            if self.forecast_generated_at_ms > now_ms:
                raise ForecastError(
                    "FORECAST_MALFORMED", "forecast generation time is in the future"
                )
            if now_ms >= self.forecast_valid_until_ms:
                raise ForecastError("FORECAST_STALE", "forecast has expired")

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class TemporalMetrics:
    sample_count: int
    calibration_sample_count: int
    qps_mae_milli: float
    qps_wape: float | None
    concurrency_mae: float
    concurrency_wape: float | None
    qps_upper_bound_coverage: float
    concurrency_upper_bound_coverage: float
    empirical_upper_bound_coverage: float
    qps_underprediction_p95_milli: float
    concurrency_underprediction_p95: float
    qps_overprediction_cost_milli: float
    concurrency_overprediction_cost: float
    qps_calibration_margin_milli: float
    concurrency_calibration_margin: float


class ForecastModel(Protocol):
    name: str

    def predict(self, history: Sequence[Observation], target_at_ms: int) -> tuple[float, float]: ...


def _require_int(name: str, value: object, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ForecastError("FORECAST_MALFORMED", f"{name} must be an integer >= {minimum}")
    return value


def _require_finite(name: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ForecastError("FORECAST_MALFORMED", f"{name} must be finite")
    return float(value)


def _demand_value(value: object) -> float:
    demand = _require_finite("model_demand", value)
    if demand < 0:
        raise ForecastError("FORECAST_MALFORMED", "model demand cannot be negative")
    return demand


def validate_observations(observations: Sequence[Observation], generated_at_ms: int) -> None:
    _require_int("forecast_generated_at_ms", generated_at_ms, 0)
    if generated_at_ms > _MAX_I64:
        raise ForecastError("FORECAST_MALFORMED", "forecast timestamp exceeds signed 64-bit range")
    previous = -1
    for item in observations:
        _require_int("observed_at_ms", item.observed_at_ms, 0)
        if item.observed_at_ms > _MAX_I64:
            raise ForecastError(
                "FORECAST_MALFORMED", "observation timestamp exceeds signed 64-bit range"
            )
        _require_int("qps_milli", item.qps_milli, 0)
        _require_int("concurrency", item.concurrency, 0)
        if item.observed_at_ms <= previous or item.observed_at_ms > generated_at_ms:
            raise ForecastError(
                "FORECAST_MALFORMED",
                "observations must be strictly chronological and not future dated",
            )
        previous = item.observed_at_ms


def _quantile(values: Sequence[float], probability: float) -> float:
    if not values:
        raise ForecastError("FORECAST_INSUFFICIENT_HISTORY", "no samples for quantile")
    ordered = sorted(values)
    index = min(len(ordered) - 1, math.ceil((len(ordered) + 1) * probability) - 1)
    return ordered[max(0, index)]


class RollingQuantileModel:
    name = "rolling_quantile"

    def predict(self, history: Sequence[Observation], target_at_ms: int) -> tuple[float, float]:
        if len(history) < 4:
            raise ForecastError(
                "FORECAST_INSUFFICIENT_HISTORY", "rolling quantile needs four samples"
            )
        recent = history[-60:]
        return (
            _quantile([float(item.qps_milli) for item in recent], 0.5),
            _quantile([float(item.concurrency) for item in recent], 0.5),
        )


class EwmaTrendModel:
    name = "ewma_trend"

    def __init__(self, alpha: float = 0.3):
        if not 0 < alpha <= 1:
            raise ValueError("alpha must be in (0, 1]")
        self.alpha = alpha

    def _predict(self, values: Sequence[int], steps: float) -> float:
        if len(values) < 6:
            raise ForecastError("FORECAST_INSUFFICIENT_HISTORY", "EWMA trend needs six samples")
        split = max(2, len(values) * 3 // 4)

        def ewma(items: Sequence[int]) -> float:
            result = float(items[0])
            for value in items[1:]:
                result = self.alpha * value + (1 - self.alpha) * result
            return result

        prior = ewma(values[:split])
        current = ewma(values[split:])
        trend_per_sample = (current - prior) / max(1, len(values) - split)
        return max(0.0, ewma(values) + max(0.0, trend_per_sample) * steps)

    def predict(self, history: Sequence[Observation], target_at_ms: int) -> tuple[float, float]:
        if len(history) < 6:
            raise ForecastError("FORECAST_INSUFFICIENT_HISTORY", "EWMA trend needs six samples")
        gaps = [
            b.observed_at_ms - a.observed_at_ms for a, b in zip(history, history[1:], strict=False)
        ]
        cadence_ms = max(1, int(statistics.median(gaps)))
        steps = max(1.0, (target_at_ms - history[-1].observed_at_ms) / cadence_ms)
        return (
            self._predict([item.qps_milli for item in history], steps),
            self._predict([item.concurrency for item in history], steps),
        )


class SeasonalTimeOfDayModel:
    name = "seasonal_time_of_day"

    def predict(self, history: Sequence[Observation], target_at_ms: int) -> tuple[float, float]:
        hourly_buckets = {item.observed_at_ms // _HOUR_MS for item in history}
        if len(hourly_buckets) < 48:
            raise ForecastError(
                "FORECAST_INSUFFICIENT_HISTORY",
                "seasonal model needs observations across 48 distinct hours",
            )
        slot = (target_at_ms // _HOUR_MS) % 24
        target_day = target_at_ms // (24 * _HOUR_MS)
        daily: dict[int, list[Observation]] = {}
        for item in history:
            day = item.observed_at_ms // (24 * _HOUR_MS)
            if day < target_day and (item.observed_at_ms // _HOUR_MS) % 24 == slot:
                daily.setdefault(day, []).append(item)
        if len(daily) < 2:
            raise ForecastError(
                "FORECAST_INSUFFICIENT_HISTORY", "seasonal slot needs two prior daily cycles"
            )
        cycles = [daily[day] for day in sorted(daily)[-8:]]
        return (
            _quantile([statistics.median(item.qps_milli for item in day) for day in cycles], 0.5),
            _quantile([statistics.median(item.concurrency for item in day) for day in cycles], 0.5),
        )


def _wape(errors: Sequence[float], actuals: Sequence[int]) -> float | None:
    denominator = sum(actuals)
    return sum(errors) / denominator if denominator else None


def _temporal_predictions(
    observations: Sequence[Observation],
    model: ForecastModel,
    start: int,
    end: int,
    horizon_ms: int,
) -> list[tuple[Observation, float, float]]:
    predictions: list[tuple[Observation, float, float]] = []
    timestamps = [item.observed_at_ms for item in observations]
    for index in range(start, end):
        actual = observations[index]
        origin_index = bisect_right(timestamps, actual.observed_at_ms - horizon_ms, 0, index) - 1
        if origin_index < 0:
            continue
        try:
            qps, concurrency = model.predict(
                observations[: origin_index + 1], actual.observed_at_ms
            )
        except ForecastError as error:
            if error.reason == "FORECAST_INSUFFICIENT_HISTORY":
                continue
            raise
        predictions.append((actual, _demand_value(qps), _demand_value(concurrency)))
    return predictions


def evaluate_temporally(
    observations: Sequence[Observation],
    model: ForecastModel,
    target_coverage: float = 0.95,
    minimum_calibration_samples: int = 20,
    minimum_holdout_samples: int = 20,
    horizon_ms: int | None = None,
) -> TemporalMetrics:
    """Use fixed chronological train/calibration/holdout windows; never shuffle time series."""
    if not 0 < target_coverage < 1:
        raise ValueError("target_coverage must be in (0, 1)")
    if observations:
        validate_observations(observations, observations[-1].observed_at_ms)
    if horizon_ms is None:
        gaps = [
            later.observed_at_ms - earlier.observed_at_ms
            for earlier, later in zip(observations, observations[1:], strict=False)
        ]
        horizon_ms = int(statistics.median(gaps)) if gaps else 1
    if isinstance(horizon_ms, bool) or not isinstance(horizon_ms, int) or horizon_ms <= 0:
        raise ValueError("horizon_ms must be a positive integer")
    n = len(observations)
    calibration_start = n * 3 // 5
    holdout_start = n * 4 // 5
    calibration = _temporal_predictions(
        observations, model, calibration_start, holdout_start, horizon_ms
    )
    holdout = _temporal_predictions(observations, model, holdout_start, n, horizon_ms)
    if len(calibration) < minimum_calibration_samples or len(holdout) < minimum_holdout_samples:
        raise ForecastError(
            "FORECAST_INSUFFICIENT_HISTORY", "temporal calibration or holdout window is too small"
        )
    qps_residuals = [max(0.0, actual.qps_milli - predicted) for actual, predicted, _ in calibration]
    concurrency_residuals = [
        max(0.0, actual.concurrency - predicted) for actual, _, predicted in calibration
    ]
    qps_margin = _quantile(qps_residuals, target_coverage)
    concurrency_margin = _quantile(concurrency_residuals, target_coverage)
    qps_errors = [abs(actual.qps_milli - predicted) for actual, predicted, _ in holdout]
    concurrency_errors = [abs(actual.concurrency - predicted) for actual, _, predicted in holdout]
    qps_actuals = [actual.qps_milli for actual, _, _ in holdout]
    concurrency_actuals = [actual.concurrency for actual, _, _ in holdout]
    qps_under = [max(0.0, actual.qps_milli - predicted) for actual, predicted, _ in holdout]
    concurrency_under = [
        max(0.0, actual.concurrency - predicted) for actual, _, predicted in holdout
    ]
    qps_covered = [actual.qps_milli <= predicted + qps_margin for actual, predicted, _ in holdout]
    concurrency_covered = [
        actual.concurrency <= predicted + concurrency_margin for actual, _, predicted in holdout
    ]
    count = len(holdout)
    qps_coverage = sum(qps_covered) / count
    concurrency_coverage = sum(concurrency_covered) / count
    joint_coverage = (
        sum(a and b for a, b in zip(qps_covered, concurrency_covered, strict=True)) / count
    )
    return TemporalMetrics(
        sample_count=count,
        calibration_sample_count=len(calibration),
        qps_mae_milli=statistics.mean(qps_errors),
        qps_wape=_wape(qps_errors, qps_actuals),
        concurrency_mae=statistics.mean(concurrency_errors),
        concurrency_wape=_wape(concurrency_errors, concurrency_actuals),
        qps_upper_bound_coverage=qps_coverage,
        concurrency_upper_bound_coverage=concurrency_coverage,
        empirical_upper_bound_coverage=joint_coverage,
        qps_underprediction_p95_milli=_quantile(qps_under, 0.95),
        concurrency_underprediction_p95=_quantile(concurrency_under, 0.95),
        qps_overprediction_cost_milli=statistics.mean(
            max(0.0, predicted + qps_margin - actual.qps_milli) for actual, predicted, _ in holdout
        ),
        concurrency_overprediction_cost=statistics.mean(
            max(0.0, predicted + concurrency_margin - actual.concurrency)
            for actual, _, predicted in holdout
        ),
        qps_calibration_margin_milli=qps_margin,
        concurrency_calibration_margin=concurrency_margin,
    )


def forecast_demand(
    workload_id: str,
    observations: Sequence[Observation],
    model: ForecastModel,
    generated_at_ms: int,
    horizon_ms: int,
    target_coverage: float = 0.95,
    minimum_calibration_samples: int = 20,
    minimum_holdout_samples: int = 20,
) -> DemandForecast:
    validate_observations(observations, generated_at_ms)
    if (
        not workload_id
        or not isinstance(horizon_ms, int)
        or isinstance(horizon_ms, bool)
        or horizon_ms <= 0
        or not observations
    ):
        raise ForecastError(
            "FORECAST_INSUFFICIENT_HISTORY", "workload, horizon and history are required"
        )
    if generated_at_ms + horizon_ms > _MAX_I64:
        raise ForecastError("FORECAST_MALFORMED", "forecast horizon overflows the timestamp unit")
    metrics = evaluate_temporally(
        observations,
        model,
        target_coverage,
        minimum_calibration_samples,
        minimum_holdout_samples,
        horizon_ms,
    )
    target_at_ms = generated_at_ms + horizon_ms
    expected_qps, expected_concurrency = model.predict(observations, target_at_ms)
    expected_qps = _demand_value(expected_qps)
    expected_concurrency = _demand_value(expected_concurrency)
    canonical = json.dumps(
        [asdict(item) for item in observations], sort_keys=True, separators=(",", ":")
    ).encode()
    forecast = DemandForecast(
        schema_version=SCHEMA_VERSION,
        workload_id=workload_id,
        forecast_generated_at_ms=generated_at_ms,
        forecast_valid_until_ms=generated_at_ms + horizon_ms,
        horizon_ms=horizon_ms,
        expected_qps_milli=math.ceil(expected_qps),
        qps_upper_bound_milli=math.ceil(expected_qps + metrics.qps_calibration_margin_milli),
        expected_concurrency=math.ceil(expected_concurrency),
        concurrency_upper_bound=math.ceil(
            expected_concurrency + metrics.concurrency_calibration_margin
        ),
        model_name=model.name,
        model_version=MODEL_VERSION,
        training_window_start_ms=observations[0].observed_at_ms,
        training_window_end_ms=observations[-1].observed_at_ms,
        training_window_sha256=hashlib.sha256(canonical).hexdigest(),
        confidence=metrics.empirical_upper_bound_coverage,
        target_upper_bound_coverage=target_coverage,
        calibration_sample_count=metrics.calibration_sample_count,
        qps_mae_milli=metrics.qps_mae_milli,
        qps_wape=metrics.qps_wape,
        concurrency_mae=metrics.concurrency_mae,
        concurrency_wape=metrics.concurrency_wape,
        qps_upper_bound_coverage=metrics.qps_upper_bound_coverage,
        concurrency_upper_bound_coverage=metrics.concurrency_upper_bound_coverage,
        empirical_upper_bound_coverage=metrics.empirical_upper_bound_coverage,
        qps_underprediction_p95_milli=metrics.qps_underprediction_p95_milli,
        concurrency_underprediction_p95=metrics.concurrency_underprediction_p95,
        qps_overprediction_cost_milli=metrics.qps_overprediction_cost_milli,
        concurrency_overprediction_cost=metrics.concurrency_overprediction_cost,
        calibration_bucket=f"{workload_id}:{model.name}:{horizon_ms}ms:utc-hourly-v1",
    )
    forecast.validate()
    return forecast


def forecast_decision(
    workload_id: str,
    observations: Sequence[Observation],
    model: ForecastModel,
    generated_at_ms: int,
    horizon_ms: int,
    minimum_confidence: float = 0.95,
    target_coverage: float = 0.95,
    maximum_observation_age_ms: int | None = None,
) -> tuple[DemandForecast | None, str, str | None]:
    """Return reactive fallback metadata instead of failing the planning request."""
    try:
        if observations:
            validate_observations(observations, generated_at_ms)
            if maximum_observation_age_ms is None:
                gaps = [
                    later.observed_at_ms - earlier.observed_at_ms
                    for earlier, later in zip(observations, observations[1:], strict=False)
                ]
                maximum_age = max(
                    60_000,
                    2 * int(statistics.median(gaps)) if gaps else 60_000,
                )
            else:
                maximum_age = _require_int(
                    "maximum_observation_age_ms", maximum_observation_age_ms, 1
                )
            if generated_at_ms - observations[-1].observed_at_ms > maximum_age:
                return None, "REACTIVE", "FORECAST_STALE"
        forecast = forecast_demand(
            workload_id, observations, model, generated_at_ms, horizon_ms, target_coverage
        )
        forecast.validate(generated_at_ms)
        if forecast.confidence < minimum_confidence:
            return None, "REACTIVE", "FORECAST_LOW_CONFIDENCE"
        if forecast.empirical_upper_bound_coverage < target_coverage:
            return None, "REACTIVE", "FORECAST_UNCALIBRATED"
        return forecast, "PREDICTIVE", None
    except ForecastError as error:
        return None, "REACTIVE", error.reason
    except Exception:
        return None, "REACTIVE", "FORECAST_ERROR"


@dataclass(frozen=True)
class ScaleInEvidence:
    samples: Sequence[Observation]
    reference_qps_milli: int
    reference_concurrency: int
    telemetry_coverage: float
    required_stabilization_ms: int
    minimum_sample_count: int
    minimum_coverage: float
    telemetry_max_age_ms: int
    profile_valid_until_ms: int
    recent_bad_health: bool
    recovery_reserve_replicas: int
    required_recovery_reserve: int


def scale_in_eligibility(
    evidence: ScaleInEvidence | None, now_ms: int
) -> tuple[bool, tuple[str, ...]]:
    if evidence is None:
        return False, ("OBSERVED_DOWNSCALE_STABILIZATION_NOT_MET",)
    reasons: list[str] = []
    try:
        _require_int("now_ms", now_ms, 0)
        validate_observations(evidence.samples, now_ms)
        _require_int("reference_qps_milli", evidence.reference_qps_milli, 1)
        _require_int("reference_concurrency", evidence.reference_concurrency, 1)
        _require_finite("telemetry_coverage", evidence.telemetry_coverage)
        _require_finite("minimum_coverage", evidence.minimum_coverage)
        _require_int("required_stabilization_ms", evidence.required_stabilization_ms, 0)
        _require_int("minimum_sample_count", evidence.minimum_sample_count, 1)
        _require_int("telemetry_max_age_ms", evidence.telemetry_max_age_ms, 1)
        _require_int("profile_valid_until_ms", evidence.profile_valid_until_ms, 0)
        _require_int("recovery_reserve_replicas", evidence.recovery_reserve_replicas, 0)
        _require_int("required_recovery_reserve", evidence.required_recovery_reserve, 0)
    except ForecastError:
        return False, ("OBSERVED_DOWNSCALE_STABILIZATION_NOT_MET",)
    if not evidence.samples:
        return False, ("OBSERVED_DOWNSCALE_STABILIZATION_NOT_MET",)
    if not 0 <= evidence.telemetry_coverage <= 1 or not 0 < evidence.minimum_coverage <= 1:
        return False, ("TELEMETRY_COVERAGE_INSUFFICIENT",)
    if len(evidence.samples) < evidence.minimum_sample_count:
        reasons.append("OBSERVED_DOWNSCALE_STABILIZATION_NOT_MET")
    if len(evidence.samples) < 2 or (
        evidence.samples[-1].observed_at_ms - evidence.samples[0].observed_at_ms
        < evidence.required_stabilization_ms
    ):
        reasons.append("OBSERVED_DOWNSCALE_STABILIZATION_NOT_MET")
    if now_ms - evidence.samples[-1].observed_at_ms > evidence.telemetry_max_age_ms:
        reasons.append("TELEMETRY_STALE")
    if evidence.telemetry_coverage < evidence.minimum_coverage:
        reasons.append("TELEMETRY_COVERAGE_INSUFFICIENT")
    if evidence.recent_bad_health:
        reasons.append("RECENT_BAD_HEALTH")
    if evidence.profile_valid_until_ms <= now_ms:
        reasons.append("CAPACITY_PROFILE_INVALID")
    if evidence.recovery_reserve_replicas < evidence.required_recovery_reserve:
        reasons.append("RECOVERY_RESERVE_INSUFFICIENT")
    if evidence.samples and (
        max(item.qps_milli for item in evidence.samples) * 10 > evidence.reference_qps_milli * 7
        or max(item.concurrency for item in evidence.samples) * 10
        > evidence.reference_concurrency * 7
    ):
        reasons.append("OBSERVED_DEMAND_REDUCTION_NOT_SUSTAINED")
    return not reasons, tuple(dict.fromkeys(reasons))
