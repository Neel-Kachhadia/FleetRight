from __future__ import annotations

from dataclasses import replace

import pytest
from fleetright.forecast import (
    EwmaTrendModel,
    ForecastError,
    Observation,
    RollingQuantileModel,
    ScaleInEvidence,
    SeasonalTimeOfDayModel,
    evaluate_temporally,
    forecast_decision,
    forecast_demand,
    scale_in_eligibility,
)


def history(count: int = 120, *, qps: int = 1000) -> list[Observation]:
    return [Observation(index * 3_600_000, qps, 10) for index in range(count)]


@pytest.mark.parametrize(
    "model", [RollingQuantileModel(), EwmaTrendModel(), SeasonalTimeOfDayModel()]
)
def test_models_are_deterministic_and_forecast_contract_is_valid(model) -> None:
    samples = history()
    now = samples[-1].observed_at_ms + 1000
    first = forecast_demand("w", samples, model, now, 30_000)
    second = forecast_demand("w", samples, model, now, 30_000)
    assert first == second
    first.validate(now)
    assert first.expected_qps_milli == 1000
    assert first.qps_upper_bound_milli >= first.expected_qps_milli
    assert first.empirical_upper_bound_coverage == 1
    assert first.qps_mae_milli == 0


def test_temporal_validation_uses_only_past_observations() -> None:
    samples = history(120)
    horizon_ms = 3 * 3_600_000

    class SpyModel:
        name = "spy"

        def predict(self, past, target_at_ms):
            assert all(sample.observed_at_ms < target_at_ms for sample in past)
            assert target_at_ms - past[-1].observed_at_ms >= horizon_ms
            return 1000, 10

    metrics = evaluate_temporally(samples, SpyModel(), horizon_ms=horizon_ms)
    assert metrics.calibration_sample_count == 24
    assert metrics.sample_count == 24
    assert metrics.empirical_upper_bound_coverage == 1


def test_seasonal_model_aggregates_matching_hours_by_prior_day() -> None:
    samples: list[Observation] = []
    for hour in range(120):
        if hour % 24 == 9:
            day = hour // 24
            qps, concurrency = (day + 1) * 100, (day + 1) * 10
        else:
            qps, concurrency = 5, 1
        timestamp = hour * 3_600_000
        samples.append(Observation(timestamp, qps, concurrency))
        if hour == 105:
            samples.extend(
                Observation(timestamp + minute * 60_000, 1000, 100) for minute in range(1, 60)
            )

    qps, concurrency = SeasonalTimeOfDayModel().predict(samples, 129 * 3_600_000)
    assert (qps, concurrency) == (300, 30)


def test_insufficient_stale_malformed_and_undercovered_forecasts_fail_closed() -> None:
    forecast, status, reason = forecast_decision(
        "w", history(10), RollingQuantileModel(), history(10)[-1].observed_at_ms + 1000, 10_000
    )
    assert forecast is None and status == "REACTIVE"
    assert reason == "FORECAST_INSUFFICIENT_HISTORY"

    minute_history = [Observation(index * 60_000, 1000, 10) for index in range(120)]
    forecast, status, reason = forecast_decision(
        "w",
        minute_history,
        SeasonalTimeOfDayModel(),
        minute_history[-1].observed_at_ms + 1000,
        30_000,
    )
    assert forecast is None and status == "REACTIVE"
    assert reason == "FORECAST_INSUFFICIENT_HISTORY"

    valid = forecast_demand("w", history(), RollingQuantileModel(), 500_000_000, 1000)
    with pytest.raises(ForecastError, match="expired"):
        valid.validate(valid.forecast_valid_until_ms + 1)
    with pytest.raises(ForecastError, match="finite"):
        replace(valid, confidence=float("nan")).validate()
    with pytest.raises(ForecastError, match="upper bounds"):
        replace(valid, qps_upper_bound_milli=valid.expected_qps_milli - 1).validate()

    spiky = [Observation(i * 3_600_000, 1000 if i < 96 else 100_000, 10) for i in range(120)]
    metrics = evaluate_temporally(spiky, RollingQuantileModel())
    assert metrics.empirical_upper_bound_coverage == 0
    forecast, status, reason = forecast_decision(
        "w", spiky, RollingQuantileModel(), spiky[-1].observed_at_ms + 1, 10_000
    )
    assert forecast is None and status == "REACTIVE"
    assert reason in {"FORECAST_LOW_CONFIDENCE", "FORECAST_UNCALIBRATED"}


def test_model_exception_falls_back_to_reactive_planning() -> None:
    class BrokenModel:
        name = "broken"

        def predict(self, past, target_at_ms):
            raise TimeoutError("forecast provider timed out")

    forecast, status, reason = forecast_decision(
        "w", history(), BrokenModel(), history()[-1].observed_at_ms + 1, 1000
    )
    assert forecast is None and status == "REACTIVE" and reason == "FORECAST_ERROR"


def test_old_observation_window_falls_back_as_stale() -> None:
    samples = history()
    now = samples[-1].observed_at_ms + 3 * 60 * 60 * 1000
    forecast, status, reason = forecast_decision("w", samples, RollingQuantileModel(), now, 30_000)
    assert forecast is None and status == "REACTIVE" and reason == "FORECAST_STALE"


def test_prediction_alone_cannot_authorize_scale_in() -> None:
    eligible, reasons = scale_in_eligibility(None, 1_000_000)
    assert not eligible
    assert reasons == ("OBSERVED_DOWNSCALE_STABILIZATION_NOT_MET",)

    samples = [Observation(i * 60_000, 100, 5) for i in range(10)]
    evidence = ScaleInEvidence(
        samples=samples,
        reference_qps_milli=1000,
        reference_concurrency=50,
        telemetry_coverage=0.99,
        required_stabilization_ms=5 * 60_000,
        minimum_sample_count=5,
        minimum_coverage=0.95,
        telemetry_max_age_ms=60_000,
        profile_valid_until_ms=2_000_000,
        recent_bad_health=False,
        recovery_reserve_replicas=2,
        required_recovery_reserve=1,
    )
    assert scale_in_eligibility(evidence, 10 * 60_000)[0]
    assert not scale_in_eligibility(evidence, 10 * 60_000 + 60_001)[0]
    empty_evidence = replace(evidence, samples=[])
    eligible, reasons = scale_in_eligibility(empty_evidence, 10 * 60_000)
    assert not eligible
    assert reasons == ("OBSERVED_DOWNSCALE_STABILIZATION_NOT_MET",)
    expired_profile = replace(evidence, profile_valid_until_ms=10 * 60_000)
    eligible, reasons = scale_in_eligibility(expired_profile, 10 * 60_000)
    assert not eligible
    assert "CAPACITY_PROFILE_INVALID" in reasons
