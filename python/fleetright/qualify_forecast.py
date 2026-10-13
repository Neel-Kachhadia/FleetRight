"""Time-ordered forecast evaluation on the existing models (no new model architectures).

Every prediction for window i uses only observations before i; the 95% upper-bound margin for
window i is calibrated on residuals of the previous CALIBRATION windows, so there is no leakage.
Synthetic traces only; see docs/benchmark-methodology.md.
"""

from __future__ import annotations

import argparse
import json
import random
import statistics
from pathlib import Path

from fleetright.forecast import (
    EwmaTrendModel,
    ForecastError,
    Observation,
    RollingQuantileModel,
    SeasonalTimeOfDayModel,
    _quantile,
    evaluate_temporally,
    forecast_decision,
)
from fleetright.policies import EPOCH_MS
from fleetright.qualify import (
    EVAL_BASE_QPS_MILLI,
    EVAL_SEEDS,
    EVENT,
    SCENARIOS,
    WARMUP,
    WINDOW_MS,
    Scenario,
    build_trace,
)

ROOT = Path(__file__).resolve().parents[2]
CALIBRATION = 40
CASES = (
    ("regular: steady", Scenario("F1", "steady", "steady", ("cpu-api",))),
    ("regular: periodic (diurnal)", Scenario("F2", "diurnal", "diurnal", ("cpu-api",))),
    ("sudden spike: flash crowd", Scenario("F3", "flash", "flash_crowd", ("cpu-api",))),
    ("sudden spike: correlated", Scenario("F4", "corr", "correlated_spike", ("cpu-api",))),
    ("trend change: ramp", Scenario("F5", "ramp", "ramp", ("cpu-api",))),
    (
        "trend change: level shift up (+80%)",
        Scenario("F6", "up", "diurnal", ("cpu-api",), shift_permille=1_800),
    ),
    (
        "trend change: level shift down (-50%)",
        Scenario("F7", "down", "diurnal", ("cpu-api",), shift_permille=500),
    ),
    ("noisy: bursty", Scenario("F8", "bursty", "bursty", ("cpu-api",))),
    ("noisy: random-walk tenant", Scenario("F9", "noisy", "noisy_tenant", ("cpu-api",))),
)
MODELS = {"rolling_quantile": RollingQuantileModel, "ewma_trend": EwmaTrendModel}


def _series(scenario: Scenario, seed: int) -> list[Observation]:
    trace = build_trace(scenario, seed, EVAL_BASE_QPS_MILLI)
    points = trace.workloads[0].points
    return [Observation(EPOCH_MS + p.time_ms, p.qps_milli, p.concurrency) for p in points]


def evaluate_series(observations: list[Observation], model, start: int, event: int | None) -> dict:
    """Causal one-window-ahead evaluation over windows [start, len)."""
    predictions: dict[int, float] = {}
    for i in range(6, len(observations)):
        try:
            predictions[i] = model.predict(observations[:i], observations[i].observed_at_ms)[0]
        except ForecastError:
            continue
    errors, under, over, covered, waste, actuals, uppers = [], 0, 0, 0, [], [], {}
    for i in range(start, len(observations)):
        window = [
            max(0.0, observations[j].qps_milli - predictions[j])
            for j in range(max(6, i - CALIBRATION), i)
            if j in predictions
        ]
        if i not in predictions or len(window) < 20:
            continue
        actual, pred = observations[i].qps_milli, predictions[i]
        upper = pred + _quantile(window, 0.95)
        uppers[i] = upper
        errors.append(abs(actual - pred))
        actuals.append(actual)
        under += actual > pred
        over += actual < pred
        covered += actual <= upper
        waste.append(max(0.0, upper - actual) / max(1, actual))
    n = len(errors)
    result = {
        "n": n,
        "mae_qps": statistics.fmean(errors) / 1_000 if n else None,
        "wape": sum(errors) / sum(actuals) if n and sum(actuals) else None,
        "underprediction_frequency": under / n if n else None,
        "overprediction_frequency": over / n if n else None,
        "worst_abs_error_qps": max(errors) / 1_000 if n else None,
        "upper_bound_coverage_95": covered / n if n else None,
        "mean_upper_bound_overhead": statistics.fmean(waste) if n else None,
    }
    if event is not None:
        level = statistics.fmean(o.qps_milli for o in observations[event : event + 3])
        delay = next(
            (
                i - event
                for i in range(event, min(len(observations), event + 15))
                if i in uppers and uppers[i] >= 0.9 * level
            ),
            None,
        )
        result["upper_bound_reaction_delay_windows"] = delay
    return result


def seasonal_hourly(seed: int) -> list[Observation]:
    rng = random.Random(seed)
    shape = (350, 400, 500, 650, 800, 950, 1_000, 900, 750, 600, 450, 380)
    return [
        Observation(
            EPOCH_MS + hour * 3_600_000,
            600_000 * shape[(hour % 24) // 2] * rng.randint(970, 1_030) // 1_000_000,
            30,
        )
        for hour in range(24 * 14)
    ]


def run() -> dict:
    report: dict = {
        "cases": {},
        "seeds": [EVAL_SEEDS[0], EVAL_SEEDS[-1]],
        "calibration": CALIBRATION,
    }
    for label, scenario in CASES:
        event = (
            EVENT
            if scenario.shift_permille and scenario.shift_permille > 1_000
            else WARMUP
            if scenario.family == "flash_crowd"
            else None
        )
        per_model = {}
        for name, factory in MODELS.items():
            runs = [
                evaluate_series(_series(scenario, s), factory(), WARMUP, event) for s in EVAL_SEEDS
            ]
            per_model[name] = {
                key: statistics.fmean(r[key] for r in runs if r.get(key) is not None)
                for key in runs[0]
                if any(r.get(key) is not None for r in runs) and key != "n"
            }
            if event is not None:
                delays = [r["upper_bound_reaction_delay_windows"] for r in runs]
                per_model[name]["upper_bound_reaction_delay_windows"] = delays
        report["cases"][label] = per_model
    season = {}
    for name, factory in {**MODELS, "seasonal_time_of_day": SeasonalTimeOfDayModel}.items():
        runs = [evaluate_series(seasonal_hourly(s), factory(), 24 * 4, None) for s in EVAL_SEEDS]
        season[name] = {
            k: statistics.fmean(r[k] for r in runs if r.get(k) is not None)
            if runs[0][k] is not None
            else None
            for k in runs[0]
        }
    report["seasonal_14_day_hourly"] = season
    # insufficient history and stale measurement handling through the production gate
    sample = _series(SCENARIOS[2], EVAL_SEEDS[0])
    gate = {}
    for label, history in (
        ("30 observations", sample[:30]),
        ("100 observations", sample[:100]),
        ("200 observations", sample[:200]),
    ):
        _, mode, reason = forecast_decision(
            "cpu-api", history, EwmaTrendModel(), history[-1].observed_at_ms, 40_000
        )
        gate[label] = reason or mode
    stale = sample[:200]
    _, mode, reason = forecast_decision(
        "cpu-api", stale, EwmaTrendModel(), stale[-1].observed_at_ms + 10 * WINDOW_MS, 40_000
    )
    gate["200 observations, newest 10 windows old"] = reason or mode
    report["production_gate"] = gate
    folds = {}
    for name, factory in MODELS.items():
        try:
            m = evaluate_temporally(
                _series(SCENARIOS[2], EVAL_SEEDS[0]), factory(), horizon_ms=40_000
            )
            folds[name] = {
                "holdout_samples": m.sample_count,
                "qps_wape": m.qps_wape,
                "empirical_upper_bound_coverage": m.empirical_upper_bound_coverage,
            }
        except ForecastError as error:
            folds[name] = {"error": error.reason}
    report["module_temporal_evaluation_diurnal_seed100"] = folds
    return report


def decision_impact(summary: dict) -> dict:
    """Paired predictive-vs-reactive FleetRight differences taken from summary.json."""
    impact = {}
    for sid, scenario in summary["scenarios"].items():
        row = {}
        for label, key in (
            ("predictive_vs_reactive", "fleetright_predictive__vs__fleetright_reactive"),
            (
                "same_headroom_vs_reactive",
                "fleetright_predictive_same_headroom__vs__fleetright_reactive",
            ),
        ):
            pair = scenario["paired"][key]
            row[label] = {
                metric: {
                    "mean_diff": pair[metric]["mean_diff_fr_minus_other"],
                    "ci95": pair[metric]["ci95"],
                    "significant": pair[metric]["significant"],
                }
                for metric in (
                    "node_cost_micros",
                    "soft_violation_windows",
                    "hard_violation_windows",
                )
            }
        impact[sid] = row
    return impact


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "results/phase3/forecast.json")
    parser.add_argument("--summary", type=Path, help="summary.json for decision-impact table")
    args = parser.parse_args()
    report = run()
    if args.summary:
        report["decision_impact"] = decision_impact(json.loads(args.summary.read_text()))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
