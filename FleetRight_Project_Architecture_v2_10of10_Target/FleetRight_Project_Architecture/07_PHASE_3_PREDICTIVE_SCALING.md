# 7. Phase 3 — Predictive Scaling and Capacity Planning

## Goal

Use forecasting to prepare capacity before demand arrives while keeping destructive scale-in tied to stronger observed evidence.

## Forecast contract

```text
DemandForecast {
    workload_id
    forecast_generated_at
    horizon_ms
    expected_qps
    qps_upper_bound
    expected_concurrency
    concurrency_upper_bound
    model_error_metrics
    calibration_bucket
    confidence
    model_version
    training_window_hash
}
```

The planner uses the **upper demand bound** for safety-sensitive capacity calculation.

## Mandatory models

1. rolling quantile baseline;
2. EWMA/trend model;
3. seasonal/time-of-day baseline.

Optional models may be added only if held-out performance improves on predefined metrics without unacceptable complexity.

## Model selection and calibration

Evaluate on rolling/blocked time-series splits rather than random shuffling.

Measure:

- MAE/WAPE or suitable scale-aware error;
- upper-bound coverage;
- late-capacity events;
- overprovisioning caused by forecast error;
- calibration by confidence bucket.

A forecast that claims 95% upper-bound coverage should be tested for empirical coverage. Persistent undercoverage automatically widens reserve or disables predictive down-risk actions.

## Predictive scale-out

For startup delay `L`:

```text
required_capacity_now = capacity_needed_for(
    demand_upper_bound(t + L + scheduling_buffer)
)
```

Scale out early enough for replicas/nodes to become ready before predicted demand.

## Scale-in rule

Prediction alone must not trigger aggressive capacity removal.

Scale-in requires all of:

- observed demand confirms the decrease;
- downscale stabilization window satisfied;
- telemetry coverage/freshness sufficient;
- capacity profile confident;
- recovery reserve preserved;
- no recent compensation/rollback lock;
- independent verifier pass.

Forecasts may delay scale-in if future demand is expected to return soon.

## Uncertainty-aware reserve

Reserve increases with:

- upper-bound calibration error;
- recent underprediction;
- traffic burstiness;
- slow startup;
- critical priority;
- degraded cluster state.

Reserve reduces only gradually after sustained calibration quality.

## Offline capacity planning

Given a future demand distribution/trace, machine catalog and failure policy, report:

- minimum feasible machine mix;
- expected cost;
- reserve required for configured N-1 failures;
- utilization distribution;
- uncertainty sensitivity.

## Forecast fallback

If forecasting fails, becomes stale or loses calibration quality:

```text
predictive mode -> conservative reactive mode
```

No control-loop liveness depends on the forecast service.

## Phase-3 exit gates

- forecasting benchmark uses held-out temporal evaluation;
- uncertainty bounds have measured empirical coverage;
- predictive mode reduces late scale-outs relative to HPA-like baseline on predictable traces;
- flash-crowd results show reactive fallback behavior;
- prediction is never the sole evidence for destructive scale-in;
- model failure cleanly falls back to reactive mode;
- forecast-version and training-data hashes make decisions replayable;
- cost/SLO tradeoff of predictive vs reactive FleetRight is reported, including cases where predictive mode loses.
