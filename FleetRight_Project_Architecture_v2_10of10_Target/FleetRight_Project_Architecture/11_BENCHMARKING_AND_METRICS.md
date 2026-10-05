# 11. Benchmarking and Metrics

## Benchmark principle

FleetRight must beat credible alternatives under **equivalent safety requirements**, not win against an intentionally weak baseline.

## Compared policies

| Policy | Purpose |
|---|---|
| Static peak + reserve | conservative cost baseline |
| Naive threshold | educational reference |
| HPA-like target tracker | serious reactive baseline |
| FleetRight reactive | optimization benefit without forecast |
| FleetRight predictive | incremental forecast benefit |
| Offline lookahead/oracle | lower-bound/reference with future knowledge |

## Primary outcome metrics

### Cost

- total node-hours;
- simulated cost;
- cost per million successful requests;
- idle CPU-hours;
- idle GiB-hours;
- consolidation savings;
- migration/actuation cost proxy.

### Reliability / SLO

- aggregate p50/p95/p99 latency from histograms;
- fraction of windows over latency SLO;
- error/reject rate;
- dropped requests;
- queue growth duration;
- time to recover after node loss;
- time to recover after harmful capacity reduction;
- minutes without N-1 reserve for workloads that require it.

### Control quality

- scale-outs/hour;
- scale-ins/hour;
- migrations/hour;
- node drains/hour;
- oscillation count;
- stale-plan count;
- rejected-plan count by reason;
- compensation count;
- late scale-out events;
- excess-capacity area over time;
- time spent near configured safety boundary.

### Forecast quality

- MAE/WAPE;
- upper-bound empirical coverage;
- underprediction severity;
- overprediction capacity cost;
- confidence calibration.

### Optimizer quality

- planner solve time;
- verifier time;
- peak memory;
- candidate actions explored;
- objective improvement;
- small-instance optimality gap versus exact oracle.

## Headline report table

```text
Policy | Cost | SLO-window violation % | p99 | Late scale-outs |
       | Actions | Idle CPUh | Recovery p95 | N-1 deficit min
```

Do not compress reliability into a single cost-adjusted score for headline claims.

## Fairness rules

All compared policies must use:

- identical workload trace;
- identical startup/actuation delays;
- identical machine catalog;
- identical min/max replicas;
- identical hard availability constraints;
- comparable telemetry availability;
- same measured interval after warmup.

If a policy cannot express a FleetRight-specific feature, state that limitation rather than silently weakening FleetRight constraints.

## Statistical protocol

For stochastic/chaos scenarios:

- predefine seed count before viewing final results;
- use at least enough seeds to show dispersion meaningfully;
- use the **same seeds/traces across policies** so policy comparisons are paired;
- report median plus IQR and/or confidence interval as appropriate;
- report paired absolute and percentage deltas, not only independent summaries;
- preserve every run;
- report failures/outliers rather than deleting them;
- separate warmup from measured interval;
- do not select the best run.

For threshold-form SLOs, report both quantiles and the fraction of requests/windows satisfying the threshold. A p99 point estimate without request count/sample evidence is insufficient.

For deterministic trace experiments, repeat real-system runs to quantify runtime/environment noise.

## Regression thresholds

CI benchmark smoke tests are not scientific headline runs. Use loose thresholds to catch major regressions. Release benchmarks run on an identified reference host with minimized background load and preserved host metadata.

## Reproducibility manifest

Every run includes:

```text
git_commit
optimizer_build_id
verifier_build_id
scenario_schema_version
scenario_id
random_seed
trace_hash
snapshot_schema_version
policy_version + hash
capacity_profile_set_version + hashes
machine_catalog_version + hash
forecast_model_version
container image digests
host CPU/RAM/kernel
start/end wall-clock timestamps
monotonic measured duration
```

## Strong claim format

Prefer claims like:

> FleetRight predictive reduced median simulated cost by X% versus the HPA-like baseline across Y predefined traces while meeting the same latency/error constraints; its median cost was Z% above the offline lookahead reference.

This is much stronger than “saved X% versus a threshold autoscaler.”

## Resume metric rule

A number can appear on a resume only if:

1. the benchmark command is versioned;
2. raw evidence is preserved or reproducible;
3. the baseline is fully defined;
4. traces/configs are identical where comparison requires it;
5. the claim survives repeated runs;
6. the exact commit/tag containing the result is identifiable;
7. limitations are documented.
