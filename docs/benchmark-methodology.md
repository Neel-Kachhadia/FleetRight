# Benchmark methodology

`scripts/run_demo.sh` creates an append-only `results/phase1/<run-id>/` artifact containing seeded
traces, synthetic profile fixtures, raw simulator windows, static/threshold/HPA-like/lookahead
outputs, protobuf snapshot/plan/verifier artifacts, deterministic replay hashes, and the bounded
exact-oracle comparison. It labels all capacity inputs as synthetic. Simulator replica-cost proxies
are reported separately from node-hour optimizer cost.

`scripts/run_benchmarks.sh` generates the declared 1,000-workload/500-node synthetic snapshot with
one additional eligible shape for a workload. This exercises the bounded full-plan profile
comparison. It measures 30 direct C++ planner/verifier calls after loading protobuf inputs once, and separately
measures 30 end-to-end CLI runs including process startup and file I/O. The 250 ms planner and 25 ms
verifier targets apply to the direct API measurements; CLI p95 is retained as an end-to-end view.
Every output is checked for deterministic replay and independent verifier acceptance. The manifest
records commit, build IDs, compiler/build type, host/kernel/CPU/RAM, schema, policy/profile/catalog
hashes, and raw direct/CLI timings. Runs do not overwrite prior run IDs.

The final timeboxed 1,000-workload/500-node run is
`results/phase2/20261005T140756.643058Z-1000w-500n/` and contains 20 direct API and CLI samples.
On GNU 13.3.0 Release, planner API p95 was 313.986794 ms against the 250 ms target; verifier API
p95 was 16.996607 ms against the 25 ms target. Deterministic replay passed and the independent
verifier accepted the plan. The planner target miss is retained as a performance limitation; the
CLI p95 values include process start and file I/O and are reported separately.

The exact oracle exhaustively enumerates eligible node subsets for a single workload on at most 12
nodes. It uses the same compatible profile point, effective capacity, resource shape, headroom,
catalog price, and anti-affinity rules. Its bounded comparison uses a one-hour horizon and zero
movement/churn/uncertainty/boundary weights, so the exact and heuristic objective both reduce to
infrastructure micros/hour. It deliberately rejects N-1 and topology cases it does not model. The
reported gap is heuristic objective relative to the exact optimum; a heuristic result below the
oracle is treated as a model mismatch error.

The seeded Phase 1 simulator run covers 27 workload/trace cases per policy. Aggregated latency-SLO
violations were 4 for static peak plus reserve, 57 for HPA-like, 64 for the naive threshold policy,
and 1 for offline lookahead; failed requests were 0, 190,571, 199,284, and 0 respectively. These
are simulated window counts, not production results. Lookahead targets 55% of the profiled QPS and
concurrency bounds under the current convex latency model. Its remaining miss is the memory-api
bursty trace, where the simulator adds latency for exogenous memory pressure independent of replica
count; this case is not an SLO-feasible replica-only reference and is not presented as a win.

Baselines receive identical generated traces and simulator delays. HPA-like behavior includes
metric-ratio targeting, deadband, scale-up/down velocity, downscale stabilization, startup/readiness
scale-in exclusion, and conservative missing metrics. It is representative reactive behavior, not a
claim of byte-for-byte Kubernetes HPA equivalence. The lookahead schedule is offline and not
deployable policy.

## Prompt 3 qualification protocol (v1.0.0)

Everything in this section is **simulated or deterministic-fixture evidence**. None of it is production
or cloud-billing evidence, and none of it replaces the live Kubernetes qualification in
`docs/V2_EXECUTION_PROGRESS.md`. Reproduce with `scripts/run_qualification.sh`; the committed
artifacts are in `docs/benchmarks/v1.0.0/`.

### Evidence categories

| Category | What it is | Where |
|---|---|---|
| SIM-PAIRED | Seeded synthetic traces through the declared convex simulator, all policies on the identical trace | `fleetright.qualify`, scenarios S01-S06, S08-S16 |
| PLANNER-REAL | The real C++ planner and independent verifier on deterministic snapshots (no simulator) | `fleetright.qualify_placement` (S07, S16 placement, S17, S18, E01-E05), `fleetright.optimizer_quality` |
| FORECAST | Causal evaluation of the existing forecast models on the same synthetic series | `fleetright.qualify_forecast` |
| LIVE-K8S | Real Kind API server operations | `python/tests/test_prompt2b_live_*.py` (S19 controller restart, S20 node-drain interruption) |

No scenario uses captured production traces or real load generation; the Docker calibration profiles
from Phase 1 are narrow (see `docs/limitations.md`) and are not used by the paired runs.

### Fair-comparison controls

* Same trace object (content hash recorded per case), machine catalog, per-workload effective capacity
  (`safe point x profile-confidence x model-accuracy x degraded-cluster factors`, floored once), SLOs,
  min/max replicas, startup/actuation delays, warm-up and evaluation window for every policy.
* Every policy observes the same metric window (cpu ratio, qps, concurrency, ready and starting
  replicas). FleetRight receives no information the baselines lack. The offline lookahead policy sees
  future demand and is labelled **offline oracle (upper bound), not a deployable competitor**.
* Warm-up: windows 0-119 run for all policies but are not scored; windows 120-359 (240 x 60 s) are scored.
  120 windows are the minimum history the production forecast gate needs, so predictive FleetRight is
  not handicapped by cold start.
* Seeds: evaluation seeds 100-119 (n = 20 paired traces per scenario); tuning seeds 1-5. Every trace has
  +-3% seeded multiplicative jitter so deterministic families still vary across seeds.

### Policies

| Policy | Definition |
|---|---|
| `static_peak_reserve` | Replicas for the trace's planned peak plus a reserve (tuned, see below) |
| `naive_threshold` | +1/-1 replica on CPU-ratio thresholds with consecutive-window hysteresis (existing `ThresholdBaseline`) |
| `hpa_like` | Target tracking on cpu, qps and concurrency with tolerance, stabilization window, bounded velocity (existing `HpaLikeBaseline`) |
| `fleetright_reactive` | The planner sizing rule (`RequiredReplicas`, availability floor, +1 for N-1) on observed demand; scale-in gated by the production `scale_in_eligibility` rule. `tests/test_qualify.py` checks the rule against the C++ planner on random demands |
| `fleetright_predictive` | Reactive, plus the 95% upper bound from `forecast_decision` (EWMA-trend model, horizon = actuation + startup lead). Falls back to reactive on insufficient history, stale, low-confidence or uncalibrated forecasts. The forecast can only add capacity |
| `fleetright_predictive_same_headroom` | Ablation: predictive at the reactive policy's tuned knob, isolating the forecast effect |
| `offline_lookahead_oracle` | Existing offline lookahead at 55% target utilization (upper bound) |

### Tuning and hold-out

Each policy has exactly one tunable knob (static reserve, threshold, HPA target, FleetRight target
utilization). Knobs are chosen **once** on separate tuning runs (different seeds, lower base load, four
trace families: steady, ramp, diurnal, bursty) with one rule applied to every policy: the cheapest knob
whose hard+soft violation windows are at most 3% of scored workload-windows (otherwise fewest
violations). The evaluation scenarios then use the frozen knobs. The tuning *families* overlap
evaluation families S01-S03/S05; seeds and load level are disjoint. This is a documented weakness:
it is seed-level, not family-level, hold-out. The first tuning rule (zero hard-violation windows) was
unattainable for every reactive policy and was replaced before any evaluation data was generated.
A first evaluation run was then inspected before two fairness additions were made: the static-reserve
baseline was tuned like the others (it had a fixed 20% reserve) and the same-headroom ablation was
added. Neither changed a FleetRight parameter, and the published run is a fresh run of the final code.

### Metrics

* Cost: `cost-model-v1`, **synthetic** prices (demo catalog, `small` node = 50,000 micro-USD/h). Replicas are
  packed first-fit-decreasing onto `small` nodes with the planner's one-replica-per-workload-per-node
  rule and 5% headroom; node-hours and cost follow from the packed node count per window. Also reported:
  replica-hours, idle CPU/memory fraction, provisioned/required replicas, cost per million successful
  requests, scaling actions, replicas added, node-release events (a proxy for drained nodes), longest
  capacity gap.
* SLO: per workload-window. **Hard** violation = error rate above the workload error SLO (overload or
  queue overflow) or ready replicas below the availability floor. **Soft** violation = window p99 above
  the latency SLO. Latency percentiles are percentiles of the simulator's per-window p99 series; the
  simulator has no per-request latency. Individual violation windows are retained in `results.jsonl`.
* Statistics: per-seed paired differences (FleetRight minus other); percentile bootstrap, 10,000
  resamples over seeds, fixed RNG seed, 95% CI. Verdict rule (`qualify.verdict`): cost and total
  violation windows, with hard violations dominant (a significantly higher hard-violation count, or hard
  windows against a policy with none, makes FleetRight "riskier" however low the cost); "better/worse"
  requires a CI excluding 0 on one axis and not significantly worse on the other; opposite significant
  signs are a "trade-off". A first version of the rule summed hard and soft windows and mislabelled S04/S11
  as improvements; the independent review caught it and the published tables are recomputed (`qualify render`). No p-values are reported. n = 20 supports
  large effects only; for steady and flash crowd every seed has a distinct trace (distinct hashes) but the outcome is
  identical across seeds (one distinct cost value), so those intervals have an effective n of about 1 and
  say nothing about sampling variability. S09 differs from the other scenarios only in trace family
  (bursty); every scenario has three tenants with different SLOs (80/120/150 ms).
* A lower cost is not an improvement when it comes with significantly more violations (verdict rule).

### Optimizer performance contract

Direct C++ API timings (protobuf loaded once), Release build, 1,000 workloads / 500 nodes / 1 extra
profile alternative; planner target 250 ms p95, verifier target 25 ms p95, 100 measured iterations
(`scripts/run_benchmarks.sh --iterations 100`). One untimed planner run and one untimed verifier
run (which also validate the supplied plan) precede the measured loop; p50/p95/p99/max are over all
measured iterations. Peak RSS is a single CLI run under GNU `time`.

### Not covered

Real traffic, real cloud prices, multi-cluster behaviour, per-request latency, noisy-neighbour
interference at the kernel level (S10 is demand-side only), and long-running soak behaviour.
