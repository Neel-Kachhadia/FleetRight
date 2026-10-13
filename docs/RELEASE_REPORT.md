# FleetRight v1.0.0 - release benchmark report

Scope statement: this report separates **SIMULATED** policy evidence (synthetic traces, a declared
convex latency model, a synthetic cost model), **REAL-PLANNER** evidence (the actual C++ planner and
verifier on deterministic snapshots), **FORECAST** evidence (synthetic series) and **LIVE-K8S** evidence
(a disposable Kind lab). Nothing here is production, real-traffic or cloud-billing evidence. Artifacts:
[`docs/benchmarks/v1.0.0/`](benchmarks/v1.0.0/); method: [`benchmark-methodology.md`](benchmark-methodology.md);
reproduce: `scripts/run_qualification.sh`.

## 1. Executive summary

* **Optimizer speed.** At 1,000 workloads / 500 nodes (Release, GCC 13.3, i9-13980HX, 100 measured
  iterations) planner API p95 is **196.9 ms against the 250 ms target (met)**, p50 192.4, p99 200.6,
  max 215.9 ms; independent verifier p95 **20.7 ms against 25 ms (met)**, p50 17.8, p99 21.4. Before
  this phase the same host measured 332.4 ms (target missed; the historical record was 314.0 ms). The
  margin is real but thin for the verifier (17% under target): a concurrent run on a loaded host measured
  26.0 ms. Peak RSS: planner 22.6 MiB, verifier 20.7 MiB.
* **Planner/verifier agreement was not perfect and is now.** A 60-instance randomized oracle study found
  that 11 clean planner plans were rejected by the independent verifier (planner moved replicas onto a
  node still hosting a sibling the plan removes; planner ignored the per-workload move budget). Fixed in
  the planner; verifier unchanged; 60/60 now accepted, 60/60 deterministic, mutated plans rejected
  360/360 for the intended invariant. Exact-oracle gap where the move budget is non-binding: optimal in
  23/30, mean +7.6%, max +40% (a staging-safety restriction costs optimality; see Limitations).
* **Cost and SLO are a trade-off map, not a win.** Against tuned baselines on 15 paired simulated
  scenarios (n = 20 seeds each), reactive FleetRight had a median node-cost difference of
  **-8.6% vs HPA-like, -18.8% vs naive threshold, -12.1% vs static peak** but with *more* SLO-violation
  windows in the scenarios where it was cheaper (the "trade-off" verdicts). Verdict tallies (cost, total violation windows and hard violations, 95% paired-bootstrap; a significantly
  higher hard-violation count always counts against FleetRight): vs HPA-like 6 better / 3 worse /
  6 trade-off; vs threshold 6 / 2 / 7; vs static peak 2 better / 1 worse / 3 no significant difference /
  9 trade-off (4 of them because FleetRight takes hard violations that static peak never does).
  Predictive FleetRight did **not** improve on reactive: 5 worse, 10 trade-offs, 0 better. Part of FleetRight's cost advantage comes from a tuned 60-70% utilization target that the C++
  planner does not expose (Section 4).
* **Hard constraints.** Over all scenarios, static peak and the offline oracle had zero hard-violation
  windows; reactive FleetRight had hard violations in S04, S08, S09, S11, predictive also in S05 and S15,
  HPA-like in five scenarios. A policy with any hard-violating seed is **not classified successful** in that
  scenario, whatever its cost. Scenarios free of hard violations: static peak 15/15, offline oracle 15/15,
  naive threshold 11/15, **FleetRight reactive 11/15**, HPA-like 10/15, FleetRight predictive 9/15.
* **Execution safety (live).** Kind qualification re-run in this phase (first at c88f24b by an independent subagent, then again
  at 7d4f0c4; the execution path is unchanged between those and the release commit, see
  `PROMPT3_QUALIFICATION.md`): 15 live tests passed both times (admission
  negatives, node ownership, replacement + PDB permit/deny, lost/held CREATE across takeover, drain
  variants including PDB-blocked, 4-Pod drain with controller kill and takeover, API evacuation, held
  Eviction, unplaceable compensation) plus 3 Docker E2E. Two gaps remain unproven live: PostgreSQL
  connection loss (now covered deterministically) and a real Kubernetes API outage.
* **Verdict.** Claims supported: the planner meets its latency contract; the verifier is independent and
  effective; execution recovers safely in the scoped live scenarios. Claims **not** supported: that
  FleetRight reduces cost without SLO cost versus a well-tuned HPA, that forecasting improves decisions
  in this model, or any production or billing saving.

## 2. Methodology (summary)

Identical trace/capacity/SLO/delay/warm-up per policy; seeded paired trials; one tunable knob per
policy chosen on separate seeds by one shared rule (cheapest knob with <= 3% violation windows);
offline lookahead labelled an upper bound; bootstrap CIs on per-seed paired differences; cost model
`cost-model-v1` synthetic. Full protocol and its disclosed weaknesses (family-overlapping tuning set,
disclosed inspection of a first evaluation run before two fairness additions): `benchmark-methodology.md`.

Tuned knobs (frozen before the published run, `benchmarks/v1.0.0/tuning.json`): static reserve 5%,
threshold scale-up at 50%, HPA target 50%, FleetRight reactive target utilization 60%, predictive 70%.
Using the planner rule at 100% (the shipped C++ sizing) FleetRight reactive had 154 hard + 3,611 soft
violation windows of 14,400 on the tuning set (cost 12.5 M) versus 79 + 229 at 60% (cost 14.5 M).

## 3. Workloads

| Scenario | Source | Category |
|---|---|---|
| S01-S06, S08-S16 | seeded synthetic traces (`generate_trace` families + level shift / drift / node loss / telemetry gaps), 3 tenants, 360 windows of 60 s (120 warm-up, 240 scored), +-3% jitter | SIM-PAIRED |
| S07 mixed CPU/memory | demo cluster snapshot through real planner + verifier | PLANNER-REAL |
| S16 (placement), S17, S18, E01-E05 | deterministic snapshots through real planner + verifier (11 cases, all met their declared expectation) | PLANNER-REAL |
| S19 controller restart, S20 node-drain interruption | `test_prompt2b_live_lost_create`, `test_live_v2_authenticated_node_drain_partial_crash_recovery` | LIVE-K8S |

S07 (planner on the 10-node mixed demo cluster): 500,000 -> 250,000 micro-USD/h, 4 nodes used, verifier
accepted. S17b (all nodes in one zone while 2 domains are required) and S18a/b (one node for the fleet;
demand beyond max replicas) are correctly **infeasible**. S16 with an unhealthy in-use node returns
**infeasible** ("surviving replicas cannot preserve demand after failure of zone"): FleetRight declines
rather than emitting an emergency repair plan.

## 4. Cost

Per-scenario costs, intervals and idle/over-provisioning are in `comparison.md` and `summary.csv`;
headline verdicts in `headline.md`. Across the 15 scenarios (mean node cost relative to static peak):
static 1.00, naive threshold 1.02, HPA-like 0.91, **FleetRight reactive 0.85**, predictive 0.89,
predictive at reactive's knob 0.99, offline oracle 0.79. FleetRight reactive's cost advantage over HPA-like
ranged from -20% to +26% by scenario (FleetRight was *more* expensive than HPA-like in S05, S09, S13 and
S14).

Caveats that bound any "saving" claim: prices are synthetic; savings come in node-count steps of 1-4
nodes; FleetRight's lower cost partly reflects a lower tuned headroom, which the same table shows costs
SLO windows; replicas are packed one-per-workload-per-node so single-service scenarios cannot consolidate.
No claim about real billing is made.

## 5. SLO

Hard / soft windows per policy and every individual violation window (workload, window index, kind) are
in `results.jsonl.gz`. Hard violations by scenario: S04 flash crowd 5x - **all four online policies**
(HPA-like, threshold, FleetRight reactive and predictive) violate in all 20 seeds for at least one window per
tenant (the lead time of 40 s plus the 60 s decision window makes a 5x single-window spike unforecastable);
only the over-provisioned static policy (about 2.4x the cost of the other policies) and the offline oracle avoid it. S08 slow start
(150 s): FleetRight reactive 15.4 hard windows/seed vs HPA-like 0.6. S09 bursty multi-tenant: reactive 18.8,
HPA-like 12.2, predictive 1.9. FleetRight predictive additionally violates in S15 (drift). Latency
percentiles are over the simulator's per-window p99 series (no per-request latency exists in the model).

## 6. Control quality

Scaling actions, replicas added and node-release events are in `summary.csv`. Capacity gaps after a lost
node (S16) lasted one window for every online policy (restart lead is shorter than the decision window).
Replacement/drain mechanics are exercised live, not in the simulator.

## 7. Forecasting (existing models only)

`forecast.json`: causal one-window-ahead MAE, WAPE, under/over frequency, worst error, rolling 95%
upper-bound coverage (calibrated on the previous 40 windows), mean bound overhead. Highlights (EWMA-trend /
rolling-median): steady WAPE 1.6% / 1.6%; ramp 1.4% / 10.0% (rolling median underpredicts every window);
diurnal (12-window day) 30.7% / 30.7%; bursty 24.3% / 14.4%; level shifts (+80% / -50%) 31-35% WAPE;
flash crowd worst error ~2,400 qps. Coverage was near nominal (0.90-0.96) everywhere but the bound is wide
(mean overhead 0.71 on diurnal, 1.10 on bursty) - nominal coverage is bought with capacity.
On a 14-day hourly series the seasonal model (WAPE 1.7%) beats EWMA (21.9%) and rolling median (32.9%),
but it needs >= 48 distinct hours, so it cannot activate at the 60 s cadence used in the policy runs;
the production gate returns the reactive fallback with `FORECAST_INSUFFICIENT_HISTORY` below ~100
observations and `FORECAST_STALE` for old data (checked).

Decision impact (ablation = predictive at reactive's knob; `headline.md`): predictive cost +0% to +42% and
fewer violation windows in 8 scenarios (trade-off), costlier with no SLO benefit in 5, no significant difference in
2, **better in none**. Forecasting bought SLO protection at a cost premium; it never dominated reactive in
this synthetic model, and with its own tuned (less conservative) knob it did worse than reactive in S04,
S11 and S12.

## 8. Optimizer

| Item | Result |
|---|---|
| Planner API p50 / p95 / p99 / max | 192.4 / 196.9 / 200.6 / 215.9 ms (target 250 ms p95: **met**) |
| Verifier API p50 / p95 / p99 / max | 17.8 / 20.7 / 21.4 / 21.6 ms (target 25 ms p95: **met**) |
| CLI (process start + protobuf I/O) planner / verifier p95 | 209.9 / 37.7 ms (not subject to the targets) |
| Before | planner p95 332.4 ms (this host, 50 it.), historical 314.0 ms |
| Optimization | choose the placement destination by `min_element` instead of sorting ~500 `(bool,u64,string)` tuples per replica: 332 -> 152 ms with a byte-identical plan; then +~45 ms for the two correctness rules => ~197 ms |
| Peak RSS | planner 22.6 MiB, verifier 20.7 MiB (single CLI run) |
| Determinism | planner/verifier replay 100/100 identical; ASan/UBSan plan equals Release plan except build id |
| Oracle (60 seeds, 3-10 nodes) | feasibility agreement 60/60; verifier acceptance 60/60 (49/60 before fix); gap (move budget non-binding) optimal 23/30, mean 7.6%, max 40%; move budget binding 30/30 optimal |
| Mutation rejection | 6 mutation classes x 60 = 360/360 rejected by the intended invariant (topology/anti-affinity, replica ranges, cost catalog, plan hash, unknown node, stale snapshot) after re-sealing the plan hash |

Profile of the original hot path (callgrind): 22% `memcmp`, 16% in the placement loop, ~10% tuple sort and
string moves. Remaining top cost: `memcmp` from string-keyed set/map lookups (26%), fragmentation scoring (7%),
O(workloads x placements) `CurrentCount` (5%); not pursued because the target is met.
Hardware: Intel Core i9-13980HX, 32 logical CPUs, 16 GB, Linux 7.0, GCC 13.3.0, `-DCMAKE_BUILD_TYPE=Release`.

## 9. Failures and recovery (LIVE / DETERMINISTIC)

Mapping of the 20 adversarial scenarios A-T to tests and results: `docs/PROMPT3_QUALIFICATION.md`
(summary: 19 of 20 covered live and/or deterministically and passing; **I** PostgreSQL connection loss covered
deterministically by `test_severed_postgres_connection_during_claim_leaves_no_partial_execution` (the
connection is killed after the body and before COMMIT: HTTP 5xx, no partial row, same idempotency key then
succeeds once); **J** Kubernetes API outage is only partially covered, via injected observation errors, not a real outage).
The three verification-deadline outcomes and unsupported-Pod-owner during a live drain are deterministic
only. The compensation-with-capacity case (6-node lab) was not re-run in this phase (it was qualified in
Milestone 4 on a throw-away lab).

## 10. Security

Authentication (401), role fences (viewer/recommender 403 on execute, added), idempotent replay, oversized
and malformed bodies, forged CREATE/Eviction/Binding identities, legacy workload refusal, RBAC (executor
cannot delete Pods, create evictions or touch Secrets; Node patches constrained by a
ValidatingAdmissionPolicy) all pass. `get secrets`-denied live probe was **not run** (needs an executor
kubeconfig not provisioned here); `rbac.yaml` contains no Secret rule. Secret scan: 1,046 raw
`detect-secrets` findings, 1,045 SHA-256 hashes in result artifacts and 1 `secretName:` reference; extra
regex scan 0 hits; no kubeconfig/.env tracked. The API is a loopback-bound local process (no TLS, static
bearer tokens); it is not hardened for network exposure.

## 11. Reproducibility

Clean clone of `e2b95f9` on this host: `scripts/bootstrap.sh` (venv, Release build, 52/52 CTest, pytest,
ruff) -> `scripts/start_test_db.sh` -> pytest with PostgreSQL (491 passed, 20 skipped, live-gated) ->
`scripts/run_demo.sh` (replay true, verifier accepted) -> `scripts/run_qualification.sh`. The paired
`results.jsonl` was **byte-identical** (SHA-256 `64bf94cf...`) between the clean clone (6 workers) and
this tree (12 workers), and between three runs. Timing is host-dependent: a loaded clean-clone run
measured 227 ms planner / 26.0 ms verifier p95. Not demonstrated: another machine, another OS, offline
install, and the Kind live suites from a clean clone.

## 12. Limitations (where FleetRight loses or fails a target)

See `docs/limitations.md` section "v1.0.0 limitations". Principal items: simulated model, synthetic
prices, uncalibrated simulator; unexposed utilization knob; FleetRight worse than HPA-like in S13, S14
(and cost-worse in S05, S09); predictive never better than reactive; all online policies breach hard SLO on
a 5x flash crowd; planner optimality gap up to 40% on small instances with the staging restriction;
verifier latency margin 17%; live evidence from one Kind lab only.

## 13. Conclusions supported by evidence

1. The C++ optimizer meets its documented latency contract on the declared benchmark.
2. The independent verifier rejects invalid plans and caught two real planner defects.
3. Staged execution (replacement, PDB-aware eviction, node drain, compensation) behaves safely in the
   scoped live and deterministic scenarios, including controller takeover.
4. Versus tuned reactive baselines, FleetRight trades SLO windows for lower cost in many scenarios; it is
   not shown to dominate. Forecasting did not help in this model.
5. The results are reproducible from a clean checkout on this host, bit-for-bit for the simulation.

Not concluded: production readiness, real cloud savings, performance on real traffic.
