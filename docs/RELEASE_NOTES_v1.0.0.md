# FleetRight v1.0.0 - release notes

Local release; not pushed, not published. Scope: SLO-constrained capacity planning with verified plans
and safe, staged Kubernetes execution on the `FleetRightWorkload` target. Evidence: `docs/RELEASE_REPORT.md`,
`docs/PROMPT3_QUALIFICATION.md`, `docs/benchmarks/v1.0.0/`.

## What is in it

* Deterministic C++ planner and independently implemented verifier (v1 snapshot/plan and V2 evacuation).
* Capacity profiles, forecasting with reactive fallback, PostgreSQL-backed authenticated control API.
* Journaled, fenced Kubernetes execution: `SCALE_UP`, `MOVE_REPLICA`, `EVICT_SOURCE`, `DRAIN_NODE` (2-4 Pods),
  `COMPENSATE`; PDB-respecting Eviction, admission webhooks, constrained Node patches, bounded verification.
  `SCALE_DOWN` disabled; `DEACTIVATE_NODE` unsupported.
* Reproducible qualification harness: `scripts/bootstrap.sh`, `scripts/start_test_db.sh`,
  `scripts/run_qualification.sh`.

## Changes since milestone-4-safe-control-plane

* **Performance:** planner API p95 332 ms -> **196.9 ms** (target 250 ms); verifier p95 20.7 ms (target 25 ms).
* **Correctness:** the planner no longer emits plans the verifier must reject (moves onto a node still hosting a
  sibling replica; per-workload `max_moves` budget). Verifier unchanged. New randomized regression test.
* **Evidence:** 15 simulated scenarios x 20 paired seeds x 6 policies (+ablation), forecast evaluation,
  60-instance exact-oracle/mutation study, real-planner placement suite, live Kind + Docker reruns.
* **Tests:** C++ 52 (was 51), Python 499 passed + 20 live-gated skips; new role-fence and PostgreSQL
  connection-severance tests.
* **Portability:** no personal paths in defaults; venv-based bootstrap; disposable test-DB script.
* **Docs:** README, methodology, release report, limitations, engineering walkthrough; milestone-3-era
  statements marked superseded.

## Honest summary of results

FleetRight is not shown to dominate tuned baselines. Against HPA-like it was cheaper in most simulated scenarios
but usually by accepting more SLO-violation windows, and it takes hard-SLO violations on a 5x flash crowd,
slow-start, bursty multi-tenant and correlated-surge traces where static peak provisioning does not. The
bundled forecasting models did not improve decisions in the simulated model. All cost figures are synthetic.

## Known limitations

See `docs/limitations.md` ("v1.0.0 limitations"). Notably: simulated evidence only, one Kind lab, no real API or
PostgreSQL outage test, loopback-only static-token API, planner optimality gap up to 40% on small instances,
thin verifier latency margin, unexposed utilization target.

## Upgrade notes

No schema or protocol change from milestone 4.
