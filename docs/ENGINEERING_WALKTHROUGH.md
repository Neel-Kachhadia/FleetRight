# FleetRight engineering walkthrough

A compact, interview-oriented tour. Every claim points at code or at evidence in this repository;
numbers live in [`docs/RELEASE_REPORT.md`](RELEASE_REPORT.md), not here.

## 1. The problem

Capacity is bought in coarse units (nodes) but consumed by services whose tail latency is nonlinear in
load. Static peak provisioning wastes money off-peak; threshold or HPA-style autoscaling reacts to a
utilization signal that does **not** say whether a smaller shape still meets the p99 SLO, and it only
adds replicas after load has already arrived (startup, readiness and node-activation delays are
tens of seconds). FleetRight's thesis: size from *measured* capacity at a given resource shape,
constrain by hard SLO/availability/N-1/topology rules, and execute changes through a protocol that
cannot make things worse when something fails halfway.

## 2. The pipeline

```
telemetry -> capacity profile -> forecast -> C++ planner -> C++ verifier -> PostgreSQL plan/journal
          -> Kubernetes executor -> health observation -> commit | compensate | fail safe
```

| Stage | Code | What to say about it |
|---|---|---|
| Capacity profile | `python/fleetright/{characterize,profiles}.py`, `proto/.../model.proto` | Hashed profile tied to image, request mix, runtime class, platform and *resource allocation*; safe points need repeated runs, Wilson bounds on latency-success and error, no queue growth. An unconstrained process run cannot promote a smaller shape. |
| Forecast | `forecast.py` | Three simple models + temporal calibration. The 95% upper bound comes from residuals of earlier windows only. Insufficient history, staleness, low confidence or poor calibration return a **reactive fallback**, never an exception into the planner. |
| Planner | `cpp/src/planner.cpp` | Deterministic: sorted IDs, integer micros, checked 128-bit arithmetic. Replica count = `max(ceil(demand/effective capacity), floor)` (+1 for N-1); greedy dominant-resource placement; bounded single-replica local search that consolidates onto cheaper nodes. Objective = prorated node cost + movement + churn + uncertainty + capacity-boundary exposure. |
| Verifier | `cpp/src/verifier.cpp` | Re-derives everything from snapshot + plan; shares only protobuf types and low-level checked-unit/hash helpers with the planner, no placement or profile-feasibility code (ADR-003). Accepts only plans whose *intermediate* states (surge, eviction, recreation, N-1 recovery order) are safe, not just the final placement. |
| Control plane | `api.py`, `control.py`, `migrations/` | Authenticated, bearer-token API with server-side roles (viewer / recommender / operator / admin); plans, approvals, idempotency keys, leases and an audit-event table in PostgreSQL. `RECOMMEND` is the default; `EXECUTE` needs explicit configuration. |
| Execution | `execution.py`, `reconciler.py` | Journaled actions with an immutable approved payload, attempt numbers, fencing tokens and a three-state outcome (applied / not applied / **unknown**). |
| Kubernetes target | `workload_controller.py`, `pod_admission.py`, `deploy/kubernetes/` | A custom `FleetRightWorkload` owns its Pods. Admission webhooks gate Pod CREATE, Eviction and Binding; a `ValidatingAdmissionPolicy` constrains Node patches. The executor has no Pod `delete`. |

## 3. Why the verifier is independent (and a real example)

During Prompt 3 a randomized oracle study (60 seeds) found that 11 clean planner outputs were
**rejected by the verifier**: (a) the planner moved a replica onto a node still hosting a sibling
replica that the same plan removes, and (b) it never enforced the per-workload `max_moves` budget.
Nothing unsafe would have run - the verifier rejected both - but the planner was producing plans
that could not be executed. Both are now fixed in the planner (the verifier is unchanged) and a
seeded C++ property test fails without the fix. This is the practical argument for two independent
implementations: a shared helper would have agreed with itself.

## 4. The Kubernetes / PostgreSQL atomicity boundary

PostgreSQL cannot make a Kubernetes mutation atomic. The contract is therefore *intent -> side effect
-> observation*:

1. Commit the dispatch intent (action, attempt, fence) in PostgreSQL.
2. Perform the Kubernetes call, guarded by UID / resourceVersion preconditions.
3. Record the response. If the process dies between 2 and 3 the outcome is **unknown**.
4. A new controller takes the lease (fencing token strictly greater), *fences the target object*, and
   observes before deciding: already applied (record), provably not applied (retry, bounded), or
   still unknown (wait; never guess).

Consequences worth being able to explain: a stale controller's late CREATE is rejected by admission
(it carries an attempt ID that is no longer the current reservation); a held Eviction response cannot
retire the wrong Pod because the source UID is checked and the retirement is a durable tombstone
written by compare-and-swap *before* Eviction; a PDB-blocked retirement simply waits (no destructive
action), and the verification deadlines fail safe instead of looping.

## 5. PDB-aware eviction, drain, compensation

* **Replacement first.** A surge Pod is created on an approved destination, must become Ready, and
  only then is the source retired through the Eviction API so the PDB is honoured. FleetRight never
  force-deletes.
* **Node drain** (2-4 Pods): cordon with ownership metadata (`cordon-owned` true only if FleetRight
  cordoned a schedulable Node; an administrator's cordon is preserved), move each Pod in a verified
  order, re-observe the Node, release metadata. The monotonic fencing token stays as an inert high-water
  mark so a stale controller cannot re-claim a released Node. FleetRight never uncordons.
* **Partial-drain recovery.** Killing the controller with a third Eviction pending, then starting a new
  one, completes with exactly four Evictions in the live test.
* **Compensation.** A fresh health regression after a retirement creates a durable action that restores
  capacity; `COMPENSATED` is written only after a Ready replica and a healthy window. If the
  recovery Pod cannot be scheduled the plan ends `FAILED_SAFE` and an operator must intervene.

## 6. Benchmark methodology in one paragraph

Identical seeded traces and effective capacities for every policy; the offline lookahead is labelled an
upper bound; every tunable policy gets one knob tuned on separate seeds with one shared rule; 20 paired
seeds per scenario; percentile-bootstrap 95% CIs on paired differences; verdicts require a significant
signal and no significant loss on the other axis; losing scenarios are listed first-class. Cost is a
synthetic, versioned model and latency is the declared simulator's, so this is evidence about
*policy behaviour under a stated model*, not about a cloud bill. Details:
[`docs/benchmark-methodology.md`](benchmark-methodology.md).

## 7. Things I would say unprompted in an interview

* The honest headline is a **trade-off map**, not a win: FleetRight is cheaper than reactive baselines
  in some regimes and loses on SLO or cost in others (see the report's losing-scenario list).
* The planner sizes at 100% of profile-effective capacity; the simulator is convex near saturation, so
  the published FleetRight numbers use a tuned utilization target (60-70%) that the C++ planner does not
  expose. Run unmodified, the sizing rule violated the SLO in a large share of tuning windows.
* Forecasting helped on some patterns and hurt on others; the ablation isolates it from tuning.
* Live Kubernetes evidence is one Kind lab with deterministic fault injection; it is not production.
* `SCALE_DOWN` and `DEACTIVATE_NODE` are intentionally not enabled.

## 8. Where to look first

`cpp/src/planner.cpp` (`PlanWithProfileOverride`), `cpp/src/verifier.cpp` (intermediate-state
check), `python/fleetright/execution.py` (journal and outcome classification),
`python/fleetright/workload_controller.py` (Pod ownership and admission binding),
`python/fleetright/qualify.py` (benchmark harness), `python/tests/test_prompt2b_*` (fault cases).
