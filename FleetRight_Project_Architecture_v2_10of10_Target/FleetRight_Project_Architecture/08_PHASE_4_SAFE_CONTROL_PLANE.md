# 8. Phase 4 — Safe Control Plane and Reconciliation

## Goal

Turn verified recommendations into bounded, crash-safe state transitions. Phase 4 is where FleetRight earns the right to call itself a control system rather than an optimizer demo.

## Modes

```text
OBSERVE    metrics/snapshots only
RECOMMEND plans + verification, no mutation
SHADOW    evaluate planned actions against real/baseline behavior
EXECUTE   apply policy-approved verified actions progressively
```

Default fresh installation mode is `RECOMMEND` or stricter, never destructive execution.

## Plan state machine

```text
PROPOSED
  -> VERIFIED
  -> APPROVED
  -> EXECUTING
  -> VERIFYING
  -> COMMITTED

terminal / exceptional:
REJECTED
STALE
PAUSED
COMPENSATING
COMPENSATED
FAILED_SAFE
CANCELLED
```

Every transition is durable and append-audited.

## Plan versioning and TOCTOU defense

Every plan binds to:

- immutable snapshot ID;
- base generation;
- target workload/node resource versions where available;
- policy/profile/catalog versions;
- expiration time.

Before each action, the reconciler rechecks action preconditions. A mismatch marks the plan `STALE` and triggers replanning rather than applying an old decision to new state.

## Independent plan verifier

The verifier receives only canonical snapshot + plan + policy and recomputes:

- resource feasibility;
- SLO envelope feasibility;
- error/queue constraints;
- replica limits;
- topology requirements;
- disruption allowance;
- migration/action budgets;
- N-1 requirements;
- freshness/confidence;
- transition/intermediate-state safety;
- plan expiration and generation.

Verifier output is deterministic and machine-readable.

## Progressive execution

Destructive plans are split into bounded steps.

Example:

```text
20 -> 18
wait readiness + observation interval
verify SLO/resource bounds
18 -> 16
observe
...
```

Step size depends on priority, recent error, recovery time and policy blast-radius limits.

## Compensation instead of instantaneous rollback

If a step causes regression, FleetRight enters `COMPENSATING`:

- restore previous replica/resource target;
- activate reserve node/capacity if needed;
- stop remaining destructive actions;
- wait for readiness/warm-up;
- verify recovery;
- record recovery-time objective and actual duration;
- apply a temporary conservative lock/headroom increase.

The journal never claims that compensation was instantaneous.

## Unknown-outcome handling

Example failure:

1. controller sends scale request;
2. target API accepts;
3. response is lost;
4. controller crashes.

On restart FleetRight **must not resend blindly**. It reads desired action + target state, inspects actual target state, and reconciles.

Idempotency keys prevent duplicate logical actions; reconciliation handles uncertain transport outcomes.

## Kubernetes execution semantics

For the local Kubernetes adapter:

- replica count changes modify the owning scalable controller;
- a replica “move” is a controlled eviction/recreation outcome, not live migration;
- voluntary pod disruption uses eviction semantics where applicable;
- current PDB `disruptionsAllowed` and generation are considered;
- direct delete is not used as a shortcut where it would bypass modeled PDB safety;
- topology-spread/anti-affinity constraints are evaluated against the target cluster state;
- node drain is incremental and reconcilable;
- readiness is required before proceeding to subsequent destructive steps;
- pod termination/startup/warm-up delay enters the observation model.

## Controller concurrency

Use database-backed leases/ownership plus resource conflict keys so two controller instances cannot independently mutate the same workload/node conflict domain.

Leader loss is safe because action state is durable and the next owner reconciles before continuing.

## Decision journal

The journal stores or content-addresses the complete replayable evidence set:

```text
snapshot content hash + retrievable normalized snapshot
policy version/content hash
capacity profile versions/hashes
machine catalog version/hash
forecast output + model version
scenario seed / trace hash when experimental
optimizer/verifier build IDs
candidate plan
verifier result
approval
state transitions
action attempts
observed target state
outcome metrics
compensation events
```

A hash verifies content; it does not replace the content needed for replay.

## Phase-4 exit gates

- restart during every major action boundary passes deterministic recovery tests;
- duplicate/timeout execution never creates duplicate logical side effects;
- stale plan is rejected after concurrent state mutation;
- verifier is required for all execution paths;
- progressive downscale and compensation validated under induced regression;
- Kubernetes adapter respects PDB/topology/readiness semantics in integration tests;
- concurrent controller ownership test passes;
- multi-hour shadow run preserves journal integrity;
- every saved experiment can rehydrate the exact decision inputs.
