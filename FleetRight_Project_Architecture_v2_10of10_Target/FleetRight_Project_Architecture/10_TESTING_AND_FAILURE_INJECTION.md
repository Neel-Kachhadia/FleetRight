# 10. Testing and Failure Injection

## Testing philosophy

FleetRight is a control system. Tests must target **state transitions and failure boundaries**, not just pure functions.

## Unit tests

Cover:

- explicit unit arithmetic and overflow/bounds;
- capacity-profile lookup/interpolation rules;
- replica calculations;
- placement scoring;
- objective decomposition;
- forecast bounds;
- verifier constraints;
- stabilization/cooldown rules;
- plan state transitions;
- compensation triggers;
- idempotency behavior.

## Property-based tests

Generate randomized workloads, profiles, nodes, topologies and plans.

Required properties:

- verifier never accepts capacity overflow;
- min/max replicas preserved;
- every planned replica has exactly one target placement;
- unhealthy/forbidden nodes receive no new placement;
- topology constraints hold;
- cost arithmetic is internally consistent;
- accepted N-1 plans remain feasible after configured failure removal;
- increasing demand cannot reduce required capacity absent another explicit model change;
- lowering a resource allocation cannot increase modeled capacity without a different validated profile;
- same normalized input/build yields same planner and verifier output.

## Model-based state-machine tests

Generate sequences over:

```text
PROPOSE -> VERIFY -> APPROVE -> EXECUTE -> OBSERVE
restart / timeout / concurrent change / compensate
```

Assert legal transitions and durable recovery.

## Differential tests

For small optimizer cases, compare heuristic feasibility/objective with exact oracle output. The heuristic need not match the exact optimum but must never beat the exact objective under identical constraints; such a result indicates model mismatch/bug.

## Integration tests

Mandatory pipelines:

```text
telemetry -> snapshot -> planner -> verifier -> simulator -> evaluator
telemetry -> snapshot -> planner -> verifier -> Kubernetes reconciler -> evaluator
```

## Failure-injection matrix

| Failure | Required behavior |
|---|---|
| Prometheus unavailable | block destructive actions; fail safe |
| partial/missing metrics | conservative baseline semantics; block unsafe scale-in |
| stale snapshot | mark plan stale |
| capacity profile stale | block destructive resize/downscale using profile |
| node disappears | prioritize recovery and recompute |
| flash crowd | scale out/react; preserve reserve |
| forecast underestimates | reactive recovery + widen uncertainty reserve |
| memory leak | reject memory downsize |
| planner timeout | keep current state; no mutation |
| verifier timeout/failure | no execution |
| executor response lost after success | reconcile actual target; no blind retry |
| DB write fails before side effect | do not mutate |
| DB unavailable after possible side effect | recover DB, inspect target, reconcile |
| controller crash before request | resume from durable state without mutation duplication |
| controller crash after request | reconcile uncertain outcome |
| duplicate execute request | return/reconcile same logical action |
| two controllers race | lease/conflict ownership permits one mutator |
| policy changes before execution | precondition mismatch -> stale/replan |
| workload deleted mid-plan | cancel/reconcile safely |
| PDB blocks eviction | pause; do not bypass with direct delete |
| topology changes | stale/reverify/replan |
| partial node drain | preserve completed safe steps; reconcile remaining plan |
| p99/error/queue regresses | stop destructive steps; compensate |
| startup slows unexpectedly | hold progression; compensate/scale out if threshold breached |
| machine catalog malformed | reject cost optimization dependent on invalid catalog |
| clock jumps | use monotonic duration logic for local timers; wall clock only for audit timestamps |
| corrupted snapshot artifact | hash mismatch -> reject replay/execution |

## Chaos scenarios

Script and preserve seeds for:

- random node loss;
- delayed/dropped telemetry;
- 10x traffic burst;
- correlated burst across services;
- startup delay multiplication;
- memory pressure;
- executor/API timeout;
- control-plane process kill at each action boundary;
- PostgreSQL restart;
- conflicting external replica change;
- PDB saturation.

## Fuzzing

Targets:

- protobuf/JSON bridge if present;
- policy parser;
- capacity-profile parser;
- snapshot loader;
- resource vector math;
- plan parser;
- journal replay reader.

## Sanitizers

- ASan mandatory;
- UBSan mandatory;
- TSan for concurrency-sensitive paths;
- leak detection where stable in CI.

## Soak testing

The soak must contain real state churn, not an idle controller. Measure:

- RSS growth;
- open file/socket growth;
- DB table/journal growth;
- action throughput;
- stuck-state count;
- reconciliation latency;
- stale-plan count;
- compensation count.

A stuck plan without an explicit terminal or recoverable state is a failure.
