# 18. Architecture Validation Matrix

This matrix is the final design-review checklist. “10/10 target” means the architecture now contains the mechanisms and evidence gates expected of an exceptional portfolio implementation; it is not a claim that unimplemented code is automatically perfect.

| Category | Earlier weakness | Corrective architecture | Evidence required for 10/10 target |
|---|---|---|---|
| Business problem | already strong | explicit SLO/cost/failure tradeoff and stateless scope | measured cost + SLO report |
| Project positioning | risk of “predictive autoscaler” framing | joint rightsizing/replicas/placement/consolidation + independent verifier | architecture/demo clearly shows differentiated control problem |
| High-level architecture | planner/gate flow too coarse | immutable snapshot, profile store, planner, independent verifier, journal, reconciler, outcome verifier | end-to-end trace through every boundary |
| Algorithm specification | CPU quantile formulas too weak | explicit units, empirical capacity envelopes, multidimensional packing, bounded local search, exact oracle | optimality-gap and invariant reports |
| Control loop | command-oriented execution | durable state machine, preconditions, staleness checks, reconciliation | crash/unknown-outcome tests |
| Safety engineering | “predicted p99” undefined | profile-derived SLO envelope, demand UCB, N-1 check, hard constraints | verifier/property/failure evidence |
| Failure engineering | missing race/crash classes | controller crash, DB/action ambiguity, concurrency, policy mutation, PDB blockage, clock jump | automated chaos matrix |
| Benchmark methodology | weak threshold baseline | HPA-like baseline + offline lookahead reference + fairness protocol | repeated comparable benchmark suite |
| Reproducibility | hash mistaken for replay input | full content-addressed snapshots + versioned policy/profile/catalog/model/build | exact logical replay from saved run |
| Security | largely absent | authn/authz, least privilege, redaction, integrity, audit, threat model | security regression suite |
| Production realism | generic move/drain semantics | eviction/recreation, PDB, topology, readiness, progressive node drain | local Kubernetes E2E |
| Resume/interview strength | good but generic | falsifiable claims, exact-oracle quality, crash-safe reconciliation, measured envelopes | tagged evidence-backed resume bullets |

## Mandatory review questions before implementation freeze

### Model validity

- Can every quantity in the optimizer be assigned a unit?
- Can every destructive recommendation name the capacity profile that justifies it?
- Does the system distinguish observed usage from demand and capacity?
- Is extrapolation outside measured capacity envelopes rejected or explicitly conservative?

### Safety

- Can a planner bug be caught by the independent verifier?
- Can a stale plan execute after another controller/user changes state?
- Can a node drain bypass a PDB?
- Does a healthy final placement still remain recoverable under required N-1 failure?
- Does prediction ever become the sole evidence for destructive scale-in?

### Distributed systems

- What happens if an external mutation succeeds but the response is lost?
- What happens if the controller dies after each durable/side-effect boundary?
- How is duplicate execution distinguished from unknown outcome?
- Can two controllers mutate the same conflict domain?

### Measurement

- Are p99 values derived from aggregatable distributions rather than averaged quantiles?
- Are low-sample tail metrics marked low-confidence?
- Are simulator saturation/startup models calibrated against real services?

### Benchmark fairness

- Is the serious reactive baseline stabilized and readiness-aware?
- Do all policies share traces, delays and safety floors?
- Is there a reference/oracle showing how far FleetRight remains from ideal?
- Are losing scenarios reported?

### Security

- Can an anonymous request execute a plan?
- Does the Kubernetes credential have more access than required?
- Can a malformed/tampered snapshot become an action?
- Can secrets appear in persisted evidence?

## Architecture freeze criterion

Do not start large Phase-4 implementation work until every question above has an explicit answer in code/contracts/tests or a documented v1 exclusion.
