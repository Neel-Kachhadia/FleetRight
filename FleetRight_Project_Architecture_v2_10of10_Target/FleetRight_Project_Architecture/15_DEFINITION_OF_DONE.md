# 15. Definition of Done

FleetRight is complete only when every mandatory gate below is satisfied on a tagged release.

## Scope and model gates

- [ ] v1 is explicitly limited to stateless replicated services;
- [ ] canonical CPU/memory/network/demand/latency units are explicit;
- [ ] capacity/SLO profile schema is implemented;
- [ ] low-confidence or stale profiles block destructive optimization;
- [ ] latency aggregation uses histograms correctly;
- [ ] simulator nonlinear saturation assumptions are documented and calibrated.

## Functional gates

- [ ] immutable snapshot builder;
- [ ] static baseline;
- [ ] naive threshold baseline;
- [ ] HPA-like serious reactive baseline;
- [ ] deterministic simulator;
- [ ] capacity characterization harness;
- [ ] C++ rightsizer/replica planner;
- [ ] constraint-aware placement;
- [ ] consolidation planner;
- [ ] exact small-instance benchmark oracle;
- [ ] forecasting with upper-bound uncertainty;
- [ ] independent verifier;
- [ ] shadow mode;
- [ ] progressive reconciler;
- [ ] compensation path;
- [ ] durable decision journal;
- [ ] deterministic logical replay;
- [ ] Kubernetes adapter.

## Safety/correctness gates

- [ ] no verifier-accepted capacity overflow in property corpus;
- [ ] no min-replica violation;
- [ ] topology rules verified;
- [ ] N-1 requirement verified where enabled;
- [ ] stale snapshot/plan rejected;
- [ ] policy/profile/catalog version mismatch rejected;
- [ ] scale-down blocked on insufficient telemetry;
- [ ] prediction alone cannot aggressively scale in;
- [ ] PDB-constrained disruption is respected in Kubernetes test;
- [ ] intermediate consolidation states verified;
- [ ] action preconditions rechecked before mutation;
- [ ] unknown executor outcome reconciled before retry;
- [ ] duplicate request produces no duplicate logical side effect;
- [ ] concurrent controller race permits one owner;
- [ ] compensation recovers induced regression within measured objective for supported scenario.

## Security gates

- [ ] mutating APIs authenticated;
- [ ] authorization roles enforced;
- [ ] audit identity recorded;
- [ ] default bind/config does not expose unauthenticated execute mode;
- [ ] Kubernetes RBAC is least privilege for supported actions;
- [ ] secrets are redacted from journal/logs;
- [ ] malformed/untrusted policy and snapshot input validated;
- [ ] dependency/vulnerability scan documented;
- [ ] security limitations documented.

## Quality gates

- [ ] C++ warnings gate clean;
- [ ] clang-format/clang-tidy clean to configured policy;
- [ ] ASan clean;
- [ ] UBSan clean;
- [ ] targeted TSan clean;
- [ ] Python lint/type/test clean;
- [ ] fuzz smoke passes with saved corpus;
- [ ] CI passes from clean checkout;
- [ ] ADRs match implementation.

## Benchmark gates

- [ ] static vs naive vs HPA-like vs FleetRight reactive vs predictive report;
- [ ] offline lookahead/reference result for supported scenarios;
- [ ] small-instance optimizer optimality-gap report;
- [ ] multiple workload shapes;
- [ ] predefined multi-seed protocol where stochastic;
- [ ] median + dispersion reported;
- [ ] optimizer and verifier latency measured;
- [ ] cost accounting validated independently;
- [ ] SLO impact quantified from aggregate latency distributions;
- [ ] scaling/disruption churn quantified;
- [ ] recovery time measured;
- [ ] raw evidence preserved.

## Failure gates

- [ ] node loss;
- [ ] telemetry loss/partial metrics;
- [ ] flash crowd;
- [ ] forecast miss;
- [ ] capacity-profile staleness;
- [ ] executor timeout after possible success;
- [ ] controller crash at action boundaries;
- [ ] database restart;
- [ ] concurrent controller race;
- [ ] external state mutation causing stale plan;
- [ ] PDB-blocked drain;
- [ ] topology change;
- [ ] policy update mid-plan;
- [ ] compensation after SLO regression;
- [ ] malformed cost catalog;
- [ ] corrupted replay artifact.

## Reproducibility gates

- [ ] snapshot content needed for replay is retained/content-addressed, not only hashed;
- [ ] policy/profile/catalog/forecast/build versions included;
- [ ] container/image versions included;
- [ ] exact logical replay test passes;
- [ ] release benchmark command produces full manifest/report from clean checkout.

## Portfolio gates

- [ ] README explains business problem and safety model before tech stack;
- [ ] architecture diagram shows planner/verifier/reconciler separation;
- [ ] benchmark table contains serious baseline and oracle/reference;
- [ ] cost/SLO tradeoff plot;
- [ ] capacity/SLO envelope plot;
- [ ] failure timeline plot for one compensation case;
- [ ] one-command demo;
- [ ] limitations section is explicit;
- [ ] resume bullets use only tagged measured numbers.

## Final standard

The project is release-ready when a skeptical engineer can reproduce the evidence, understand the model assumptions, force adverse failures, and still find that FleetRight either maintains its declared safety constraints or explicitly fails closed.
