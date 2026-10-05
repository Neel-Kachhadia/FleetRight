# FleetRight — SLO-Constrained Autonomous Capacity Optimizer

FleetRight is a production-style infrastructure control system for **stateless replicated services**. It observes live demand, learns empirically validated service-capacity envelopes, forecasts near-term load, proposes lower-cost resource and placement plans, independently verifies safety invariants, and applies approved changes through a crash-safe reconciler.

The core problem is not “autoscale when CPU is high.” It is:

> **Find the lowest-cost feasible cluster state that preserves latency, availability, disruption, and failure-tolerance constraints under uncertain demand and partial failure.**

FleetRight is intentionally scoped so it can be built and proven locally without paid cloud infrastructure while still exercising real systems, optimization, reliability, and control-plane engineering.

## Core objective

FleetRight treats SLO and safety requirements as **hard feasibility constraints**, not soft penalties.

```text
minimize
    infrastructure_cost
  + migration_cost
  + actuation_churn_cost
  + uncertainty_margin_cost
  + near_boundary_risk_cost

subject to
    predicted_p99_upper_bound <= latency_slo
    predicted_error_rate_upper_bound <= error_slo
    queue_growth_upper_bound <= 0 at planned peak
    cpu_headroom >= policy_reserve
    memory_headroom >= policy_reserve
    replica_count >= availability_floor
    topology / anti-affinity constraints satisfied
    disruption_budget respected
    migration_budget respected
    required N-1 feasibility satisfied
    telemetry and profile confidence >= action threshold
```

An SLO violation does not become acceptable because it is “cheap.” If a hard constraint fails, the plan is rejected.

## What makes FleetRight different

The project is not positioned as merely a predictive autoscaler. Predictive scaling already exists in production systems. FleetRight’s differentiator is the **joint optimization and verification loop** across:

- vertical rightsizing;
- horizontal replica planning;
- multi-resource placement;
- node consolidation;
- predictive scale-out;
- failure reserve;
- disruption-aware execution;
- deterministic evidence and replay.

The optimizer is deliberately separated from a simpler, independent verifier. The planner may be complex; the verifier must be small enough to trust.

## Safety model in one sentence

```text
measure -> build immutable snapshot -> forecast -> plan -> independently verify
       -> journal -> reconcile progressively -> observe -> commit or compensate
```

## Mandatory implementation boundary

Mandatory:

- Linux;
- C++20 optimization core;
- Python control plane and experiment tooling;
- Docker Compose synthetic workloads;
- Prometheus telemetry;
- PostgreSQL durable control state;
- deterministic event-driven simulator;
- local Kubernetes integration using kind or k3d;
- reproducible benchmark and failure suite.

Optional:

- provider pricing importers;
- public cloud execution adapters;
- Grafana demo dashboards.

Explicitly out of scope for v1:

- stateful/quorum workloads;
- persistent-volume relocation;
- GPU scheduling;
- reinforcement learning;
- generic FinOps dashboards;
- multi-cluster federation;
- LLM-controlled execution.

## Five implementation phases

1. **Measurement foundation** — workloads, simulator, capacity/SLO profiling, telemetry and serious baselines.
2. **Optimization core** — rightsizing, replicas, placement, consolidation, exact-oracle comparison.
3. **Prediction and planning** — forecast uncertainty, safe predictive scale-out, offline capacity planning.
4. **Safe control plane** — independent verification, versioned plans, reconciliation, progressive execution and compensation.
5. **Validation and release** — adversarial failures, security, soak tests, statistical benchmarks and reproducibility evidence.

## Documentation map

- `01_PROBLEM_AND_GOALS.md`
- `02_SYSTEM_ARCHITECTURE.md`
- `03_TECH_STACK.md`
- `04_DOMAIN_MODEL_AND_ALGORITHMS.md`
- `05_PHASE_1_BASELINE_AND_SIMULATOR.md`
- `06_PHASE_2_RIGHTSIZING_AND_PLACEMENT.md`
- `07_PHASE_3_PREDICTIVE_SCALING.md`
- `08_PHASE_4_SAFE_CONTROL_PLANE.md`
- `09_PHASE_5_VALIDATION_AND_RELEASE.md`
- `10_TESTING_AND_FAILURE_INJECTION.md`
- `11_BENCHMARKING_AND_METRICS.md`
- `12_REPOSITORY_STRUCTURE.md`
- `13_API_AND_DATA_CONTRACTS.md`
- `14_RESUME_AND_INTERVIEW_EVIDENCE.md`
- `15_DEFINITION_OF_DONE.md`
- `16_SECURITY_AND_THREAT_MODEL.md`
- `17_CAPACITY_AND_SLO_MODEL.md`
- `18_ARCHITECTURE_VALIDATION_MATRIX.md`
- `SOURCES_AND_REFERENCES.md`

## Engineering standard

FleetRight is considered successful only when an independent engineer can:

1. clone the repository from a clean machine;
2. reproduce at least one complete benchmark suite;
3. inspect the immutable input snapshot for a decision;
4. recompute or verify every hard safety invariant;
5. reproduce the selected plan from the same versioned inputs;
6. inject failures and observe fail-safe behavior;
7. challenge every headline number using preserved raw evidence.
