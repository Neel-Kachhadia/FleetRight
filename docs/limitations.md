# Current limitations

- Docker calibration is blocked on hosts without an accessible Docker daemon. Synthetic profiles
  verify planner integration only; they do not establish real service capacity.
- The simulator is a per-workload model. It does not reproduce kernel scheduling, real network
  contention, node failure timing distributions, or container runtime behavior. Docker calibration
  is required before using it to support service-capacity claims.
- The optimizer uses deterministic greedy multidimensional placement, a bounded single-replica
  neighborhood, and a bounded portfolio of single-profile alternatives. It does not provide a
  general global optimum or evaluate every cross-workload profile combination; the exact oracle is
  limited to small, single-workload, non-N-1 cases.
- C++ planner/verifier code is single-threaded. TSan is not applicable until concurrency is added.
- The fixed-horizon objective supports movement, churn, uncertainty, and capacity-boundary costs,
  but the checked-in coefficient defaults are synthetic. Operators must calibrate policy weights
  against their own economic and operational costs before using them outside this benchmark.
- Capacity characterization currently covers the three checked-in workload shapes at one
  resource allocation per service. Additional allocations require new Docker measurements.
- Python protobuf generated runtime bindings and Hypothesis are not installed in this offline host;
  Python boundary encoding uses `protoc`, and property checks use a fixed-seed standard-library
  corpus. Ruff lint/format is in CI. Mypy was evaluated locally but reports unresolved dynamic JSON
  and protobuf-contract typing in seven modules, so it is not represented as a passing CI check.

Prompt 2 forecasting, PostgreSQL control state, Kubernetes reconciliation, live mutation, and
production deployment remain deferred.
