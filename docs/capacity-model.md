# Capacity and SLO model

Capacity is an empirical joint operating envelope for one workload build, request mix, resource
allocation, runtime configuration class, and platform. Every safe point retains offered QPS,
concurrency, p99 bucket upper bound, threshold-success and request counts, raw error count, Wilson
latency lower bound, Wilson error upper bound, queue growth, working-set pressure, throttling,
run dispersion, variance, and histogram configuration.

For an SLO `latency <= T`, the point records the number of requests at or below `T` and requires a
95% Wilson lower bound of at least 0.99. Error rate uses the corresponding Wilson upper bound.
Queue growth must be nonpositive; p99 upper bucket, memory pressure, throttling, and across-run
dispersion must remain within the workload policy. Two or more repeated runs are required. Capacity
is the minimum sustainable throughput across the repeated runs after the configured haircut.

Planning applies three explicit, bounded PPM factors: profile confidence, recent model accuracy,
and degraded-cluster reserve. C++ multiplies all factors in a wide integer and rounds down once.
The selected profile point must cover both demand and concurrency; CPU percentage is never treated
as capacity evidence. Stale, low-confidence, mismatched, unconstrained, inconsistent, or out-of-region
profiles cannot authorize destructive changes.

The planner records the selected profile-point index and binds reported effective QPS/concurrency,
uncertainty exposure, and the max of QPS/concurrency utilization to that same point. The verifier
recomputes these values from the immutable profile. Boundary exposure contributes a bounded
uncertainty penalty to the weighted objective; the safe profile and replica-count checks remain
hard constraints. A minimum-improvement gate applies only when the current placement is itself
demonstrably within profile capacity. It cannot suppress a capacity repair for an overloaded current
state or turn missing evidence into proof of safety.

The planner ranks alternate profiled shapes with separable churn, uncertainty, and boundary penalties,
then rebuilds and scores a bounded number of complete plans. It keeps an alternate only when its
independently verifiable hard constraints pass and its full weighted objective improves. The bounded
search evaluates one-profile substitutions, not their Cartesian combinations; reports therefore do
not claim global optimality outside the exact small-instance oracle.

The workload containers expose classic Prometheus histograms with a bucket at each configured SLO
threshold. Service p99 is derived from pooled bucket counts and never from an average of per-instance
p99 values. Samples below the tail-confidence minimum are marked low-confidence. Rejected requests
are included in latency histogram counts.

`scripts/characterize_capacity.sh` warms each container, steps concurrency through a fixed sequence,
repeats every point, retains raw run measurements, and emits an immutable profile only for safe
points. It does not infer Docker resource enforcement from host utilization. Current-host calibration
status and any blocker are recorded in each run manifest.
