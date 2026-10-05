# ADR-003: Independent verifier

Status: Accepted

The verifier consumes the immutable snapshot and proposed plan and recomputes hard constraints in
its own implementation. It shares only protobuf data types and low-level checked units/hash helpers
with the planner. It does not call planner placement or profile-feasibility functions. Adversarial
tests mutate planner outputs to ensure the verifier rejects inconsistencies.
