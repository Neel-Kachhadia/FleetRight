# ADR-001: Deterministic planner

Status: Accepted

The planner uses canonical protobuf snapshots, stable identifier tie-breaks, fixed-point capacity
factors, and a size-scaled deterministic neighborhood budget. It does not use a wall-clock search
deadline. Identical snapshot/policy/build inputs therefore produce identical plans and replay hashes.
The bounded greedy search is a heuristic; the small exact oracle measures its gap where both models
support the same constraints.
