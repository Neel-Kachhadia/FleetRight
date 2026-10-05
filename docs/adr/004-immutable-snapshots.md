# ADR-004: Immutable snapshots and evidence

Status: Accepted

Snapshots, machine catalogs, profiles, traces, plans, and verifier results are content-hashed.
Evaluation time is fixed inside the snapshot, and profile freshness is checked against that time.
Measurement and benchmark runs use unique append-only IDs and retain the raw evidence needed to
recompute derived statistics.
