# 9. Phase 5 — Validation, Hardening and Release

## Goal

Demonstrate that FleetRight is safe under its declared model, measurably useful against serious baselines, reproducible, secure enough for a local controller, and honest about limitations.

## Mandatory benchmark scenarios

1. stable low utilization;
2. predictable daily peak;
3. sudden flash crowd;
4. CPU-dominant service;
5. memory-dominant service;
6. slow-starting service;
7. correlated multi-service surge;
8. node loss during consolidation;
9. stale/partial telemetry;
10. forecast underprediction;
11. noisy tenant;
12. cost-catalog change;
13. capacity-profile staleness;
14. controller restart mid-action;
15. policy update while a plan is pending.

## Policies compared

- static peak + reserve;
- naive threshold baseline;
- HPA-like target-tracking baseline;
- FleetRight reactive;
- FleetRight predictive;
- offline perfect-lookahead/reference policy where applicable.

All policies receive the same underlying workload trace and declared action/startup delays.

## Correctness evidence

- zero verifier-accepted invalid plans in regression/property corpus;
- deterministic replay for stored runs;
- plan expiry/staleness protection demonstrated;
- N-1 checks validated on hand-constructed and randomized cases;
- intermediate states checked, not only final state;
- compensation restores service within measured recovery objective in supported scenarios.

## Production-realism evidence

At least one end-to-end Kubernetes run demonstrates:

- HPA-like baseline behavior;
- FleetRight replica mutation;
- PDB-constrained eviction;
- topology spread;
- readiness/warm-up gating;
- node drain sequence;
- controller restart/reconciliation.

## Security evidence

Run the checklist in `16_SECURITY_AND_THREAT_MODEL.md` and include:

- authenticated mutating API tests;
- authorization/role tests;
- least-privilege Kubernetes manifest review;
- secret-redaction regression test;
- malformed policy/contract tests;
- audit-log integrity checks.

## Soak and chaos

Run an accelerated multi-hour soak with demand cycles, forecast misses, process restarts, node loss, telemetry gaps and policy changes. Memory usage, journal growth and state-machine progress must remain bounded/understood.

## Release command

A clean checkout exposes one top-level command, for example:

```text
./scripts/reproduce_release.sh
```

It emits:

```text
run-manifest.json
raw metrics / traces
normalized snapshots
capacity profile references
policy outputs
plan/verifier journals
summary.json
benchmark tables
plots
report.md
```

## Reproducibility levels

### Level 1 — exact logical replay

Same snapshot, policy/profile/catalog versions and build -> identical normalized plan/verifier result.

### Level 2 — experiment replay

Same trace/seed/config -> statistically equivalent measured outcome; wall-clock timing may vary.

### Level 3 — cross-host reproducibility

Headline results preserve direction and conclusions on another reasonable Linux host; performance timing is reported per host.

## Final release artifacts

- architecture docs;
- ADRs;
- benchmark methodology;
- failure model;
- security/threat model;
- limitations document;
- reproducibility guide;
- generated benchmark report;
- raw evidence manifest;
- demo script;
- release notes;
- measured resume bullets.

## Phase-5 exit gate

A release is complete only if another engineer can reproduce the benchmark, inspect why each destructive decision passed the verifier, intentionally break assumptions, and trace every result back to versioned raw evidence.
