# 16. Security and Threat Model

## Security objective

FleetRight can mutate compute capacity. The security goal is to prevent an unauthorized or malformed request from becoming infrastructure mutation and to ensure evidence/audit records do not leak secrets.

This is a local portfolio controller, not a claim of enterprise zero-trust compliance. The design still demonstrates the correct production instincts.

## Assets

Protect:

- execution authority;
- policies/SLOs;
- Kubernetes credentials;
- decision journal integrity;
- workload metadata;
- benchmark evidence;
- service secrets accidentally present in environment/telemetry.

## Trust boundaries

```text
user/client
    -> HTTP API authentication/authorization
    -> control plane
    -> planner/verifier process boundary
    -> PostgreSQL/artifact store
    -> Docker/Kubernetes adapter
    -> target cluster
```

Prometheus and workload telemetry are **inputs**, not trusted commands.

## Threats and controls

### Unauthorized plan execution

Controls:

- mutating endpoints require authentication;
- role-based authorization separates viewer, recommender/operator and admin;
- approval/execution identities are journaled;
- execute mode cannot start with anonymous mutation enabled.

### Credential theft/leak

Controls:

- Kubernetes service account with minimal verbs/resources;
- no cluster-admin requirement;
- credentials loaded from environment/file/secret mechanisms, never committed;
- secret patterns redacted from structured logs/journal payloads;
- benchmark artifacts exclude process environments by default.

### Malformed policy/snapshot/profile

Controls:

- strict schema validation;
- numeric bounds;
- unknown safety-critical enums fail closed;
- content hash validation for immutable artifacts;
- parser fuzzing;
- size limits for API payloads.

### Replay/tampering

Controls:

- stable plan/action IDs;
- idempotency keys;
- artifact hashes;
- append-audited state transitions;
- run manifests link hashes of input evidence.

### API exposure

Defaults:

- bind loopback for local development unless explicitly configured;
- TLS required if exposed beyond a trusted local network;
- rate limits/request size limits on mutating endpoints;
- health endpoints expose no secrets.

### Privilege escalation through Kubernetes adapter

Apply **least privilege**: the adapter receives only the permissions required for supported v1 operations, such as reading workloads/nodes/PDBs and changing scale/eviction/cordon state as explicitly needed. Destructive operations outside the supported scope are not granted merely for convenience.

## Availability/security interaction

If authentication, authorization, verifier integrity or durable journal persistence is unavailable, FleetRight does not initiate new destructive mutations.

Emergency/recovery scale-out may be separately policy-authorized because adding capacity can be safer than blocking recovery, but such a path must still be authenticated and audited.

## Audit record

For each mutation:

```text
who
authenticated_role
request_id
idempotency_key
plan_id
action_id
base_snapshot_id
verifier_result_hash
policy_version
requested_at
executed_at
observed_result
```

## Security testing

- unauthenticated mutating request -> denied;
- viewer attempting execute -> denied;
- expired/invalid credential -> denied;
- tampered snapshot/profile hash -> rejected;
- oversized/malformed request -> bounded failure;
- secret-like fixture -> redacted from logs/report;
- Kubernetes service account cannot perform unrelated privileged operations;
- audit event exists for every attempted mutation, including denied attempts where appropriate.

## Supply-chain hygiene

- lock/pin important dependencies where practical;
- record container image digests for release evidence;
- run dependency/security scanning in CI;
- avoid executing downloaded scripts in release reproduction;
- document compiler/runtime versions.

## Explicit limitations

FleetRight v1 does not attempt:

- multi-tenant hostile isolation;
- hardware root of trust;
- signed policy distribution;
- enterprise identity federation;
- internet-facing production deployment certification.

Those are exclusions, not silently implied guarantees.
