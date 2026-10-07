CREATE TABLE control_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    snapshot_id TEXT NOT NULL,
    snapshot_generation BIGINT NOT NULL CHECK (snapshot_generation >= 0),
    binding JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE artifacts (
    sha256 CHAR(64) PRIMARY KEY,
    kind TEXT NOT NULL,
    content BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

ALTER TABLE control_state ADD COLUMN snapshot_artifact_sha256 CHAR(64) REFERENCES artifacts(sha256);

CREATE TABLE plans (
    plan_id TEXT PRIMARY KEY,
    created_at_ms BIGINT NOT NULL,
    expires_at_ms BIGINT NOT NULL,
    snapshot_id TEXT NOT NULL,
    snapshot_generation BIGINT NOT NULL,
    policy_version TEXT NOT NULL,
    policy_hash CHAR(64) NOT NULL,
    profile_hashes JSONB NOT NULL,
    machine_catalog_hash CHAR(64) NOT NULL,
    forecast_metadata JSONB NOT NULL,
    planner_build_id TEXT NOT NULL,
    verifier_build_id TEXT NOT NULL,
    verification_hash CHAR(64) NOT NULL,
    verification_accepted BOOLEAN NOT NULL,
    admission_accepted BOOLEAN NOT NULL,
    reason_codes JSONB NOT NULL,
    binding JSONB NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('PROPOSED', 'VERIFIED', 'APPROVED', 'PAUSED', 'STALE', 'REJECTED', 'CANCELLED')),
    state_version INTEGER NOT NULL DEFAULT 0 CHECK (state_version >= 0),
    created_by TEXT NOT NULL
);

CREATE TABLE plan_artifacts (
    plan_id TEXT NOT NULL REFERENCES plans(plan_id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    artifact_sha256 CHAR(64) NOT NULL REFERENCES artifacts(sha256),
    PRIMARY KEY (plan_id, role)
);

CREATE TABLE plan_transitions (
    transition_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    plan_id TEXT NOT NULL REFERENCES plans(plan_id),
    old_state TEXT NOT NULL,
    new_state TEXT NOT NULL,
    state_version INTEGER NOT NULL,
    actor TEXT NOT NULL,
    role TEXT NOT NULL,
    reason TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    request_id TEXT NOT NULL,
    idempotency_key TEXT
);

CREATE TABLE action_intents (
    plan_id TEXT NOT NULL REFERENCES plans(plan_id) ON DELETE CASCADE,
    intent_id TEXT NOT NULL,
    action_type TEXT NOT NULL,
    workload_id TEXT,
    payload JSONB NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PLANNED', 'APPROVED', 'CANCELLED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (plan_id, intent_id)
);

CREATE TABLE idempotency_keys (
    principal TEXT NOT NULL,
    operation TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_sha256 CHAR(64) NOT NULL,
    response_status SMALLINT,
    response JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (principal, operation, idempotency_key),
    CHECK ((response_status IS NULL) = (response IS NULL))
);

CREATE TABLE conflict_leases (
    conflict_key TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    principal TEXT NOT NULL,
    fencing_token BIGINT NOT NULL CHECK (fencing_token > 0),
    expires_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE plan_leases (
    plan_id TEXT NOT NULL REFERENCES plans(plan_id) ON DELETE CASCADE,
    conflict_key TEXT NOT NULL REFERENCES conflict_leases(conflict_key),
    owner_id TEXT NOT NULL,
    fencing_token BIGINT NOT NULL,
    PRIMARY KEY (plan_id, conflict_key)
);

CREATE TABLE audit_events (
    audit_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    actor TEXT NOT NULL,
    role TEXT NOT NULL,
    request_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    plan_id TEXT,
    snapshot_id TEXT,
    idempotency_key TEXT,
    before_state TEXT,
    after_state TEXT,
    result TEXT NOT NULL,
    reason TEXT NOT NULL
);

CREATE INDEX audit_events_time_idx ON audit_events (occurred_at DESC, audit_id DESC);
CREATE INDEX plans_state_created_idx ON plans (state, created_at_ms DESC);
