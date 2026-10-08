ALTER TABLE plans DROP CONSTRAINT plans_state_check;
ALTER TABLE plans ADD CONSTRAINT plans_state_check
    CHECK (state IN (
        'PROPOSED', 'VERIFIED', 'APPROVED', 'EXECUTING', 'VERIFYING', 'COMMITTED',
        'COMPENSATING', 'COMPENSATED', 'FAILED_SAFE', 'PAUSED', 'STALE', 'REJECTED',
        'CANCELLED'
    ));

ALTER TABLE action_intents ADD COLUMN action_sequence INTEGER;
-- ponytail: legacy insertion order was not stored; retain the previous stable API ordering.
WITH numbered AS (
    SELECT plan_id, intent_id,
        row_number() OVER (PARTITION BY plan_id ORDER BY intent_id) - 1 AS sequence
    FROM action_intents
)
UPDATE action_intents ai SET action_sequence = numbered.sequence
FROM numbered WHERE numbered.plan_id = ai.plan_id AND numbered.intent_id = ai.intent_id;
ALTER TABLE action_intents ALTER COLUMN action_sequence SET NOT NULL;
ALTER TABLE action_intents ADD CONSTRAINT action_intents_sequence_nonnegative
    CHECK (action_sequence >= 0);
CREATE UNIQUE INDEX action_intents_sequence_idx
    ON action_intents (plan_id, action_sequence);

ALTER TABLE action_intents ADD COLUMN execution_state TEXT NOT NULL DEFAULT 'NOT_STARTED'
    CHECK (execution_state IN ('NOT_STARTED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'OUTCOME_UNKNOWN'));
ALTER TABLE action_intents ADD COLUMN execution_started_at TIMESTAMPTZ;
ALTER TABLE action_intents ADD COLUMN execution_finished_at TIMESTAMPTZ;
ALTER TABLE action_intents ADD COLUMN execution_result JSONB;
ALTER TABLE action_intents ADD COLUMN execution_error TEXT;
ALTER TABLE action_intents ADD COLUMN attempt_number INTEGER NOT NULL DEFAULT 0
    CHECK (attempt_number >= 0);
ALTER TABLE action_intents ADD COLUMN compensates_intent_id TEXT;
ALTER TABLE action_intents ADD CONSTRAINT action_intents_compensates_intent_fk
    FOREIGN KEY (plan_id, compensates_intent_id)
    REFERENCES action_intents (plan_id, intent_id) ON DELETE CASCADE;
CREATE UNIQUE INDEX action_intents_compensation_once_idx
    ON action_intents (plan_id, compensates_intent_id)
    WHERE compensates_intent_id IS NOT NULL;
ALTER TABLE action_intents ADD CONSTRAINT action_intents_execution_timestamps_check CHECK (
    (execution_state = 'NOT_STARTED' AND execution_started_at IS NULL AND execution_finished_at IS NULL)
    OR (execution_state = 'RUNNING' AND execution_started_at IS NOT NULL AND execution_finished_at IS NULL)
    OR (execution_state IN ('SUCCEEDED', 'FAILED', 'OUTCOME_UNKNOWN')
        AND execution_started_at IS NOT NULL AND execution_finished_at IS NOT NULL)
);
CREATE INDEX action_intents_execution_idx
    ON action_intents (plan_id, execution_state, action_sequence);

CREATE SEQUENCE execution_controller_fence_seq AS BIGINT START WITH 1;

CREATE TABLE plan_executions (
    plan_id TEXT PRIMARY KEY REFERENCES plans(plan_id) ON DELETE CASCADE,
    principal TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_sha256 CHAR(64) NOT NULL,
    status TEXT NOT NULL CHECK (status IN (
        'EXECUTING', 'VERIFYING', 'PAUSED', 'CANCELLED', 'COMMITTED', 'COMPENSATING',
        'COMPENSATED', 'FAILED_SAFE'
    )),
    controller_id TEXT NOT NULL,
    controller_fencing_token BIGINT NOT NULL CHECK (controller_fencing_token > 0),
    controller_lease_expires_at TIMESTAMPTZ NOT NULL,
    pending_controller_id TEXT,
    pending_controller_fencing_token BIGINT,
    pending_controller_lease_expires_at TIMESTAMPTZ,
    pause_requested BOOLEAN NOT NULL DEFAULT FALSE,
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    outcome JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at TIMESTAMPTZ,
    CHECK ((status IN ('COMMITTED', 'CANCELLED', 'COMPENSATED', 'FAILED_SAFE')) = (finished_at IS NOT NULL)),
    CHECK (NOT (pause_requested AND cancel_requested)),
    CHECK (
        (pending_controller_id IS NULL AND pending_controller_fencing_token IS NULL
            AND pending_controller_lease_expires_at IS NULL)
        OR (pending_controller_id IS NOT NULL
            AND pending_controller_fencing_token IS NOT NULL
            AND pending_controller_fencing_token > 0
            AND pending_controller_lease_expires_at IS NOT NULL)
    )
);

CREATE TABLE execution_takeover_fences (
    plan_id TEXT NOT NULL REFERENCES plan_executions(plan_id) ON DELETE CASCADE,
    intent_id TEXT NOT NULL,
    controller_fencing_token BIGINT NOT NULL CHECK (controller_fencing_token > 0),
    target_ref TEXT NOT NULL,
    receipt JSONB NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (plan_id, intent_id, controller_fencing_token),
    FOREIGN KEY (plan_id, intent_id) REFERENCES action_intents(plan_id, intent_id)
        ON DELETE CASCADE
);
