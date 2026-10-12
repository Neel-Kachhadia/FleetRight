from __future__ import annotations

import hashlib
import json
import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

MIGRATIONS = Path(__file__).resolve().parents[2] / "migrations"
STATES = {
    "PROPOSED",
    "VERIFIED",
    "APPROVED",
    "EXECUTING",
    "VERIFYING",
    "COMMITTED",
    "COMPENSATING",
    "COMPENSATED",
    "FAILED_SAFE",
    "PAUSED",
    "STALE",
    "REJECTED",
    "CANCELLED",
}
TRANSITIONS = {
    "PROPOSED": {"VERIFIED", "REJECTED", "STALE", "CANCELLED"},
    "VERIFIED": {"APPROVED", "PAUSED", "STALE", "REJECTED", "CANCELLED"},
    "APPROVED": {"EXECUTING", "PAUSED", "STALE", "CANCELLED"},
    "EXECUTING": {"VERIFYING", "COMPENSATING", "PAUSED", "CANCELLED", "FAILED_SAFE"},
    "VERIFYING": {"COMMITTED", "COMPENSATING", "PAUSED", "CANCELLED", "FAILED_SAFE"},
    "COMPENSATING": {"COMPENSATED", "FAILED_SAFE"},
    "COMMITTED": set(),
    "COMPENSATED": set(),
    "FAILED_SAFE": set(),
    "PAUSED": {"STALE", "CANCELLED"},
    "STALE": set(),
    "REJECTED": set(),
    "CANCELLED": set(),
}
EXECUTION_STATES = {
    "EXECUTING",
    "VERIFYING",
    "PAUSED",
    "CANCELLED",
    "COMMITTED",
    "COMPENSATING",
    "COMPENSATED",
    "FAILED_SAFE",
}
EXECUTION_TRANSITIONS = {
    "EXECUTING": {"VERIFYING", "COMPENSATING", "PAUSED", "CANCELLED", "FAILED_SAFE"},
    "VERIFYING": {"COMMITTED", "COMPENSATING", "PAUSED", "CANCELLED", "FAILED_SAFE"},
    "COMPENSATING": {"COMPENSATED", "FAILED_SAFE"},
    "PAUSED": {"CANCELLED"},
    "CANCELLED": set(),
    "COMMITTED": set(),
    "COMPENSATED": set(),
    "FAILED_SAFE": set(),
}
ACTION_EXECUTION_STATES = {"NOT_STARTED", "RUNNING", "SUCCEEDED", "FAILED", "OUTCOME_UNKNOWN"}
ACTION_EXECUTION_TRANSITIONS = {
    "NOT_STARTED": {"RUNNING"},
    "RUNNING": {"SUCCEEDED", "FAILED", "OUTCOME_UNKNOWN"},
    "OUTCOME_UNKNOWN": {"RUNNING", "SUCCEEDED", "FAILED"},
    "SUCCEEDED": set(),
    "FAILED": set(),
}


class ControlError(RuntimeError):
    pass


class IdempotencyConflict(ControlError):
    pass


class StaleState(ControlError):
    pass


class ArtifactIntegrityError(ControlError):
    pass


class LeaseConflict(ControlError):
    pass


class ExecutionConflict(ControlError):
    pass


def canonical_json(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def transition_allowed(old_state: str, new_state: str) -> bool:
    return old_state in STATES and new_state in TRANSITIONS[old_state]


class Repository:
    def __init__(self, database_url: str, migrations_dir: Path = MIGRATIONS):
        if not database_url:
            raise ValueError("database URL is required")
        self.database_url = database_url
        self.migrations_dir = migrations_dir

    @contextmanager
    def transaction(self) -> Iterator[psycopg.Connection]:
        with psycopg.connect(self.database_url, row_factory=dict_row) as conn:
            yield conn

    def migrate(self) -> str:
        files = sorted(self.migrations_dir.glob("[0-9]*.sql"))
        if not files:
            raise ControlError("no database migrations found")
        with self.transaction() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations "
                "(version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL "
                "DEFAULT clock_timestamp())"
            )
            for path in files:
                applied = conn.execute(
                    "SELECT 1 FROM schema_migrations WHERE version = %s", (path.name,)
                ).fetchone()
                if applied is None:
                    conn.execute(path.read_text(encoding="utf-8"))
                    conn.execute(
                        "INSERT INTO schema_migrations (version) VALUES (%s)", (path.name,)
                    )
        return files[-1].name

    def readiness(self) -> tuple[bool, str]:
        try:
            with self.transaction() as conn:
                row = conn.execute(
                    "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1"
                ).fetchone()
                expected = max(path.name for path in self.migrations_dir.glob("[0-9]*.sql"))
                if row is None or row["version"] != expected:
                    return False, "migrations-pending"
            return True, "ready"
        except psycopg.Error:
            return False, "database-unavailable"

    def current_state(self) -> dict[str, object] | None:
        with self.transaction() as conn:
            return conn.execute("SELECT * FROM control_state WHERE singleton").fetchone()

    def create_plan(
        self,
        conn: psycopg.Connection,
        plan: dict[str, object],
        artifact_contents: dict[str, tuple[str, bytes]],
        intents: list[dict[str, object]],
        actor: str,
        role: str,
        request_id: str,
        idempotency_key: str,
    ) -> None:
        conn.execute(
            "INSERT INTO plans (plan_id, created_at_ms, expires_at_ms, snapshot_id, "
            "snapshot_generation, "
            "policy_version, policy_hash, profile_hashes, machine_catalog_hash, forecast_metadata, "
            "planner_build_id, verifier_build_id, verification_hash, verification_accepted, "
            "admission_accepted, reason_codes, binding, state, state_version, created_by) "
            "VALUES (%(plan_id)s, %(created_at_ms)s, %(expires_at_ms)s, "
            "%(snapshot_id)s, "
            "%(snapshot_generation)s, %(policy_version)s, %(policy_hash)s, %(profile_hashes)s, "
            "%(machine_catalog_hash)s, %(forecast_metadata)s, %(planner_build_id)s, "
            "%(verifier_build_id)s, "
            "%(verification_hash)s, %(verification_accepted)s, "
            "%(admission_accepted)s, %(reason_codes)s, "
            "%(binding)s, 'PROPOSED', 0, %(created_by)s)",
            {
                **plan,
                "profile_hashes": Jsonb(plan["profile_hashes"]),
                "forecast_metadata": Jsonb(plan["forecast_metadata"]),
                "reason_codes": Jsonb(plan["reason_codes"]),
                "binding": Jsonb(plan["binding"]),
            },
        )
        for artifact_role, (kind, content) in artifact_contents.items():
            digest = self.store_artifact(conn, kind, content)
            conn.execute(
                "INSERT INTO plan_artifacts (plan_id, role, artifact_sha256) VALUES (%s, %s, %s)",
                (plan["plan_id"], artifact_role, digest),
            )
        for action_sequence, intent in enumerate(intents):
            conn.execute(
                "INSERT INTO action_intents (plan_id, intent_id, action_sequence, action_type, "
                "workload_id, payload, status) "
                "VALUES (%s, %s, %s, %s, %s, %s, 'PLANNED')",
                (
                    plan["plan_id"],
                    intent["intent_id"],
                    action_sequence,
                    intent["action_type"],
                    intent.get("workload_id"),
                    Jsonb(intent["payload"]),
                ),
            )
        new_state = (
            "VERIFIED"
            if plan["verification_accepted"] and plan["admission_accepted"]
            else "REJECTED"
        )
        reason = (
            "verified-and-admitted" if new_state == "VERIFIED" else ",".join(plan["reason_codes"])
        )
        self.transition_plan(
            conn,
            str(plan["plan_id"]),
            new_state,
            actor,
            role,
            reason or "verification-or-admission-failed",
            request_id,
            idempotency_key,
        )
        self.audit(
            conn,
            actor=actor,
            role=role,
            request_id=request_id,
            operation="optimize",
            plan_id=str(plan["plan_id"]),
            snapshot_id=str(plan["snapshot_id"]),
            idempotency_key=idempotency_key,
            before_state="PROPOSED",
            after_state=new_state,
            result="ACCEPTED" if new_state == "VERIFIED" else "REJECTED",
            reason=reason or "verification-or-admission-failed",
        )

    @staticmethod
    def stale_prior_plans(
        conn: psycopg.Connection,
        generation: int,
        actor: str,
        request_id: str,
        reason: str = "base-snapshot-generation-changed",
    ) -> None:
        rows = conn.execute(
            "SELECT plan_id FROM plans WHERE snapshot_generation < %s "
            "AND state IN ('VERIFIED', 'APPROVED', 'PAUSED') "
            "AND NOT EXISTS (SELECT 1 FROM plan_executions pe WHERE pe.plan_id = plans.plan_id "
            "AND pe.status IN ('EXECUTING', 'VERIFYING', 'COMPENSATING', 'PAUSED')) FOR UPDATE",
            (generation,),
        ).fetchall()
        for row in rows:
            plan_id = row["plan_id"]
            old_state, _ = Repository.transition_plan(
                conn, plan_id, "STALE", actor, "system", reason, request_id, None
            )
            leases = conn.execute(
                "SELECT conflict_key, owner_id, fencing_token FROM plan_leases WHERE plan_id = %s",
                (plan_id,),
            ).fetchall()
            for lease in leases:
                Repository.release_lease(
                    conn, lease["conflict_key"], lease["owner_id"], lease["fencing_token"]
                )
            conn.execute("DELETE FROM plan_leases WHERE plan_id = %s", (plan_id,))
            Repository.audit(
                conn,
                actor=actor,
                role="system",
                request_id=request_id,
                operation="mark-stale",
                plan_id=plan_id,
                before_state=old_state,
                after_state="STALE",
                result="STALE",
                reason=reason,
            )

    @staticmethod
    def stale_incompatible_plans(
        conn: psycopg.Connection,
        current_binding: dict[str, object],
        actor: str,
        request_id: str,
        reason: str = "base-snapshot-content-changed-without-generation-increment",
    ) -> None:
        rows = conn.execute(
            "SELECT plan_id, binding FROM plans WHERE state IN "
            "('VERIFIED', 'APPROVED', 'PAUSED') AND NOT EXISTS ("
            "SELECT 1 FROM plan_executions pe WHERE pe.plan_id = plans.plan_id "
            "AND pe.status IN ('EXECUTING', 'VERIFYING', 'COMPENSATING', 'PAUSED')) FOR UPDATE"
        ).fetchall()
        for row in rows:
            if row["binding"].get("source", {}) == current_binding:
                continue
            plan_id = row["plan_id"]
            old_state, _ = Repository.transition_plan(
                conn, plan_id, "STALE", actor, "system", reason, request_id, None
            )
            leases = conn.execute(
                "SELECT conflict_key, owner_id, fencing_token FROM plan_leases WHERE plan_id = %s",
                (plan_id,),
            ).fetchall()
            for lease in leases:
                Repository.release_lease(
                    conn, lease["conflict_key"], lease["owner_id"], lease["fencing_token"]
                )
            conn.execute("DELETE FROM plan_leases WHERE plan_id = %s", (plan_id,))
            Repository.audit(
                conn,
                actor=actor,
                role="system",
                request_id=request_id,
                operation="mark-stale",
                plan_id=plan_id,
                before_state=old_state,
                after_state="STALE",
                result="STALE",
                reason=reason,
            )

    @staticmethod
    def store_artifact(conn: psycopg.Connection, kind: str, content: bytes) -> str:
        digest = hashlib.sha256(content).hexdigest()
        conn.execute(
            "INSERT INTO artifacts (sha256, kind, content) VALUES (%s, %s, %s) "
            "ON CONFLICT (sha256) DO NOTHING",
            (digest, kind, content),
        )
        row = conn.execute("SELECT content FROM artifacts WHERE sha256 = %s", (digest,)).fetchone()
        if row is None or bytes(row["content"]) != content:
            raise ArtifactIntegrityError("content-address collision or corrupt artifact")
        return digest

    @staticmethod
    def load_artifacts(conn: psycopg.Connection, plan_id: str) -> dict[str, bytes]:
        rows = conn.execute(
            "SELECT pa.role, pa.artifact_sha256, a.content FROM plan_artifacts pa "
            "JOIN artifacts a ON a.sha256 = pa.artifact_sha256 "
            "WHERE pa.plan_id = %s ORDER BY pa.role",
            (plan_id,),
        ).fetchall()
        artifacts: dict[str, bytes] = {}
        for row in rows:
            body = bytes(row["content"])
            digest = row["artifact_sha256"].strip()
            if hashlib.sha256(body).hexdigest() != digest:
                raise ArtifactIntegrityError(f"artifact integrity check failed for {row['role']}")
            artifacts[row["role"]] = body
        return artifacts

    @staticmethod
    def bind_current_state(
        conn: psycopg.Connection,
        snapshot_id: str,
        generation: int,
        binding: dict[str, object],
        snapshot_artifact_sha256: str,
    ) -> bool:
        current = conn.execute(
            "SELECT snapshot_id, snapshot_generation, binding FROM control_state "
            "WHERE singleton FOR UPDATE"
        ).fetchone()
        if current is None:
            conn.execute(
                "INSERT INTO control_state (singleton, snapshot_id, snapshot_generation, "
                "binding, snapshot_artifact_sha256) "
                "VALUES (TRUE, %s, %s, %s, %s)",
                (snapshot_id, generation, Jsonb(binding), snapshot_artifact_sha256),
            )
            return True
        if generation < current["snapshot_generation"]:
            raise StaleState("submitted snapshot generation is older than current control state")
        if generation == current["snapshot_generation"]:
            if snapshot_id != current["snapshot_id"] or binding != current["binding"]:
                conn.execute(
                    "UPDATE control_state SET snapshot_id = %s, binding = %s, "
                    "snapshot_artifact_sha256 = %s, "
                    "updated_at = clock_timestamp() WHERE singleton",
                    (snapshot_id, Jsonb(binding), snapshot_artifact_sha256),
                )
                return False
            conn.execute(
                "UPDATE control_state SET snapshot_artifact_sha256 = %s, "
                "updated_at = clock_timestamp() "
                "WHERE singleton",
                (snapshot_artifact_sha256,),
            )
            return True
        conn.execute(
            "UPDATE control_state SET snapshot_id = %s, snapshot_generation = %s, "
            "binding = %s, "
            "snapshot_artifact_sha256 = %s, updated_at = clock_timestamp() WHERE singleton",
            (snapshot_id, generation, Jsonb(binding), snapshot_artifact_sha256),
        )
        return True

    @staticmethod
    def begin_idempotent(
        conn: psycopg.Connection,
        principal: str,
        operation: str,
        key: str,
        request: object,
    ) -> tuple[int, dict[str, object]] | None:
        digest = hashlib.sha256(canonical_json(request)).hexdigest()
        conn.execute(
            "INSERT INTO idempotency_keys (principal, operation, idempotency_key, request_sha256) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (principal, operation, key, digest),
        )
        row = conn.execute(
            "SELECT request_sha256, response_status, response FROM idempotency_keys "
            "WHERE principal = %s AND operation = %s AND idempotency_key = %s FOR UPDATE",
            (principal, operation, key),
        ).fetchone()
        if row is None:
            raise ControlError("idempotency claim disappeared")
        if row["request_sha256"].strip() != digest:
            raise IdempotencyConflict("idempotency key was already used with a different request")
        if row["response_status"] is not None:
            return int(row["response_status"]), row["response"]
        return None

    @staticmethod
    def finish_idempotent(
        conn: psycopg.Connection,
        principal: str,
        operation: str,
        key: str,
        status: int,
        response: dict[str, object],
    ) -> None:
        conn.execute(
            "UPDATE idempotency_keys SET response_status = %s, response = %s "
            "WHERE principal = %s AND operation = %s AND idempotency_key = %s",
            (status, Jsonb(response), principal, operation, key),
        )

    @staticmethod
    def transition_plan(
        conn: psycopg.Connection,
        plan_id: str,
        new_state: str,
        actor: str,
        role: str,
        reason: str,
        request_id: str,
        idempotency_key: str | None,
    ) -> tuple[str, int]:
        if new_state not in STATES:
            raise ControlError("unknown plan state")
        row = conn.execute(
            "SELECT state, state_version FROM plans WHERE plan_id = %s FOR UPDATE", (plan_id,)
        ).fetchone()
        if row is None:
            raise KeyError(plan_id)
        old_state = row["state"]
        if not transition_allowed(old_state, new_state):
            raise ControlError(f"illegal plan transition {old_state} -> {new_state}")
        version = row["state_version"] + 1
        conn.execute(
            "UPDATE plans SET state = %s, state_version = %s WHERE plan_id = %s",
            (new_state, version, plan_id),
        )
        conn.execute(
            "INSERT INTO plan_transitions (plan_id, old_state, new_state, state_version, "
            "actor, role, reason, request_id, idempotency_key) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                plan_id,
                old_state,
                new_state,
                version,
                actor,
                role,
                reason,
                request_id,
                idempotency_key,
            ),
        )
        return old_state, version

    @staticmethod
    def audit(
        conn: psycopg.Connection,
        *,
        actor: str,
        role: str,
        request_id: str,
        operation: str,
        result: str,
        reason: str,
        plan_id: str | None = None,
        snapshot_id: str | None = None,
        idempotency_key: str | None = None,
        before_state: str | None = None,
        after_state: str | None = None,
    ) -> None:
        conn.execute(
            "INSERT INTO audit_events (actor, role, request_id, operation, plan_id, snapshot_id, "
            "idempotency_key, before_state, after_state, result, reason) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                actor,
                role,
                request_id,
                operation,
                plan_id,
                snapshot_id,
                idempotency_key,
                before_state,
                after_state,
                result,
                reason,
            ),
        )

    @staticmethod
    def acquire_lease(
        conn: psycopg.Connection,
        conflict_key: str,
        owner_id: str,
        principal: str,
        ttl_ms: int,
    ) -> int | None:
        if not conflict_key or not owner_id or not principal or ttl_ms <= 0:
            raise ValueError("lease key, owner, principal and positive TTL are required")
        row = conn.execute(
            "INSERT INTO conflict_leases (conflict_key, owner_id, principal, "
            "fencing_token, expires_at) "
            "VALUES (%s, %s, %s, 1, clock_timestamp() + (%s * interval '1 millisecond')) "
            "ON CONFLICT (conflict_key) DO UPDATE SET owner_id = EXCLUDED.owner_id, "
            "principal = EXCLUDED.principal, fencing_token = conflict_leases.fencing_token + 1, "
            "expires_at = EXCLUDED.expires_at, updated_at = clock_timestamp() "
            "WHERE conflict_leases.owner_id = EXCLUDED.owner_id OR "
            "(conflict_leases.expires_at <= clock_timestamp() AND NOT EXISTS ("
            "SELECT 1 FROM plan_leases pl JOIN plan_executions pe USING (plan_id) "
            "WHERE pl.conflict_key = conflict_leases.conflict_key "
            "AND pl.owner_id = conflict_leases.owner_id "
            "AND pl.fencing_token = conflict_leases.fencing_token "
            "AND pe.status IN ('EXECUTING', 'VERIFYING', 'COMPENSATING', 'PAUSED'))) "
            "RETURNING fencing_token",
            (conflict_key, owner_id, principal, ttl_ms),
        ).fetchone()
        return None if row is None else int(row["fencing_token"])

    @staticmethod
    def release_lease(
        conn: psycopg.Connection, conflict_key: str, owner_id: str, token: int
    ) -> bool:
        result = conn.execute(
            "UPDATE conflict_leases SET expires_at = clock_timestamp(), "
            "updated_at = clock_timestamp() "
            "WHERE conflict_key = %s AND owner_id = %s AND fencing_token = %s",
            (conflict_key, owner_id, token),
        )
        return result.rowcount == 1

    @staticmethod
    def release_plan_leases(conn: psycopg.Connection, plan_id: str) -> None:
        leases = conn.execute(
            "SELECT conflict_key, owner_id, fencing_token FROM plan_leases "
            "WHERE plan_id = %s FOR UPDATE",
            (plan_id,),
        ).fetchall()
        for lease in leases:
            Repository.release_lease(
                conn, lease["conflict_key"], lease["owner_id"], lease["fencing_token"]
            )
        conn.execute("DELETE FROM plan_leases WHERE plan_id = %s", (plan_id,))

    @staticmethod
    def assert_current_plan_leases(conn: psycopg.Connection, plan_id: str) -> dict[str, int]:
        plan = conn.execute("SELECT binding FROM plans WHERE plan_id = %s", (plan_id,)).fetchone()
        if plan is None:
            raise KeyError(plan_id)
        expected = plan["binding"].get("conflict_keys", [])
        if not isinstance(expected, list) or any(not isinstance(key, str) for key in expected):
            raise ControlError("plan conflict keys are invalid")
        if len(expected) != len(set(expected)):
            raise ControlError("plan conflict keys are duplicated")
        held = conn.execute(
            "SELECT conflict_key, owner_id, fencing_token FROM plan_leases "
            "WHERE plan_id = %s ORDER BY conflict_key FOR UPDATE",
            (plan_id,),
        ).fetchall()
        if {row["conflict_key"] for row in held} != set(expected):
            raise LeaseConflict("plan approval leases do not match the plan")
        fences: dict[str, int] = {}
        for lease in held:
            current = conn.execute(
                "SELECT owner_id, fencing_token, expires_at > clock_timestamp() AS active "
                "FROM conflict_leases WHERE conflict_key = %s FOR UPDATE",
                (lease["conflict_key"],),
            ).fetchone()
            if (
                current is None
                or not current["active"]
                or current["owner_id"] != lease["owner_id"]
                or current["fencing_token"] != lease["fencing_token"]
            ):
                raise LeaseConflict("plan approval lease expired or lost its fencing token")
            fences[lease["conflict_key"]] = int(lease["fencing_token"])
        return fences

    @staticmethod
    def claim_execution(
        conn: psycopg.Connection,
        plan_id: str,
        principal: str,
        idempotency_key: str,
        request: object,
        controller_id: str,
        lease_ttl_ms: int,
    ) -> dict[str, object]:
        if not principal or not idempotency_key or not controller_id or lease_ttl_ms <= 0:
            raise ValueError(
                "principal, idempotency key, controller and positive lease TTL are required"
            )
        operation = f"execute:{plan_id}"
        replay = Repository.begin_idempotent(conn, principal, operation, idempotency_key, request)
        if replay is not None:
            return {"claimed": False, "response_status": replay[0], "response": replay[1]}

        plan = conn.execute(
            "SELECT state, expires_at_ms <= floor(extract(epoch FROM clock_timestamp()) * 1000) "
            "AS expired FROM plans WHERE plan_id = %s FOR UPDATE",
            (plan_id,),
        ).fetchone()
        if plan is None:
            raise KeyError(plan_id)
        request_digest = hashlib.sha256(canonical_json(request)).hexdigest()
        existing = conn.execute(
            "SELECT * FROM plan_executions WHERE plan_id = %s FOR UPDATE", (plan_id,)
        ).fetchone()
        if existing is not None:
            if (
                existing["principal"] != principal
                or existing["idempotency_key"] != idempotency_key
                or existing["request_sha256"].strip() != request_digest
            ):
                raise ExecutionConflict("plan already has a different execution claim")
            return {"claimed": False, "execution": existing}
        if plan["state"] != "APPROVED":
            raise ControlError("plan is not approved for execution")
        if plan["expired"]:
            raise StaleState("plan expired before execution")
        fencing_tokens = Repository.assert_current_plan_leases(conn, plan_id)
        execution = conn.execute(
            "INSERT INTO plan_executions (plan_id, principal, idempotency_key, request_sha256, "
            "status, controller_id, controller_fencing_token, controller_lease_expires_at) "
            "VALUES (%s, %s, %s, %s, 'EXECUTING', %s, "
            "nextval('execution_controller_fence_seq'), "
            "clock_timestamp() + (%s * interval '1 millisecond')) RETURNING *",
            (plan_id, principal, idempotency_key, request_digest, controller_id, lease_ttl_ms),
        ).fetchone()
        Repository._extend_execution_plan_leases(
            conn, plan_id, execution["controller_lease_expires_at"]
        )
        return {
            "claimed": True,
            "execution": execution,
            "fencing_tokens": fencing_tokens,
        }

    @staticmethod
    def prepare_execution_takeover(
        conn: psycopg.Connection, plan_id: str, controller_id: str, lease_ttl_ms: int
    ) -> int | None:
        if not controller_id or lease_ttl_ms <= 0:
            raise ValueError("controller and positive lease TTL are required")
        row = conn.execute(
            "SELECT *, controller_lease_expires_at <= clock_timestamp() AS expired, "
            "pending_controller_lease_expires_at > clock_timestamp() AS pending_active "
            "FROM plan_executions WHERE plan_id = %s FOR UPDATE",
            (plan_id,),
        ).fetchone()
        if row is None or row["status"] in {
            "COMMITTED",
            "CANCELLED",
            "COMPENSATED",
            "FAILED_SAFE",
        }:
            return None
        pending_owner = row["pending_controller_id"]
        if pending_owner is not None:
            if pending_owner == controller_id:
                pending = conn.execute(
                    "UPDATE plan_executions SET pending_controller_lease_expires_at = "
                    "clock_timestamp() + (%s * interval '1 millisecond'), "
                    "updated_at = clock_timestamp() WHERE plan_id = %s "
                    "RETURNING pending_controller_lease_expires_at",
                    (lease_ttl_ms, plan_id),
                ).fetchone()
                Repository._extend_execution_plan_leases(
                    conn, plan_id, pending["pending_controller_lease_expires_at"]
                )
                return int(row["pending_controller_fencing_token"])
            if row["pending_active"]:
                return None
        if not row["expired"]:
            return None
        token = int(
            conn.execute("SELECT nextval('execution_controller_fence_seq') AS token").fetchone()[
                "token"
            ]
        )
        conn.execute(
            "UPDATE plan_executions SET pending_controller_id = %s, "
            "pending_controller_fencing_token = %s, pending_controller_lease_expires_at = "
            "clock_timestamp() + (%s * interval '1 millisecond'), updated_at = clock_timestamp() "
            "WHERE plan_id = %s",
            (controller_id, token, lease_ttl_ms, plan_id),
        )
        expiry = conn.execute(
            "SELECT pending_controller_lease_expires_at FROM plan_executions WHERE plan_id = %s",
            (plan_id,),
        ).fetchone()["pending_controller_lease_expires_at"]
        Repository._extend_execution_plan_leases(conn, plan_id, expiry)
        return token

    @staticmethod
    def _extend_execution_plan_leases(
        conn: psycopg.Connection, plan_id: str, expiry: object
    ) -> None:
        count = conn.execute(
            "SELECT count(*) AS count FROM plan_leases WHERE plan_id = %s", (plan_id,)
        ).fetchone()["count"]
        updated = conn.execute(
            "UPDATE conflict_leases cl SET expires_at = GREATEST(cl.expires_at, %s), "
            "updated_at = clock_timestamp() FROM plan_leases pl "
            "WHERE pl.plan_id = %s AND cl.conflict_key = pl.conflict_key "
            "AND cl.owner_id = pl.owner_id AND cl.fencing_token = pl.fencing_token",
            (expiry, plan_id),
        )
        if updated.rowcount != count:
            raise LeaseConflict("execution lost a plan conflict lease")

    @staticmethod
    def assert_pending_execution_controller(
        conn: psycopg.Connection, plan_id: str, controller_id: str, fencing_token: int
    ) -> dict[str, object]:
        row = conn.execute(
            "SELECT *, pending_controller_lease_expires_at > clock_timestamp() AS pending_active "
            "FROM plan_executions WHERE plan_id = %s FOR UPDATE",
            (plan_id,),
        ).fetchone()
        if (
            row is None
            or row["pending_controller_id"] != controller_id
            or row["pending_controller_fencing_token"] != fencing_token
            or not row["pending_active"]
        ):
            raise LeaseConflict("execution takeover reservation expired or lost its fencing token")
        return row

    @staticmethod
    def fail_takeover_for_missing_target(
        conn: psycopg.Connection,
        plan_id: str,
        action_id: str,
        controller_id: str,
        fencing_token: int,
    ) -> None:
        execution = Repository.assert_pending_execution_controller(
            conn, plan_id, controller_id, fencing_token
        )
        if execution["status"] not in {"EXECUTING", "VERIFYING", "COMPENSATING"}:
            raise ControlError("execution cannot fail from its current phase")
        action = conn.execute(
            "SELECT execution_state FROM action_intents WHERE plan_id = %s "
            "AND intent_id = %s FOR UPDATE",
            (plan_id, action_id),
        ).fetchone()
        if action is None or action["execution_state"] not in {"RUNNING", "OUTCOME_UNKNOWN"}:
            raise ControlError("missing-target action is no longer unresolved")
        other = conn.execute(
            "SELECT 1 FROM action_intents WHERE plan_id = %s AND intent_id <> %s "
            "AND execution_state IN ('RUNNING', 'OUTCOME_UNKNOWN') LIMIT 1",
            (plan_id, action_id),
        ).fetchone()
        if other is not None:
            raise ControlError("other actions remain unresolved")
        conn.execute(
            "UPDATE action_intents SET execution_state = 'FAILED', "
            "execution_finished_at = clock_timestamp(), execution_error = 'target-not-found', "
            "execution_result = COALESCE(execution_result, '{}'::jsonb) || "
            "jsonb_build_object('target_observation', %s::jsonb) "
            "WHERE plan_id = %s AND intent_id = %s",
            (Jsonb({"exists": False}), plan_id, action_id),
        )
        transferred = conn.execute(
            "UPDATE plan_executions SET controller_id = pending_controller_id, "
            "controller_fencing_token = pending_controller_fencing_token, "
            "controller_lease_expires_at = pending_controller_lease_expires_at, "
            "pending_controller_id = NULL, pending_controller_fencing_token = NULL, "
            "pending_controller_lease_expires_at = NULL, updated_at = clock_timestamp() "
            "WHERE plan_id = %s AND pending_controller_id = %s "
            "AND pending_controller_fencing_token = %s RETURNING controller_fencing_token",
            (plan_id, controller_id, fencing_token),
        ).fetchone()
        if transferred is None:
            raise LeaseConflict("execution takeover reservation changed")
        Repository.transition_execution_and_plan(
            conn,
            plan_id,
            execution["status"],
            "FAILED_SAFE",
            controller_id,
            fencing_token,
            actor="fleetright-controller",
            role="controller",
            reason="target-deleted-during-controller-takeover",
            request_id=f"reconcile:{plan_id}",
            outcome={"reason": "target-not-found", "action_id": action_id},
        )
        Repository.release_plan_leases(conn, plan_id)

    @staticmethod
    def authorize_takeover_fence(
        conn: psycopg.Connection,
        plan_id: str,
        intent_id: str,
        controller_id: str,
        pending_token: int,
    ) -> dict[str, object]:
        action = conn.execute(
            "SELECT * FROM action_intents WHERE plan_id = %s AND intent_id = %s FOR UPDATE",
            (plan_id, intent_id),
        ).fetchone()
        if action is None:
            raise KeyError((plan_id, intent_id))
        Repository.assert_pending_execution_controller(conn, plan_id, controller_id, pending_token)
        execution_result = action["execution_result"]
        final_result = (
            execution_result.get("result") if isinstance(execution_result, dict) else None
        )
        completed_retirement = (
            action["action_type"] == "PREPARE_SOURCE_RETIREMENT"
            and action["execution_state"] == "SUCCEEDED"
            and isinstance(final_result, dict)
            and final_result.get("stage") == "RETIREMENT_COMPLETE"
        )
        if (
            action["execution_state"] not in {"RUNNING", "OUTCOME_UNKNOWN"}
            and not completed_retirement
        ):
            raise ControlError(
                "only unresolved or completed retirement actions need takeover fencing"
            )
        return action

    @staticmethod
    def record_execution_fence(
        conn: psycopg.Connection,
        plan_id: str,
        intent_id: str,
        controller_id: str,
        pending_token: int,
        target_ref: str,
        receipt: dict[str, object],
    ) -> dict[str, object]:
        if not target_ref or not isinstance(receipt, dict):
            raise ValueError("target reference and fence receipt are required")
        receipt = {"state": "COMPLETE", **receipt}
        if receipt["state"] not in {"PENDING", "COMPLETE"}:
            raise ValueError("fence receipt state is invalid")
        Repository.authorize_takeover_fence(conn, plan_id, intent_id, controller_id, pending_token)
        conn.execute(
            "INSERT INTO execution_takeover_fences "
            "(plan_id, intent_id, controller_fencing_token, target_ref, receipt) "
            "VALUES (%s, %s, %s, %s, %s) ON CONFLICT "
            "(plan_id, intent_id, controller_fencing_token) "
            "DO UPDATE SET receipt = EXCLUDED.receipt, recorded_at = clock_timestamp() "
            "WHERE execution_takeover_fences.target_ref = EXCLUDED.target_ref "
            "AND COALESCE(execution_takeover_fences.receipt->>'state', 'COMPLETE') <> 'COMPLETE'",
            (plan_id, intent_id, pending_token, target_ref, Jsonb(receipt)),
        )
        row = conn.execute(
            "SELECT * FROM execution_takeover_fences WHERE plan_id = %s AND intent_id = %s "
            "AND controller_fencing_token = %s",
            (plan_id, intent_id, pending_token),
        ).fetchone()
        if row is None or row["target_ref"] != target_ref:
            raise ExecutionConflict("target fence receipt changed for a reserved token")
        return row

    @staticmethod
    def execution_fence_receipt(
        conn: psycopg.Connection, plan_id: str, intent_id: str, fencing_token: int
    ) -> dict[str, object] | None:
        row = conn.execute(
            "SELECT receipt FROM execution_takeover_fences WHERE plan_id = %s "
            "AND intent_id = %s AND controller_fencing_token = %s",
            (plan_id, intent_id, fencing_token),
        ).fetchone()
        receipt = row["receipt"] if row is not None else None
        return receipt if isinstance(receipt, dict) else None

    @staticmethod
    def claim_execution_controller(
        conn: psycopg.Connection,
        plan_id: str,
        controller_id: str,
        pending_token: int,
        lease_ttl_ms: int,
    ) -> int:
        if not controller_id or pending_token <= 0 or lease_ttl_ms <= 0:
            raise ValueError(
                "controller, positive reserved token and positive lease TTL are required"
            )
        Repository.assert_pending_execution_controller(conn, plan_id, controller_id, pending_token)
        row = conn.execute(
            "SELECT controller_lease_expires_at <= clock_timestamp() AS expired, status "
            "FROM plan_executions WHERE plan_id = %s",
            (plan_id,),
        ).fetchone()
        if (
            row is None
            or not row["expired"]
            or row["status"] in {"COMMITTED", "CANCELLED", "COMPENSATED", "FAILED_SAFE"}
        ):
            raise LeaseConflict("old execution controller has not expired")
        unresolved = conn.execute(
            "SELECT ai.intent_id FROM action_intents ai LEFT JOIN execution_takeover_fences f "
            "ON f.plan_id = ai.plan_id AND f.intent_id = ai.intent_id "
            "AND f.controller_fencing_token = %s WHERE ai.plan_id = %s "
            "AND ai.execution_state IN ('RUNNING', 'OUTCOME_UNKNOWN') "
            "AND (f.intent_id IS NULL OR COALESCE(f.receipt->>'state', 'COMPLETE') <> 'COMPLETE')",
            (pending_token, plan_id),
        ).fetchone()
        if unresolved is not None:
            raise ControlError("all unresolved target actions must be fenced before takeover")
        result = conn.execute(
            "UPDATE plan_executions SET controller_id = pending_controller_id, "
            "controller_fencing_token = pending_controller_fencing_token, "
            "controller_lease_expires_at = clock_timestamp() + (%s * interval '1 millisecond'), "
            "pending_controller_id = NULL, pending_controller_fencing_token = NULL, "
            "pending_controller_lease_expires_at = NULL, updated_at = clock_timestamp() "
            "WHERE plan_id = %s AND pending_controller_id = %s "
            "AND pending_controller_fencing_token = %s RETURNING controller_fencing_token",
            (lease_ttl_ms, plan_id, controller_id, pending_token),
        ).fetchone()
        if result is None:
            raise LeaseConflict("execution takeover reservation changed")
        return int(result["controller_fencing_token"])

    @staticmethod
    def expire_execution_controller(
        conn: psycopg.Connection, plan_id: str, controller_id: str, fencing_token: int
    ) -> None:
        Repository.assert_execution_controller(conn, plan_id, controller_id, fencing_token)
        row = conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp(), "
            "updated_at = clock_timestamp() WHERE plan_id = %s AND controller_id = %s "
            "AND controller_fencing_token = %s RETURNING plan_id",
            (plan_id, controller_id, fencing_token),
        ).fetchone()
        if row is None:
            raise LeaseConflict("execution controller lease changed before yielding")

    @staticmethod
    def renew_execution_controller(
        conn: psycopg.Connection,
        plan_id: str,
        controller_id: str,
        fencing_token: int,
        lease_ttl_ms: int,
    ) -> bool:
        if not controller_id or fencing_token <= 0 or lease_ttl_ms <= 0:
            raise ValueError(
                "controller, positive fencing token and positive lease TTL are required"
            )
        try:
            Repository.assert_current_plan_leases(conn, plan_id)
        except LeaseConflict:
            return False
        result = conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = "
            "clock_timestamp() + (%s * interval '1 millisecond'), updated_at = clock_timestamp() "
            "WHERE plan_id = %s AND controller_id = %s AND controller_fencing_token = %s "
            "AND controller_lease_expires_at > clock_timestamp() "
            "AND pending_controller_id IS NULL",
            (lease_ttl_ms, plan_id, controller_id, fencing_token),
        )
        if result.rowcount != 1:
            return False
        expiry = conn.execute(
            "SELECT controller_lease_expires_at FROM plan_executions WHERE plan_id = %s",
            (plan_id,),
        ).fetchone()["controller_lease_expires_at"]
        Repository._extend_execution_plan_leases(conn, plan_id, expiry)
        return True

    @staticmethod
    def assert_execution_controller(
        conn: psycopg.Connection, plan_id: str, controller_id: str, fencing_token: int
    ) -> dict[str, object]:
        """Lock the controller claim through commit; target-side adapters still need fencing."""
        # ponytail: DB locks cannot fence target effects; adapters must honor fencing tokens.
        row = conn.execute(
            "SELECT *, controller_lease_expires_at > clock_timestamp() AS active "
            "FROM plan_executions WHERE plan_id = %s FOR UPDATE",
            (plan_id,),
        ).fetchone()
        if (
            row is None
            or row["controller_id"] != controller_id
            or row["controller_fencing_token"] != fencing_token
            or not row["active"]
        ):
            raise LeaseConflict("execution controller lost its fencing token")
        return row

    @staticmethod
    def transition_execution(
        conn: psycopg.Connection,
        plan_id: str,
        expected_state: str,
        new_state: str,
        controller_id: str,
        fencing_token: int,
        *,
        outcome: object = None,
    ) -> str:
        row = Repository.assert_execution_controller(conn, plan_id, controller_id, fencing_token)
        if row["status"] != expected_state:
            raise StaleState("execution state changed")
        if (
            expected_state not in EXECUTION_STATES
            or new_state not in EXECUTION_STATES
            or new_state not in EXECUTION_TRANSITIONS[expected_state]
        ):
            raise ControlError(f"illegal execution transition {expected_state} -> {new_state}")
        if new_state == "COMMITTED":
            incomplete = conn.execute(
                "SELECT 1 FROM action_intents WHERE plan_id = %s "
                "AND compensates_intent_id IS NULL AND execution_state <> 'SUCCEEDED' LIMIT 1",
                (plan_id,),
            ).fetchone()
            if incomplete is not None:
                raise ControlError("cannot commit while actions are incomplete")
        if new_state == "COMPENSATED":
            compensations = conn.execute(
                "SELECT count(*) AS total, "
                "count(*) FILTER (WHERE execution_state = 'SUCCEEDED') AS done "
                "FROM action_intents WHERE plan_id = %s AND compensates_intent_id IS NOT NULL",
                (plan_id,),
            ).fetchone()
            if (
                not compensations["total"]
                or compensations["done"] != compensations["total"]
                or outcome is None
            ):
                raise ControlError("compensation requires completed actions and observed recovery")
        if new_state == "FAILED_SAFE":
            unresolved = conn.execute(
                "SELECT 1 FROM action_intents WHERE plan_id = %s "
                "AND execution_state IN ('RUNNING', 'OUTCOME_UNKNOWN') LIMIT 1",
                (plan_id,),
            ).fetchone()
            if unresolved is not None:
                raise ControlError("cannot fail safe while an action outcome is unresolved")
        transition_outcome = outcome
        if new_state == "COMPENSATING" and (row["pause_requested"] or row["cancel_requested"]):
            transition_outcome = dict(outcome) if isinstance(outcome, dict) else {}
            stored_outcome = row["outcome"]
            prior_request = (
                stored_outcome.get("control_request", {})
                if isinstance(stored_outcome, dict)
                else {}
            )
            transition_outcome["deferred_control_request"] = prior_request
        if new_state == "PAUSED":
            if (
                expected_state not in {"EXECUTING", "VERIFYING"}
                or not row["pause_requested"]
                or row["cancel_requested"]
            ):
                raise ControlError("pause was not requested for this execution phase")
        if new_state == "CANCELLED":
            if (
                expected_state not in {"EXECUTING", "VERIFYING", "PAUSED"}
                or not row["cancel_requested"]
            ):
                raise ControlError("cancellation was not requested for this execution phase")
        if new_state in {"PAUSED", "CANCELLED"}:
            unresolved = conn.execute(
                "SELECT 1 FROM action_intents WHERE plan_id = %s "
                "AND execution_state IN ('RUNNING', 'OUTCOME_UNKNOWN') LIMIT 1",
                (plan_id,),
            ).fetchone()
            if unresolved is not None:
                raise ControlError("cannot pause or cancel while an action outcome is unresolved")
        if (
            new_state in {"COMMITTED", "CANCELLED", "COMPENSATED", "FAILED_SAFE"}
            and outcome is None
        ):
            raise ControlError("terminal execution state requires observed outcome")
        result = conn.execute(
            "UPDATE plan_executions SET status = %s, "
            "outcome = COALESCE(outcome, '{}'::jsonb) || "
            "jsonb_build_object(%s::text, %s::jsonb), "
            "pause_requested = CASE WHEN %s = 'COMPENSATING' THEN FALSE ELSE pause_requested END, "
            "cancel_requested = CASE WHEN %s = 'COMPENSATING' "
            "THEN FALSE ELSE cancel_requested END, "
            "updated_at = clock_timestamp(), "
            "finished_at = CASE WHEN %s IN "
            "('COMMITTED', 'CANCELLED', 'COMPENSATED', 'FAILED_SAFE') "
            "THEN clock_timestamp() ELSE NULL END WHERE plan_id = %s RETURNING status",
            (
                new_state,
                new_state,
                None if transition_outcome is None else Jsonb(transition_outcome),
                new_state,
                new_state,
                new_state,
                plan_id,
            ),
        ).fetchone()
        return str(result["status"])

    @staticmethod
    def transition_execution_and_plan(
        conn: psycopg.Connection,
        plan_id: str,
        expected_state: str,
        new_state: str,
        controller_id: str,
        fencing_token: int,
        *,
        actor: str,
        role: str,
        reason: str,
        request_id: str,
        idempotency_key: str | None = None,
        outcome: object = None,
    ) -> tuple[str, int]:
        """Transition the execution journal and its plan history in one DB transaction."""
        plan = conn.execute(
            "SELECT state FROM plans WHERE plan_id = %s FOR UPDATE", (plan_id,)
        ).fetchone()
        if plan is None:
            raise KeyError(plan_id)
        if plan["state"] != expected_state:
            raise StaleState("plan state changed")
        execution = Repository.assert_execution_controller(
            conn, plan_id, controller_id, fencing_token
        )
        if execution["status"] != expected_state:
            raise StaleState("execution state changed")
        Repository.transition_execution(
            conn,
            plan_id,
            expected_state,
            new_state,
            controller_id,
            fencing_token,
            outcome=outcome,
        )
        return Repository.transition_plan(
            conn,
            plan_id,
            new_state,
            actor,
            role,
            reason,
            request_id,
            idempotency_key,
        )

    @staticmethod
    def runnable_executions(conn: psycopg.Connection) -> list[dict[str, object]]:
        """List active executions with enough plan and lease data for restart recovery."""
        return conn.execute(
            "SELECT pe.plan_id, pe.status, p.state AS plan_state, p.state_version, "
            "p.binding, p.expires_at_ms, pe.controller_id, pe.controller_fencing_token, "
            "pe.controller_lease_expires_at, "
            "pe.controller_lease_expires_at <= clock_timestamp() AS controller_lease_expired, "
            "pe.pending_controller_id, pe.pending_controller_fencing_token, "
            "pe.pending_controller_lease_expires_at, "
            "pe.pending_controller_lease_expires_at > clock_timestamp() "
            "AS pending_controller_lease_active, pe.pause_requested, pe.cancel_requested, "
            "pe.updated_at "
            "FROM plan_executions pe JOIN plans p USING (plan_id) "
            "WHERE pe.status IN ('EXECUTING', 'VERIFYING', 'COMPENSATING') "
            "OR (pe.status = 'PAUSED' AND pe.cancel_requested) "
            "ORDER BY pe.updated_at, pe.plan_id"
        ).fetchall()

    @staticmethod
    def request_execution_control(
        conn: psycopg.Connection, plan_id: str, request: str, reason: str
    ) -> dict[str, object]:
        """Persist a pause/cancel request for the controller to honor at a safe point."""
        if request not in {"PAUSE", "CANCEL"} or not isinstance(reason, str) or not reason.strip():
            raise ValueError("execution control request must be PAUSE or CANCEL")
        plan = conn.execute(
            "SELECT state FROM plans WHERE plan_id = %s FOR UPDATE", (plan_id,)
        ).fetchone()
        if plan is None:
            raise KeyError(plan_id)
        execution = conn.execute(
            "SELECT status, pause_requested, cancel_requested FROM plan_executions "
            "WHERE plan_id = %s FOR UPDATE",
            (plan_id,),
        ).fetchone()
        if execution is None:
            raise ControlError("plan has no execution to control")
        if plan["state"] != execution["status"]:
            raise StaleState("plan and execution states do not match")
        if request == "PAUSE" and execution["status"] not in {"EXECUTING", "VERIFYING"}:
            raise ControlError("only an executing or verifying plan can be paused")
        if request == "CANCEL" and execution["status"] not in {
            "EXECUTING",
            "VERIFYING",
            "PAUSED",
        }:
            raise ControlError("only an executing, verifying or paused plan can be cancelled")
        if request == "PAUSE":
            if execution["cancel_requested"]:
                return conn.execute(
                    "SELECT * FROM plan_executions WHERE plan_id = %s", (plan_id,)
                ).fetchone()
            conn.execute(
                "UPDATE plan_executions SET pause_requested = TRUE, "
                "outcome = COALESCE(outcome, '{}'::jsonb) || "
                "jsonb_build_object('control_request', %s::jsonb), "
                "updated_at = clock_timestamp() WHERE plan_id = %s",
                (Jsonb({"request": request, "reason": reason}), plan_id),
            )
        else:
            conn.execute(
                "UPDATE plan_executions SET pause_requested = FALSE, cancel_requested = TRUE, "
                "outcome = COALESCE(outcome, '{}'::jsonb) || "
                "jsonb_build_object('control_request', %s::jsonb), "
                "updated_at = clock_timestamp() WHERE plan_id = %s",
                (Jsonb({"request": request, "reason": reason}), plan_id),
            )
        return conn.execute(
            "SELECT * FROM plan_executions WHERE plan_id = %s", (plan_id,)
        ).fetchone()

    @staticmethod
    def runnable_actions(
        conn: psycopg.Connection,
        plan_id: str,
        controller_id: str,
        fencing_token: int,
        *,
        compensation: bool = False,
    ) -> list[dict[str, object]]:
        execution = Repository.assert_execution_controller(
            conn, plan_id, controller_id, fencing_token
        )
        plan = conn.execute("SELECT state FROM plans WHERE plan_id = %s", (plan_id,)).fetchone()
        required_state = "COMPENSATING" if compensation else "EXECUTING"
        if execution["status"] != required_state or plan is None or plan["state"] != required_state:
            raise ControlError("plan is not accepting actions in this phase")
        if execution["pause_requested"] or execution["cancel_requested"]:
            return []
        Repository.assert_current_plan_leases(conn, plan_id)
        rows = conn.execute(
            "SELECT intent_id, action_sequence, attempt_number, action_type, "
            "workload_id, payload, status, "
            "compensates_intent_id, "
            "execution_state FROM action_intents WHERE plan_id = %s AND status = 'APPROVED' "
            "AND execution_state <> 'SUCCEEDED' "
            "AND (%s = (compensates_intent_id IS NOT NULL)) ORDER BY action_sequence",
            (plan_id, compensation),
        ).fetchall()
        if not rows:
            return []
        first = rows[0]
        if first["execution_state"] != "NOT_STARTED":
            return []
        return [first]

    @staticmethod
    def record_action_progress(
        conn: psycopg.Connection,
        plan_id: str,
        intent_id: str,
        controller_id: str,
        fencing_token: int,
        observation: object,
        result: object,
    ) -> dict[str, object]:
        if observation is None:
            raise ValueError("an observation is required for action progress")
        action = conn.execute(
            "SELECT execution_state, compensates_intent_id "
            "FROM action_intents WHERE plan_id = %s AND intent_id = %s FOR UPDATE",
            (plan_id, intent_id),
        ).fetchone()
        if action is None:
            raise KeyError((plan_id, intent_id))
        if action["execution_state"] != "RUNNING":
            raise ControlError("progress can only be recorded for a running action")
        execution = Repository.assert_execution_controller(
            conn, plan_id, controller_id, fencing_token
        )
        allowed_phases = (
            {"COMPENSATING"}
            if action["compensates_intent_id"] is not None
            else {"EXECUTING", "VERIFYING"}
        )
        plan = conn.execute("SELECT state FROM plans WHERE plan_id = %s", (plan_id,)).fetchone()
        if (
            execution["status"] not in allowed_phases
            or plan is None
            or plan["state"] != execution["status"]
        ):
            raise ControlError("execution is not active for action progress")
        Repository.assert_current_plan_leases(conn, plan_id)
        row = conn.execute(
            "UPDATE action_intents SET execution_result = "
            "COALESCE(execution_result, '{}'::jsonb) || jsonb_build_object("
            "'progress', COALESCE(execution_result->'progress', '[]'::jsonb) || "
            "jsonb_build_array(jsonb_build_object('observation', %s::jsonb, "
            "'result', %s::jsonb, 'recorded_at', clock_timestamp()))) "
            "WHERE plan_id = %s AND intent_id = %s "
            "RETURNING intent_id, action_sequence, execution_state, execution_result",
            (
                Jsonb(observation),
                None if result is None else Jsonb(result),
                plan_id,
                intent_id,
            ),
        ).fetchone()
        return row

    @staticmethod
    def incomplete_actions(conn: psycopg.Connection, plan_id: str) -> list[dict[str, object]]:
        return conn.execute(
            "SELECT intent_id, action_sequence, action_type, workload_id, payload, status, "
            "compensates_intent_id, "
            "attempt_number, execution_state, execution_started_at, "
            "execution_finished_at, execution_result, "
            "execution_error FROM action_intents WHERE plan_id = %s "
            "AND execution_state IN ('NOT_STARTED', 'RUNNING', 'OUTCOME_UNKNOWN') "
            "ORDER BY action_sequence",
            (plan_id,),
        ).fetchall()

    @staticmethod
    def completed_retirement_actions(
        conn: psycopg.Connection, plan_id: str
    ) -> list[dict[str, object]]:
        return conn.execute(
            "SELECT intent_id, action_type, workload_id, payload, execution_result "
            "FROM action_intents WHERE plan_id = %s AND action_type = 'PREPARE_SOURCE_RETIREMENT' "
            "AND execution_state = 'SUCCEEDED' ORDER BY action_sequence",
            (plan_id,),
        ).fetchall()

    @staticmethod
    def authorize_action(
        conn: psycopg.Connection,
        plan_id: str,
        intent_id: str,
        controller_id: str,
        fencing_token: int,
    ) -> dict[str, object]:
        """Hold action, controller and approval fences through one bounded target call."""
        plan = conn.execute(
            "SELECT state FROM plans WHERE plan_id = %s FOR SHARE", (plan_id,)
        ).fetchone()
        if plan is None:
            raise KeyError(plan_id)
        action = conn.execute(
            "SELECT * FROM action_intents WHERE plan_id = %s AND intent_id = %s FOR UPDATE",
            (plan_id, intent_id),
        ).fetchone()
        if action is None:
            raise KeyError((plan_id, intent_id))
        execution = Repository.assert_execution_controller(
            conn, plan_id, controller_id, fencing_token
        )
        phase = "COMPENSATING" if action["compensates_intent_id"] is not None else "EXECUTING"
        if (
            action["status"] != "APPROVED"
            or action["execution_state"] != "RUNNING"
            or execution["pause_requested"]
            or execution["cancel_requested"]
            or execution["status"] != phase
            or plan["state"] != phase
        ):
            raise ControlError("action is not authorized for target dispatch")
        Repository.assert_current_plan_leases(conn, plan_id)
        return action

    @staticmethod
    def transition_action(
        conn: psycopg.Connection,
        plan_id: str,
        intent_id: str,
        expected_state: str,
        new_state: str,
        controller_id: str,
        fencing_token: int,
        *,
        pre_observation: object = None,
        target_observation: object = None,
        result: object = None,
        error: str | None = None,
        retry_proven_not_applied: bool = False,
        max_attempts: int | None = None,
    ) -> dict[str, object]:
        """Transition an action; retry evidence is the controller's target observation."""
        start_plan = None
        if new_state == "RUNNING":
            start_plan = conn.execute(
                "SELECT state FROM plans WHERE plan_id = %s FOR SHARE", (plan_id,)
            ).fetchone()
            if start_plan is None:
                raise KeyError(plan_id)
        row = conn.execute(
            "SELECT execution_state, action_sequence, attempt_number, status, "
            "compensates_intent_id "
            "FROM action_intents "
            "WHERE plan_id = %s AND intent_id = %s FOR UPDATE",
            (plan_id, intent_id),
        ).fetchone()
        if row is None:
            raise KeyError((plan_id, intent_id))
        if row["execution_state"] != expected_state:
            raise StaleState("action execution state changed")
        if (
            expected_state not in ACTION_EXECUTION_STATES
            or new_state not in ACTION_EXECUTION_STATES
            or new_state not in ACTION_EXECUTION_TRANSITIONS[expected_state]
        ):
            raise ControlError(f"illegal action transition {expected_state} -> {new_state}")
        execution = Repository.assert_execution_controller(
            conn, plan_id, controller_id, fencing_token
        )
        compensation = row["compensates_intent_id"] is not None
        running_state = "COMPENSATING" if compensation else "EXECUTING"
        if expected_state == "NOT_STARTED":
            if (
                new_state != "RUNNING"
                or row["status"] != "APPROVED"
                or start_plan is None
                or start_plan["state"] != running_state
                or execution["status"] != running_state
                or pre_observation is None
            ):
                raise ControlError("action is not approved for dispatch")
            prior = conn.execute(
                "SELECT 1 FROM action_intents WHERE plan_id = %s AND action_sequence < %s "
                "AND execution_state <> 'SUCCEEDED' "
                "AND (%s = (compensates_intent_id IS NOT NULL)) LIMIT 1",
                (plan_id, row["action_sequence"], compensation),
            ).fetchone()
            if prior is not None:
                raise ControlError("earlier action is incomplete")
            Repository.assert_current_plan_leases(conn, plan_id)
        elif expected_state == "OUTCOME_UNKNOWN" and new_state == "RUNNING":
            if (
                start_plan is None
                or start_plan["state"] != running_state
                or execution["status"] != running_state
            ):
                raise ControlError("action is not approved for retry")
            Repository.assert_current_plan_leases(conn, plan_id)
        if new_state == "RUNNING" and (
            execution["pause_requested"] or execution["cancel_requested"]
        ):
            raise ControlError("execution has a pending pause or cancellation request")
        if expected_state in {"RUNNING", "OUTCOME_UNKNOWN"} and execution["status"] not in {
            "EXECUTING",
            "VERIFYING",
            "COMPENSATING",
        }:
            raise ControlError("execution is no longer active")
        if new_state in {"SUCCEEDED", "FAILED"} and target_observation is None:
            raise ControlError("terminal action state requires a target observation")
        if expected_state == "OUTCOME_UNKNOWN" and target_observation is None:
            raise ControlError("unknown action outcome requires an observer result")
        if retry_proven_not_applied and not (
            expected_state == "OUTCOME_UNKNOWN" and new_state == "RUNNING"
        ):
            raise ValueError("not-applied retry evidence only applies to an unknown action")
        if new_state == "RUNNING" and max_attempts is not None:
            if max_attempts <= 0:
                raise ValueError("maximum action attempts must be positive")
            if row["attempt_number"] >= max_attempts and (
                expected_state == "NOT_STARTED" or retry_proven_not_applied
            ):
                raise ControlError("action attempt limit reached")
        if retry_proven_not_applied:
            if max_attempts is None or max_attempts <= 0:
                raise ValueError("retrying an unknown action requires a positive attempt limit")
        conn.execute(
            "UPDATE action_intents SET execution_state = %s, "
            "attempt_number = attempt_number + CASE "
            "WHEN %s = 'RUNNING' AND execution_state = 'NOT_STARTED' THEN 1 "
            "WHEN %s = 'RUNNING' AND execution_state = 'OUTCOME_UNKNOWN' AND %s THEN 1 "
            "ELSE 0 END, "
            "execution_started_at = CASE WHEN %s = 'RUNNING' AND "
            "(execution_state = 'NOT_STARTED' OR "
            "(execution_state = 'OUTCOME_UNKNOWN' AND %s)) THEN clock_timestamp() "
            "ELSE execution_started_at END, "
            "execution_finished_at = CASE WHEN %s = 'RUNNING' THEN NULL "
            "ELSE clock_timestamp() END, "
            "execution_result = CASE "
            "WHEN %s = 'RUNNING' AND execution_state = 'NOT_STARTED' "
            "THEN jsonb_build_object('pre_observation', %s::jsonb) "
            "WHEN %s = 'RUNNING' AND execution_state = 'OUTCOME_UNKNOWN' "
            "THEN COALESCE(execution_result, '{}'::jsonb) || jsonb_build_object("
            "'unknown_resolutions', "
            "COALESCE(execution_result->'unknown_resolutions', '[]'::jsonb) || "
            "jsonb_build_array(jsonb_build_object('attempt_number', "
            "attempt_number + CASE WHEN %s THEN 1 ELSE 0 END, "
            "'target_observation', %s::jsonb, 'retry_proven_not_applied', %s, "
            "'recorded_at', clock_timestamp()))) "
            "ELSE COALESCE(execution_result, '{}'::jsonb) || "
            "jsonb_build_object('target_observation', %s::jsonb, 'result', %s::jsonb) END, "
            "execution_error = %s "
            "WHERE plan_id = %s AND intent_id = %s",
            (
                new_state,
                new_state,
                new_state,
                retry_proven_not_applied,
                new_state,
                retry_proven_not_applied,
                new_state,
                new_state,
                None if pre_observation is None else Jsonb(pre_observation),
                new_state,
                retry_proven_not_applied,
                None if target_observation is None else Jsonb(target_observation),
                retry_proven_not_applied,
                None if target_observation is None else Jsonb(target_observation),
                None if result is None else Jsonb(result),
                error,
                plan_id,
                intent_id,
            ),
        )
        return conn.execute(
            "SELECT intent_id, action_sequence, attempt_number, execution_state, "
            "execution_started_at, "
            "execution_finished_at, execution_result, execution_error FROM action_intents "
            "WHERE plan_id = %s AND intent_id = %s",
            (plan_id, intent_id),
        ).fetchone()

    @staticmethod
    def persist_compensation_action(
        conn: psycopg.Connection,
        plan_id: str,
        source_intent_id: str,
        action_type: str,
        workload_id: str | None,
        payload: dict[str, object],
        controller_id: str,
        fencing_token: int,
    ) -> dict[str, object]:
        if not action_type or not isinstance(payload, dict):
            raise ValueError("compensation action type and payload are required")
        execution = Repository.assert_execution_controller(
            conn, plan_id, controller_id, fencing_token
        )
        plan = conn.execute("SELECT state FROM plans WHERE plan_id = %s", (plan_id,)).fetchone()
        if execution["status"] != "COMPENSATING" or plan is None or plan["state"] != "COMPENSATING":
            raise ControlError("plan is not compensating")
        source = conn.execute(
            "SELECT execution_state, execution_result, compensates_intent_id "
            "FROM action_intents WHERE plan_id = %s AND intent_id = %s FOR UPDATE",
            (plan_id, source_intent_id),
        ).fetchone()
        if (
            source is None
            or source["compensates_intent_id"] is not None
            or source["execution_state"] not in {"SUCCEEDED", "FAILED"}
        ):
            raise ControlError("source action must have a known outcome before compensation")
        if not source["execution_result"] or "pre_observation" not in source["execution_result"]:
            raise ControlError("source action has no durable pre-mutation observation")
        compensation_id = f"compensate:{source_intent_id}"
        existing = conn.execute(
            "SELECT * FROM action_intents WHERE plan_id = %s AND compensates_intent_id = %s",
            (plan_id, source_intent_id),
        ).fetchone()
        if existing is not None:
            if (
                existing["action_type"] != action_type
                or existing["workload_id"] != workload_id
                or existing["payload"] != payload
            ):
                raise IdempotencyConflict("compensation intent changed for the same source action")
            return existing
        sequence = conn.execute(
            "SELECT COALESCE(max(action_sequence), -1) + 1 AS next "
            "FROM action_intents WHERE plan_id = %s",
            (plan_id,),
        ).fetchone()["next"]
        return conn.execute(
            "INSERT INTO action_intents (plan_id, intent_id, action_sequence, action_type, "
            "workload_id, payload, status, compensates_intent_id) "
            "VALUES (%s, %s, %s, %s, %s, %s, 'APPROVED', %s) RETURNING *",
            (
                plan_id,
                compensation_id,
                sequence,
                action_type,
                workload_id,
                Jsonb(payload),
                source_intent_id,
            ),
        ).fetchone()

    def list_plans(self, limit: int = 100) -> list[dict[str, object]]:
        with self.transaction() as conn:
            return conn.execute(
                "SELECT plan_id, created_at_ms, expires_at_ms, snapshot_id, snapshot_generation, "
                "state, state_version, reason_codes FROM plans "
                "ORDER BY created_at_ms DESC LIMIT %s",
                (limit,),
            ).fetchall()

    def get_plan(self, plan_id: str) -> dict[str, object] | None:
        with self.transaction() as conn:
            row = conn.execute("SELECT * FROM plans WHERE plan_id = %s", (plan_id,)).fetchone()
            if row is None:
                return None
            artifact_rows = conn.execute(
                "SELECT role, artifact_sha256 FROM plan_artifacts WHERE plan_id = %s ORDER BY role",
                (plan_id,),
            ).fetchall()
            row["artifact_hashes"] = {
                item["role"]: item["artifact_sha256"].strip() for item in artifact_rows
            }
            row["candidate_actions"] = conn.execute(
                "SELECT intent_id, action_sequence, action_type, workload_id, payload, status, "
                "execution_state, attempt_number, execution_started_at, execution_finished_at, "
                "execution_error, execution_result, compensates_intent_id "
                "FROM action_intents WHERE plan_id = %s ORDER BY action_sequence",
                (plan_id,),
            ).fetchall()
            row["execution"] = conn.execute(
                "SELECT status, controller_id, controller_fencing_token, "
                "controller_lease_expires_at, pending_controller_id, "
                "pending_controller_fencing_token, pending_controller_lease_expires_at, "
                "pause_requested, cancel_requested, outcome, created_at, updated_at, finished_at "
                "FROM plan_executions WHERE plan_id = %s",
                (plan_id,),
            ).fetchone()
            return row

    def verify_plan_artifacts(self, plan_id: str) -> dict[str, bytes]:
        with self.transaction() as conn:
            return self.load_artifacts(conn, plan_id)

    def list_audit(self, limit: int = 100) -> list[dict[str, object]]:
        with self.transaction() as conn:
            return conn.execute(
                "SELECT audit_id, occurred_at, actor, role, request_id, operation, "
                "plan_id, snapshot_id, "
                "idempotency_key, before_state, after_state, result, reason "
                "FROM audit_events ORDER BY audit_id DESC LIMIT %s",
                (limit,),
            ).fetchall()


def main() -> None:
    if sys.argv[1:] != ["migrate"]:
        raise SystemExit("usage: python -m fleetright.control migrate")
    database_url = os.getenv("FLEETRIGHT_DATABASE_URL", "")
    if not database_url:
        raise SystemExit("FLEETRIGHT_DATABASE_URL is required")
    print(Repository(database_url).migrate())


if __name__ == "__main__":
    main()
