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
STATES = {"PROPOSED", "VERIFIED", "APPROVED", "PAUSED", "STALE", "REJECTED", "CANCELLED"}
TRANSITIONS = {
    "PROPOSED": {"VERIFIED", "REJECTED", "STALE", "CANCELLED"},
    "VERIFIED": {"APPROVED", "PAUSED", "STALE", "REJECTED", "CANCELLED"},
    "APPROVED": {"PAUSED", "STALE", "CANCELLED"},
    "PAUSED": {"STALE", "CANCELLED"},
    "STALE": set(),
    "REJECTED": set(),
    "CANCELLED": set(),
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
        for intent in intents:
            conn.execute(
                "INSERT INTO action_intents (plan_id, intent_id, action_type, "
                "workload_id, payload, status) "
                "VALUES (%s, %s, %s, %s, %s, 'PLANNED')",
                (
                    plan["plan_id"],
                    intent["intent_id"],
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
            "AND state IN ('VERIFIED', 'APPROVED', 'PAUSED') FOR UPDATE",
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
            "('VERIFIED', 'APPROVED', 'PAUSED') FOR UPDATE"
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
            "WHERE conflict_leases.expires_at <= clock_timestamp() "
            "OR conflict_leases.owner_id = EXCLUDED.owner_id "
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
                "SELECT intent_id, action_type, workload_id, payload, status "
                "FROM action_intents WHERE plan_id = %s ORDER BY intent_id",
                (plan_id,),
            ).fetchall()
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
