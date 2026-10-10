from __future__ import annotations

import os
import time
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import psycopg
import pytest
from fleetright.control import (
    ACTION_EXECUTION_TRANSITIONS,
    EXECUTION_TRANSITIONS,
    ControlError,
    ExecutionConflict,
    LeaseConflict,
    Repository,
    transition_allowed,
)
from fleetright.execution import (
    ActionBlocked,
    OutcomeUnknown,
    TargetNotFound,
    TargetPreconditionFailed,
)
from fleetright.reconciler import ExecutionController
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb


@dataclass(frozen=True)
class Database:
    repository: Repository
    dsn: str = field(repr=False)


@pytest.fixture(scope="module")
def execution_database() -> Iterator[Database]:
    admin_dsn = os.getenv("FLEETRIGHT_TEST_DATABASE_URL")
    if not admin_dsn:
        pytest.skip("FLEETRIGHT_TEST_DATABASE_URL is required for PostgreSQL journal tests")
    schema = f"prompt2b_{uuid.uuid4().hex}"
    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    dsn = make_conninfo(admin_dsn, options=f"-csearch_path={schema}")
    repo = Repository(dsn)
    repo.migrate()
    try:
        yield Database(repo, dsn)
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.fixture
def clean_database(execution_database: Database) -> Database:
    with execution_database.repository.transaction() as conn:
        conn.execute(
            "TRUNCATE control_state, plans, artifacts, idempotency_keys, conflict_leases, "
            "audit_events RESTART IDENTITY CASCADE"
        )
    return execution_database


def _insert_plan(conn: psycopg.Connection, plan_id: str, state: str = "APPROVED") -> None:
    conn.execute(
        "INSERT INTO plans (plan_id, created_at_ms, expires_at_ms, snapshot_id, "
        "snapshot_generation, policy_version, policy_hash, profile_hashes, machine_catalog_hash, "
        "forecast_metadata, planner_build_id, verifier_build_id, verification_hash, "
        "verification_accepted, admission_accepted, reason_codes, binding, state, state_version, "
        "created_by) VALUES (%s, 1, %s, 'snapshot', 1, '1', %s, %s, %s, %s, 'planner', "
        "'verifier', %s, TRUE, TRUE, %s, %s, %s, 0, 'test')",
        (
            plan_id,
            time.time_ns() // 1_000_000 + 3_600_000,
            "a" * 64,
            Jsonb({"profile": "b" * 64}),
            "c" * 64,
            Jsonb({}),
            "d" * 64,
            Jsonb([]),
            Jsonb({"source": {}, "conflict_keys": []}),
            state,
        ),
    )


def _claim(repo: Repository, plan_id: str, key: str = "execute-1") -> dict[str, object]:
    with repo.transaction() as conn:
        claim = Repository.claim_execution(
            conn, plan_id, "operator", key, {}, "controller-a", 60_000
        )
        if claim["claimed"]:
            Repository.transition_plan(
                conn, plan_id, "EXECUTING", "operator", "operator", "execute", "request", key
            )
        return claim


class FakeScaleTarget:
    def __init__(self, replicas: int, *, lose_next_response: bool = False) -> None:
        self.uid = "deployment-uid"
        self.generation = 1
        self.resource_version = "1"
        self.replicas = replicas
        self.fencing_token = 0
        self.generation_tracks_spec = False
        self.lose_next_response = lose_next_response
        self.lose_before_apply = False
        self.crash_after_apply = False
        self.crash_after_fence = False
        self.change_generation_before_fence = False
        self.mutations: list[int] = []
        self.fence_calls = 0
        self.missing = False
        self.revert_after_response: int | None = None

    def observe_scale(self, _workload_id: str) -> dict[str, object]:
        if self.missing:
            raise TargetNotFound("injected target deletion")
        return {
            "uid": self.uid,
            "generation": self.generation,
            "observed_generation": self.generation,
            "resource_version": self.resource_version,
            "fencing_token": self.fencing_token,
            "generation_tracks_spec": self.generation_tracks_spec,
            "replicas": self.replicas,
            "ready_replicas": self.replicas,
            "available_replicas": self.replicas,
        }

    def fence_scale(
        self, _workload_id: str, token: int, *, expected: dict[str, object] | None = None
    ) -> dict[str, object]:
        if self.change_generation_before_fence:
            self.change_generation_before_fence = False
            self.generation += 1
            self.resource_version = f"rv-{self.generation}"
        current = self.observe_scale(_workload_id)
        if token < self.fencing_token:
            raise TargetPreconditionFailed("stale fencing token")
        self.fence_calls += 1
        already_fenced = token == self.fencing_token
        expected_matches = expected is None or all(
            current.get(key) == expected.get(key) for key in ("uid", "generation", "replicas")
        )
        prior_fence_applied = (
            already_fenced
            and expected is not None
            and current.get("uid") == expected.get("uid")
            and current.get("replicas") == expected.get("replicas")
            and current.get("generation") == expected.get("generation", -2) + 1
        )
        if not expected_matches and not prior_fence_applied:
            raise TargetPreconditionFailed("target changed before fencing")
        if not already_fenced:
            self.fencing_token = token
            if self.generation_tracks_spec:
                self.generation += 1
                self.resource_version = f"rv-{self.generation}"
        if self.crash_after_fence:
            self.crash_after_fence = False
            raise SystemExit("injected controller crash after target fence")
        return {
            **self.observe_scale(_workload_id),
            "fence_precondition": {
                key: expected[key] if prior_fence_applied and expected is not None else current[key]
                for key in ("uid", "generation", "replicas")
            },
        }

    def set_replicas(
        self,
        _workload_id: str,
        desired: int,
        _observed: dict[str, object],
        *,
        expected_replicas: int,
        fencing_token: int,
    ) -> dict[str, object]:
        if fencing_token != self.fencing_token or expected_replicas != self.replicas:
            raise TargetPreconditionFailed("stale target precondition")
        if self.lose_before_apply:
            self.lose_before_apply = False
            raise OutcomeUnknown("injected timeout before target application")
        if desired != self.replicas:
            self.mutations.append(desired)
            self.replicas = desired
            self.generation += 1
            self.resource_version = f"rv-{self.generation}"
        result = self.observe_scale(_workload_id)
        if self.revert_after_response is not None:
            self.replicas = self.revert_after_response
            self.generation += 1
            self.resource_version = f"rv-{self.generation}"
            self.revert_after_response = None
        if self.crash_after_apply:
            self.crash_after_apply = False
            raise SystemExit("injected controller crash after target success")
        if self.lose_next_response:
            self.lose_next_response = False
            raise OutcomeUnknown("injected lost mutation response")
        return result


class FakeFleetRightWorkloadTarget:
    supports_workload_execution = True

    def __init__(self, replicas: int, *, lose_next_response: bool = False) -> None:
        self.uid = "deployment-uid"
        self.generation = 1
        self.resource_version = "1"
        self.replicas = replicas
        self.observed_replicas = replicas
        self.ready_replicas = replicas
        self.available_replicas = replicas
        self.availability_floor = replicas
        self.observed_generation = 1
        self.fencing_token = 0
        self.execution_request: dict[str, object] | None = None
        self.execution_fence: dict[str, object] | None = None
        self.execution: dict[str, object] | None = None
        self.reconciled = True
        self.lose_next_response = lose_next_response
        self.lose_before_authorize = False
        self.crash_after_fence = False
        self.authorizations = 0

    def observe_scale(self, _workload_id: str) -> dict[str, object]:
        return {
            "uid": self.uid,
            "generation": self.generation,
            "resource_version": self.resource_version,
            "generation_tracks_spec": True,
            "workload_execution": True,
            "replicas": self.replicas,
            "observed_replicas": self.observed_replicas,
            "ready_replicas": self.ready_replicas,
            "available_replicas": self.available_replicas,
            "availability_floor": self.availability_floor,
            "observed_generation": self.observed_generation,
            "fencing_token": self.fencing_token,
            "execution_request": self.execution_request,
            "execution_fence": self.execution_fence,
            "execution": self.execution,
            "reconciled": self.reconciled,
        }

    def fence_scale(
        self,
        _workload_id: str,
        token: int,
        *,
        expected: dict[str, object],
        plan_id: str,
        operation_id: str,
        target_replicas: int,
    ) -> dict[str, object]:
        current = self.observe_scale(_workload_id)
        if self.fencing_token > token:
            raise TargetPreconditionFailed("newer workload execution generation exists")
        request = self.execution_request
        same_request = (
            isinstance(request, dict)
            and request.get("planID") == plan_id
            and request.get("operationID") == operation_id
            and request.get("targetReplicas") == target_replicas
            and self.replicas == target_replicas
        )
        same_fence = (
            isinstance(self.execution_fence, dict)
            and self.execution_fence.get("generation") == token
            and self.execution_fence.get("planID") == plan_id
            and self.execution_fence.get("operationID") == operation_id
        )
        expected_unchanged = all(
            current.get(key) == expected.get(key) for key in ("uid", "generation", "replicas")
        )
        if same_fence and current["generation"] != self.execution_fence["fencedGeneration"]:
            raise TargetPreconditionFailed("workload changed after the authority fence")
        if same_request and not same_fence and not expected_unchanged:
            raise TargetPreconditionFailed("workload changed before fencing")
        if (
            not same_fence
            and not same_request
            and not expected_unchanged
        ):
            raise TargetPreconditionFailed("workload precondition changed")
        prior_fence = self.execution_fence
        before = (
            {
                "uid": current["uid"],
                "generation": self.execution_fence["fencedGeneration"] - 1,
                "replicas": (
                    current["replicas"]
                    if same_request
                    else self.execution_fence["expectedReplicas"]
                ),
            }
            if same_fence
            else {key: current[key] for key in ("uid", "generation", "replicas")}
        )
        if not same_fence:
            prior = self.execution_fence
            self.execution_fence = {
                "planID": plan_id,
                "operationID": operation_id,
                "generation": token,
                "expectedGeneration": (
                    prior["expectedGeneration"]
                    if isinstance(prior, dict)
                    and (prior.get("planID"), prior.get("operationID")) == (plan_id, operation_id)
                    else expected["generation"]
                ),
                "expectedReplicas": (
                    prior["expectedReplicas"]
                    if isinstance(prior, dict)
                    and (prior.get("planID"), prior.get("operationID")) == (plan_id, operation_id)
                    else expected["replicas"]
                ),
                "fencedGeneration": self.generation + 1,
            }
            self.fencing_token = token
            self.generation += 1
            self.resource_version = str(int(self.resource_version) + 1)
        if self.crash_after_fence:
            self.crash_after_fence = False
            raise SystemExit("injected controller crash after target fence")
        return {
            **self.observe_scale(_workload_id),
            "fence_precondition": before,
            "fence_applied": True,
            "fence_replayed": same_fence,
            "prior_execution_fence": prior_fence if not same_fence else None,
            "operation_applied_before_fence": bool(same_request),
        }

    def authorize_scale(
        self,
        _workload_id: str,
        token: int,
        *,
        expected: dict[str, object],
        plan_id: str,
        operation_id: str,
        target_replicas: int,
    ) -> dict[str, object]:
        current = self.observe_scale(_workload_id)
        same = (
            isinstance(self.execution_request, dict)
            and self.execution_request.get("planID") == plan_id
            and self.execution_request.get("operationID") == operation_id
            and self.execution_request.get("targetReplicas") == target_replicas
        )
        recovered = (
            same
            and token == self.fencing_token
            and current["uid"] == expected.get("uid")
            and current["generation"] == expected.get("generation", -2) + 1
            and current["replicas"] == target_replicas
        )
        if current["uid"] != expected.get("uid"):
            raise TargetPreconditionFailed("workload identity changed")
        if not recovered and any(
            current.get(key) != expected.get(key) for key in ("generation", "replicas")
        ):
            raise TargetPreconditionFailed("workload precondition changed")
        if same and token == self.fencing_token:
            return {**current, "fence_precondition": expected, "already_authorized": True}
        if token <= self.fencing_token:
            raise TargetPreconditionFailed("newer workload execution generation exists")
        if target_replicas < self.replicas:
            raise ActionBlocked("WORKLOAD_SCALE_DOWN_DISABLED")
        if self.lose_before_authorize:
            self.lose_before_authorize = False
            raise OutcomeUnknown("injected lost workload authorization before apply")
        if (
            self.execution_request is not None
            and not same
            and not (
                isinstance(self.execution, dict)
                and self.execution.get("phase") == "APPLIED"
                and self.observed_generation == self.generation
            )
        ):
            raise ActionBlocked("PRIOR_WORKLOAD_OPERATION_UNRESOLVED")
        self.fencing_token = token
        prior_fence = self.execution_fence
        self.execution_fence = {
            "planID": plan_id,
            "operationID": operation_id,
            "generation": token,
            "expectedGeneration": (
                prior_fence["expectedGeneration"]
                if isinstance(prior_fence, dict)
                and (prior_fence.get("planID"), prior_fence.get("operationID"))
                == (plan_id, operation_id)
                else self.generation
            ),
            "expectedReplicas": (
                prior_fence["expectedReplicas"]
                if isinstance(prior_fence, dict)
                and (prior_fence.get("planID"), prior_fence.get("operationID"))
                == (plan_id, operation_id)
                else self.replicas
            ),
            "fencedGeneration": self.generation + 1,
        }
        self.execution_request = {
            "planID": plan_id,
            "operationID": operation_id,
            "generation": token,
            "expectedReplicas": self.replicas,
            "targetReplicas": target_replicas,
        }
        self.replicas = target_replicas
        self.generation += 1
        self.resource_version = str(int(self.resource_version) + 1)
        self.reconciled = False
        self.authorizations += 1
        return {**self.observe_scale(_workload_id), "fence_precondition": expected}

    def set_replicas(
        self,
        _workload_id: str,
        replicas: int,
        _observed: dict[str, object],
        *,
        expected_replicas: int,
        fencing_token: int,
        plan_id: str,
        operation_id: str,
    ) -> dict[str, object]:
        if (
            self.fencing_token != fencing_token
            or self.replicas != replicas
            or not isinstance(self.execution_request, dict)
            or self.execution_request.get("planID") != plan_id
            or self.execution_request.get("operationID") != operation_id
            or self.execution_request.get("targetReplicas") != replicas
            or expected_replicas > replicas
        ):
            raise TargetPreconditionFailed("durable workload authorization changed")
        if self.lose_next_response:
            self.lose_next_response = False
            raise OutcomeUnknown("injected lost workload authorization response")
        return {**self.observe_scale(_workload_id), "applied": True}

    def mark_applied(self) -> None:
        assert isinstance(self.execution_request, dict)
        self.execution = {
            **{
                key: self.execution_request[key]
                for key in ("planID", "operationID", "generation", "targetReplicas")
            },
            "phase": "APPLIED",
        }
        self.observed_generation = self.generation
        self.observed_replicas = self.replicas
        self.ready_replicas = self.replicas
        self.available_replicas = self.replicas
        self.reconciled = True


class FakeHealth:
    def __init__(self, samples: list[bool] | None = None) -> None:
        self.samples = samples or [True]
        self.calls = 0
        self.fresh = True

    def observe(self, _workload_id: str, **_thresholds: object) -> dict[str, object]:
        healthy = self.samples[min(self.calls, len(self.samples) - 1)]
        self.calls += 1
        return {
            "ready": True,
            "fresh_sample": self.fresh,
            "healthy": healthy,
            "p99_within_slo": healthy,
            "error_rate_within_slo": healthy,
            "queue_growth_within_limit": healthy,
            "observed_at_ms": time.time_ns() // 1_000_000,
        }


def _prepare_controller(
    repo: Repository,
    plan_id: str,
    current: int,
    desired: int,
    conflict_keys: list[str] | None = None,
) -> tuple[ExecutionController, FakeScaleTarget, FakeHealth]:
    health_policy = {
        "p99_limit_ms": 120,
        "error_slo_ppm": 1_000,
        "max_queue_growth_milli": 0,
    }
    binding = {"source": {}, "conflict_keys": conflict_keys or []}
    with repo.transaction() as conn:
        _insert_plan(conn, plan_id)
        conn.execute("UPDATE plans SET binding = %s WHERE plan_id = %s", (Jsonb(binding), plan_id))
        conn.execute(
            "INSERT INTO control_state "
            "(singleton, snapshot_id, snapshot_generation, binding) VALUES "
            "(TRUE, 'snapshot', 1, %s)",
            (Jsonb({}),),
        )
        conn.execute(
            "INSERT INTO action_intents (plan_id, intent_id, action_sequence, action_type, "
            "workload_id, payload, status) VALUES (%s, 'scale', 0, 'SET_REPLICA_COUNT', "
            "'cpu-api', %s, 'APPROVED')",
            (
                plan_id,
                Jsonb(
                    {
                        "current_replicas": current,
                        "proposed_replicas": desired,
                        "target_precondition": {
                            "uid": "deployment-uid",
                            "generation": 1,
                            "resource_version": "1",
                            "replicas": current,
                        },
                        "health": health_policy,
                    }
                ),
            ),
        )
        for conflict_key in conflict_keys or []:
            fence = Repository.acquire_lease(conn, conflict_key, plan_id, "operator", 60_000)
            assert fence is not None
            conn.execute(
                "INSERT INTO plan_leases (plan_id, conflict_key, owner_id, fencing_token) "
                "VALUES (%s, %s, %s, %s)",
                (plan_id, conflict_key, plan_id, fence),
            )
    _claim(repo, plan_id)
    target = FakeScaleTarget(current)
    health = FakeHealth()
    controller = ExecutionController(
        repo,
        target,
        health,
        "controller-a",
        readiness_timeout_seconds=2,
    )
    return controller, target, health


def test_journal_transitions_match_plan_schema() -> None:
    assert transition_allowed("APPROVED", "EXECUTING")
    assert transition_allowed("EXECUTING", "VERIFYING")
    assert transition_allowed("VERIFYING", "COMMITTED")
    assert transition_allowed("COMPENSATING", "COMPENSATED")
    assert not transition_allowed("EXECUTING", "COMMITTED")
    assert ACTION_EXECUTION_TRANSITIONS["OUTCOME_UNKNOWN"] == {
        "RUNNING",
        "SUCCEEDED",
        "FAILED",
    }
    assert EXECUTION_TRANSITIONS["COMPENSATING"] == {"COMPENSATED", "FAILED_SAFE"}


def test_execution_claim_is_idempotent_unique_and_controller_fenced(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    with repo.transaction() as conn:
        _insert_plan(conn, "claim")
    first = _claim(repo, "claim")
    assert first["claimed"] is True
    first_token = first["execution"]["controller_fencing_token"]
    assert first_token > 0
    replay = _claim(repo, "claim")
    assert replay["claimed"] is False
    assert replay["execution"]["idempotency_key"] == "execute-1"

    with pytest.raises(ExecutionConflict):
        with repo.transaction() as conn:
            Repository.claim_execution(
                conn, "claim", "operator", "execute-2", {}, "controller-b", 60_000
            )

    with repo.transaction() as conn:
        assert Repository.renew_execution_controller(
            conn, "claim", "controller-a", first_token, 60_000
        )
        assert Repository.prepare_execution_takeover(conn, "claim", "controller-b", 60_000) is None
        conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp() "
            "WHERE plan_id = 'claim'"
        )
        pending = Repository.prepare_execution_takeover(conn, "claim", "controller-b", 60_000)
        assert pending is not None and pending > first_token
        token = Repository.claim_execution_controller(
            conn, "claim", "controller-b", pending, 60_000
        )
        assert token == pending
        with pytest.raises(LeaseConflict):
            Repository.assert_execution_controller(conn, "claim", "controller-a", first_token)
        assert (
            Repository.assert_execution_controller(conn, "claim", "controller-b", token)["status"]
            == "EXECUTING"
        )


def test_concurrent_execution_claims_allow_one_controller(clean_database: Database) -> None:
    repo = clean_database.repository
    with repo.transaction() as conn:
        _insert_plan(conn, "race")

    def contender(key: str) -> str:
        try:
            with repo.transaction() as conn:
                claim = Repository.claim_execution(
                    conn, "race", "operator", key, {}, f"controller-{key}", 60_000
                )
                if claim["claimed"]:
                    Repository.transition_plan(
                        conn, "race", "EXECUTING", "operator", "operator", "execute", key, key
                    )
                return "claimed" if claim["claimed"] else "replayed"
        except ExecutionConflict:
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(contender, ("first", "second")))
    assert sorted(outcomes) == ["claimed", "conflict"]


def test_pause_and_cancel_wait_for_known_action_outcomes(clean_database: Database) -> None:
    repo = clean_database.repository
    with repo.transaction() as conn:
        _insert_plan(conn, "pause-cancel")
        conn.execute(
            "UPDATE plans SET binding = %s WHERE plan_id = 'pause-cancel'",
            (Jsonb({"source": {}, "conflict_keys": ["workload:test"]}),),
        )
        conn.execute(
            "INSERT INTO conflict_leases (conflict_key, owner_id, principal, fencing_token, "
            "expires_at) VALUES ('workload:test', 'lease-owner', 'operator', 7, "
            "clock_timestamp() + interval '1 hour')"
        )
        conn.execute(
            "INSERT INTO plan_leases (plan_id, conflict_key, owner_id, fencing_token) "
            "VALUES ('pause-cancel', 'workload:test', 'lease-owner', 7)"
        )
        conn.execute(
            "INSERT INTO action_intents (plan_id, intent_id, action_sequence, action_type, "
            "payload, status) VALUES ('pause-cancel', 'scale', 0, 'SET_REPLICA_COUNT', %s, "
            "'APPROVED')",
            (Jsonb({"proposed_replicas": 2}),),
        )
    claim = _claim(repo, "pause-cancel")
    token = claim["execution"]["controller_fencing_token"]

    with repo.transaction() as conn:
        Repository.transition_action(
            conn,
            "pause-cancel",
            "scale",
            "NOT_STARTED",
            "RUNNING",
            "controller-a",
            token,
            pre_observation={"replicas": 1},
        )
        request = Repository.request_execution_control(
            conn, "pause-cancel", "PAUSE", "operator requests pause"
        )
        assert request["pause_requested"] is True
        assert Repository.runnable_actions(conn, "pause-cancel", "controller-a", token) == []
        with pytest.raises(ControlError, match="unresolved"):
            Repository.transition_execution_and_plan(
                conn,
                "pause-cancel",
                "EXECUTING",
                "PAUSED",
                "controller-a",
                token,
                actor="operator",
                role="operator",
                reason="pause requested",
                request_id="pause-1",
            )
        Repository.transition_action(
            conn,
            "pause-cancel",
            "scale",
            "RUNNING",
            "SUCCEEDED",
            "controller-a",
            token,
            target_observation={"replicas": 2, "ready": 2},
        )
        Repository.transition_execution_and_plan(
            conn,
            "pause-cancel",
            "EXECUTING",
            "PAUSED",
            "controller-a",
            token,
            actor="operator",
            role="operator",
            reason="pause requested",
            request_id="pause-1",
        )
        assert (
            conn.execute("SELECT state FROM plans WHERE plan_id = 'pause-cancel'").fetchone()[
                "state"
            ]
            == "PAUSED"
        )
        assert (
            conn.execute(
                "SELECT count(*) AS total FROM plan_leases WHERE plan_id = 'pause-cancel'"
            ).fetchone()["total"]
            == 1
        )

        request = Repository.request_execution_control(
            conn, "pause-cancel", "CANCEL", "operator requests cancellation"
        )
        assert request["cancel_requested"] is True
        assert request["pause_requested"] is False
        assert [row["plan_id"] for row in Repository.runnable_executions(conn)] == ["pause-cancel"]
        Repository.transition_execution_and_plan(
            conn,
            "pause-cancel",
            "PAUSED",
            "CANCELLED",
            "controller-a",
            token,
            actor="operator",
            role="operator",
            reason="cancel requested",
            request_id="cancel-1",
            outcome={"applied_action": "scale", "replicas": 2},
        )
        assert (
            conn.execute(
                "SELECT status, finished_at FROM plan_executions WHERE plan_id = 'pause-cancel'"
            ).fetchone()["status"]
            == "CANCELLED"
        )
        assert (
            conn.execute(
                "SELECT count(*) AS total FROM plan_leases WHERE plan_id = 'pause-cancel'"
            ).fetchone()["total"]
            == 1
        )


def test_takeover_fences_unknown_targets_before_final_claim(clean_database: Database) -> None:
    repo = clean_database.repository
    with repo.transaction() as conn:
        _insert_plan(conn, "takeover")
        conn.execute(
            "INSERT INTO action_intents (plan_id, intent_id, action_sequence, action_type, "
            "workload_id, payload, status) VALUES ('takeover', 'scale', 0, "
            "'SET_REPLICA_COUNT', 'cpu-api', %s, 'APPROVED')",
            (Jsonb({"proposed_replicas": 3}),),
        )
    claim = _claim(repo, "takeover")
    old_token = claim["execution"]["controller_fencing_token"]
    with repo.transaction() as conn:
        Repository.transition_action(
            conn,
            "takeover",
            "scale",
            "NOT_STARTED",
            "RUNNING",
            "controller-a",
            old_token,
            pre_observation={"replicas": 2, "uid": "deployment-1"},
        )
        conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp() "
            "WHERE plan_id = 'takeover'"
        )
        pending_token = Repository.prepare_execution_takeover(
            conn, "takeover", "controller-b", 60_000
        )
        assert pending_token is not None and pending_token > old_token

    with pytest.raises(ControlError, match="must be fenced"):
        with repo.transaction() as conn:
            Repository.claim_execution_controller(
                conn, "takeover", "controller-b", pending_token, 60_000
            )

    with repo.transaction() as conn:
        assert (
            Repository.authorize_takeover_fence(
                conn, "takeover", "scale", "controller-b", pending_token
            )["execution_state"]
            == "RUNNING"
        )
        Repository.record_execution_fence(
            conn,
            "takeover",
            "scale",
            "controller-b",
            pending_token,
            "deployment:cpu-api",
            {"fencing_token": pending_token, "uid": "deployment-1"},
        )
        assert (
            Repository.claim_execution_controller(
                conn, "takeover", "controller-b", pending_token, 60_000
            )
            == pending_token
        )
        with pytest.raises(LeaseConflict):
            Repository.assert_execution_controller(conn, "takeover", "controller-a", old_token)
        assert Repository.runnable_actions(conn, "takeover", "controller-b", pending_token) == []


def test_workload_takeover_recovers_preapply_crash(clean_database: Database) -> None:
    repo = clean_database.repository
    with repo.transaction() as conn:
        _insert_plan(conn, "workload-preapply")
        conn.execute(
            "INSERT INTO action_intents (plan_id, intent_id, action_sequence, action_type, "
            "workload_id, payload, status) VALUES ('workload-preapply', 'scale', 0, "
            "'SET_REPLICA_COUNT', 'cpu-api', %s, 'APPROVED')",
            (Jsonb({"current_replicas": 2, "proposed_replicas": 3}),),
        )
        conn.execute(
            "INSERT INTO control_state (singleton, snapshot_id, snapshot_generation, binding) "
            "VALUES (TRUE, 'snapshot', 1, %s)",
            (Jsonb({}),),
        )
    original = _claim(repo, "workload-preapply")
    old_token = original["execution"]["controller_fencing_token"]
    with repo.transaction() as conn:
        Repository.transition_action(
            conn,
            "workload-preapply",
            "scale",
            "NOT_STARTED",
            "RUNNING",
            "controller-a",
            old_token,
            pre_observation={"replicas": 2, "uid": "deployment-uid", "generation": 1},
        )
        conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp() "
            "WHERE plan_id = 'workload-preapply'"
        )
        execution = conn.execute(
            "SELECT plan_id, controller_id, controller_fencing_token, "
            "controller_lease_expires_at <= clock_timestamp() AS controller_lease_expired "
            "FROM plan_executions WHERE plan_id = 'workload-preapply'"
        ).fetchone()

    target = FakeFleetRightWorkloadTarget(2)
    controller = ExecutionController(
        repo, target, FakeHealth(), "controller-b", readiness_timeout_seconds=2
    )
    token = controller._claim(execution)

    assert token is not None
    assert target.replicas == 2
    assert target.authorizations == 0
    with repo.transaction() as conn:
        receipt = Repository.execution_fence_receipt(conn, "workload-preapply", "scale", token)
        owner = conn.execute(
            "SELECT controller_id FROM plan_executions WHERE plan_id = 'workload-preapply'"
        ).fetchone()["controller_id"]
    assert receipt is not None and receipt["state"] == "COMPLETE"
    assert owner == "controller-b"

    with repo.transaction() as conn:
        action = Repository.incomplete_actions(conn, "workload-preapply")[0]
    controller._reconcile_action("workload-preapply", token, action, False)
    with repo.transaction() as conn:
        execution = conn.execute(
            "SELECT status, outcome FROM plan_executions WHERE plan_id = 'workload-preapply'"
        ).fetchone()
    assert target.replicas == 3, execution
    assert target.authorizations == 1


def test_workload_second_controller_recovers_fence_written_before_first_claim(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    controller_a, _legacy_target, _health = _prepare_controller(
        repo, "workload-second-fence", 2, 3
    )
    target = FakeFleetRightWorkloadTarget(2)
    target.lose_before_authorize = True
    controller_a.target = target
    controller_a.run_once()
    with repo.transaction() as conn:
        conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp() "
            "WHERE plan_id = 'workload-second-fence'"
        )

    controller_b = ExecutionController(repo, target, FakeHealth(), "controller-b")
    target.crash_after_fence = True
    with pytest.raises(SystemExit, match="crash after target fence"):
        controller_b.run_once()
    with repo.transaction() as conn:
        row = conn.execute(
            "SELECT pending_controller_fencing_token FROM plan_executions "
            "WHERE plan_id = 'workload-second-fence'"
        ).fetchone()
        token_b = row["pending_controller_fencing_token"]
        pending = Repository.execution_fence_receipt(
            conn, "workload-second-fence", "scale", token_b
        )
        assert pending is not None and pending["state"] == "PENDING"
        conn.execute(
            "UPDATE plan_executions SET pending_controller_lease_expires_at = clock_timestamp() "
            "WHERE plan_id = 'workload-second-fence'"
        )
        execution = conn.execute(
            "SELECT plan_id, controller_id, controller_fencing_token, "
            "controller_lease_expires_at <= clock_timestamp() AS controller_lease_expired "
            "FROM plan_executions WHERE plan_id = 'workload-second-fence'"
        ).fetchone()

    controller_c = ExecutionController(repo, target, FakeHealth(), "controller-c")
    token_c = controller_c._claim(execution)

    assert token_c is not None and token_c > token_b
    assert target.replicas == 2
    assert target.authorizations == 0
    assert target.execution_fence["generation"] == token_c
    with repo.transaction() as conn:
        receipt = Repository.execution_fence_receipt(
            conn, "workload-second-fence", "scale", token_c
        )
    assert receipt is not None and receipt["state"] == "COMPLETE"
    assert receipt["prior_execution_fence"]["generation"] == token_b
    assert receipt["fence_precondition"]["generation"] + 1 == receipt["generation"]


def test_old_takeover_receipt_cannot_close_or_unfence_a_new_dispatch(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    controller_a, _legacy_target, _health = _prepare_controller(
        repo, "workload-stale-receipt", 8, 9
    )
    target = FakeFleetRightWorkloadTarget(8)
    target.lose_before_authorize = True
    controller_a.target = target
    controller_a.run_once()
    with repo.transaction() as conn:
        conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp() "
            "WHERE plan_id = 'workload-stale-receipt'"
        )

    controller_b = ExecutionController(repo, target, FakeHealth(), "controller-b")
    target.lose_before_authorize = True
    controller_b.run_once()
    with repo.transaction() as conn:
        action = Repository.incomplete_actions(conn, "workload-stale-receipt")[0]
        execution = conn.execute(
            "SELECT plan_id, controller_id, controller_fencing_token, "
            "controller_lease_expires_at <= clock_timestamp() AS controller_lease_expired "
            "FROM plan_executions WHERE plan_id = 'workload-stale-receipt'"
        ).fetchone()
        token_b = execution["controller_fencing_token"]
        dispatch = ExecutionController._latest_dispatch(action)
        receipt_b = Repository.execution_fence_receipt(
            conn, "workload-stale-receipt", "scale", token_b
        )
    assert dispatch is not None and receipt_b is not None
    target_token = dispatch["target_fencing_token"]
    stale_observation = next(
        item["observation"]
        for item in reversed(action["execution_result"]["progress"])
        if isinstance(item.get("result"), dict)
        and "dispatch_target_replicas" in item["result"]
    )
    assert target_token > token_b
    assert target.replicas == 8 and target.execution_request is None

    controller_b.run_once()
    with repo.transaction() as conn:
        action = Repository.incomplete_actions(conn, "workload-stale-receipt")[0]
        execution = conn.execute(
            "SELECT plan_id, controller_id, controller_fencing_token, status, "
            "controller_lease_expires_at <= clock_timestamp() AS controller_lease_expired "
            "FROM plan_executions WHERE plan_id = 'workload-stale-receipt'"
        ).fetchone()
    assert action["execution_state"] == "OUTCOME_UNKNOWN"
    assert execution["status"] == "EXECUTING"
    assert execution["controller_lease_expired"]

    controller_c = ExecutionController(
        repo, target, FakeHealth(), "controller-c", max_attempts=3
    )
    token_c = controller_c._claim(execution)
    assert token_c is not None and token_c > target_token
    with pytest.raises(TargetPreconditionFailed):
        target.authorize_scale(
            "cpu-api",
            target_token,
            expected=stale_observation,
            plan_id="workload-stale-receipt",
            operation_id="scale",
            target_replicas=9,
        )
    assert target.replicas == 8 and target.execution_request is None

    with repo.transaction() as conn:
        action = Repository.incomplete_actions(conn, "workload-stale-receipt")[0]
    controller_c._reconcile_action("workload-stale-receipt", token_c, action, False)
    target.mark_applied()
    for _ in range(5):
        controller_c.run_once()
    with repo.transaction() as conn:
        action_state = conn.execute(
            "SELECT execution_state FROM action_intents WHERE plan_id = 'workload-stale-receipt'"
        ).fetchone()["execution_state"]
        execution_state = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'workload-stale-receipt'"
        ).fetchone()["status"]
    assert action_state == "SUCCEEDED"
    assert execution_state == "COMMITTED"
    assert target.authorizations == 1


def test_workload_takeover_retry_only_fences_when_authority_cas_conflicts(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    with repo.transaction() as conn:
        _insert_plan(conn, "workload-fence-conflict")
        conn.execute(
            "INSERT INTO action_intents (plan_id, intent_id, action_sequence, action_type, "
            "workload_id, payload, status) VALUES ('workload-fence-conflict', 'scale', 0, "
            "'SET_REPLICA_COUNT', 'cpu-api', %s, 'APPROVED')",
            (Jsonb({"current_replicas": 2, "proposed_replicas": 3}),),
        )
        conn.execute(
            "INSERT INTO control_state (singleton, snapshot_id, snapshot_generation, binding) "
            "VALUES (TRUE, 'snapshot', 1, %s)",
            (Jsonb({}),),
        )
    original = _claim(repo, "workload-fence-conflict")
    old_token = original["execution"]["controller_fencing_token"]
    with repo.transaction() as conn:
        Repository.transition_action(
            conn,
            "workload-fence-conflict",
            "scale",
            "NOT_STARTED",
            "RUNNING",
            "controller-a",
            old_token,
            pre_observation={"replicas": 2, "uid": "deployment-uid", "generation": 1},
        )
        Repository.request_execution_control(
            conn, "workload-fence-conflict", "CANCEL", "operator cancel"
        )
        conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp() "
            "WHERE plan_id = 'workload-fence-conflict'"
        )
        execution = conn.execute(
            "SELECT plan_id, controller_id, controller_fencing_token, "
            "controller_lease_expires_at <= clock_timestamp() AS controller_lease_expired "
            "FROM plan_executions WHERE plan_id = 'workload-fence-conflict'"
        ).fetchone()

    target = FakeFleetRightWorkloadTarget(2)
    original_fence = target.fence_scale
    conflicted = False

    def conflict_once(*args: object, **kwargs: object) -> dict[str, object]:
        nonlocal conflicted
        if not conflicted:
            conflicted = True
            target.resource_version = "2"
            raise TargetPreconditionFailed("simulated status resourceVersion conflict")
        return original_fence(*args, **kwargs)  # type: ignore[arg-type]

    target.fence_scale = conflict_once  # type: ignore[method-assign]
    controller = ExecutionController(repo, target, FakeHealth(), "controller-b")
    token = controller._claim(execution)

    assert token is not None
    assert target.replicas == 2
    assert target.authorizations == 0
    with repo.transaction() as conn:
        action = Repository.incomplete_actions(conn, "workload-fence-conflict")[0]
        receipt = Repository.execution_fence_receipt(
            conn, "workload-fence-conflict", "scale", token
        )
    controller._reconcile_action(
        "workload-fence-conflict", token, action, False, allow_dispatch=False
    )
    assert target.replicas == 2
    assert target.authorizations == 0
    assert receipt is not None and receipt["state"] == "COMPLETE"


def test_workload_takeover_refreshes_precondition_after_delayed_request_wins_race(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    controller_a, _legacy_target, health = _prepare_controller(
        repo, "workload-delayed-request", 8, 9
    )
    target = FakeFleetRightWorkloadTarget(8)
    target.lose_before_authorize = True
    controller_a.target = target
    controller_a.run_once()
    with repo.transaction() as conn:
        action = Repository.incomplete_actions(conn, "workload-delayed-request")[0]
        execution = conn.execute(
            "SELECT plan_id, controller_id, controller_fencing_token, "
            "controller_lease_expires_at <= clock_timestamp() AS controller_lease_expired "
            "FROM plan_executions WHERE plan_id = 'workload-delayed-request'"
        ).fetchone()
        conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp() "
            "WHERE plan_id = 'workload-delayed-request'"
        )
    dispatch = ExecutionController._latest_dispatch(action)
    assert dispatch is not None and target.replicas == 8

    original_fence = target.fence_scale
    delayed = False

    def publish_delayed_request(*args: object, **kwargs: object) -> dict[str, object]:
        nonlocal delayed
        if not delayed:
            delayed = True
            precondition = action["execution_result"]["pre_observation"]
            target.authorize_scale(
                "cpu-api",
                dispatch["target_fencing_token"],
                expected=precondition,
                plan_id="workload-delayed-request",
                operation_id="scale",
                target_replicas=9,
            )
        return original_fence(*args, **kwargs)  # type: ignore[arg-type]

    target.fence_scale = publish_delayed_request  # type: ignore[method-assign]
    controller_b = ExecutionController(repo, target, health, "controller-b")
    token = controller_b._claim(execution)

    assert token is not None and delayed
    assert target.replicas == 9
    assert target.authorizations == 1
    with repo.transaction() as conn:
        receipt = Repository.execution_fence_receipt(
            conn, "workload-delayed-request", "scale", token
        )
    assert receipt is not None and receipt["state"] == "COMPLETE"
    assert receipt["operation_applied_before_fence"] is True
    assert receipt["precondition"] == receipt["fence_precondition"]
    assert receipt["precondition"]["replicas"] == 9

    controller_b.run_once()
    assert target.authorizations == 1
    target.mark_applied()
    for _ in range(5):
        controller_b.run_once()
    with repo.transaction() as conn:
        action_state = conn.execute(
            "SELECT execution_state FROM action_intents "
            "WHERE plan_id = 'workload-delayed-request'"
        ).fetchone()["execution_state"]
        execution_state = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'workload-delayed-request'"
        ).fetchone()["status"]
    assert action_state == "SUCCEEDED"
    assert execution_state == "COMMITTED"
    assert target.authorizations == 1


def test_workload_takeover_recovers_response_recorded_before_crash(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    controller_a, _target, _health = _prepare_controller(repo, "workload-response-crash", 8, 9)
    target = FakeFleetRightWorkloadTarget(8)
    controller_a.target = target
    with repo.transaction() as conn:
        execution = conn.execute(
            "SELECT controller_fencing_token FROM plan_executions "
            "WHERE plan_id = 'workload-response-crash'"
        ).fetchone()
        token = execution["controller_fencing_token"]
        action = Repository.runnable_actions(
            conn, "workload-response-crash", "controller-a", token
        )[0]
        Repository.transition_action(
            conn,
            "workload-response-crash",
            action["intent_id"],
            "NOT_STARTED",
            "RUNNING",
            "controller-a",
            token,
            pre_observation=target.observe_scale("cpu-api"),
            result={"desired_replicas": 9},
        )
        action = Repository.incomplete_actions(conn, "workload-response-crash")[0]

    def crash_after_dispatch(*_args: object, **_kwargs: object) -> None:
        raise SystemExit("injected crash after durable response")

    controller_a._reconcile_action = crash_after_dispatch  # type: ignore[method-assign]
    with pytest.raises(SystemExit, match="after durable response"):
        controller_a._dispatch(
            "workload-response-crash",
            token,
            action,
            target.observe_scale("cpu-api"),
            9,
            False,
        )
    assert target.replicas == 9
    with repo.transaction() as conn:
        conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp() "
            "WHERE plan_id = 'workload-response-crash'"
        )
        execution = conn.execute(
            "SELECT plan_id, controller_id, controller_fencing_token, "
            "controller_lease_expires_at <= clock_timestamp() AS controller_lease_expired "
            "FROM plan_executions WHERE plan_id = 'workload-response-crash'"
        ).fetchone()

    controller_b = ExecutionController(repo, target, FakeHealth(), "controller-b")
    token = controller_b._claim(execution)
    assert token is not None
    target.mark_applied()
    with repo.transaction() as conn:
        action = Repository.incomplete_actions(conn, "workload-response-crash")[0]
    controller_b._reconcile_action("workload-response-crash", token, action, False)

    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state FROM action_intents WHERE plan_id = 'workload-response-crash'"
        ).fetchone()
    assert action["execution_state"] == "SUCCEEDED"
    assert target.replicas == 9
    assert target.authorizations == 1


def test_unknown_action_blocks_dispatch_until_observed(clean_database: Database) -> None:
    repo = clean_database.repository
    with repo.transaction() as conn:
        _insert_plan(conn, "actions")
        for sequence, intent_id, desired_replicas in ((0, "a", 2), (1, "b", 4)):
            conn.execute(
                "INSERT INTO action_intents (plan_id, intent_id, action_sequence, action_type, "
                "payload, status) VALUES ('actions', %s, %s, 'SET_REPLICA_COUNT', %s, 'APPROVED')",
                (intent_id, sequence, Jsonb({"proposed_replicas": desired_replicas})),
            )
    claim = _claim(repo, "actions")
    token = claim["execution"]["controller_fencing_token"]

    with repo.transaction() as conn:
        assert [
            row["intent_id"]
            for row in Repository.runnable_actions(conn, "actions", "controller-a", token)
        ] == ["a"]
        Repository.transition_action(
            conn,
            "actions",
            "a",
            "NOT_STARTED",
            "RUNNING",
            "controller-a",
            token,
            pre_observation={"replicas": 1},
        )
        assert (
            conn.execute(
                "SELECT attempt_number FROM action_intents "
                "WHERE plan_id = 'actions' AND intent_id = 'a'"
            ).fetchone()["attempt_number"]
            == 1
        )
        Repository.authorize_action(conn, "actions", "a", "controller-a", token)
        progress = Repository.record_action_progress(
            conn,
            "actions",
            "a",
            "controller-a",
            token,
            observation={"replicas": 1, "ready": 1},
            result={"step": "wait-not-applied"},
        )
        assert progress["execution_result"]["progress"][0]["observation"] == {
            "replicas": 1,
            "ready": 1,
        }
        Repository.transition_action(
            conn,
            "actions",
            "a",
            "RUNNING",
            "OUTCOME_UNKNOWN",
            "controller-a",
            token,
            error="target timeout",
        )
        assert Repository.runnable_actions(conn, "actions", "controller-a", token) == []
        assert [
            row["execution_state"] for row in Repository.incomplete_actions(conn, "actions")
        ] == ["OUTCOME_UNKNOWN", "NOT_STARTED"]
        with pytest.raises(ControlError, match="observer result"):
            Repository.transition_action(
                conn,
                "actions",
                "a",
                "OUTCOME_UNKNOWN",
                "RUNNING",
                "controller-a",
                token,
            )
        with pytest.raises(ValueError, match="positive attempt limit"):
            Repository.transition_action(
                conn,
                "actions",
                "a",
                "OUTCOME_UNKNOWN",
                "RUNNING",
                "controller-a",
                token,
                target_observation={"replicas": 1, "ready": 1},
                retry_proven_not_applied=True,
            )
        with pytest.raises(ControlError, match="attempt limit reached"):
            Repository.transition_action(
                conn,
                "actions",
                "a",
                "OUTCOME_UNKNOWN",
                "RUNNING",
                "controller-a",
                token,
                target_observation={"replicas": 1, "ready": 1},
                retry_proven_not_applied=True,
                max_attempts=1,
            )
        retry = Repository.transition_action(
            conn,
            "actions",
            "a",
            "OUTCOME_UNKNOWN",
            "RUNNING",
            "controller-a",
            token,
            target_observation={"replicas": 1, "ready": 1},
            retry_proven_not_applied=True,
            max_attempts=2,
        )
        assert retry["attempt_number"] == 2
        assert retry["execution_result"]["pre_observation"] == {"replicas": 1}
        assert retry["execution_result"]["progress"][0]["result"] == {"step": "wait-not-applied"}
        assert retry["execution_result"]["unknown_resolutions"][0]["target_observation"] == {
            "replicas": 1,
            "ready": 1,
        }
        assert (
            retry["execution_result"]["unknown_resolutions"][0]["retry_proven_not_applied"] is True
        )
        Repository.authorize_action(conn, "actions", "a", "controller-a", token)
        completed = Repository.transition_action(
            conn,
            "actions",
            "a",
            "RUNNING",
            "SUCCEEDED",
            "controller-a",
            token,
            target_observation={"replicas": 2, "ready": 2},
        )
        assert completed["attempt_number"] == 2
        assert completed["execution_result"]["target_observation"] == {
            "replicas": 2,
            "ready": 2,
        }
        assert completed["execution_result"]["result"] is None
        assert completed["execution_result"]["progress"][0]["result"] == {
            "step": "wait-not-applied"
        }
        assert [
            row["intent_id"]
            for row in Repository.runnable_actions(conn, "actions", "controller-a", token)
        ] == ["b"]
        started = Repository.transition_action(
            conn,
            "actions",
            "b",
            "NOT_STARTED",
            "RUNNING",
            "controller-a",
            token,
            pre_observation={"replicas": 2, "ready": 2},
        )
        assert started["attempt_number"] == 1
        Repository.authorize_action(conn, "actions", "b", "controller-a", token)
        Repository.transition_action(
            conn,
            "actions",
            "b",
            "RUNNING",
            "OUTCOME_UNKNOWN",
            "controller-a",
            token,
            error="partial progress response unknown",
        )
        resumed = Repository.transition_action(
            conn,
            "actions",
            "b",
            "OUTCOME_UNKNOWN",
            "RUNNING",
            "controller-a",
            token,
            target_observation={"replicas": 3, "ready": 2},
        )
        assert resumed["attempt_number"] == 1
        assert (
            resumed["execution_result"]["unknown_resolutions"][0]["retry_proven_not_applied"]
            is False
        )
        Repository.authorize_action(conn, "actions", "b", "controller-a", token)
        completed_b = Repository.transition_action(
            conn,
            "actions",
            "b",
            "RUNNING",
            "SUCCEEDED",
            "controller-a",
            token,
            target_observation={"replicas": 4, "ready": 4},
        )
        assert completed_b["attempt_number"] == 1


def test_compensation_is_persisted_once_before_dispatch(clean_database: Database) -> None:
    repo = clean_database.repository
    with repo.transaction() as conn:
        _insert_plan(conn, "compensate")
        conn.execute(
            "INSERT INTO action_intents (plan_id, intent_id, action_sequence, action_type, "
            "workload_id, payload, status) VALUES ('compensate', 'scale', 0, "
            "'SET_REPLICA_COUNT', 'cpu-api', %s, 'APPROVED')",
            (Jsonb({"current_replicas": 2, "proposed_replicas": 3}),),
        )
    claim = _claim(repo, "compensate")
    token = claim["execution"]["controller_fencing_token"]

    with repo.transaction() as conn:
        Repository.transition_action(
            conn,
            "compensate",
            "scale",
            "NOT_STARTED",
            "RUNNING",
            "controller-a",
            token,
            pre_observation={"replicas": 2, "uid": "deployment-1"},
        )
        Repository.transition_action(
            conn,
            "compensate",
            "scale",
            "RUNNING",
            "SUCCEEDED",
            "controller-a",
            token,
            target_observation={"replicas": 3, "ready": 3},
        )
        Repository.request_execution_control(
            conn, "compensate", "PAUSE", "operator requested pause before health rollback"
        )
        Repository.transition_execution_and_plan(
            conn,
            "compensate",
            "EXECUTING",
            "COMPENSATING",
            "controller-a",
            token,
            actor="system",
            role="controller",
            reason="restore",
            request_id="r2",
            idempotency_key="execute-1",
            outcome={"observed_health": "regressed"},
        )
        execution_row = conn.execute(
            "SELECT pause_requested, cancel_requested, outcome FROM plan_executions "
            "WHERE plan_id = 'compensate'"
        ).fetchone()
        assert execution_row["pause_requested"] is False
        assert execution_row["cancel_requested"] is False
        assert execution_row["outcome"]["control_request"]["reason"] == (
            "operator requested pause before health rollback"
        )
        assert execution_row["outcome"]["COMPENSATING"]["deferred_control_request"]["reason"] == (
            "operator requested pause before health rollback"
        )
        payload = {"replicas": 2, "expected_replicas": 3, "uid": "deployment-1"}
        first = Repository.persist_compensation_action(
            conn,
            "compensate",
            "scale",
            "RESTORE_REPLICA_COUNT",
            "cpu-api",
            payload,
            "controller-a",
            token,
        )
        replay = Repository.persist_compensation_action(
            conn,
            "compensate",
            "scale",
            "RESTORE_REPLICA_COUNT",
            "cpu-api",
            payload,
            "controller-a",
            token,
        )
        assert first["intent_id"] == replay["intent_id"] == "compensate:scale"
        assert first["action_sequence"] == replay["action_sequence"] == 1
        assert (
            Repository.runnable_actions(
                conn, "compensate", "controller-a", token, compensation=True
            )[0]["intent_id"]
            == "compensate:scale"
        )
        recovering = Repository.runnable_executions(conn)
        assert len(recovering) == 1
        assert recovering[0]["plan_id"] == "compensate"
        assert recovering[0]["status"] == recovering[0]["plan_state"] == "COMPENSATING"
        assert recovering[0]["state_version"] == 2
        assert recovering[0]["binding"] == {"source": {}, "conflict_keys": []}
        assert recovering[0]["expires_at_ms"] > 0
        assert recovering[0]["controller_id"] == "controller-a"
        assert recovering[0]["controller_fencing_token"] == token
        assert recovering[0]["controller_lease_expires_at"] is not None
        Repository.transition_action(
            conn,
            "compensate",
            "compensate:scale",
            "NOT_STARTED",
            "RUNNING",
            "controller-a",
            token,
            pre_observation={"replicas": 3, "uid": "deployment-1"},
        )
        conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp() "
            "WHERE plan_id = 'compensate'"
        )
        pending = Repository.prepare_execution_takeover(conn, "compensate", "controller-b", 60_000)
        assert pending is not None and pending > token
        with pytest.raises(ControlError, match="must be fenced"):
            Repository.claim_execution_controller(
                conn, "compensate", "controller-b", pending, 60_000
            )
        Repository.authorize_takeover_fence(
            conn, "compensate", "compensate:scale", "controller-b", pending
        )
        Repository.record_execution_fence(
            conn,
            "compensate",
            "compensate:scale",
            "controller-b",
            pending,
            "deployment:cpu-api",
            {"fencing_token": pending, "uid": "deployment-1"},
        )
        assert (
            Repository.claim_execution_controller(
                conn, "compensate", "controller-b", pending, 60_000
            )
            == pending
        )
        incomplete = Repository.incomplete_actions(conn, "compensate")
        assert [
            (action["intent_id"], action["action_sequence"], action["execution_state"])
            for action in incomplete
        ] == [("compensate:scale", 1, "RUNNING")]
        assert (
            Repository.runnable_actions(
                conn, "compensate", "controller-b", pending, compensation=True
            )
            == []
        )


def test_lost_response_after_target_success_recovers_without_duplicate_mutation(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    with repo.transaction() as conn:
        _insert_plan(conn, "lost-response")
        conn.execute(
            "INSERT INTO control_state "
            "(singleton, snapshot_id, snapshot_generation, binding) VALUES "
            "(TRUE, 'snapshot', 1, %s)",
            (Jsonb({}),),
        )
        conn.execute(
            "INSERT INTO action_intents (plan_id, intent_id, action_sequence, action_type, "
            "workload_id, payload, status) VALUES ('lost-response', 'scale', 0, "
            "'SET_REPLICA_COUNT', 'cpu-api', %s, 'APPROVED')",
            (
                Jsonb(
                    {
                        "current_replicas": 8,
                        "proposed_replicas": 9,
                        "target_precondition": {
                            "uid": "deployment-uid",
                            "generation": 1,
                            "resource_version": "1",
                            "replicas": 8,
                        },
                        "health": {
                            "p99_limit_ms": 120,
                            "error_slo_ppm": 1_000,
                            "max_queue_growth_milli": 0,
                        },
                    }
                ),
            ),
        )
    _claim(repo, "lost-response")
    target = FakeScaleTarget(8, lose_next_response=True)
    controller_a = ExecutionController(repo, target, FakeHealth(), "controller-a")
    controller_a.run_once()
    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state, attempt_number FROM action_intents "
            "WHERE plan_id = 'lost-response'"
        ).fetchone()
        assert action["execution_state"] == "OUTCOME_UNKNOWN"
        assert action["attempt_number"] == 1
        conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp() "
            "WHERE plan_id = 'lost-response'"
        )

    controller_b = ExecutionController(repo, target, FakeHealth(), "controller-b")
    controller_b.run_once()
    controller_b.run_once()
    controller_b.run_once()
    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state, attempt_number FROM action_intents "
            "WHERE plan_id = 'lost-response'"
        ).fetchone()
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'lost-response'"
        ).fetchone()
        assert action["execution_state"] == "SUCCEEDED"
        assert action["attempt_number"] == 1
        assert execution["status"] == "COMMITTED"
    assert target.replicas == 9
    assert target.mutations == [9]
    assert target.fence_calls == 2


def test_cancel_fences_unknown_not_applied_request_before_terminal_action(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    controller, target, _health = _prepare_controller(repo, "cancel-unknown", 8, 9)
    target.lose_before_apply = True
    target.generation_tracks_spec = True
    controller.run_once()
    assert target.replicas == 8

    with repo.transaction() as conn:
        Repository.request_execution_control(conn, "cancel-unknown", "CANCEL", "operator cancel")

    for _ in range(4):
        controller.run_once()

    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state, execution_result FROM action_intents "
            "WHERE plan_id = 'cancel-unknown'"
        ).fetchone()
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'cancel-unknown'"
        ).fetchone()
    assert action["execution_state"] == "FAILED"
    assert action["execution_result"]["result"]["target_fence"]["token"] == target.fencing_token
    assert execution["status"] == "CANCELLED"
    assert target.mutations == []
    assert target.fence_calls == 2


def test_cancel_keeps_workload_outcome_unknown_until_controller_applies_it(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    controller, _legacy_target, _health = _prepare_controller(repo, "workload-cancel-unknown", 8, 9)
    target = FakeFleetRightWorkloadTarget(8, lose_next_response=True)
    controller.target = target
    controller.run_once()

    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state FROM action_intents WHERE plan_id = 'workload-cancel-unknown'"
        ).fetchone()
        assert action["execution_state"] == "OUTCOME_UNKNOWN"
        Repository.request_execution_control(
            conn, "workload-cancel-unknown", "CANCEL", "operator cancel"
        )

    for _ in range(4):
        controller.run_once()
    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state FROM action_intents WHERE plan_id = 'workload-cancel-unknown'"
        ).fetchone()
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'workload-cancel-unknown'"
        ).fetchone()
    assert action["execution_state"] == "OUTCOME_UNKNOWN"
    assert execution["status"] == "EXECUTING"
    assert target.observed_replicas == 8
    assert target.replicas == 9

    target.mark_applied()
    for _ in range(5):
        controller.run_once()

    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state FROM action_intents WHERE plan_id = 'workload-cancel-unknown'"
        ).fetchone()
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'workload-cancel-unknown'"
        ).fetchone()
    assert action["execution_state"] == "SUCCEEDED"
    assert execution["status"] == "CANCELLED"
    assert target.authorizations == 1
    assert target.replicas == target.observed_replicas == 9


@pytest.mark.parametrize("control, terminal", [("CANCEL", "CANCELLED"), ("PAUSE", "PAUSED")])
def test_control_resolves_applied_unknown_action_without_replay(
    clean_database: Database, control: str, terminal: str
) -> None:
    repo = clean_database.repository
    controller, target, _health = _prepare_controller(repo, f"{control.lower()}-applied", 8, 9)
    target.generation_tracks_spec = True
    target.lose_next_response = True
    controller.run_once()
    assert target.replicas == 9
    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state FROM action_intents WHERE plan_id = %s",
            (f"{control.lower()}-applied",),
        ).fetchone()
        assert action["execution_state"] == "OUTCOME_UNKNOWN"
        Repository.request_execution_control(
            conn, f"{control.lower()}-applied", control, f"operator {control.lower()}"
        )

    for _ in range(5):
        controller.run_once()

    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state FROM action_intents WHERE plan_id = %s",
            (f"{control.lower()}-applied",),
        ).fetchone()
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = %s",
            (f"{control.lower()}-applied",),
        ).fetchone()
    assert execution["status"] == terminal
    assert action["execution_state"] == "SUCCEEDED"
    assert target.replicas == 9
    assert target.mutations == [9]


def test_unknown_action_rejects_external_generation_change_before_retry(
    clean_database: Database,
) -> None:
    controller, target, _health = _prepare_controller(
        clean_database.repository, "unknown-generation-change", 8, 9
    )
    target.lose_before_apply = True
    target.generation_tracks_spec = True
    controller.run_once()
    target.generation += 1

    for _ in range(5):
        controller.run_once()

    with clean_database.repository.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state, execution_error FROM action_intents "
            "WHERE plan_id = 'unknown-generation-change'"
        ).fetchone()
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'unknown-generation-change'"
        ).fetchone()
    assert action["execution_state"] == "FAILED"
    assert action["execution_error"] == "target-generation-changed-during-unknown-action"
    assert execution["status"] == "FAILED_SAFE"
    assert target.mutations == []


def test_takeover_recovers_crash_after_target_fence_before_receipt_commit(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    controller_a, target, _health = _prepare_controller(repo, "fence-crash", 8, 9)
    target.generation_tracks_spec = True
    target.lose_before_apply = True
    controller_a.run_once()
    target.crash_after_fence = True
    with repo.transaction() as conn:
        conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp() "
            "WHERE plan_id = 'fence-crash'"
        )

    controller_b = ExecutionController(repo, target, FakeHealth(), "controller-b")
    with pytest.raises(SystemExit, match="crash after target fence"):
        controller_b.run_once()
    with repo.transaction() as conn:
        pending_token = conn.execute(
            "SELECT pending_controller_fencing_token FROM plan_executions "
            "WHERE plan_id = 'fence-crash'"
        ).fetchone()["pending_controller_fencing_token"]
        pending = Repository.execution_fence_receipt(conn, "fence-crash", "scale", pending_token)
        assert pending is not None and pending["state"] == "PENDING"
        assert (
            conn.execute(
                "SELECT execution_state FROM action_intents WHERE plan_id = 'fence-crash'"
            ).fetchone()["execution_state"]
            == "OUTCOME_UNKNOWN"
        )

    for _ in range(5):
        controller_b.run_once()
    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state, attempt_number FROM action_intents "
            "WHERE plan_id = 'fence-crash'"
        ).fetchone()
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'fence-crash'"
        ).fetchone()
        receipt = Repository.execution_fence_receipt(conn, "fence-crash", "scale", pending_token)
    assert action["execution_state"] == "SUCCEEDED"
    assert action["attempt_number"] == 2
    assert execution["status"] == "COMMITTED"
    assert receipt is not None and receipt["state"] == "COMPLETE"
    assert receipt["precondition"]["generation"] + 1 == receipt["generation"]
    assert target.replicas == 9
    assert target.mutations == [9]


def test_takeover_recovers_published_fence_then_detects_external_generation_change(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    controller_a, target, _health = _prepare_controller(repo, "fence-crash-external", 8, 9)
    target.generation_tracks_spec = True
    target.lose_before_apply = True
    controller_a.run_once()
    target.crash_after_fence = True
    with repo.transaction() as conn:
        conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp() "
            "WHERE plan_id = 'fence-crash-external'"
        )

    controller_b = ExecutionController(repo, target, FakeHealth(), "controller-b")
    with pytest.raises(SystemExit, match="crash after target fence"):
        controller_b.run_once()
    target.generation += 1

    for _ in range(4):
        controller_b.run_once()

    with repo.transaction() as conn:
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'fence-crash-external'"
        ).fetchone()
        action = conn.execute(
            "SELECT execution_state, execution_error FROM action_intents "
            "WHERE plan_id = 'fence-crash-external'"
        ).fetchone()
    assert execution["status"] == "FAILED_SAFE"
    assert action["execution_state"] == "FAILED"
    assert action["execution_error"] == "target-generation-changed-during-unknown-action"
    assert target.mutations == []


def test_external_change_during_takeover_is_fenced_then_failed_safe(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    controller_a, target, _health = _prepare_controller(repo, "takeover-race", 8, 9)
    target.generation_tracks_spec = True
    target.lose_before_apply = True
    controller_a.run_once()
    target.change_generation_before_fence = True
    with repo.transaction() as conn:
        conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp() "
            "WHERE plan_id = 'takeover-race'"
        )

    controller_b = ExecutionController(repo, target, FakeHealth(), "controller-b")
    for _ in range(4):
        controller_b.run_once()

    with repo.transaction() as conn:
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'takeover-race'"
        ).fetchone()
        action = conn.execute(
            "SELECT execution_state, execution_error FROM action_intents "
            "WHERE plan_id = 'takeover-race'"
        ).fetchone()
    assert execution["status"] == "FAILED_SAFE"
    assert action["execution_state"] == "FAILED"
    assert action["execution_error"] == "target-generation-changed-during-unknown-action"
    assert target.mutations == []


def test_expired_plan_can_close_fenced_unknown_action_without_retry(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    controller, target, _health = _prepare_controller(repo, "expired-unknown", 8, 9)
    target.generation_tracks_spec = True
    target.lose_before_apply = True
    controller.run_once()
    with repo.transaction() as conn:
        conn.execute(
            "UPDATE plans SET expires_at_ms = "
            "floor(extract(epoch FROM clock_timestamp()) * 1000) - 1 "
            "WHERE plan_id = 'expired-unknown'"
        )

    for _ in range(5):
        controller.run_once()

    with repo.transaction() as conn:
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'expired-unknown'"
        ).fetchone()
        action = conn.execute(
            "SELECT execution_state, execution_error FROM action_intents "
            "WHERE plan_id = 'expired-unknown'"
        ).fetchone()
    assert execution["status"] == "FAILED_SAFE"
    assert action["execution_state"] == "FAILED"
    assert action["execution_error"] == "target-precondition-changed"
    assert target.mutations == []


def test_restart_after_external_success_observes_before_any_replay(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    controller, target, _health = _prepare_controller(repo, "crash-after-success", 8, 9)
    target.crash_after_apply = True
    with pytest.raises(SystemExit, match="injected controller crash"):
        controller.run_once()
    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state, attempt_number, execution_result FROM action_intents "
            "WHERE plan_id = 'crash-after-success'"
        ).fetchone()
        assert action["execution_state"] == "RUNNING"
        assert action["attempt_number"] == 1
        assert action["execution_result"]["progress"][-1]["result"]["dispatch_target_replicas"] == 9

    restarted = ExecutionController(repo, target, FakeHealth(), "controller-a")
    for _ in range(5):
        restarted.run_once()
    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state, attempt_number FROM action_intents "
            "WHERE plan_id = 'crash-after-success'"
        ).fetchone()
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'crash-after-success'"
        ).fetchone()
    assert action["execution_state"] == "SUCCEEDED"
    assert action["attempt_number"] == 1
    assert execution["status"] == "COMMITTED"
    assert target.mutations == [9]


def test_destructive_slo_regression_restores_capacity_and_measures_recovery(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    controller, target, health = _prepare_controller(repo, "regression", 10, 9)
    health.samples = [True, False, True, True]

    controller.run_once()
    controller.run_once()
    controller.run_once()

    with repo.transaction() as conn:
        execution = conn.execute(
            "SELECT status, outcome FROM plan_executions WHERE plan_id = 'regression'"
        ).fetchone()
        compensation = conn.execute(
            "SELECT execution_state, execution_result FROM action_intents "
            "WHERE plan_id = 'regression' AND compensates_intent_id = 'scale'"
        ).fetchone()
    assert execution["status"] == "COMPENSATED"
    assert compensation["execution_state"] == "SUCCEEDED"
    assert execution["outcome"]["COMPENSATED"]["recovery_duration_ms"] >= 0
    assert target.replicas == 10
    assert target.mutations == [9, 10]


def test_approval_target_generation_change_fails_before_mutation(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    controller, target, _health = _prepare_controller(
        repo, "stale-target", 8, 9, ["workload:cpu-api"]
    )
    target.generation += 1

    controller.run_once()

    with repo.transaction() as conn:
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'stale-target'"
        ).fetchone()
        action = conn.execute(
            "SELECT execution_state FROM action_intents WHERE plan_id = 'stale-target'"
        ).fetchone()
        lease = conn.execute(
            "SELECT count(*) AS n FROM plan_leases WHERE plan_id = 'stale-target'"
        ).fetchone()
        expired = conn.execute(
            "SELECT expires_at <= clock_timestamp() AS value FROM conflict_leases "
            "WHERE conflict_key = 'workload:cpu-api'"
        ).fetchone()
    assert execution["status"] == "FAILED_SAFE"
    assert action["execution_state"] == "NOT_STARTED"
    assert target.mutations == []
    assert lease["n"] == 0
    assert expired["value"] is True


def test_external_target_state_conflict_resolves_unknown_without_wedging(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    controller, target, _health = _prepare_controller(repo, "external-conflict", 8, 9)
    target.lose_next_response = True
    controller.run_once()
    target.replicas = 7
    target.generation += 1

    for _ in range(4):
        controller.run_once()

    with repo.transaction() as conn:
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'external-conflict'"
        ).fetchone()
        action = conn.execute(
            "SELECT execution_state, execution_error FROM action_intents "
            "WHERE plan_id = 'external-conflict'"
        ).fetchone()
    assert execution["status"] == "FAILED_SAFE"
    assert action["execution_state"] == "FAILED"
    assert target.mutations == [9]


def test_known_response_followed_by_external_revert_is_not_redispatched(
    clean_database: Database,
) -> None:
    controller, target, _health = _prepare_controller(
        clean_database.repository, "known-revert", 8, 9
    )
    target.revert_after_response = 8

    for _ in range(4):
        controller.run_once()

    with clean_database.repository.transaction() as conn:
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'known-revert'"
        ).fetchone()
        action = conn.execute(
            "SELECT execution_state FROM action_intents WHERE plan_id = 'known-revert'"
        ).fetchone()
    assert execution["status"] == "FAILED_SAFE"
    assert action["execution_state"] == "FAILED"
    assert target.replicas == 8
    assert target.mutations == [9]


def test_takeover_of_deleted_target_fails_safe_and_releases_plan_lease(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    controller_a, target, _health = _prepare_controller(
        repo, "deleted-target", 8, 9, ["workload:cpu-api"]
    )
    target.lose_next_response = True
    controller_a.run_once()
    with repo.transaction() as conn:
        conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp() "
            "WHERE plan_id = 'deleted-target'"
        )
    target.missing = True

    ExecutionController(repo, target, FakeHealth(), "controller-b").run_once()

    with repo.transaction() as conn:
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'deleted-target'"
        ).fetchone()
        action = conn.execute(
            "SELECT execution_state, execution_result FROM action_intents "
            "WHERE plan_id = 'deleted-target'"
        ).fetchone()
        leases = conn.execute(
            "SELECT count(*) AS n FROM plan_leases WHERE plan_id = 'deleted-target'"
        ).fetchone()
    assert execution["status"] == "FAILED_SAFE"
    assert action["execution_state"] == "FAILED"
    assert action["execution_result"]["target_observation"] == {"exists": False}
    assert leases["n"] == 0
    assert target.mutations == [9]


def test_deleted_target_during_final_health_verification_fails_safe(
    clean_database: Database,
) -> None:
    controller, target, _health = _prepare_controller(
        clean_database.repository, "deleted-during-verification", 8, 9
    )
    controller.run_once()
    controller.run_once()
    target.missing = True

    controller.run_once()

    with clean_database.repository.transaction() as conn:
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'deleted-during-verification'"
        ).fetchone()
    assert execution["status"] == "FAILED_SAFE"
    assert target.mutations == [9]


def test_status_resource_version_change_does_not_stale_approved_action(
    clean_database: Database,
) -> None:
    controller, target, _health = _prepare_controller(
        clean_database.repository, "status-resource-version", 8, 9
    )
    target.resource_version = "status-only-update"

    controller.run_once()
    controller.run_once()
    controller.run_once()

    with clean_database.repository.transaction() as conn:
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'status-resource-version'"
        ).fetchone()
    assert execution["status"] == "COMMITTED"
    assert target.mutations == [9]


def test_scale_out_health_regression_never_compensates_by_scaling_down(
    clean_database: Database,
) -> None:
    controller, target, health = _prepare_controller(
        clean_database.repository, "scale-out-regression", 8, 9
    )
    health.samples = [False]
    controller.readiness_timeout_seconds = 0

    controller.run_once()

    with clean_database.repository.transaction() as conn:
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'scale-out-regression'"
        ).fetchone()
        action = conn.execute(
            "SELECT execution_state FROM action_intents WHERE plan_id = 'scale-out-regression'"
        ).fetchone()
    assert execution["status"] == "FAILED_SAFE"
    assert action["execution_state"] == "FAILED"
    assert target.replicas == 9
    assert target.mutations == [9]


def test_destructive_action_waits_for_fresh_health_sample(
    clean_database: Database,
) -> None:
    controller, target, health = _prepare_controller(
        clean_database.repository, "stale-health", 10, 9
    )
    health.fresh = False
    controller.readiness_timeout_seconds = 1

    controller.run_once()

    with clean_database.repository.transaction() as conn:
        waiting = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'stale-health'"
        ).fetchone()
    assert waiting["status"] == "EXECUTING"
    assert target.mutations == []

    controller.readiness_timeout_seconds = 0
    controller.run_once()

    with clean_database.repository.transaction() as conn:
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'stale-health'"
        ).fetchone()
        action = conn.execute(
            "SELECT execution_state FROM action_intents WHERE plan_id = 'stale-health'"
        ).fetchone()
    assert execution["status"] == "FAILED_SAFE"
    assert action["execution_state"] == "NOT_STARTED"
    assert target.mutations == []
