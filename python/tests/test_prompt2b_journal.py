from __future__ import annotations

import os
import time
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
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
    FleetRightWorkloadTarget,
    OutcomeUnknown,
    TargetError,
    TargetNotFound,
    TargetPreconditionFailed,
    TargetUnavailable,
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
            "pod_create_protocol": "reserved-create-v1",
            "pod_create_admission_protocol": "claimed-create-v1",
            "pending_pod_create": None,
            "pod_create_history_uncertain": False,
            "unverified_pod_creates": 0,
            "retired_replica_slots": [],
            "authorized_pod_creates": [],
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
        **_retirement_args: object,
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
        if not same_fence and not same_request and not expected_unchanged:
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


@pytest.mark.parametrize("action_state", ["RUNNING", "FAILED"])
def test_failed_safe_keeps_lock_when_a_workload_create_is_pending(
    clean_database: Database, action_state: str
) -> None:
    class PendingCreateTarget(FakeScaleTarget):
        supports_workload_execution = True

        def observe_scale(self, workload_id: str) -> dict[str, object]:
            return {
                **super().observe_scale(workload_id),
                "fencing_token": 14,
                "execution_fence": {"generation": 14},
                "pending_pod_create": {"operationID": "scale"},
                "pod_create_history_uncertain": False,
                "unverified_pod_creates": 0,
            }

    repo = clean_database.repository
    controller, _, _ = _prepare_controller(
        repo, "pending-create-failsafe", 8, 9, ["workload:cpu-api"]
    )
    with repo.transaction() as conn:
        execution = conn.execute(
            "SELECT controller_fencing_token FROM plan_executions "
            "WHERE plan_id = 'pending-create-failsafe'"
        ).fetchone()
        token = execution["controller_fencing_token"]
        Repository.transition_action(
            conn,
            "pending-create-failsafe",
            "scale",
            "NOT_STARTED",
            "RUNNING",
            "controller-a",
            token,
            pre_observation={"uid": "deployment-uid", "generation": 1, "replicas": 8},
        )
    target = PendingCreateTarget(8)
    controller.target = target
    if action_state == "FAILED":
        with repo.transaction() as conn:
            Repository.transition_action(
                conn,
                "pending-create-failsafe",
                "scale",
                "RUNNING",
                "FAILED",
                "controller-a",
                token,
                target_observation=target.observe_scale("cpu-api"),
                error="test-failure",
            )
    receipt = {
        "token": 14,
        "generation": target.generation,
        "observation": target.observe_scale("cpu-api"),
    }
    controller._takeover_fence_receipt = lambda *_args, **_kwargs: receipt  # type: ignore[method-assign]
    controller._takeover_fence_matches = lambda *_args, **_kwargs: True  # type: ignore[method-assign]

    controller._fail_safe("pending-create-failsafe", token, "test-failure")

    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state FROM action_intents WHERE plan_id = 'pending-create-failsafe'"
        ).fetchone()
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'pending-create-failsafe'"
        ).fetchone()
        lease = conn.execute(
            "SELECT 1 FROM plan_leases WHERE plan_id = 'pending-create-failsafe'"
        ).fetchone()
    assert action["execution_state"] == action_state
    assert execution["status"] == "EXECUTING"
    assert lease is not None


def test_failed_safe_keeps_lock_when_workload_name_is_recreated_with_new_uid(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    controller, _, _ = _prepare_controller(
        repo, "recreated-workload-failsafe", 8, 9, ["workload:cpu-api"]
    )
    with repo.transaction() as conn:
        execution = conn.execute(
            "SELECT controller_fencing_token FROM plan_executions "
            "WHERE plan_id = 'recreated-workload-failsafe'"
        ).fetchone()
        token = execution["controller_fencing_token"]
        Repository.transition_action(
            conn,
            "recreated-workload-failsafe",
            "scale",
            "NOT_STARTED",
            "RUNNING",
            "controller-a",
            token,
            pre_observation={"uid": "deployment-uid", "generation": 1, "replicas": 8},
        )
        Repository.transition_action(
            conn,
            "recreated-workload-failsafe",
            "scale",
            "RUNNING",
            "FAILED",
            "controller-a",
            token,
            target_observation={"uid": "deployment-uid", "generation": 1, "replicas": 8},
            error="test-failure",
        )

    target = FakeFleetRightWorkloadTarget(8)
    target.uid = "replacement-workload-uid"
    controller.target = target
    controller._fail_safe("recreated-workload-failsafe", token, "test-failure")

    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state FROM action_intents "
            "WHERE plan_id = 'recreated-workload-failsafe'"
        ).fetchone()
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'recreated-workload-failsafe'"
        ).fetchone()
        lease = conn.execute(
            "SELECT 1 FROM plan_leases WHERE plan_id = 'recreated-workload-failsafe'"
        ).fetchone()
    assert action["execution_state"] == "FAILED"
    assert execution["status"] == "EXECUTING"
    assert lease is not None


def test_takeover_keeps_workload_action_locked_when_owner_disappears(
    clean_database: Database,
) -> None:
    class DeletedWorkloadTarget(FakeScaleTarget):
        supports_workload_execution = True

        def observe_scale(self, _workload_id: str) -> dict[str, object]:
            raise TargetNotFound("FleetRightWorkload is absent")

    repo = clean_database.repository
    _controller_a, _, _ = _prepare_controller(
        repo, "deleted-workload-takeover", 8, 9, ["workload:cpu-api"]
    )
    with repo.transaction() as conn:
        execution = conn.execute(
            "SELECT controller_fencing_token FROM plan_executions "
            "WHERE plan_id = 'deleted-workload-takeover'"
        ).fetchone()
        token = execution["controller_fencing_token"]
        Repository.transition_action(
            conn,
            "deleted-workload-takeover",
            "scale",
            "NOT_STARTED",
            "RUNNING",
            "controller-a",
            token,
            pre_observation={"uid": "deployment-uid", "generation": 1, "replicas": 8},
        )
        conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp() "
            "WHERE plan_id = 'deleted-workload-takeover'"
        )
    target = DeletedWorkloadTarget(8)
    controller_b = ExecutionController(repo, target, FakeHealth(), "controller-b")

    controller_b.run_once()

    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state FROM action_intents WHERE plan_id = 'deleted-workload-takeover'"
        ).fetchone()
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'deleted-workload-takeover'"
        ).fetchone()
        lease = conn.execute(
            "SELECT 1 FROM plan_leases WHERE plan_id = 'deleted-workload-takeover'"
        ).fetchone()
    assert action["execution_state"] == "RUNNING"
    assert execution["status"] == "EXECUTING"
    assert lease is not None


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


def test_takeover_recovers_running_node_drain_verification(clean_database: Database) -> None:
    repo = clean_database.repository
    payload = {
        "operation_id": "verify-operation",
        "cordon_operation_id": "cordon-operation",
        "source_node_id": "worker-1",
        "source_node_uid": "worker-uid",
        "source_pods": [{"workload_id": "api", "pod_name": "api-0", "pod_uid": "pod-uid"}],
    }
    observation = {
        "stage": "NODE_DRAIN_WAITING",
        "source_node_id": "worker-1",
        "source_node_uid": "worker-uid",
        "plan_id": "verify-takeover",
        "operation_id": "cordon-operation",
        "unschedulable": True,
        "remaining_source_pod_uids": ["pod-uid"],
        "exempt_pods": [],
        "inventory_pod_count": 1,
    }
    with repo.transaction() as conn:
        _insert_plan(conn, "verify-takeover")
        conn.execute(
            "INSERT INTO action_intents (plan_id, intent_id, action_sequence, action_type, "
            "payload, status) VALUES ('verify-takeover', 'verify-node', 0, "
            "'VERIFY_NODE_DRAIN', %s, 'APPROVED')",
            (Jsonb(payload),),
        )
    claim = _claim(repo, "verify-takeover")
    old_token = claim["execution"]["controller_fencing_token"]
    with repo.transaction() as conn:
        Repository.transition_action(
            conn,
            "verify-takeover",
            "verify-node",
            "NOT_STARTED",
            "RUNNING",
            "controller-a",
            old_token,
            pre_observation={"source_node_uid": "worker-uid"},
            result={"stage": "NODE_DRAIN_VERIFY_INTENT"},
        )
        conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp() "
            "WHERE plan_id = 'verify-takeover'"
        )

    class Target:
        supports_workload_execution = True

        def __init__(self) -> None:
            self.cordons: list[int] = []
            self.observations: list[int] = []

        def cordon_source_node(self, _plan_id: str, _payload: dict[str, object], token: int):
            self.cordons.append(token)
            return {**observation, "stage": "NODE_CORDONED", "fencing_token": token}

        def observe_source_node_drain(self, _plan_id: str, _payload: dict[str, object], token: int):
            self.observations.append(token)
            return observation

    target = Target()
    controller = ExecutionController(repo, target, object(), "controller-b")
    with repo.transaction() as conn:
        execution = next(
            row
            for row in Repository.runnable_executions(conn)
            if row["plan_id"] == "verify-takeover"
        )
    token = controller._claim(execution)
    assert token is not None and token > old_token
    assert target.cordons == [token] and target.observations == []
    with repo.transaction() as conn:
        receipt = Repository.execution_fence_receipt(conn, "verify-takeover", "verify-node", token)
        assert receipt is not None
        assert receipt["state"] == "COMPLETE"
        assert receipt["observation"] == {
            **observation,
            "stage": "NODE_CORDONED",
            "fencing_token": token,
        }
        assert Repository.assert_execution_controller(
            conn, "verify-takeover", "controller-b", token
        )

    controller._reconcile("verify-takeover", token, None)
    assert target.observations == [token]
    with repo.transaction() as conn:
        action = Repository.incomplete_actions(conn, "verify-takeover")[0]
        assert action["execution_state"] == "RUNNING"
        assert action["execution_result"]["progress"][-1]["result"]["stage"] == (
            "NODE_DRAIN_WAITING"
        )


def test_persistent_node_drain_observation_error_is_journaled_then_fails_safe(
    clean_database: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fleetright import reconciler

    monkeypatch.setattr(reconciler, "NODE_DRAIN_OBSERVATION_ERROR_LIMIT", 2)
    repo = clean_database.repository
    payload = {
        "operation_id": "verify-operation",
        "cordon_operation_id": "cordon-operation",
        "source_node_id": "worker-1",
        "source_node_uid": "worker-uid",
        "source_pods": [{"workload_id": "api", "pod_name": "api-0", "pod_uid": "pod-uid"}],
    }
    with repo.transaction() as conn:
        _insert_plan(conn, "verify-forbidden")
        conn.execute(
            "INSERT INTO action_intents (plan_id, intent_id, action_sequence, action_type, "
            "payload, status) VALUES ('verify-forbidden', 'verify-node', 0, "
            "'VERIFY_NODE_DRAIN', %s, 'APPROVED')",
            (Jsonb(payload),),
        )
    token = _claim(repo, "verify-forbidden")["execution"]["controller_fencing_token"]

    class Target:
        supports_workload_execution = True

        def cordon_source_node(self, plan_id: str, node_payload: dict[str, object], token: int):
            return {
                "source_node_id": "worker-1",
                "source_node_uid": "worker-uid",
                "plan_id": plan_id,
                "operation_id": node_payload["operation_id"],
                "fencing_token": token,
                "unschedulable": True,
            }

        def observe_source_node_drain(self, *_args: object):
            raise TargetError("daemonsets is forbidden")

    controller = ExecutionController(repo, Target(), object(), "controller-a")
    for _ in range(3):
        controller._reconcile("verify-forbidden", token, None)
    with repo.transaction() as conn:
        row = conn.execute(
            "SELECT execution_state, execution_result FROM action_intents "
            "WHERE plan_id = 'verify-forbidden'"
        ).fetchone()
        status = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'verify-forbidden'"
        ).fetchone()["status"]
    stages = [item["result"]["stage"] for item in row["execution_result"]["progress"]]
    assert stages == ["NODE_DRAIN_OBSERVATION_ERROR"] * 2
    assert row["execution_state"] == "FAILED"
    assert status == "FAILED_SAFE"


def test_node_drain_plan_commits_after_final_fenced_reobservation(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    node = {"source_node_id": "worker-1", "source_node_uid": "worker-uid"}
    with repo.transaction() as conn:
        _insert_plan(conn, "drain-commit")
        for sequence, (intent, action_type, payload) in enumerate(
            [
                ("cordon", "CORDON_NODE", {**node, "operation_id": "cordon-operation"}),
                (
                    "verify",
                    "VERIFY_NODE_DRAIN",
                    {
                        **node,
                        "operation_id": "verify-operation",
                        "cordon_operation_id": "cordon-operation",
                        "source_pods": [
                            {"workload_id": "api", "pod_name": "api-0", "pod_uid": "pod-uid"}
                        ],
                    },
                ),
            ]
        ):
            conn.execute(
                "INSERT INTO action_intents (plan_id, intent_id, action_sequence, action_type, "
                "payload, status) VALUES ('drain-commit', %s, %s, %s, %s, 'APPROVED')",
                (intent, sequence, action_type, Jsonb(payload)),
            )
    _claim(repo, "drain-commit")

    class Target:
        """Mirrors the real signatures: observation fails unless the Node fence matches."""

        supports_workload_execution = True

        def __init__(self) -> None:
            self.node_token = 0
            self.observed_tokens: list[int] = []

        def _view(self, plan_id: str, operation_id: object) -> dict[str, object]:
            return {
                **node,
                "plan_id": plan_id,
                "operation_id": operation_id,
                "fencing_token": self.node_token,
                "unschedulable": True,
            }

        def observe_source_node_state(self, _plan_id: str, _payload: dict[str, object]):
            return {**node, "unschedulable": True}

        def cordon_source_node(self, plan_id: str, payload: dict[str, object], token: int):
            self.node_token = max(self.node_token, token)
            return {**self._view(plan_id, payload["operation_id"]), "stage": "NODE_CORDONED"}

        def observe_source_node_drain(self, plan_id: str, payload: dict[str, object], token: int):
            if token != self.node_token:
                raise TargetPreconditionFailed("Node drain authority is not held")
            self.observed_tokens.append(token)
            return {
                **self._view(plan_id, payload["cordon_operation_id"]),
                "stage": "NODE_DRAIN_COMPLETE",
                "remaining_source_pod_uids": [],
                "exempt_pods": [],
                "inventory_pod_count": 1,
            }

    target = Target()
    controller = ExecutionController(repo, target, FakeHealth(), "controller-a")
    for _ in range(10):
        controller.run_once()
    with repo.transaction() as conn:
        status = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'drain-commit'"
        ).fetchone()["status"]
    assert status == "COMMITTED"
    assert len(target.observed_tokens) >= 2  # action observation + final re-observation


def test_lost_drain_check_response_stays_running_and_counted(
    clean_database: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fleetright import reconciler

    monkeypatch.setattr(reconciler, "NODE_DRAIN_OBSERVATION_ERROR_LIMIT", 2)
    repo = clean_database.repository
    node = {"source_node_id": "worker-1", "source_node_uid": "worker-uid"}
    with repo.transaction() as conn:
        _insert_plan(conn, "drain-timeout")
        conn.execute(
            "INSERT INTO action_intents (plan_id, intent_id, action_sequence, action_type, "
            "payload, status) VALUES ('drain-timeout', 'verify-node', 0, 'VERIFY_NODE_DRAIN', %s, "
            "'APPROVED')",
            (
                Jsonb(
                    {
                        **node,
                        "operation_id": "verify-operation",
                        "cordon_operation_id": "cordon-operation",
                        "source_pods": [{"workload_id": "a", "pod_name": "a-0", "pod_uid": "u"}],
                    }
                ),
            ),
        )
    token = _claim(repo, "drain-timeout")["execution"]["controller_fencing_token"]

    class Target:
        supports_workload_execution = True

        def cordon_source_node(self, plan_id: str, payload: dict[str, object], token: int):
            return {
                **node,
                "plan_id": plan_id,
                "operation_id": payload["operation_id"],
                "fencing_token": token,
                "unschedulable": True,
            }

        def observe_source_node_drain(self, *_args: object):
            raise OutcomeUnknown("kubectl timed out listing Pods")

    controller = ExecutionController(repo, Target(), object(), "controller-a")
    for _ in range(3):
        controller._reconcile("drain-timeout", token, None)
    with repo.transaction() as conn:
        row = conn.execute(
            "SELECT execution_state, execution_result FROM action_intents "
            "WHERE plan_id = 'drain-timeout'"
        ).fetchone()
        status = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'drain-timeout'"
        ).fetchone()["status"]
    stages = [item["result"]["stage"] for item in row["execution_result"]["progress"]]
    # A timeout never parks the action in OUTCOME_UNKNOWN, so the bound still applies.
    assert stages == ["NODE_DRAIN_OBSERVATION_ERROR"] * 2
    assert row["execution_state"] == "FAILED" and status == "FAILED_SAFE"


def test_cordon_journals_pre_existing_unschedulable_state(clean_database: Database) -> None:
    repo = clean_database.repository
    node = {"source_node_id": "worker-1", "source_node_uid": "worker-uid"}
    with repo.transaction() as conn:
        _insert_plan(conn, "cordon-pre")
        conn.execute(
            "INSERT INTO action_intents (plan_id, intent_id, action_sequence, action_type, "
            "payload, status) VALUES ('cordon-pre', 'cordon', 0, 'CORDON_NODE', %s, 'APPROVED')",
            (Jsonb({**node, "operation_id": "cordon-operation"}),),
        )
    token = _claim(repo, "cordon-pre")["execution"]["controller_fencing_token"]

    class Target:
        supports_workload_execution = True

        def observe_source_node_state(self, *_args: object):
            return {**node, "unschedulable": True}  # an administrator cordoned it first

        def cordon_source_node(self, plan_id: str, payload: dict[str, object], token: int):
            return {
                **node,
                "plan_id": plan_id,
                "operation_id": payload["operation_id"],
                "fencing_token": token,
                "unschedulable": True,
            }

    ExecutionController(repo, Target(), object(), "controller-a")._reconcile(
        "cordon-pre", token, None
    )
    with repo.transaction() as conn:
        row = conn.execute(
            "SELECT execution_state, execution_result FROM action_intents "
            "WHERE plan_id = 'cordon-pre'"
        ).fetchone()
    assert row["execution_state"] == "SUCCEEDED"
    assert row["execution_result"]["pre_observation"]["unschedulable_before"] is True


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
    controller_a, _legacy_target, _health = _prepare_controller(repo, "workload-second-fence", 2, 3)
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


def test_workload_takeover_replays_published_request_after_fence_response_loss(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    controller_a, _legacy_target, _health = _prepare_controller(
        repo, "workload-published-fence-replay", 2, 3
    )
    target = FakeFleetRightWorkloadTarget(2)
    authorize_scale = target.authorize_scale

    def lose_authorization_response(*args: object, **kwargs: object) -> dict[str, object]:
        authorize_scale(*args, **kwargs)  # type: ignore[arg-type]
        raise OutcomeUnknown("injected lost workload authorization response")

    target.authorize_scale = lose_authorization_response  # type: ignore[method-assign]
    controller_a.target = target
    controller_a.run_once()

    assert target.replicas == 3
    assert target.authorizations == 1
    assert target.execution_request is not None
    published_generation = target.execution_request["generation"]

    with repo.transaction() as conn:
        conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp() "
            "WHERE plan_id = 'workload-published-fence-replay'"
        )
    controller_b = ExecutionController(repo, target, FakeHealth(), "controller-b")
    target.crash_after_fence = True
    with pytest.raises(SystemExit, match="crash after target fence"):
        controller_b.run_once()

    with repo.transaction() as conn:
        pending_token = conn.execute(
            "SELECT pending_controller_fencing_token FROM plan_executions "
            "WHERE plan_id = 'workload-published-fence-replay'"
        ).fetchone()["pending_controller_fencing_token"]
        pending = Repository.execution_fence_receipt(
            conn, "workload-published-fence-replay", "scale", pending_token
        )
    assert pending is not None and pending["state"] == "PENDING"
    assert target.execution_fence is not None
    assert target.execution_fence["generation"] == pending_token
    assert target.execution_request["generation"] == published_generation

    target.mark_applied()
    for _ in range(5):
        controller_b.run_once()

    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state FROM action_intents "
            "WHERE plan_id = 'workload-published-fence-replay'"
        ).fetchone()
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'workload-published-fence-replay'"
        ).fetchone()
        receipt = Repository.execution_fence_receipt(
            conn, "workload-published-fence-replay", "scale", pending_token
        )
    assert action["execution_state"] == "SUCCEEDED"
    assert execution["status"] == "COMMITTED"
    assert receipt is not None and receipt["state"] == "COMPLETE"
    assert receipt["fence_replayed"] is True
    assert receipt["operation_applied_before_fence"] is True
    assert target.execution_request["generation"] == published_generation
    assert target.replicas == 3
    assert target.authorizations == 1


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
        if isinstance(item.get("result"), dict) and "dispatch_target_replicas" in item["result"]
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

    controller_c = ExecutionController(repo, target, FakeHealth(), "controller-c", max_attempts=3)
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
            "SELECT execution_state FROM action_intents WHERE plan_id = 'workload-delayed-request'"
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


class RetirementGateTarget(FakeScaleTarget):
    source_retirement_preparation_enabled = True

    def __init__(self, failure: Exception) -> None:
        super().__init__(1)
        self.failure = failure
        self.retirement_calls = 0

    def prepare_source_retirement(
        self, _workload_id: str, _plan_id: str, _payload: dict[str, object]
    ) -> dict[str, object]:
        self.retirement_calls += 1
        raise self.failure


def _prepare_retirement_action(
    repo: Repository, plan_id: str, conflict_key: str | None = None
) -> None:
    with repo.transaction() as conn:
        _insert_plan(conn, plan_id)
        if conflict_key is not None:
            conn.execute(
                "UPDATE plans SET binding = %s WHERE plan_id = %s",
                (Jsonb({"source": {}, "conflict_keys": [conflict_key]}), plan_id),
            )
            fencing_token = Repository.acquire_lease(
                conn, conflict_key, f"plan:{plan_id}", "operator", 60_000
            )
            assert fencing_token is not None
            conn.execute(
                "INSERT INTO plan_leases (plan_id, conflict_key, owner_id, fencing_token) "
                "VALUES (%s, %s, %s, %s)",
                (plan_id, conflict_key, f"plan:{plan_id}", fencing_token),
            )
        conn.execute(
            "INSERT INTO control_state "
            "(singleton, snapshot_id, snapshot_generation, binding) VALUES "
            "(TRUE, 'snapshot', 1, %s)",
            (Jsonb({}),),
        )
        conn.execute(
            "INSERT INTO action_intents (plan_id, intent_id, action_sequence, action_type, "
            "workload_id, payload, status) VALUES (%s, 'retire', 0, "
            "'PREPARE_SOURCE_RETIREMENT', 'cpu-api', %s, 'APPROVED')",
            (
                plan_id,
                Jsonb(
                    {
                        "operation_id": "retire",
                        "source_owner_uid": "deployment-uid",
                        "workload_generation": 1,
                    }
                ),
            ),
        )
    _claim(repo, plan_id)


def test_retirement_action_fails_safe_at_unproven_create_fence(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    _prepare_retirement_action(repo, "retirement-blocked")
    target = RetirementGateTarget(ActionBlocked("POD_CREATE_PERSISTENCE_FENCE_UNPROVEN"))
    controller = ExecutionController(repo, target, FakeHealth(), "controller-a")

    controller.run_once()

    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state, execution_error, execution_result FROM action_intents "
            "WHERE plan_id = 'retirement-blocked' AND intent_id = 'retire'"
        ).fetchone()
        execution = conn.execute(
            "SELECT status, outcome FROM plan_executions WHERE plan_id = 'retirement-blocked'"
        ).fetchone()
        plan = conn.execute(
            "SELECT state FROM plans WHERE plan_id = 'retirement-blocked'"
        ).fetchone()

    assert target.retirement_calls == 1
    assert action["execution_state"] == "FAILED"
    assert action["execution_error"] == "POD_CREATE_PERSISTENCE_FENCE_UNPROVEN"
    assert action["execution_result"]["progress"][0]["result"] == {"stage": "RETIREMENT_REQUESTED"}
    assert action["execution_result"]["result"] == {
        "stage": "BLOCKED",
        "blocked_at": "VALIDATE_CREATE_PERSISTENCE_FENCE",
        "reason": "POD_CREATE_PERSISTENCE_FENCE_UNPROVEN",
    }
    assert execution["status"] == plan["state"] == "FAILED_SAFE"


@pytest.mark.parametrize("blocked_stage", ["PDB_BLOCKED", "PDB_STATUS_PENDING"])
def test_pre_tombstone_pdb_block_retries_replacement_preparation(
    clean_database: Database, blocked_stage: str
) -> None:
    class PDBRetryTarget(FakeScaleTarget):
        source_retirement_preparation_enabled = True

        def __init__(self) -> None:
            super().__init__(1)
            self.prepare_calls = 0
            self.observe_retirement_calls = 0

        def prepare_source_retirement(self, *_args: object) -> dict[str, object]:
            self.prepare_calls += 1
            if self.prepare_calls == 1:
                return {"stage": blocked_stage}
            return {
                "stage": "READY_FOR_SAFE_EVICTION",
                "source_retirement_intent_recorded": True,
            }

        def observe_source_retirement(self, *_args: object, **_kwargs: object):
            self.observe_retirement_calls += 1
            raise AssertionError("PDB block without a tombstone cannot enter eviction recovery")

    repo = clean_database.repository
    _prepare_retirement_action(repo, f"retirement-pdb-{blocked_stage.lower()}")
    target = PDBRetryTarget()
    controller = ExecutionController(repo, target, FakeHealth(), "controller-a")

    controller.run_once()
    controller.run_once()

    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state, execution_result FROM action_intents "
            "WHERE plan_id = %s AND intent_id = 'retire'",
            (f"retirement-pdb-{blocked_stage.lower()}",),
        ).fetchone()

    results = [item["result"] for item in action["execution_result"]["progress"]]
    assert target.prepare_calls == 2
    assert target.observe_retirement_calls == 0
    assert action["execution_state"] == "RUNNING"
    assert results[-1]["stage"] == "READY_FOR_SAFE_EVICTION"
    assert results[-1]["source_retirement_intent_recorded"] is True


def test_pre_eviction_resource_version_conflict_retries_after_reobservation(
    clean_database: Database,
) -> None:
    class RetryTarget(FakeScaleTarget):
        source_retirement_preparation_enabled = True

        def __init__(self) -> None:
            super().__init__(1)
            self.prepare_calls = 0

        def prepare_source_retirement(self, *_args: object) -> dict[str, object]:
            self.prepare_calls += 1
            if self.prepare_calls == 1:
                raise TargetPreconditionFailed("target resource version changed")
            return {"stage": "REPLACEMENT_REQUESTED"}

    repo = clean_database.repository
    plan_id = "retirement-status-cas-retry"
    _prepare_retirement_action(repo, plan_id, f"workload:{plan_id}")
    target = RetryTarget()
    controller = ExecutionController(repo, target, FakeHealth(), "controller-a")

    controller.run_once()
    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state FROM action_intents WHERE plan_id = %s",
            (plan_id,),
        ).fetchone()
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = %s", (plan_id,)
        ).fetchone()
        lock = conn.execute(
            "SELECT count(*) AS total FROM plan_leases WHERE plan_id = %s", (plan_id,)
        ).fetchone()

    assert target.prepare_calls == 1
    assert action["execution_state"] == "RUNNING"
    assert execution["status"] == "EXECUTING"
    assert lock["total"] == 1

    controller.run_once()

    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state, execution_error, execution_result "
            "FROM action_intents WHERE plan_id = %s",
            (plan_id,),
        ).fetchone()
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = %s", (plan_id,)
        ).fetchone()
    results = [item["result"] for item in action["execution_result"]["progress"]]
    assert target.prepare_calls == 2
    assert action["execution_state"] == "RUNNING"
    assert action["execution_error"] is None
    assert results[-1]["stage"] == "REPLACEMENT_REQUESTED"
    assert execution["status"] == "EXECUTING"


def test_post_eviction_readiness_deadline_starts_at_observed_source_removal(
    clean_database: Database,
) -> None:
    class WaitingRetirementTarget(FakeFleetRightWorkloadTarget):
        source_retirement_preparation_enabled = True

        def __init__(self) -> None:
            super().__init__(2)
            self.complete = False
            self.retirement_calls = 0

        def prepare_source_retirement(self, *_args: object, **_kwargs: object):
            raise AssertionError("resumed retirement must not start a second replacement")

        def observe_source_retirement(self, *_args: object, **_kwargs: object):
            self.retirement_calls += 1
            if not self.complete:
                return {
                    "stage": "RETIREMENT_WAITING_FINAL_HEALTH",
                    "source_removed_observed": True,
                    "observation": self.observe_scale("cpu-api"),
                }
            return {
                "stage": "RETIREMENT_COMPLETE",
                "observation": self.observe_scale("cpu-api"),
            }

    repo = clean_database.repository
    plan_id = "retirement-post-source-readiness"
    _prepare_retirement_action(repo, plan_id, f"workload:{plan_id}")
    target = WaitingRetirementTarget()
    health = FakeHealth()
    controller = ExecutionController(
        repo, target, health, "controller-a", readiness_timeout_seconds=30
    )
    payload = {
        "operation_id": "retire",
        "health": {
            "p99_limit_ms": 120,
            "error_slo_ppm": 1000,
            "max_queue_growth_milli": 0,
        },
    }
    with repo.transaction() as conn:
        execution = conn.execute(
            "SELECT controller_fencing_token FROM plan_executions WHERE plan_id = %s",
            (plan_id,),
        ).fetchone()
        token = execution["controller_fencing_token"]
        conn.execute(
            "UPDATE action_intents SET payload = %s WHERE plan_id = %s",
            (Jsonb(payload), plan_id),
        )
        Repository.transition_action(
            conn,
            plan_id,
            "retire",
            "NOT_STARTED",
            "RUNNING",
            "controller-a",
            token,
            pre_observation=target.observe_scale("cpu-api"),
        )
        conn.execute(
            "UPDATE action_intents SET execution_started_at = clock_timestamp() "
            "- interval '5 minutes' "
            "WHERE plan_id = %s",
            (plan_id,),
        )
        Repository.record_action_progress(
            conn,
            plan_id,
            "retire",
            "controller-a",
            token,
            observation=target.observe_scale("cpu-api"),
            result={"stage": "EVICTION_ACCEPTED", "source_retirement_intent_recorded": True},
        )
        action = conn.execute(
            "SELECT * FROM action_intents WHERE plan_id = %s", (plan_id,)
        ).fetchone()

    controller._start_source_retirement(plan_id, token, action, payload, None)

    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state, execution_result FROM action_intents WHERE plan_id = %s",
            (plan_id,),
        ).fetchone()
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = %s", (plan_id,)
        ).fetchone()
        lock = conn.execute(
            "SELECT count(*) AS total FROM plan_leases WHERE plan_id = %s", (plan_id,)
        ).fetchone()
    waiting = [
        item
        for item in action["execution_result"]["progress"]
        if item.get("result", {}).get("stage") == "RETIREMENT_WAITING_FINAL_HEALTH"
    ]
    assert action["execution_state"] == "RUNNING"
    assert execution["status"] == "EXECUTING"
    assert lock["total"] == 1
    assert len(waiting) == 1, action
    assert waiting[0]["result"]["source_removed_observed"] is True
    assert waiting[0]["recorded_at"]
    assert health.calls == 1

    target.complete = True
    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT * FROM action_intents WHERE plan_id = %s", (plan_id,)
        ).fetchone()
    controller._start_source_retirement(plan_id, token, action, payload, None)
    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state, execution_result FROM action_intents WHERE plan_id = %s",
            (plan_id,),
        ).fetchone()
    assert action["execution_state"] == "SUCCEEDED"
    assert action["execution_result"]["result"]["stage"] == "RETIREMENT_COMPLETE"


def test_retirement_does_not_evict_without_fresh_healthy_slo(
    clean_database: Database,
) -> None:
    class ReadyRetirementTarget(FakeScaleTarget):
        supports_workload_execution = True
        source_retirement_preparation_enabled = True

        def observe_source_retirement(self, *_args: object, **_kwargs: object):
            return {"stage": "EVICTION_RETRY_SAFE"}

        def evict_source(self, *_args: object, **_kwargs: object):
            raise AssertionError("Eviction dispatched with unhealthy SLO")

    repo = clean_database.repository
    plan_id = "retirement-unhealthy-slo"
    _prepare_retirement_action(repo, plan_id, f"resource:{plan_id}")
    target = ReadyRetirementTarget(2)
    health = FakeHealth([False])
    controller = ExecutionController(repo, target, health, "controller-a")
    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT * FROM action_intents WHERE plan_id = %s AND intent_id = 'retire'",
            (plan_id,),
        ).fetchone()
        payload = {
            **action["payload"],
            "health": {
                "p99_limit_ms": 120,
                "error_slo_ppm": 1000,
                "max_queue_growth_milli": 0,
            },
        }
        conn.execute(
            "UPDATE action_intents SET payload = %s WHERE plan_id = %s AND intent_id = 'retire'",
            (Jsonb(payload), plan_id),
        )
        claim = conn.execute(
            "SELECT controller_fencing_token FROM plan_executions WHERE plan_id = %s",
            (plan_id,),
        ).fetchone()
        token = claim["controller_fencing_token"]
        observation = target.observe_scale("cpu-api")
        Repository.transition_action(
            conn,
            plan_id,
            "retire",
            "NOT_STARTED",
            "RUNNING",
            "controller-a",
            token,
            pre_observation=observation,
        )
        Repository.record_action_progress(
            conn,
            plan_id,
            "retire",
            "controller-a",
            token,
            observation=observation,
            result={"stage": "READY_FOR_SAFE_EVICTION", "source_retirement_intent_recorded": True},
        )
        action = conn.execute(
            "SELECT * FROM action_intents WHERE plan_id = %s AND intent_id = 'retire'",
            (plan_id,),
        ).fetchone()

    controller._start_source_retirement(plan_id, token, action, payload, None)

    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state, execution_result FROM action_intents "
            "WHERE plan_id = %s AND intent_id = 'retire'",
            (plan_id,),
        ).fetchone()
        assert action["execution_state"] == "RUNNING"
        assert not any(
            item.get("result", {}).get("stage") == "EVICTION_INTENT"
            for item in action["execution_result"]["progress"]
        )


def test_retirement_takeover_stays_unresolved_without_target_fence(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    _prepare_retirement_action(repo, "retirement-crash-before-read")
    target = RetirementGateTarget(ActionBlocked("POD_CREATE_PERSISTENCE_FENCE_UNPROVEN"))
    with repo.transaction() as conn:
        execution = conn.execute(
            "SELECT controller_fencing_token FROM plan_executions "
            "WHERE plan_id = 'retirement-crash-before-read'"
        ).fetchone()
        Repository.transition_action(
            conn,
            "retirement-crash-before-read",
            "retire",
            "NOT_STARTED",
            "RUNNING",
            "controller-a",
            execution["controller_fencing_token"],
            pre_observation=target.observe_scale("cpu-api"),
        )
        conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp() "
            "WHERE plan_id = 'retirement-crash-before-read'"
        )

    ExecutionController(repo, target, FakeHealth(), "controller-b").run_once()

    with repo.transaction() as conn:
        execution = conn.execute(
            "SELECT status, controller_fencing_token FROM plan_executions "
            "WHERE plan_id = 'retirement-crash-before-read'"
        ).fetchone()
        action = conn.execute(
            "SELECT execution_state, execution_result FROM action_intents "
            "WHERE plan_id = 'retirement-crash-before-read'"
        ).fetchone()
        fence = Repository.execution_fence_receipt(
            conn,
            "retirement-crash-before-read",
            "retire",
            execution["controller_fencing_token"],
        )
    assert target.retirement_calls == 0
    assert action["execution_state"] == "RUNNING"
    assert execution["status"] == "EXECUTING"
    assert fence is None


def _completed_retirement_takeover(
    repo: Repository, plan_id: str
) -> tuple[ExecutionController, FakeFleetRightWorkloadTarget, int]:
    _prepare_retirement_action(repo, plan_id)
    payload = {
        "operation_id": "retire",
        "source_owner_uid": "deployment-uid",
        "workload_generation": 1,
        "baseline_replicas": 1,
        "surge_desired_replicas": 2,
        "final_desired_replicas": 2,
        "destination_nodes": [{"name": "worker-b", "uid": "node-b-uid"}],
        "profile_required_replicas": 1,
        "source_logical_slot_id": "slot-0",
        "source_pod_name": "source-pod",
        "source_pod_uid": "source-pod-uid",
        "source_node_id": "worker-a",
        "source_node_uid": "node-a-uid",
        "maximum_live_replicas": 3,
        "maximum_temporary_surge_replicas": 2,
        "n_minus_1_recovery_target_replicas": 2,
        "recovery_destination_nodes": [{"name": "worker-c", "uid": "node-c-uid"}],
        "health": {
            "p99_limit_ms": 120,
            "error_slo_ppm": 1_000,
            "max_queue_growth_milli": 0,
        },
    }
    with repo.transaction() as conn:
        old_token = conn.execute(
            "SELECT controller_fencing_token FROM plan_executions WHERE plan_id = %s",
            (plan_id,),
        ).fetchone()["controller_fencing_token"]
        conn.execute(
            "UPDATE action_intents SET payload = %s WHERE plan_id = %s AND intent_id = 'retire'",
            (Jsonb(payload), plan_id),
        )

    class CompletedRetirementTarget(FakeFleetRightWorkloadTarget):
        source_retirement_preparation_enabled = True

        def __init__(self) -> None:
            super().__init__(2)
            self.generation = self.observed_generation = 2
            self.fencing_token = old_token
            self.execution_request = {
                "planID": plan_id,
                "operationID": "retire",
                "generation": old_token,
                "expectedReplicas": 1,
                "targetReplicas": 2,
                "destinationNodes": payload["destination_nodes"],
                "purpose": "staged-replacement-v1",
                "minimumReadyReplicas": 1,
            }
            self.execution_fence = {
                "planID": plan_id,
                "operationID": "retire",
                "generation": old_token,
                "expectedGeneration": 1,
                "expectedReplicas": 1,
                "fencedGeneration": 2,
            }
            self.mark_applied()
            self.retired_replica_slots = [
                {
                    "replicaID": "slot-0",
                    "workloadUID": "deployment-uid",
                    "podUID": "source-pod-uid",
                    "planID": plan_id,
                    "operationID": "retire",
                }
            ]
            self.retirement_calls = 0
            self.source_identity_valid = True

        def observe_scale(self, workload_id: str) -> dict[str, object]:
            return {
                **super().observe_scale(workload_id),
                "retired_replica_slots": self.retired_replica_slots,
            }

        def observe_source_retirement(
            self,
            workload_id: str,
            verified_plan_id: str,
            source_payload: dict[str, object],
            prepared: dict[str, object],
            *,
            fencing_token: int,
        ) -> dict[str, object]:
            self.retirement_calls += 1
            if (
                not self.source_identity_valid
                or verified_plan_id != plan_id
                or source_payload["source_pod_uid"] != "source-pod-uid"
                or prepared["source_pod_uid"] != "source-pod-uid"
                or fencing_token != self.fencing_token
            ):
                raise ActionBlocked("RETIREMENT_IDENTITY_OR_AUTHORITY_CHANGED")
            return {
                **prepared,
                "stage": "RETIREMENT_COMPLETE",
                "observation": self.observe_scale(workload_id),
            }

    target = CompletedRetirementTarget()
    final_result = {
        "stage": "RETIREMENT_COMPLETE",
        "observation": target.observe_scale("cpu-api"),
        "source_pod_uid": "source-pod-uid",
        "replacement_pod_uid": "replacement-pod-uid",
        "replacement_node_id": "worker-b",
        "replacement_node_uid": "node-b-uid",
        "final_desired_replicas": 2,
    }
    controller_a = ExecutionController(repo, target, FakeHealth(), "controller-a")
    with repo.transaction() as conn:
        Repository.transition_action(
            conn,
            plan_id,
            "retire",
            "NOT_STARTED",
            "RUNNING",
            "controller-a",
            old_token,
            pre_observation={"uid": "deployment-uid", "generation": 1, "replicas": 1},
        )
        Repository.transition_action(
            conn,
            plan_id,
            "retire",
            "RUNNING",
            "SUCCEEDED",
            "controller-a",
            old_token,
            target_observation=target.observe_scale("cpu-api"),
            result=final_result,
        )
    controller_a._transition(plan_id, old_token, "EXECUTING", "VERIFYING", {"done": True})
    with repo.transaction() as conn:
        conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp() "
            "WHERE plan_id = %s",
            (plan_id,),
        )
        execution = next(
            item for item in Repository.runnable_executions(conn) if item["plan_id"] == plan_id
        )
    return controller_a, target, execution


def test_takeover_refences_completed_retirement_before_verification(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    plan_id = "retirement-completed-takeover"
    _controller_a, target, execution = _completed_retirement_takeover(repo, plan_id)
    controller_b = ExecutionController(repo, target, FakeHealth(), "controller-b")
    target.crash_after_fence = True

    with pytest.raises(SystemExit, match="crash after target fence"):
        controller_b._claim(execution)
    with repo.transaction() as conn:
        pending_token = conn.execute(
            "SELECT pending_controller_fencing_token FROM plan_executions WHERE plan_id = %s",
            (plan_id,),
        ).fetchone()["pending_controller_fencing_token"]
        pending = Repository.execution_fence_receipt(conn, plan_id, "retire", pending_token)
    assert pending is not None and pending["state"] == "PENDING"
    assert target.execution_fence["generation"] == pending_token
    assert target.retired_replica_slots[0]["podUID"] == "source-pod-uid"

    with repo.transaction() as conn:
        execution = next(
            item for item in Repository.runnable_executions(conn) if item["plan_id"] == plan_id
        )
    token = controller_b._claim(execution)

    assert token == pending_token
    assert target.fencing_token == pending_token
    assert target.execution_fence["expectedGeneration"] == 1
    assert target.execution_fence["expectedReplicas"] == 1
    assert target.retirement_calls == 2
    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state FROM action_intents "
            "WHERE plan_id = %s AND intent_id = 'retire'",
            (plan_id,),
        ).fetchone()
        receipt = Repository.execution_fence_receipt(conn, plan_id, "retire", pending_token)
    assert action["execution_state"] == "SUCCEEDED"
    assert receipt is not None and receipt["state"] == "COMPLETE"
    assert receipt["fence_replayed"] is True

    with repo.transaction() as conn:
        conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp() "
            "WHERE plan_id = %s",
            (plan_id,),
        )
        execution = next(
            item for item in Repository.runnable_executions(conn) if item["plan_id"] == plan_id
        )
    controller_c = ExecutionController(repo, target, FakeHealth(), "controller-c")
    next_token = controller_c._claim(execution)
    assert next_token == pending_token + 1
    assert target.fencing_token == next_token
    assert target.retirement_calls == 4


def test_completed_retirement_takeover_verifies_the_new_fenced_generation(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    plan_id = "retirement-completed-takeover-verifies"
    _controller_a, target, execution = _completed_retirement_takeover(repo, plan_id)
    controller_b = ExecutionController(repo, target, FakeHealth(), "controller-b")

    token = controller_b._claim(execution)
    assert token is not None
    target.mark_applied()
    controller_b._reconcile(plan_id, token, None)

    with repo.transaction() as conn:
        result = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = %s", (plan_id,)
        ).fetchone()
        action = conn.execute(
            "SELECT execution_state FROM action_intents "
            "WHERE plan_id = %s AND intent_id = 'retire'",
            (plan_id,),
        ).fetchone()
    assert result["status"] == "COMMITTED"
    assert action["execution_state"] == "SUCCEEDED"
    assert target.fencing_token == token
    assert target.retirement_calls == 3


def test_completed_retirement_takeover_compensates_on_fresh_health_regression(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    plan_id = "retirement-completed-takeover-health-regression"
    _controller_a, target, execution = _completed_retirement_takeover(repo, plan_id)
    health = FakeHealth([False])
    controller_b = ExecutionController(repo, target, health, "controller-b")

    token = controller_b._claim(execution)
    assert token is not None
    target.mark_applied()
    controller_b._reconcile(plan_id, token, None)

    with repo.transaction() as conn:
        result = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = %s", (plan_id,)
        ).fetchone()
        compensation = conn.execute(
            "SELECT execution_state, payload FROM action_intents "
            "WHERE plan_id = %s AND compensates_intent_id = 'retire'",
            (plan_id,),
        ).fetchone()
    assert result["status"] == "COMPENSATING"
    assert compensation["execution_state"] == "NOT_STARTED"
    assert compensation["payload"]["proposed_replicas"] == 3
    assert health.calls == 1


def test_completed_retirement_takeover_rejects_post_fence_generation_change(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    plan_id = "retirement-completed-takeover-external-change"
    _controller_a, target, execution = _completed_retirement_takeover(repo, plan_id)
    health = FakeHealth()
    controller_b = ExecutionController(repo, target, health, "controller-b")

    token = controller_b._claim(execution)
    assert token is not None
    target.mark_applied()
    target.generation += 1
    target.observed_generation += 1
    controller_b._reconcile(plan_id, token, None)

    with repo.transaction() as conn:
        result = conn.execute(
            "SELECT status, outcome FROM plan_executions WHERE plan_id = %s", (plan_id,)
        ).fetchone()
    assert result["status"] == "FAILED_SAFE"
    assert result["outcome"]["FAILED_SAFE"]["reason"] == (
        "retirement-target-changed-during-verification"
    )
    assert health.calls == 0


def test_takeover_does_not_adopt_completed_retirement_after_identity_change(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    plan_id = "retirement-completed-identity-change"
    _controller_a, target, execution = _completed_retirement_takeover(repo, plan_id)
    target.source_identity_valid = False

    token = ExecutionController(repo, target, FakeHealth(), "controller-b")._claim(execution)

    assert token is None
    with repo.transaction() as conn:
        pending_token = conn.execute(
            "SELECT pending_controller_fencing_token FROM plan_executions WHERE plan_id = %s",
            (plan_id,),
        ).fetchone()["pending_controller_fencing_token"]
    assert target.fencing_token == execution["controller_fencing_token"] < pending_token
    assert target.retirement_calls == 1
    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state FROM action_intents "
            "WHERE plan_id = %s AND intent_id = 'retire'",
            (plan_id,),
        ).fetchone()
        execution_state = conn.execute(
            "SELECT controller_id, status FROM plan_executions WHERE plan_id = %s", (plan_id,)
        ).fetchone()
        assert (
            Repository.execution_fence_receipt(
                conn,
                plan_id,
                "retire",
                conn.execute(
                    "SELECT pending_controller_fencing_token FROM plan_executions "
                    "WHERE plan_id = %s",
                    (plan_id,),
                ).fetchone()["pending_controller_fencing_token"],
            )
            is None
        )
    assert action["execution_state"] == "SUCCEEDED"
    assert execution_state["controller_id"] == "controller-a"
    assert execution_state["status"] == "VERIFYING"


def test_new_controller_recovers_expired_pending_retirement_fence(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    plan_id = "retirement-completed-expired-pending-fence"
    _controller_a, target, execution = _completed_retirement_takeover(repo, plan_id)
    controller_b = ExecutionController(repo, target, FakeHealth(), "controller-b")
    target.crash_after_fence = True

    with pytest.raises(SystemExit, match="crash after target fence"):
        controller_b._claim(execution)
    with repo.transaction() as conn:
        old_pending_token = conn.execute(
            "SELECT pending_controller_fencing_token FROM plan_executions WHERE plan_id = %s",
            (plan_id,),
        ).fetchone()["pending_controller_fencing_token"]
        conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp(), "
            "pending_controller_lease_expires_at = clock_timestamp() WHERE plan_id = %s",
            (plan_id,),
        )
        execution = next(
            item for item in Repository.runnable_executions(conn) if item["plan_id"] == plan_id
        )

    controller_c = ExecutionController(repo, target, FakeHealth(), "controller-c")
    token = controller_c._claim(execution)

    assert token is not None and token > old_pending_token
    assert target.fencing_token == token
    assert target.retirement_calls == 3
    with repo.transaction() as conn:
        old_receipt = Repository.execution_fence_receipt(conn, plan_id, "retire", old_pending_token)
        receipt = Repository.execution_fence_receipt(conn, plan_id, "retire", token)
    assert old_receipt is not None and old_receipt["state"] == "PENDING"
    assert receipt is not None and receipt["state"] == "COMPLETE"


def test_retirement_control_request_waits_for_target_fence_before_settling(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    _prepare_retirement_action(repo, "retirement-pause")
    target = RetirementGateTarget(ActionBlocked("POD_CREATE_PERSISTENCE_FENCE_UNPROVEN"))
    with repo.transaction() as conn:
        execution = conn.execute(
            "SELECT controller_fencing_token FROM plan_executions "
            "WHERE plan_id = 'retirement-pause'"
        ).fetchone()
        Repository.transition_action(
            conn,
            "retirement-pause",
            "retire",
            "NOT_STARTED",
            "RUNNING",
            "controller-a",
            execution["controller_fencing_token"],
            pre_observation=target.observe_scale("cpu-api"),
        )
        Repository.request_execution_control(conn, "retirement-pause", "PAUSE", "operator pause")

    controller = ExecutionController(repo, target, FakeHealth(), "controller-a")
    controller.run_once()
    controller.run_once()

    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state, execution_error FROM action_intents "
            "WHERE plan_id = 'retirement-pause'"
        ).fetchone()
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'retirement-pause'"
        ).fetchone()
    assert target.retirement_calls == 0
    assert action["execution_state"] == "RUNNING"
    assert action["execution_error"] is None
    assert execution["status"] == "EXECUTING"


@pytest.mark.parametrize(
    ("control_request", "pending_create", "prepared_stage", "expected_state"),
    [
        ("PAUSE", False, "RETIREMENT_REQUESTED", "PAUSED"),
        ("CANCEL", False, "RETIREMENT_REQUESTED", "CANCELLED"),
        ("PAUSE", True, "RETIREMENT_REQUESTED", "EXECUTING"),
        ("CANCEL", True, "RETIREMENT_REQUESTED", "EXECUTING"),
        ("PAUSE", False, "REPLACEMENT_REQUESTED", "PAUSED"),
        ("CANCEL", False, "PDB_BLOCKED", "CANCELLED"),
        ("PAUSE", True, "REPLACEMENT_REQUESTED", "EXECUTING"),
        ("CANCEL", True, "PDB_BLOCKED", "EXECUTING"),
    ],
)
def test_control_settles_only_undispatched_retirement_request(
    clean_database: Database,
    control_request: str,
    pending_create: bool,
    prepared_stage: str,
    expected_state: str,
) -> None:
    class PreDispatchTarget(FakeFleetRightWorkloadTarget):
        source_retirement_preparation_enabled = True

        def __init__(self) -> None:
            super().__init__(1)
            self.pending_pod_create = {"operationID": "retire"} if pending_create else None

        def observe_scale(self, workload_id: str) -> dict[str, object]:
            return {
                **super().observe_scale(workload_id),
                "pending_pod_create": self.pending_pod_create,
            }

        def prepare_source_retirement(self, *_args: object, **_kwargs: object) -> dict[str, object]:
            raise AssertionError("control request must stop before target dispatch")

        def get_pod(self, pod_name: str) -> dict[str, object] | None:
            assert pod_name == "cpu-api-0-pod"
            return {
                "metadata": {"uid": "source-pod-uid"},
                "spec": {"nodeName": "worker-a"},
            }

    repo = clean_database.repository
    plan_id = f"retirement-control-{control_request.lower()}-{pending_create}"
    _prepare_retirement_action(repo, plan_id, f"resource:{plan_id}")
    target = PreDispatchTarget()
    payload = {
        "operation_id": "retire",
        "source_owner_uid": target.uid,
        "workload_generation": target.generation,
        "baseline_replicas": 1,
        "surge_desired_replicas": 2,
        "final_desired_replicas": 2,
        "profile_required_replicas": 1,
        "destination_nodes": [{"name": "worker-b", "uid": "worker-b-uid"}],
        "source_logical_slot_id": "cpu-api-0",
        "source_pod_uid": "source-pod-uid",
        "source_pod_name": "cpu-api-0-pod",
        "source_node_id": "worker-a",
    }
    with repo.transaction() as conn:
        execution = conn.execute(
            "SELECT controller_fencing_token FROM plan_executions WHERE plan_id = %s",
            (plan_id,),
        ).fetchone()
        token = execution["controller_fencing_token"]
        conn.execute(
            "UPDATE action_intents SET payload = %s WHERE plan_id = %s AND intent_id = 'retire'",
            (Jsonb(payload), plan_id),
        )
        Repository.transition_action(
            conn,
            plan_id,
            "retire",
            "NOT_STARTED",
            "RUNNING",
            "controller-a",
            token,
            pre_observation=target.observe_scale("cpu-api"),
            result={"stage": "RETIREMENT_REQUESTED"},
        )
        if prepared_stage != "RETIREMENT_REQUESTED":
            target.pending_pod_create = None
            target.authorize_scale(
                "cpu-api",
                token,
                expected=target.observe_scale("cpu-api"),
                plan_id=plan_id,
                operation_id="retire",
                target_replicas=2,
            )
            assert target.execution_request is not None
            target.execution_request.update(
                purpose="staged-replacement-v1",
                destinationNodes=payload["destination_nodes"],
            )
            target.mark_applied()
            assert target.execution is not None
            target.execution["purpose"] = "staged-replacement-v1"
            target.pending_pod_create = {"operationID": "retire"} if pending_create else None
        Repository.record_action_progress(
            conn,
            plan_id,
            "retire",
            "controller-a",
            token,
            observation=target.observe_scale("cpu-api"),
            result={"stage": prepared_stage},
        )
        Repository.request_execution_control(conn, plan_id, control_request, "test control")
        conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp() "
            "WHERE plan_id = %s",
            (plan_id,),
        )

    controller_b = ExecutionController(repo, target, FakeHealth(), "controller-b")
    controller_b.run_once()
    controller_b.run_once()

    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state FROM action_intents WHERE plan_id = %s", (plan_id,)
        ).fetchone()
        execution = conn.execute(
            "SELECT status, controller_id, controller_fencing_token, "
            "pending_controller_id, pause_requested, cancel_requested "
            "FROM plan_executions WHERE plan_id = %s",
            (plan_id,),
        ).fetchone()
        lease = conn.execute("SELECT 1 FROM plan_leases WHERE plan_id = %s", (plan_id,)).fetchone()
        receipt = Repository.execution_fence_receipt(
            conn, plan_id, "retire", execution["controller_fencing_token"]
        )
        saved_action = conn.execute(
            "SELECT * FROM action_intents WHERE plan_id = %s AND intent_id = 'retire'",
            (plan_id,),
        ).fetchone()
    assert action["execution_state"] == ("FAILED" if not pending_create else "RUNNING"), (
        action,
        execution,
        lease,
        receipt,
        target.observe_scale("cpu-api"),
        target.fencing_token,
        controller_b._takeover_fence_matches(
            saved_action, plan_id, execution["controller_fencing_token"], receipt
        )
        if receipt is not None
        else None,
    )
    assert execution["status"] == expected_state
    if pending_create:
        assert lease is not None


@pytest.mark.parametrize("action_kind", ["SET_REPLICA_COUNT", "PREPARE_SOURCE_RETIREMENT"])
@pytest.mark.parametrize("pause_first", [False, True])
def test_cancel_retains_plan_lock_until_failed_action_create_resolves(
    clean_database: Database, action_kind: str, pause_first: bool
) -> None:
    class HeldCreateTarget(FakeFleetRightWorkloadTarget):
        def __init__(self) -> None:
            super().__init__(2)
            self.pending_pod_create = {"operationID": "earlier-create"}

        def observe_scale(self, workload_id: str) -> dict[str, object]:
            return {
                **super().observe_scale(workload_id),
                "pending_pod_create": self.pending_pod_create,
            }

    repo = clean_database.repository
    plan_id = f"cancel-held-create-{action_kind.lower()}-{pause_first}"
    conflict_key = f"resource:{plan_id}"
    if action_kind == "PREPARE_SOURCE_RETIREMENT":
        _prepare_retirement_action(repo, plan_id, conflict_key)
        action_id = "retire"
    else:
        _, _, _ = _prepare_controller(repo, plan_id, 2, 3, [conflict_key])
        action_id = "scale"
    target = HeldCreateTarget()
    controller = ExecutionController(repo, target, FakeHealth(), "controller-a")

    with repo.transaction() as conn:
        token = conn.execute(
            "SELECT controller_fencing_token FROM plan_executions WHERE plan_id = %s",
            (plan_id,),
        ).fetchone()["controller_fencing_token"]
        observation = target.observe_scale("cpu-api")
        Repository.transition_action(
            conn,
            plan_id,
            action_id,
            "NOT_STARTED",
            "RUNNING",
            "controller-a",
            token,
            pre_observation=observation,
        )
        Repository.transition_action(
            conn,
            plan_id,
            action_id,
            "RUNNING",
            "FAILED",
            "controller-a",
            token,
            target_observation=observation,
            error="POD_CREATE_OUTCOME_UNKNOWN",
        )

    controller._fail_safe(plan_id, token, "POD_CREATE_OUTCOME_UNKNOWN")
    with repo.transaction() as conn:
        assert (
            conn.execute(
                "SELECT status FROM plan_executions WHERE plan_id = %s", (plan_id,)
            ).fetchone()["status"]
            == "EXECUTING"
        )

    if pause_first:
        with repo.transaction() as conn:
            Repository.request_execution_control(conn, plan_id, "PAUSE", "test pause")
        controller.run_once()
        with repo.transaction() as conn:
            assert (
                conn.execute(
                    "SELECT status FROM plan_executions WHERE plan_id = %s", (plan_id,)
                ).fetchone()["status"]
                == "PAUSED"
            )

    with repo.transaction() as conn:
        Repository.request_execution_control(conn, plan_id, "CANCEL", "test cancel")
    controller.run_once()
    with repo.transaction() as conn:
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = %s", (plan_id,)
        ).fetchone()
        lease = conn.execute("SELECT 1 FROM plan_leases WHERE plan_id = %s", (plan_id,)).fetchone()
    assert execution["status"] != "CANCELLED"
    assert lease is not None

    target.pending_pod_create = None
    controller.run_once()
    with repo.transaction() as conn:
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = %s", (plan_id,)
        ).fetchone()
        lease = conn.execute("SELECT 1 FROM plan_leases WHERE plan_id = %s", (plan_id,)).fetchone()
    assert execution["status"] == "CANCELLED"
    assert lease is None


def test_retirement_unknown_outcome_stays_blocked_after_controller_takeover(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    _prepare_retirement_action(repo, "retirement-unknown")
    target = RetirementGateTarget(TargetUnavailable("retirement observation unavailable"))
    controller_a = ExecutionController(repo, target, FakeHealth(), "controller-a")
    controller_a.run_once()
    with repo.transaction() as conn:
        conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp() "
            "WHERE plan_id = 'retirement-unknown'"
        )

    controller_b = ExecutionController(repo, target, FakeHealth(), "controller-b")
    controller_b.run_once()

    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state FROM action_intents WHERE plan_id = 'retirement-unknown'"
        ).fetchone()
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'retirement-unknown'"
        ).fetchone()
    assert target.retirement_calls == 1
    assert action["execution_state"] == "OUTCOME_UNKNOWN"
    assert execution["status"] == "EXECUTING"


def test_admitted_eviction_readiness_loss_after_takeover_keeps_plan_unresolved(
    clean_database: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    import json
    from unittest.mock import Mock

    from fleetright.pod_admission import PodCreateAdmission
    from fleetright.workload_controller import _pod_create_intent, _pod_object
    from test_pod_admission import EvictionAPI, _eviction_request

    repo = clean_database.repository
    api = EvictionAPI()
    workload = api.workload
    source_name = api.source["metadata"]["name"]
    payload = {
        "operation_id": "operation-1",
        "source_owner_name": "api",
        "source_owner_uid": "workload-uid",
        "source_pod_name": source_name,
        "source_pod_uid": "source-uid",
        "source_node_id": "worker-a",
        "source_node_uid": "node-a-uid",
        "source_logical_slot_id": "api-0",
        "baseline_replicas": 1,
        "surge_desired_replicas": 2,
        "final_desired_replicas": 2,
        "profile_required_replicas": 1,
        "destination_nodes": [{"name": "worker-b", "uid": "node-b-uid"}],
    }
    with repo.transaction() as conn:
        _insert_plan(conn, "plan-1")
        conn.execute(
            "UPDATE plans SET binding = %s WHERE plan_id = 'plan-1'",
            (Jsonb({"source": {}, "conflict_keys": ["workload:api"]}),),
        )
        conn.execute(
            "INSERT INTO control_state (singleton, snapshot_id, snapshot_generation, binding) "
            "VALUES (TRUE, 'snapshot', 1, %s)",
            (Jsonb({}),),
        )
        conn.execute(
            "INSERT INTO conflict_leases (conflict_key, owner_id, principal, fencing_token, "
            "expires_at) VALUES ('workload:api', 'lease-owner', 'operator', 7, "
            "clock_timestamp() + interval '1 hour')"
        )
        conn.execute(
            "INSERT INTO plan_leases (plan_id, conflict_key, owner_id, fencing_token) "
            "VALUES ('plan-1', 'workload:api', 'lease-owner', 7)"
        )
        conn.execute(
            "INSERT INTO action_intents (plan_id, intent_id, action_sequence, action_type, "
            "workload_id, payload, status) VALUES ('plan-1', 'operation-1', 0, "
            "'PREPARE_SOURCE_RETIREMENT', 'api', %s, 'APPROVED')",
            (Jsonb(payload),),
        )
    old_token = _claim(repo, "plan-1")["execution"]["controller_fencing_token"]
    workload["spec"]["executionRequest"]["generation"] = old_token
    workload["spec"]["executionFence"]["generation"] = old_token
    workload["status"]["execution"]["generation"] = old_token
    request = _eviction_request(api)
    request["object"]["metadata"]["annotations"]["fleetright.io/fencing-token"] = str(old_token)
    assert PodCreateAdmission(api, "fleetright-v2", "fleetright-workload-controller").admit(
        request
    ) == (True, "")
    admitted_delete = request["object"]["deleteOptions"]["preconditions"]
    prepared = {
        "stage": "EVICTION_INTENT",
        "source_retirement_intent_recorded": True,
        "workload_uid": "workload-uid",
        "source_pod_name": source_name,
        "source_pod_uid": "source-uid",
        "source_pod_resource_version": "3",
        "replacement_pod_name": api.replacement["metadata"]["name"],
        "replacement_pod_uid": "replacement-uid",
        "replacement_replica_id": "api-1",
        "replacement_node_id": "worker-b",
        "replacement_node_uid": "node-b-uid",
        "replacement_create_id": "replacement-create-id",
        "replacement_workload_generation": 2,
    }
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    extra_pods: list[dict[str, object]] = []

    def target_json(*args: str) -> dict[str, object]:
        if args[:2] == ("patch", "fleetrightworkload"):
            patch = json.loads(args[args.index("-p") + 1])
            assert patch["metadata"]["resourceVersion"] == workload["metadata"]["resourceVersion"]
            workload["spec"].update(patch["spec"])
            workload["metadata"]["generation"] += 1
            workload["metadata"]["resourceVersion"] = str(
                int(workload["metadata"]["resourceVersion"]) + 1
            )
            return deepcopy(workload)
        if args[:2] == ("get", "pods"):
            return {"items": [deepcopy(api.replacement), *deepcopy(extra_pods)]}
        assert args[:2] == ("get", "node")
        return deepcopy(next(node for node in api.nodes if node["metadata"]["name"] == args[2]))

    monkeypatch.setattr(target, "_json", target_json)
    monkeypatch.setattr(target, "_require_fencing_schema", lambda: None)
    monkeypatch.setattr(
        target, "observe_scale", lambda _: target._observe(deepcopy(workload), "api")
    )
    monkeypatch.setattr(
        target,
        "get_pod",
        lambda name: deepcopy(api.source if name == source_name else api.replacement),
    )
    monkeypatch.setattr(
        target,
        "_kubectl",
        lambda *_args, **_kwargs: pytest.fail("a second Eviction was dispatched"),
    )
    eviction_dispatch = Mock(wraps=target.evict_source)
    monkeypatch.setattr(target, "evict_source", eviction_dispatch)
    with repo.transaction() as conn:
        Repository.transition_action(
            conn,
            "plan-1",
            "operation-1",
            "NOT_STARTED",
            "RUNNING",
            "controller-a",
            old_token,
            pre_observation={"uid": "workload-uid", "generation": 1, "replicas": 1},
        )
        Repository.record_action_progress(
            conn,
            "plan-1",
            "operation-1",
            "controller-a",
            old_token,
            observation=target.observe_scale("api"),
            result=prepared,
        )
        Repository.transition_action(
            conn,
            "plan-1",
            "operation-1",
            "RUNNING",
            "OUTCOME_UNKNOWN",
            "controller-a",
            old_token,
            error="response lost while admitted source deletion is outstanding",
        )
        conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp() "
            "WHERE plan_id = 'plan-1'"
        )
        execution = Repository.runnable_executions(conn)[0]
    controller = ExecutionController(repo, target, FakeHealth(), "controller-b")
    token = controller._claim(execution)
    assert token is not None and token > old_token
    workload["status"]["observedGeneration"] = workload["metadata"]["generation"]
    source_before = api.source
    pending_create = {
        "workloadUID": "workload-uid",
        "workloadGeneration": workload["metadata"]["generation"],
        "podName": "api-2",
        "replicaID": "api-2",
        "planID": "plan-1",
        "operationID": "late-create",
        "createID": "late-create-attempt",
    }
    workload["status"]["pendingPodCreate"] = pending_create
    api.source = None
    waiting = target.observe_source_retirement(
        "api", "plan-1", payload, prepared, fencing_token=token
    )
    assert waiting["stage"] == "RETIREMENT_WAITING_FINAL_HEALTH"
    assert waiting["source_removed_observed"] is True
    assert waiting["observation"]["pending_pod_create"] == pending_create
    workload["status"].pop("pendingPodCreate")
    api.source = source_before
    api.replacement["status"]["conditions"][0]["status"] = "False"
    workload["status"].update(readyReplicas=1, availableReplicas=1)
    assert (
        target.observe_source_retirement("api", "plan-1", payload, prepared, fencing_token=token)[
            "stage"
        ]
        != "EVICTION_RETRY_SAFE"
    )

    controller._reconcile("plan-1", token, None)

    with repo.transaction() as conn:
        action = conn.execute(
            "SELECT execution_state, execution_result FROM action_intents WHERE plan_id = 'plan-1'"
        ).fetchone()
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'plan-1'"
        ).fetchone()
        assert action["execution_state"] in {"RUNNING", "OUTCOME_UNKNOWN"}
        assert any(
            item.get("result", {}).get("stage") == "EVICTION_INTENT"
            for item in action["execution_result"]["progress"]
        )
        assert execution["status"] == "EXECUTING"
        assert (
            conn.execute("SELECT state FROM plans WHERE plan_id = 'plan-1'").fetchone()["state"]
            == "EXECUTING"
        )
        assert (
            conn.execute(
                "SELECT count(*) AS total FROM plan_leases WHERE plan_id = 'plan-1'"
            ).fetchone()["total"]
            == 1
        )
        assert (
            Repository.acquire_lease(conn, "workload:api", "competing-owner", "operator", 60_000)
            is None
        )
    eviction_dispatch.assert_not_called()
    assert admitted_delete == {
        key: api.source["metadata"][key] for key in ("uid", "resourceVersion")
    }

    # Release the original exact-UID delete; readiness still prevents completion.
    api.source = None
    workload["status"].update(replicas=1, readyReplicas=0, availableReplicas=0)
    controller.run_once()
    with repo.transaction() as conn:
        assert Repository.incomplete_actions(conn, "plan-1")[0]["execution_state"] in {
            "RUNNING",
            "OUTCOME_UNKNOWN",
        }
    api.replacement["status"]["conditions"][0]["status"] = "True"
    create = _pod_create_intent(workload, 2)
    final_pod = _pod_object(workload, 2, create)
    final_pod["metadata"]["uid"] = "post-retirement-pod-uid"
    final_pod["spec"]["nodeName"] = "worker-b"
    final_pod["status"] = deepcopy(api.replacement["status"])
    extra_pods.append(final_pod)
    workload["status"]["authorizedPodCreates"].append(
        {**create, "podUID": "post-retirement-pod-uid"}
    )
    workload["status"].update(
        replicas=2,
        readyReplicas=2,
        availableReplicas=2,
        conditions=[{"type": "Reconciled", "status": "True"}],
    )
    controller.run_once()
    with repo.transaction() as conn:
        completed = conn.execute(
            "SELECT execution_state, execution_result FROM action_intents WHERE plan_id = 'plan-1'"
        ).fetchone()
    assert completed["execution_state"] == "SUCCEEDED"
    assert completed["execution_result"]["result"]["source_pod_uid"] == admitted_delete["uid"]
    assert api.source is None
    eviction_dispatch.assert_not_called()


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


@pytest.mark.parametrize(
    ("verification_failure", "pending_create", "expected_execution_state"),
    [(False, False, "COMPENSATED"), (True, False, "FAILED_SAFE"), (False, True, "VERIFYING")],
)
def test_source_retirement_health_regression_restores_bounded_kubernetes_capacity(
    clean_database: Database,
    verification_failure: bool,
    pending_create: bool,
    expected_execution_state: str,
) -> None:
    repo = clean_database.repository
    plan_id = "retirement-compensation"
    recovery_nodes = [{"name": "worker-c", "uid": "worker-c-uid"}]
    health_policy = {
        "p99_limit_ms": 120,
        "error_slo_ppm": 1_000,
        "max_queue_growth_milli": 0,
    }
    payload = {
        "operation_id": "retire",
        "source_owner_uid": "deployment-uid",
        "workload_generation": 1,
        "baseline_replicas": 1,
        "surge_desired_replicas": 2,
        "final_desired_replicas": 2,
        "maximum_live_replicas": 3,
        "maximum_temporary_surge_replicas": 2,
        "n_minus_1_recovery_target_replicas": 2,
        "recovery_destination_nodes": recovery_nodes,
        "destination_nodes": [{"name": "worker-b", "uid": "worker-b-uid"}],
        "health": health_policy,
    }
    with repo.transaction() as conn:
        _insert_plan(conn, plan_id)
        conn.execute(
            "INSERT INTO control_state "
            "(singleton, snapshot_id, snapshot_generation, binding) VALUES "
            "(TRUE, 'snapshot', 1, %s)",
            (Jsonb({}),),
        )
        conn.execute(
            "INSERT INTO action_intents (plan_id, intent_id, action_sequence, action_type, "
            "workload_id, payload, status) VALUES (%s, 'retire', 0, "
            "'PREPARE_SOURCE_RETIREMENT', 'cpu-api', %s, 'APPROVED')",
            (plan_id, Jsonb(payload)),
        )
    claim = _claim(repo, plan_id)
    token = claim["execution"]["controller_fencing_token"]
    with repo.transaction() as conn:
        Repository.transition_action(
            conn,
            plan_id,
            "retire",
            "NOT_STARTED",
            "RUNNING",
            "controller-a",
            token,
            pre_observation={"uid": "deployment-uid", "generation": 1, "replicas": 1},
        )
        Repository.transition_action(
            conn,
            plan_id,
            "retire",
            "RUNNING",
            "SUCCEEDED",
            "controller-a",
            token,
            target_observation={"uid": "deployment-uid", "generation": 2, "replicas": 2},
            result={"stage": "RETIREMENT_COMPLETE"},
        )

    class RecoveryTarget(FakeFleetRightWorkloadTarget):
        def __init__(self) -> None:
            super().__init__(2)
            self.generation = self.observed_generation = 2
            self.execution_request = {
                "planID": plan_id,
                "operationID": "retire",
                "generation": 0,
                "expectedReplicas": 1,
                "targetReplicas": 2,
            }
            self.mark_applied()
            self.authorized_destinations: list[dict[str, str]] | None = None

        def observe_source_retirement(self, *_args: object, **_kwargs: object) -> dict[str, object]:
            return {
                "stage": "RETIREMENT_WAITING_FINAL_HEALTH",
                "source_removed_observed": True,
                "observation": self.observe_scale("cpu-api"),
            }

        def observe_scale(self, workload_id: str) -> dict[str, object]:
            return {
                **super().observe_scale(workload_id),
                "pending_pod_create": {"operationID": "late-create"} if pending_create else None,
            }

        def authorize_scale(
            self,
            workload_id: str,
            fencing_token: int,
            *,
            expected: dict[str, object],
            plan_id: str,
            operation_id: str,
            target_replicas: int,
            destination_nodes: list[dict[str, str]] | None = None,
        ) -> dict[str, object]:
            self.authorized_destinations = destination_nodes
            assert destination_nodes == recovery_nodes
            return super().authorize_scale(
                workload_id,
                fencing_token,
                expected=expected,
                plan_id=plan_id,
                operation_id=operation_id,
                target_replicas=target_replicas,
            )

        def set_replicas(self, *args: object, **kwargs: object) -> dict[str, object]:
            super().set_replicas(*args, **kwargs)  # type: ignore[arg-type]
            self.mark_applied()
            return {**self.observe_scale("cpu-api"), "applied": True}

        def verify_recovery(
            self,
            verified_plan_id: str,
            recovery_action: dict[str, object],
            observed: dict[str, object],
        ) -> None:
            assert verified_plan_id == plan_id
            assert recovery_action["workload_id"] == "cpu-api"
            assert observed["uid"] == "deployment-uid"
            if verification_failure:
                raise TargetPreconditionFailed("recovery target identity changed")

    target = RecoveryTarget()
    controller = ExecutionController(repo, target, FakeHealth([False, True]), "controller-a")
    controller._transition(plan_id, token, "EXECUTING", "VERIFYING", {"actions": "observed"})
    controller.run_once()

    with repo.transaction() as conn:
        compensation = conn.execute(
            "SELECT payload FROM action_intents "
            "WHERE plan_id = %s AND compensates_intent_id = 'retire'",
            (plan_id,),
        ).fetchone()
    if pending_create:
        assert compensation is None
        with repo.transaction() as conn:
            execution = conn.execute(
                "SELECT status FROM plan_executions WHERE plan_id = %s", (plan_id,)
            ).fetchone()
        assert execution["status"] == "VERIFYING"
        return
    assert compensation["payload"]["proposed_replicas"] == 3
    assert compensation["payload"]["destination_nodes"] == recovery_nodes

    controller.run_once()
    controller.run_once()

    with repo.transaction() as conn:
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = %s", (plan_id,)
        ).fetchone()
        compensation = conn.execute(
            "SELECT execution_state, execution_error, execution_result "
            "FROM action_intents WHERE plan_id = %s "
            "AND compensates_intent_id = 'retire'",
            (plan_id,),
        ).fetchone()
    assert execution["status"] == expected_execution_state, (
        execution,
        compensation,
        target.observe_scale("cpu-api"),
    )
    assert compensation["execution_state"] == "SUCCEEDED", (execution, compensation)
    assert target.replicas == 3
    assert target.authorized_destinations == recovery_nodes


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


def test_compensation_that_never_becomes_ready_fails_safe_after_timeout(
    clean_database: Database,
) -> None:
    controller, target, health = _prepare_controller(
        clean_database.repository, "compensation-timeout", 10, 9
    )
    health.samples = [True, False]  # healthy before the scale-in, regressed after it
    real_observe = target.observe_scale

    def observe_with_unplaceable_replica(workload_id: str) -> dict[str, object]:
        observed = real_observe(workload_id)
        if target.mutations[-1:] == [10] and len(target.mutations) > 1:
            observed["ready_replicas"] = observed["available_replicas"] = 9  # never schedulable
        return observed

    target.observe_scale = observe_with_unplaceable_replica  # type: ignore[method-assign]
    for _ in range(6):
        controller.run_once()
        if target.mutations == [9, 10]:
            controller.readiness_timeout_seconds = 0

    with clean_database.repository.transaction() as conn:
        execution = conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = 'compensation-timeout'"
        ).fetchone()
        states = conn.execute(
            "SELECT execution_state, compensates_intent_id, execution_result->>'error' AS error "
            "FROM action_intents WHERE plan_id = 'compensation-timeout' ORDER BY action_sequence"
        ).fetchall()
    assert target.mutations == [9, 10]  # one scale-in, one bounded restore, nothing more
    assert execution["status"] == "FAILED_SAFE", states
    assert [row["execution_state"] for row in states] == ["FAILED", "FAILED"]


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


_NODE = {"source_node_id": "worker-1", "source_node_uid": "worker-uid"}


class _DrainTarget:
    """Signature-faithful Node target; `verify_error` fires only once the plan is VERIFYING."""

    supports_workload_execution = True

    def __init__(self) -> None:
        self.node_token = 0
        self.verify_error: Exception | None = None
        self.verify_failures: int | None = None  # None: fail forever once verifying
        self.verifying = False
        self.cordon_calls = 0
        self.released: list[tuple[str, int]] = []

    def _view(self, plan_id: str, operation_id: object) -> dict[str, object]:
        return {
            **_NODE,
            "plan_id": plan_id,
            "operation_id": operation_id,
            "fencing_token": self.node_token,
            "unschedulable": True,
        }

    def observe_source_node_state(self, _plan_id: str, _payload: dict[str, object]):
        return {**_NODE, "unschedulable": False}

    def cordon_source_node(self, plan_id: str, payload: dict[str, object], token: int):
        self.cordon_calls += 1
        self.node_token = max(self.node_token, token)
        return {**self._view(plan_id, payload["operation_id"]), "stage": "NODE_CORDONED"}

    def observe_source_node_drain(self, plan_id: str, payload: dict[str, object], token: int):
        if self.verifying and self.verify_error is not None:
            if self.verify_failures is None:
                raise self.verify_error
            if self.verify_failures > 0:
                self.verify_failures -= 1
                raise self.verify_error
        return {
            **self._view(plan_id, payload["cordon_operation_id"]),
            "stage": "NODE_DRAIN_COMPLETE",
            "remaining_source_pod_uids": [],
            "exempt_pods": [],
            "inventory_pod_count": 1,
        }

    def release_source_node(self, plan_id: str, _payload: dict[str, object], token: int):
        self.released.append((plan_id, token))
        return {"released": True}


def _drain_plan_in_verifying(
    repo: Repository, plan_id: str, target: _DrainTarget, **controller_options: float
) -> ExecutionController:
    with repo.transaction() as conn:
        _insert_plan(conn, plan_id)
        for sequence, (intent, action_type, payload) in enumerate(
            [
                ("cordon", "CORDON_NODE", {**_NODE, "operation_id": "cordon-operation"}),
                (
                    "verify",
                    "VERIFY_NODE_DRAIN",
                    {
                        **_NODE,
                        "operation_id": "verify-operation",
                        "cordon_operation_id": "cordon-operation",
                        "source_pods": [
                            {"workload_id": "api", "pod_name": "api-0", "pod_uid": "pod-uid"}
                        ],
                    },
                ),
            ]
        ):
            conn.execute(
                "INSERT INTO action_intents (plan_id, intent_id, action_sequence, action_type, "
                "payload, status) VALUES (%s, %s, %s, %s, %s, 'APPROVED')",
                (plan_id, intent, sequence, action_type, Jsonb(payload)),
            )
    _claim(repo, plan_id)
    controller = ExecutionController(
        repo, target, FakeHealth(), "controller-a", **controller_options
    )
    for _ in range(10):
        if _plan_status(repo, plan_id) == "VERIFYING":
            break
        controller.run_once()
    assert _plan_status(repo, plan_id) == "VERIFYING"
    target.verifying = True
    return controller


def _plan_status(repo: Repository, plan_id: str) -> str:
    with repo.transaction() as conn:
        return conn.execute(
            "SELECT status FROM plan_executions WHERE plan_id = %s", (plan_id,)
        ).fetchone()["status"]


def _age_actions(repo: Repository, plan_id: str, seconds: int) -> None:
    with repo.transaction() as conn:
        conn.execute(
            "UPDATE action_intents SET execution_finished_at = execution_finished_at "
            "- make_interval(secs => %s) WHERE plan_id = %s",
            (seconds, plan_id),
        )


def test_final_verification_succeeds_immediately_and_releases_node_metadata(
    clean_database: Database,
) -> None:
    repo, target = clean_database.repository, _DrainTarget()
    controller = _drain_plan_in_verifying(repo, "verify-immediate", target)
    controller.run_once()
    assert _plan_status(repo, "verify-immediate") == "COMMITTED"
    # Transient Node metadata is released exactly once, after the terminal state is durable.
    assert [plan for plan, _ in target.released] == ["verify-immediate"]


def test_final_verification_succeeds_after_bounded_retry(clean_database: Database) -> None:
    repo, target = clean_database.repository, _DrainTarget()
    controller = _drain_plan_in_verifying(repo, "verify-retry", target)
    target.verify_error, target.verify_failures = TargetError("API unavailable"), 3
    for _ in range(3):
        controller.run_once()
        assert _plan_status(repo, "verify-retry") == "VERIFYING"
        assert target.released == []
    controller.run_once()
    assert _plan_status(repo, "verify-retry") == "COMMITTED"


@pytest.mark.parametrize(
    "error",
    [
        TargetError("Kubernetes API unavailable"),
        TargetPreconditionFailed("Node drain authority is not held"),
    ],
)
def test_final_verification_that_never_succeeds_fails_safe_at_the_deadline(
    clean_database: Database, error: Exception
) -> None:
    repo, target = clean_database.repository, _DrainTarget()
    plan_id = f"verify-never-{type(error).__name__.lower()}"
    controller = _drain_plan_in_verifying(repo, plan_id, target, verification_deadline_seconds=60)
    target.verify_error = error  # fails forever
    controller.run_once()
    assert _plan_status(repo, plan_id) == "VERIFYING"
    _age_actions(repo, plan_id, 120)
    controller.run_once()
    assert _plan_status(repo, plan_id) == "FAILED_SAFE"
    with repo.transaction() as conn:
        outcome = conn.execute(
            "SELECT outcome FROM plan_executions WHERE plan_id = %s", (plan_id,)
        ).fetchone()["outcome"]
    assert "final-verification-deadline-exceeded" in str(outcome)
    assert len(target.released) == 1  # no destructive retry; only metadata release


def test_verification_deadline_survives_controller_takeover(clean_database: Database) -> None:
    repo, target = clean_database.repository, _DrainTarget()
    plan_id = "verify-takeover-deadline"
    first = _drain_plan_in_verifying(repo, plan_id, target, verification_deadline_seconds=60)
    target.verify_error = TargetError("API unavailable")
    first.run_once()
    _age_actions(repo, plan_id, 120)
    with repo.transaction() as conn:
        conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp() "
            "WHERE plan_id = %s",
            (plan_id,),
        )
    second = ExecutionController(
        repo, target, FakeHealth(), "controller-b", verification_deadline_seconds=60
    )
    for _ in range(4):
        second.run_once()
    # The replacement inherits the elapsed budget; it does not restart it.
    assert _plan_status(repo, plan_id) == "FAILED_SAFE"


def test_stale_controller_cannot_extend_or_finish_verification(clean_database: Database) -> None:
    repo, target = clean_database.repository, _DrainTarget()
    plan_id = "verify-stale"
    first = _drain_plan_in_verifying(repo, plan_id, target, verification_deadline_seconds=60)
    with repo.transaction() as conn:
        conn.execute(
            "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp() "
            "WHERE plan_id = %s",
            (plan_id,),
        )
    second = ExecutionController(repo, target, FakeHealth(), "controller-b")
    for _ in range(3):
        second.run_once()
    assert _plan_status(repo, plan_id) == "COMMITTED"
    released = len(target.released)
    first.run_once()  # stale: the execution is terminal / fenced to controller-b
    assert _plan_status(repo, plan_id) == "COMMITTED"
    assert len(target.released) == released


def test_node_uncordoned_after_drain_completion_blocks_commit(clean_database: Database) -> None:
    repo, target = clean_database.repository, _DrainTarget()
    plan_id = "verify-uncordoned"
    controller = _drain_plan_in_verifying(repo, plan_id, target)
    target.verify_error = ActionBlocked("DRAIN_NODE_UNCORDONED_EXTERNALLY")
    controller.run_once()
    assert _plan_status(repo, plan_id) == "FAILED_SAFE"
    assert target.cordon_calls <= 3  # the cordon action and fence re-adoption; never a loop


def test_release_sweep_recovers_a_crash_between_terminal_commit_and_node_release(
    clean_database: Database,
) -> None:
    repo, target = clean_database.repository, _DrainTarget()
    controller = _drain_plan_in_verifying(repo, "sweep-plan", target)
    controller.run_once()
    assert _plan_status(repo, "sweep-plan") == "COMMITTED"
    target.released.clear()  # the original release was lost with the crashed controller
    ExecutionController(repo, target, FakeHealth(), "controller-b")._sweep_node_metadata()
    assert [plan for plan, _ in target.released] == ["sweep-plan"]


def test_compensating_final_check_is_bounded_by_a_deadline(clean_database: Database) -> None:
    repo = clean_database.repository
    with repo.transaction() as conn:
        _insert_plan(conn, "comp-deadline")
        conn.execute(
            "INSERT INTO action_intents (plan_id, intent_id, action_sequence, action_type, "
            "workload_id, payload, status) VALUES ('comp-deadline', 'orig', 0, "
            "'SET_REPLICA_COUNT', 'cpu-api', %s, 'APPROVED')",
            (Jsonb({"proposed_replicas": 3}),),
        )
    token = _claim(repo, "comp-deadline")["execution"]["controller_fencing_token"]
    with repo.transaction() as conn:
        conn.execute(
            "INSERT INTO action_intents (plan_id, intent_id, action_sequence, action_type, "
            "workload_id, payload, status, compensates_intent_id, execution_state, "
            "execution_started_at, execution_finished_at) VALUES ('comp-deadline', 'comp', 1, "
            "'SET_REPLICA_COUNT', 'cpu-api', %s, 'APPROVED', 'orig', 'SUCCEEDED', "
            "clock_timestamp() - interval '2 hours', clock_timestamp() - interval '2 hours')",
            (Jsonb({"proposed_replicas": 2}),),
        )
        conn.execute(
            "UPDATE action_intents SET execution_state='SUCCEEDED', "
            "execution_started_at=clock_timestamp()-interval '3 hours', "
            "execution_finished_at=clock_timestamp()-interval '3 hours' "
            "WHERE plan_id='comp-deadline' AND intent_id='orig'"
        )
        conn.execute("UPDATE plans SET state='COMPENSATING' WHERE plan_id='comp-deadline'")
        conn.execute(
            "UPDATE plan_executions SET status='COMPENSATING' WHERE plan_id='comp-deadline'"
        )

    class Target:
        supports_workload_execution = False

        def observe_scale(self, _workload: str):
            raise TargetError("API unavailable")  # recovery can never be observed

    controller = ExecutionController(
        repo, Target(), FakeHealth(), "controller-a", verification_deadline_seconds=60
    )
    controller._reconcile("comp-deadline", token, None)
    assert _plan_status(repo, "comp-deadline") == "FAILED_SAFE"
    with repo.transaction() as conn:
        outcome = conn.execute(
            "SELECT outcome FROM plan_executions WHERE plan_id = 'comp-deadline'"
        ).fetchone()["outcome"]
    assert "compensation-verification-deadline-exceeded" in str(outcome)
