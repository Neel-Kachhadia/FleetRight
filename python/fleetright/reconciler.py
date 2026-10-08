from __future__ import annotations

import logging
import threading
import time
from typing import Any

from fleetright.control import ControlError, LeaseConflict, Repository, StaleState
from fleetright.execution import (
    ActionBlocked,
    OutcomeUnknown,
    TargetError,
    TargetNotFound,
    TargetPreconditionFailed,
    TargetUnavailable,
    valid_execution_health_policy,
)

LOG = logging.getLogger("fleetright.execution")


class ExecutionController:
    """One bounded PostgreSQL-backed reconciliation pass per active execution."""

    def __init__(
        self,
        repository: Repository,
        target: Any,
        health: Any,
        controller_id: str,
        *,
        lease_ttl_ms: int = 30_000,
        max_attempts: int = 2,
        readiness_timeout_seconds: float = 30,
        poll_seconds: float = 1,
    ) -> None:
        if not controller_id or lease_ttl_ms < 1_000 or max_attempts < 1:
            raise ValueError("controller ID, lease TTL and attempt limit are invalid")
        self.repository = repository
        self.target = target
        self.health = health
        self.controller_id = controller_id
        self.lease_ttl_ms = lease_ttl_ms
        self.max_attempts = max_attempts
        self.readiness_timeout_seconds = readiness_timeout_seconds
        self.poll_seconds = poll_seconds

    def run_forever(self, stop: threading.Event) -> None:
        while not stop.is_set():
            try:
                self.run_once(stop)
            except Exception:
                LOG.exception("execution reconciliation pass failed")
            stop.wait(self.poll_seconds)

    def run_once(self, stop: threading.Event | None = None) -> None:
        with self.repository.transaction() as conn:
            executions = Repository.runnable_executions(conn)
        for row in executions:
            if stop is not None and stop.is_set():
                return
            token = self._claim(row)
            if token is None:
                continue
            self._reconcile(row["plan_id"], token, stop)

    def _claim(self, execution: dict[str, Any]) -> int | None:
        plan_id = execution["plan_id"]
        if (
            execution["controller_id"] == self.controller_id
            and not execution["controller_lease_expired"]
        ):
            with self.repository.transaction() as conn:
                if Repository.renew_execution_controller(
                    conn,
                    plan_id,
                    self.controller_id,
                    execution["controller_fencing_token"],
                    self.lease_ttl_ms,
                ):
                    return int(execution["controller_fencing_token"])
            return None

        with self.repository.transaction() as conn:
            pending_token = Repository.prepare_execution_takeover(
                conn, plan_id, self.controller_id, self.lease_ttl_ms
            )
        if pending_token is None:
            return None
        with self.repository.transaction() as conn:
            unresolved = [
                action
                for action in Repository.incomplete_actions(conn, plan_id)
                if action["execution_state"] in {"RUNNING", "OUTCOME_UNKNOWN"}
            ]
        for action in unresolved:
            if action["action_type"] != "SET_REPLICA_COUNT" or not action["workload_id"]:
                return None
            try:
                receipt = self.target.fence_scale(action["workload_id"], pending_token)
            except TargetNotFound:
                with self.repository.transaction() as conn:
                    Repository.fail_takeover_for_missing_target(
                        conn,
                        plan_id,
                        action["intent_id"],
                        self.controller_id,
                        pending_token,
                    )
                return None
            except TargetError:
                return None
            with self.repository.transaction() as conn:
                Repository.record_execution_fence(
                    conn,
                    plan_id,
                    action["intent_id"],
                    self.controller_id,
                    pending_token,
                    f"deployment:{action['workload_id']}",
                    receipt,
                )
        with self.repository.transaction() as conn:
            token = Repository.claim_execution_controller(
                conn,
                plan_id,
                self.controller_id,
                pending_token,
                self.lease_ttl_ms,
            )
            for action in unresolved:
                if action["execution_state"] == "RUNNING":
                    Repository.transition_action(
                        conn,
                        plan_id,
                        action["intent_id"],
                        "RUNNING",
                        "OUTCOME_UNKNOWN",
                        self.controller_id,
                        token,
                        error="controller-takeover-requires-target-observation",
                    )
            return token

    def _reconcile(self, plan_id: str, token: int, stop: threading.Event | None) -> None:
        with self.repository.transaction() as conn:
            execution = Repository.assert_execution_controller(
                conn, plan_id, self.controller_id, token
            )
            actions = Repository.incomplete_actions(conn, plan_id)
        phase = execution["status"]
        compensating = phase == "COMPENSATING"
        if phase == "PAUSED" and execution["cancel_requested"]:
            self._transition(
                plan_id,
                token,
                "PAUSED",
                "CANCELLED",
                {"side_effects_undone": False, "future_actions": "cancelled"},
            )
            return
        control_requested = not compensating and (
            execution["pause_requested"] or execution["cancel_requested"]
        )
        if control_requested:
            unresolved = [
                action
                for action in actions
                if action["execution_state"] in {"RUNNING", "OUTCOME_UNKNOWN"}
                and action["compensates_intent_id"] is None
            ]
            if unresolved:
                if stop is not None and stop.is_set():
                    return
                self._reconcile_action(plan_id, token, unresolved[0], False, allow_dispatch=False)
                return
            cancelled = bool(execution["cancel_requested"])
            new_state = "CANCELLED" if cancelled else "PAUSED"
            self._transition(
                plan_id,
                token,
                phase,
                new_state,
                {
                    "side_effects_undone": False,
                    "future_actions": "cancelled" if cancelled else "paused",
                },
            )
            return
        if phase == "VERIFYING":
            self._verify_and_commit(plan_id, token)
            return
        candidates = [
            action
            for action in actions
            if (action["compensates_intent_id"] is not None) == compensating
        ]
        if not candidates:
            if phase == "EXECUTING":
                with self.repository.transaction() as conn:
                    completed = conn.execute(
                        "SELECT count(*) AS total, count(*) FILTER "
                        "(WHERE execution_state = 'SUCCEEDED') AS succeeded, "
                        "count(*) FILTER (WHERE execution_state = 'FAILED') AS failed "
                        "FROM action_intents WHERE plan_id = %s "
                        "AND compensates_intent_id IS NULL",
                        (plan_id,),
                    ).fetchone()
                if completed["failed"]:
                    self._fail_safe(plan_id, token, "one-or-more-actions-failed")
                    return
                if completed["total"] != completed["succeeded"]:
                    return
                self._transition(plan_id, token, "EXECUTING", "VERIFYING", {"actions": "observed"})
            elif phase == "COMPENSATING":
                try:
                    recovery = self._observe_recovery(plan_id)
                except TargetNotFound:
                    self._fail_safe(plan_id, token, "target-deleted-during-recovery-verification")
                    return
                self._transition(plan_id, token, "COMPENSATING", "COMPENSATED", recovery)
            return

        action = candidates[0]
        state = action["execution_state"]
        if state == "NOT_STARTED":
            self._start_action(plan_id, token, action, compensating, stop)
        elif state in {"RUNNING", "OUTCOME_UNKNOWN"}:
            if stop is not None and stop.is_set():
                return
            self._reconcile_action(plan_id, token, action, compensating, stop=stop)

    def _verify_and_commit(self, plan_id: str, token: int) -> None:
        with self.repository.transaction() as conn:
            actions = conn.execute(
                "SELECT intent_id, workload_id, payload, execution_state, execution_started_at, "
                "execution_finished_at, execution_result FROM action_intents "
                "WHERE plan_id = %s AND compensates_intent_id IS NULL "
                "ORDER BY action_sequence",
                (plan_id,),
            ).fetchall()
        if any(action["execution_state"] != "SUCCEEDED" for action in actions):
            self._fail_safe(plan_id, token, "verification-found-incomplete-action")
            return
        evidence = []
        for action in actions:
            payload = action["payload"]
            desired = payload.get("proposed_replicas") if isinstance(payload, dict) else None
            result = action["execution_result"] or {}
            previous = result.get("pre_observation", {})
            completed = result.get("target_observation", {})
            if not isinstance(desired, int) or not isinstance(previous, dict):
                self._fail_safe(plan_id, token, "verification-journal-invalid")
                return
            try:
                observed = self.target.observe_scale(action["workload_id"])
            except TargetNotFound:
                self._fail_safe(plan_id, token, "target-deleted-during-verification")
                return
            except TargetError:
                return
            if (
                observed["uid"] != previous.get("uid")
                or observed["replicas"] != desired
                or observed.get("generation") != completed.get("generation")
            ):
                self._fail_safe(plan_id, token, "target-changed-during-final-verification")
                return
            health = self._health_observation(action)
            healthy = (
                self._capacity_ready(observed)
                and health is not None
                and health.get("ready") is True
                and health.get("fresh_sample") is True
                and health.get("healthy") is True
            )
            if not healthy:
                fresh_bad_sample = (
                    health is not None
                    and health.get("fresh_sample") is True
                    and health.get("healthy") is False
                )
                if fresh_bad_sample and desired < previous.get("replicas", desired):
                    self._begin_compensation(plan_id, token, action, observed, "VERIFYING")
                    return
                started = action.get("execution_finished_at")
                timed_out = (
                    started is not None
                    and time.time() - started.timestamp() > self.readiness_timeout_seconds
                )
                if timed_out:
                    if desired < previous.get("replicas", desired):
                        self._begin_compensation(plan_id, token, action, observed, "VERIFYING")
                    else:
                        self._fail_safe(plan_id, token, "final-health-verification-timeout")
                return
            evidence.append(
                {"intent_id": action["intent_id"], "target": observed, "health": health}
            )
        self._transition(
            plan_id,
            token,
            "VERIFYING",
            "COMMITTED",
            {"health": "passed", "observations": evidence},
        )

    def _transition(
        self, plan_id: str, token: int, old: str, new: str, outcome: dict[str, Any]
    ) -> None:
        with self.repository.transaction() as conn:
            Repository.transition_execution_and_plan(
                conn,
                plan_id,
                old,
                new,
                self.controller_id,
                token,
                actor="fleetright-controller",
                role="controller",
                reason=f"execution-{new.lower()}",
                request_id=f"reconcile:{plan_id}",
                outcome=outcome,
            )
            if new in {"COMMITTED", "CANCELLED", "COMPENSATED", "FAILED_SAFE"}:
                Repository.release_plan_leases(conn, plan_id)
        LOG.info(
            "execution state changed",
            extra={
                "plan_id": plan_id,
                "state": new,
                "controller_id": self.controller_id,
                "fencing_token": token,
            },
        )

    @staticmethod
    def _payload(action: dict[str, Any]) -> dict[str, Any]:
        payload = action.get("payload")
        if not isinstance(payload, dict):
            raise TargetPreconditionFailed("action payload is invalid")
        return payload

    def _start_action(
        self,
        plan_id: str,
        token: int,
        action: dict[str, Any],
        compensating: bool,
        stop: threading.Event | None = None,
    ) -> None:
        payload = self._payload(action)
        workload = action["workload_id"]
        if action["action_type"] != "SET_REPLICA_COUNT" or not workload:
            self._fail_safe(plan_id, token, "unsupported-action")
            return
        try:
            observed = self.target.observe_scale(workload)
        except TargetNotFound:
            self._fail_safe(plan_id, token, "target-deleted-before-dispatch")
            return
        except TargetUnavailable:
            self._fail_safe(plan_id, token, "target-unavailable-before-dispatch")
            LOG.warning("target unavailable before action intent", extra={"plan_id": plan_id})
            return
        expected = payload.get("current_replicas")
        desired = payload.get("proposed_replicas")
        target_precondition = payload.get("target_precondition")
        if not isinstance(expected, int) or not isinstance(desired, int):
            self._fail_safe(plan_id, token, "replica-precondition-missing")
            return
        if (
            observed["replicas"] != expected
            or not isinstance(target_precondition, dict)
            or any(key not in target_precondition for key in ("uid", "generation", "replicas"))
        ):
            self._fail_safe(plan_id, token, "STALE_TARGET_PRECONDITION")
            return
        if any(
            observed.get(key) != target_precondition[key]
            for key in ("uid", "generation", "replicas")
            if key in target_precondition
        ):
            self._fail_safe(plan_id, token, "STALE_TARGET_PRECONDITION")
            return
        if desired < expected and not compensating:
            health = self._health_observation(action)
            if (
                not self._capacity_ready(observed)
                or observed["ready_replicas"] < expected
                or health is None
                or not health.get("ready")
            ):
                self._fail_safe(plan_id, token, "DESTRUCTIVE_PRECONDITION_HEALTH_OR_READINESS")
                return
            if not health.get("fresh_sample"):
                with self.repository.transaction() as conn:
                    execution = Repository.assert_execution_controller(
                        conn, plan_id, self.controller_id, token
                    )
                age = time.time() - execution["created_at"].timestamp()
                if age <= self.readiness_timeout_seconds:
                    LOG.info(
                        "waiting for a fresh health sample before scale-down",
                        extra={"plan_id": plan_id, "action_id": action["intent_id"]},
                    )
                    return
                self._fail_safe(plan_id, token, "DESTRUCTIVE_PRECONDITION_HEALTH_STALE")
                return
            if not health.get("healthy"):
                self._fail_safe(plan_id, token, "DESTRUCTIVE_PRECONDITION_HEALTH_REGRESSED")
                return
        try:
            with self.repository.transaction() as conn:
                updated = Repository.transition_action(
                    conn,
                    plan_id,
                    action["intent_id"],
                    "NOT_STARTED",
                    "RUNNING",
                    self.controller_id,
                    token,
                    pre_observation=observed,
                    result={"desired_replicas": desired},
                )
        except (ControlError, LeaseConflict, StaleState):
            return
        action = {**action, **updated}
        first_target = expected - 1 if desired < expected else desired
        self._dispatch(plan_id, token, action, observed, first_target, compensating, stop)

    def _reconcile_action(
        self,
        plan_id: str,
        token: int,
        action: dict[str, Any],
        compensating: bool,
        *,
        allow_dispatch: bool = True,
        stop: threading.Event | None = None,
    ) -> None:
        payload = self._payload(action)
        workload = action["workload_id"]
        result = action.get("execution_result") or {}
        if not isinstance(result, dict):
            self._fail_safe(plan_id, token, "action-journal-invalid")
            return
        pre = result.get("pre_observation", {})
        if not isinstance(pre, dict):
            self._fail_safe(plan_id, token, "action-precondition-missing")
            return
        progress = result.get("progress", [])
        dispatch_index = (
            next(
                (
                    index
                    for index in range(len(progress) - 1, -1, -1)
                    if isinstance(progress[index], dict)
                    and isinstance(progress[index].get("result"), dict)
                    and "dispatch_expected_replicas" in progress[index]["result"]
                ),
                None,
            )
            if isinstance(progress, list)
            else None
        )
        last_dispatch = progress[dispatch_index]["result"] if dispatch_index is not None else None
        state = action["execution_state"]
        dispatch_response_recorded = False
        dispatch_response_observation: dict[str, Any] = {}
        if state == "RUNNING" and isinstance(last_dispatch, dict):
            dispatch_target = last_dispatch.get("dispatch_target_replicas")
            response_recorded = any(
                isinstance(item, dict)
                and isinstance(item.get("result"), dict)
                and item["result"].get("requested_replicas") == dispatch_target
                for item in progress[dispatch_index + 1 :]
            )
            if response_recorded:
                dispatch_response_recorded = True
                dispatch_response_observation = progress[dispatch_index + 1].get("observation", {})
            if not response_recorded:
                try:
                    with self.repository.transaction() as conn:
                        updated = Repository.transition_action(
                            conn,
                            plan_id,
                            action["intent_id"],
                            "RUNNING",
                            "OUTCOME_UNKNOWN",
                            self.controller_id,
                            token,
                            error="restart-after-durable-dispatch-intent",
                        )
                except (ControlError, LeaseConflict, StaleState):
                    return
                action = {**action, **updated}
                state = "OUTCOME_UNKNOWN"
        try:
            observed = self.target.observe_scale(workload)
        except TargetNotFound:
            self._fail_safe(plan_id, token, "target-deleted-during-action")
            return
        except TargetUnavailable:
            if state == "RUNNING":
                self._mark_unknown(plan_id, token, action, "target-observation-unavailable")
            return
        if observed.get("uid") != pre.get("uid"):
            self._fail_safe(plan_id, token, "target-identity-changed")
            return
        if dispatch_response_recorded and (
            not isinstance(dispatch_response_observation, dict)
            or (
                "generation" in dispatch_response_observation
                and observed.get("generation") != dispatch_response_observation["generation"]
            )
        ):
            self._fail_safe(plan_id, token, "target-generation-changed-during-action")
            return
        desired = payload.get("proposed_replicas")
        original = pre.get("replicas")
        if not isinstance(desired, int) or not isinstance(original, int):
            self._fail_safe(plan_id, token, "replica-precondition-missing")
            return
        if isinstance(last_dispatch, dict):
            dispatch_expected = last_dispatch["dispatch_expected_replicas"]
            dispatch_target = last_dispatch["dispatch_target_replicas"]
        else:
            dispatch_expected = original
            dispatch_target = original - 1 if desired < original else desired
        if not isinstance(dispatch_expected, int) or not isinstance(dispatch_target, int):
            self._fail_safe(plan_id, token, "action-dispatch-journal-invalid")
            return
        current = observed["replicas"]
        if state in {"RUNNING", "OUTCOME_UNKNOWN"} and current not in {
            dispatch_expected,
            dispatch_target,
        }:
            self._fail_safe(plan_id, token, "ambiguous-or-external-target-state")
            return
        if dispatch_response_recorded and current == dispatch_expected:
            self._finish_action(
                plan_id,
                token,
                action,
                "FAILED",
                observed,
                "target-reverted-after-accepted-mutation",
            )
            self._fail_safe(plan_id, token, "target-reverted-after-accepted-mutation")
            return
        if state == "OUTCOME_UNKNOWN":
            if current == dispatch_expected and not allow_dispatch:
                self._finish_action(
                    plan_id, token, action, "FAILED", observed, "control-request-before-apply"
                )
                return
            if (
                current == dispatch_expected
                and action.get("attempt_number", 1) >= self.max_attempts
            ):
                self._finish_action(
                    plan_id, token, action, "FAILED", observed, "bounded-retry-exhausted"
                )
                self._fail_safe(plan_id, token, "ambiguous-action-not-applied")
                return
            if current == dispatch_expected:
                try:
                    with self.repository.transaction() as conn:
                        updated = Repository.transition_action(
                            conn,
                            plan_id,
                            action["intent_id"],
                            "OUTCOME_UNKNOWN",
                            "RUNNING",
                            self.controller_id,
                            token,
                            target_observation=observed,
                            result={"reconciled_by_observation": True},
                            retry_proven_not_applied=True,
                            max_attempts=self.max_attempts,
                        )
                except (ControlError, LeaseConflict, StaleState):
                    return
                action = {**action, **updated}
                state = "RUNNING"
            elif current == dispatch_target:
                try:
                    with self.repository.transaction() as conn:
                        updated = Repository.transition_action(
                            conn,
                            plan_id,
                            action["intent_id"],
                            "OUTCOME_UNKNOWN",
                            "RUNNING",
                            self.controller_id,
                            token,
                            target_observation=observed,
                            result={"reconciled_applied_step": True},
                        )
                except (ControlError, LeaseConflict, StaleState):
                    return
                action = {**action, **updated}
                state = "RUNNING"

        if current == dispatch_expected:
            if allow_dispatch:
                self._dispatch(
                    plan_id, token, action, observed, dispatch_target, compensating, stop
                )
            elif state in {"RUNNING", "OUTCOME_UNKNOWN"}:
                self._finish_action(
                    plan_id, token, action, "FAILED", observed, "control-request-before-apply"
                )
            return
        if not self._capacity_ready(observed):
            started = action.get("execution_started_at")
            if (
                started is not None
                and time.time() - started.timestamp() > self.readiness_timeout_seconds
            ):
                if compensating:
                    self._finish_action(
                        plan_id, token, action, "FAILED", observed, "readiness-timeout"
                    )
                    self._fail_safe(plan_id, token, "compensation-readiness-timeout")
                elif desired < original:
                    self._compensate(plan_id, token, action, observed)
                else:
                    self._fail_safe(plan_id, token, "replacement-readiness-timeout")
            return
        health = self._health_observation(action)
        if health is None or not health.get("ready") or not health.get("fresh_sample", True):
            started = action.get("execution_started_at")
            if (
                started is not None
                and time.time() - started.timestamp() > self.readiness_timeout_seconds
            ):
                if compensating:
                    self._finish_action(
                        plan_id, token, action, "FAILED", observed, "warmup-timeout"
                    )
                    self._fail_safe(plan_id, token, "compensation-warmup-timeout")
                elif desired < original:
                    self._compensate(plan_id, token, action, observed)
                else:
                    self._fail_safe(plan_id, token, "replacement-warmup-timeout")
            return
        if not health.get("healthy"):
            if compensating:
                if observed["replicas"] != desired:
                    if action["execution_state"] == "OUTCOME_UNKNOWN":
                        try:
                            with self.repository.transaction() as conn:
                                updated = Repository.transition_action(
                                    conn,
                                    plan_id,
                                    action["intent_id"],
                                    "OUTCOME_UNKNOWN",
                                    "RUNNING",
                                    self.controller_id,
                                    token,
                                    target_observation=observed,
                                    result={"reconciled_applied_step": True},
                                )
                        except (ControlError, LeaseConflict, StaleState):
                            return
                        action = {**action, **updated}
                    next_target = observed["replicas"] - 1 if desired < original else desired
                    self._dispatch(plan_id, token, action, observed, next_target, True, stop)
                else:
                    started = action.get("execution_started_at")
                    if (
                        started is not None
                        and time.time() - started.timestamp() > self.readiness_timeout_seconds
                    ):
                        self._finish_action(
                            plan_id, token, action, "FAILED", observed, "recovery-health-timeout"
                        )
                        self._fail_safe(plan_id, token, "compensation-health-failed")
            elif desired < original:
                self._compensate(plan_id, token, action, observed)
            else:
                started = action.get("execution_started_at")
                if (
                    started is not None
                    and time.time() - started.timestamp() > self.readiness_timeout_seconds
                ):
                    self._finish_action(
                        plan_id, token, action, "FAILED", observed, "scale-out-health-regression"
                    )
                    self._fail_safe(plan_id, token, "scale-out-health-regression")
            return
        with self.repository.transaction() as conn:
            Repository.record_action_progress(
                conn,
                plan_id,
                action["intent_id"],
                self.controller_id,
                token,
                observed,
                {"health": health},
            )
        if observed["replicas"] == desired:
            self._finish_action(plan_id, token, action, "SUCCEEDED", observed)
            return
        if compensating:
            next_target = observed["replicas"] - 1 if desired < original else desired
            self._dispatch(plan_id, token, action, observed, next_target, True)
            return
        if not allow_dispatch:
            self._finish_action(
                plan_id, token, action, "FAILED", observed, "control-request-after-partial-step"
            )
            return
        if action["execution_state"] == "OUTCOME_UNKNOWN":
            try:
                with self.repository.transaction() as conn:
                    updated = Repository.transition_action(
                        conn,
                        plan_id,
                        action["intent_id"],
                        "OUTCOME_UNKNOWN",
                        "RUNNING",
                        self.controller_id,
                        token,
                        target_observation=observed,
                        result={"reconciled_applied_step": True},
                    )
            except (ControlError, LeaseConflict, StaleState):
                return
            action = {**action, **updated}
        next_target = observed["replicas"] - 1 if desired < original else desired
        self._dispatch(plan_id, token, action, observed, next_target, compensating, stop)

    @staticmethod
    def _capacity_ready(observed: dict[str, Any]) -> bool:
        replicas = observed.get("replicas")
        generation = observed.get("generation", 0)
        observed_generation = observed.get("observed_generation", generation)
        available = observed.get("available_replicas", observed.get("ready_replicas", 0))
        ready = observed.get("ready_replicas", 0)
        return (
            isinstance(replicas, int)
            and isinstance(generation, int)
            and isinstance(observed_generation, int)
            and isinstance(available, int)
            and isinstance(ready, int)
            and observed_generation >= generation
            and ready >= replicas
            and available >= replicas
        )

    def _dispatch(
        self,
        plan_id: str,
        token: int,
        action: dict[str, Any],
        observed: dict[str, Any],
        target_replicas: int,
        compensating: bool,
        stop: threading.Event | None = None,
    ) -> None:
        if stop is not None and stop.is_set():
            return
        workload = action["workload_id"]
        expected = observed["replicas"]
        if target_replicas == expected:
            self._reconcile_action(plan_id, token, action, compensating, stop=stop)
            return
        if target_replicas < expected and expected - target_replicas > 1:
            self._fail_safe(plan_id, token, "destructive-step-exceeds-one-replica")
            return
        try:
            with self.repository.transaction() as conn:
                Repository.authorize_action(
                    conn, plan_id, action["intent_id"], self.controller_id, token
                )
                self._assert_plan_fresh(conn, plan_id, compensation=compensating)
                live = self.target.observe_scale(workload)
                if (
                    live["uid"] != observed["uid"]
                    or live["replicas"] != expected
                    or live["generation"] != observed["generation"]
                ):
                    raise TargetPreconditionFailed("target changed before mutation")
                target_token = int(
                    conn.execute(
                        "SELECT nextval('execution_controller_fence_seq') AS token"
                    ).fetchone()["token"]
                )
                fenced = self.target.fence_scale(workload, target_token)
                if (
                    fenced.get("uid") != live["uid"]
                    or fenced.get("generation") != live["generation"]
                    or fenced.get("replicas") != expected
                ):
                    raise TargetPreconditionFailed("target changed while publishing fence")
                Repository.record_action_progress(
                    conn,
                    plan_id,
                    action["intent_id"],
                    self.controller_id,
                    token,
                    live,
                    {
                        "dispatch_expected_replicas": expected,
                        "dispatch_target_replicas": target_replicas,
                        "controller_fencing_token": token,
                        "target_fencing_token": target_token,
                    },
                )
            with self.repository.transaction() as conn:
                Repository.authorize_action(
                    conn, plan_id, action["intent_id"], self.controller_id, token
                )
                self._assert_plan_fresh(conn, plan_id, compensation=compensating)
                live = self.target.observe_scale(workload)
                if (
                    live["uid"] != observed["uid"]
                    or live["replicas"] != expected
                    or live["generation"] != observed["generation"]
                ):
                    raise TargetPreconditionFailed("target changed before mutation")
                if stop is not None and stop.is_set():
                    return
                LOG.info(
                    "target mutation requested",
                    extra={
                        "plan_id": plan_id,
                        "action_id": action["intent_id"],
                        "action_state": "RUNNING",
                        "attempt_number": action.get("attempt_number"),
                        "controller_id": self.controller_id,
                        "fencing_token": token,
                        "target_fencing_token": target_token,
                        "target_observation": live,
                        "expected_replicas": expected,
                        "desired_replicas": target_replicas,
                    },
                )
                result = self.target.set_replicas(
                    workload,
                    target_replicas,
                    live,
                    expected_replicas=expected,
                    fencing_token=target_token,
                )
                progress = Repository.record_action_progress(
                    conn,
                    plan_id,
                    action["intent_id"],
                    self.controller_id,
                    token,
                    result,
                    {"requested_replicas": target_replicas},
                )
        except OutcomeUnknown:
            self._mark_unknown(plan_id, token, action, "mutation-response-unknown")
            return
        except ActionBlocked as error:
            self._fail_safe(plan_id, token, error.reason)
            return
        except TargetPreconditionFailed:
            try:
                observed_now = self.target.observe_scale(workload)
                self._finish_action(
                    plan_id, token, action, "FAILED", observed_now, "target-precondition-changed"
                )
            except TargetError:
                pass
            self._fail_safe(plan_id, token, "target-precondition-changed")
            return
        except (TargetUnavailable, TargetError):
            self._mark_unknown(plan_id, token, action, "target-request-unavailable")
            LOG.warning("target request outcome needs observation", extra={"plan_id": plan_id})
            return
        action = {**action, "execution_result": progress["execution_result"]}
        self._reconcile_action(plan_id, token, action, compensating, stop=stop)

    def _mark_unknown(self, plan_id: str, token: int, action: dict[str, Any], reason: str) -> None:
        try:
            with self.repository.transaction() as conn:
                Repository.transition_action(
                    conn,
                    plan_id,
                    action["intent_id"],
                    "RUNNING",
                    "OUTCOME_UNKNOWN",
                    self.controller_id,
                    token,
                    error=reason,
                )
        except (ControlError, LeaseConflict, StaleState):
            pass
        LOG.warning(
            "target action outcome requires observation",
            extra={
                "plan_id": plan_id,
                "action_id": action["intent_id"],
                "action_state": "OUTCOME_UNKNOWN",
                "attempt_number": action.get("attempt_number"),
                "reason": reason,
                "controller_id": self.controller_id,
                "fencing_token": token,
            },
        )

    def _health_observation(self, action: dict[str, Any]) -> dict[str, Any] | None:
        payload = self._payload(action)
        health = payload.get("health", {})
        if not valid_execution_health_policy(action["workload_id"], health):
            return None
        try:
            return self.health.observe(
                action["workload_id"],
                p99_limit_ms=health["p99_limit_ms"],
                error_slo_ppm=health["error_slo_ppm"],
                max_queue_growth_milli=health.get("max_queue_growth_milli", 0),
            )
        except TargetUnavailable:
            return None

    def _finish_action(
        self,
        plan_id: str,
        token: int,
        action: dict[str, Any],
        state: str,
        observation: dict[str, Any],
        error: str | None = None,
    ) -> None:
        try:
            with self.repository.transaction() as conn:
                Repository.transition_action(
                    conn,
                    plan_id,
                    action["intent_id"],
                    action["execution_state"],
                    state,
                    self.controller_id,
                    token,
                    target_observation=observation,
                    error=error,
                )
        except (ControlError, LeaseConflict, StaleState):
            return
        if state == "FAILED" and action.get("compensates_intent_id"):
            self._fail_safe(plan_id, token, error or "compensation-failed")
        LOG.info(
            "action outcome observed",
            extra={
                "plan_id": plan_id,
                "action_id": action["intent_id"],
                "action_state": state,
                "attempt_number": action.get("attempt_number"),
                "target_observation": observation,
                "reason": error,
                "controller_id": self.controller_id,
                "fencing_token": token,
            },
        )

    def _compensate(
        self, plan_id: str, token: int, action: dict[str, Any], observation: dict[str, Any]
    ) -> None:
        self._begin_compensation(plan_id, token, action, observation, "EXECUTING")

    def _begin_compensation(
        self,
        plan_id: str,
        token: int,
        action: dict[str, Any],
        observation: dict[str, Any],
        execution_phase: str,
    ) -> None:
        pre = (action.get("execution_result") or {}).get("pre_observation", {})
        payload = self._payload(action)
        original = pre.get("replicas") if isinstance(pre, dict) else None
        if not isinstance(original, int):
            self._fail_safe(plan_id, token, "compensation-precondition-missing")
            return
        try:
            with self.repository.transaction() as conn:
                if action["execution_state"] in {"RUNNING", "OUTCOME_UNKNOWN"}:
                    Repository.transition_action(
                        conn,
                        plan_id,
                        action["intent_id"],
                        action["execution_state"],
                        "FAILED",
                        self.controller_id,
                        token,
                        target_observation=observation,
                        error="post-action-health-regression",
                    )
                Repository.transition_execution_and_plan(
                    conn,
                    plan_id,
                    execution_phase,
                    "COMPENSATING",
                    self.controller_id,
                    token,
                    actor="fleetright-controller",
                    role="controller",
                    reason="post-action-health-regression",
                    request_id=f"reconcile:{plan_id}",
                    outcome={"observed_health": "regressed"},
                )
                Repository.persist_compensation_action(
                    conn,
                    plan_id,
                    action["intent_id"],
                    "SET_REPLICA_COUNT",
                    action["workload_id"],
                    {
                        "current_replicas": observation["replicas"],
                        "proposed_replicas": original,
                        "target_precondition": {
                            key: observation[key]
                            for key in ("uid", "generation", "replicas")
                            if key in observation
                        },
                        "health": payload["health"],
                    },
                    self.controller_id,
                    token,
                )
        except (ControlError, LeaseConflict, StaleState) as error:
            LOG.error(
                "could not persist compensation", extra={"plan_id": plan_id, "reason": str(error)}
            )

    def _fail_safe(self, plan_id: str, token: int, reason: str) -> None:
        with self.repository.transaction() as conn:
            execution = Repository.assert_execution_controller(
                conn, plan_id, self.controller_id, token
            )
            if execution["status"] in {"COMMITTED", "COMPENSATED", "FAILED_SAFE"}:
                return
            unresolved = conn.execute(
                "SELECT intent_id, action_type, workload_id, execution_state "
                "FROM action_intents WHERE plan_id = %s "
                "AND execution_state IN ('RUNNING', 'OUTCOME_UNKNOWN') LIMIT 1",
                (plan_id,),
            ).fetchall()
        for action in unresolved:
            if action["action_type"] != "SET_REPLICA_COUNT" or not action["workload_id"]:
                LOG.error(
                    "execution remains active until the external action outcome is known",
                    extra={"plan_id": plan_id, "action_id": action["intent_id"], "reason": reason},
                )
                return
            try:
                observed = self.target.observe_scale(action["workload_id"])
            except TargetError:
                LOG.error(
                    "execution remains active until target observation is available",
                    extra={"plan_id": plan_id, "action_id": action["intent_id"], "reason": reason},
                )
                return
            with self.repository.transaction() as conn:
                Repository.assert_execution_controller(conn, plan_id, self.controller_id, token)
                current = conn.execute(
                    "SELECT execution_state FROM action_intents WHERE plan_id = %s "
                    "AND intent_id = %s FOR UPDATE",
                    (plan_id, action["intent_id"]),
                ).fetchone()
                if current and current["execution_state"] in {"RUNNING", "OUTCOME_UNKNOWN"}:
                    Repository.transition_action(
                        conn,
                        plan_id,
                        action["intent_id"],
                        current["execution_state"],
                        "FAILED",
                        self.controller_id,
                        token,
                        target_observation=observed,
                        error=reason,
                    )
        with self.repository.transaction() as conn:
            execution = Repository.assert_execution_controller(
                conn, plan_id, self.controller_id, token
            )
            if conn.execute(
                "SELECT 1 FROM action_intents WHERE plan_id = %s "
                "AND execution_state IN ('RUNNING', 'OUTCOME_UNKNOWN') LIMIT 1",
                (plan_id,),
            ).fetchone():
                return
            Repository.transition_execution_and_plan(
                conn,
                plan_id,
                execution["status"],
                "FAILED_SAFE",
                self.controller_id,
                token,
                actor="fleetright-controller",
                role="controller",
                reason=reason,
                request_id=f"reconcile:{plan_id}",
                outcome={"reason": reason},
            )
            Repository.release_plan_leases(conn, plan_id)
        LOG.error("execution stopped safely", extra={"plan_id": plan_id, "reason": reason})

    def _observe_recovery(self, plan_id: str) -> dict[str, Any]:
        with self.repository.transaction() as conn:
            actions = conn.execute(
                "SELECT workload_id, payload FROM action_intents WHERE plan_id = %s "
                "AND compensates_intent_id IS NOT NULL ORDER BY action_sequence",
                (plan_id,),
            ).fetchall()
        observations = []
        for action in actions:
            observed = self.target.observe_scale(action["workload_id"])
            if observed["replicas"] != action["payload"][
                "proposed_replicas"
            ] or not self._capacity_ready(observed):
                raise TargetUnavailable("compensation target is not ready")
            observations.append(observed)
        if not observations or not all(self._health_payload_ok(action) for action in actions):
            raise TargetUnavailable("health recovery is not confirmed")
        with self.repository.transaction() as conn:
            return {
                "observations": observations,
                "recovery_duration_ms": int(
                    conn.execute(
                        "SELECT floor(extract(epoch FROM "
                        "(clock_timestamp() - started_at)) * 1000) AS ms "
                        "FROM (SELECT min(execution_started_at) AS started_at FROM action_intents "
                        "WHERE plan_id = %s AND compensates_intent_id IS NOT NULL) s",
                        (plan_id,),
                    ).fetchone()["ms"]
                    or 0
                ),
            }

    def _health_payload_ok(self, action: dict[str, Any]) -> bool:
        health = action["payload"].get("health", {})
        if not isinstance(health, dict):
            return False
        result = self._health_observation(action)
        return (
            result is not None
            and result.get("ready") is True
            and result.get("healthy") is True
            and result.get("fresh_sample", True) is True
        )

    @staticmethod
    def _assert_plan_fresh(conn: Any, plan_id: str, *, compensation: bool = False) -> None:
        if compensation:
            return
        plan = conn.execute(
            "SELECT binding, expires_at_ms <= floor(extract(epoch FROM clock_timestamp()) * 1000) "
            "AS expired FROM plans WHERE plan_id = %s FOR SHARE",
            (plan_id,),
        ).fetchone()
        state = conn.execute(
            "SELECT binding FROM control_state WHERE singleton FOR SHARE"
        ).fetchone()
        if plan is None or plan["expired"] or state is None:
            raise TargetPreconditionFailed("plan expired or current state is unavailable")
        if plan["binding"].get("source") != state["binding"]:
            raise TargetPreconditionFailed("snapshot, policy or capacity binding changed")
