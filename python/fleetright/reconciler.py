from __future__ import annotations

import logging
import threading
import time
from datetime import datetime
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
# shortcut: counts journaled failures, not wall time; upgrade to a deadline if polls get slower.
NODE_DRAIN_OBSERVATION_ERROR_LIMIT = 60


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

    @staticmethod
    def _retirement_may_have_dispatched_eviction(action: dict[str, Any]) -> bool:
        execution_result = action.get("execution_result") or {}
        history = execution_result.get("progress", []) if isinstance(execution_result, dict) else []
        for item in reversed(history):
            result = item.get("result") if isinstance(item, dict) else None
            stage = result.get("stage") if isinstance(result, dict) else None
            if stage == "RETIREMENT_RECHECKED":
                continue
            return stage in {
                "EVICTION_INTENT",
                "EVICTION_ACCEPTED",
                "EVICTION_IN_PROGRESS",
                "EVICTION_OUTCOME_UNKNOWN",
                "EVICTION_SOURCE_REMOVED",
                "RETIREMENT_WAITING_FINAL_HEALTH",
            }
        return False

    def _compensate_retirement_if_needed(
        self,
        plan_id: str,
        token: int,
        action: dict[str, Any],
        observation: dict[str, Any],
        phase: str = "EXECUTING",
    ) -> bool:
        if (
            observation.get("pending_pod_create") is not None
            or observation.get("pod_create_history_uncertain")
            or observation.get("unverified_pod_creates")
        ):
            return False
        health = self._health_observation(action)
        fresh_bad_sample = (
            health is not None
            and health.get("fresh_sample") is True
            and health.get("healthy") is False
        )
        final_wait_exists, final_wait_started_at = self._retirement_final_wait_started_at(action)
        if action.get("action_type") == "PREPARE_SOURCE_RETIREMENT" and final_wait_exists:
            timed_out = (
                final_wait_started_at is not None
                and time.time() - final_wait_started_at > self.readiness_timeout_seconds
            )
        else:
            started = action.get(
                "execution_finished_at" if phase == "VERIFYING" else "execution_started_at"
            )
            timed_out = (
                started is not None
                and time.time() - started.timestamp() > self.readiness_timeout_seconds
            )
        if fresh_bad_sample or timed_out:
            self._begin_compensation(plan_id, token, action, observation, phase)
            return True
        return False

    @staticmethod
    def _retirement_final_wait_started_at(action: dict[str, Any]) -> tuple[bool, float | None]:
        execution_result = action.get("execution_result") or {}
        progress = (
            execution_result.get("progress", []) if isinstance(execution_result, dict) else []
        )
        for item in progress:
            result = item.get("result") if isinstance(item, dict) else None
            if not (
                isinstance(result, dict)
                and result.get("stage") == "RETIREMENT_WAITING_FINAL_HEALTH"
                and result.get("source_removed_observed") is True
            ):
                continue
            recorded_at = item.get("recorded_at")
            if isinstance(recorded_at, datetime):
                return True, recorded_at.timestamp()
            if isinstance(recorded_at, str):
                try:
                    return True, datetime.fromisoformat(
                        recorded_at.replace("Z", "+00:00")
                    ).timestamp()
                except ValueError:
                    return True, None
            return True, None
        return False, None

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
                unresolved = [
                    action
                    for action in Repository.incomplete_actions(conn, plan_id)
                    if action["execution_state"] in {"RUNNING", "OUTCOME_UNKNOWN"}
                ]
                if any(
                    action["action_type"]
                    not in {
                        "SET_REPLICA_COUNT",
                        "PREPARE_SOURCE_RETIREMENT",
                        "CORDON_NODE",
                        "VERIFY_NODE_DRAIN",
                    }
                    for action in unresolved
                ):
                    return None
                if (execution.get("pause_requested") or execution.get("cancel_requested")) and any(
                    action["action_type"] == "PREPARE_SOURCE_RETIREMENT" for action in unresolved
                ):
                    Repository.expire_execution_controller(
                        conn, plan_id, self.controller_id, execution["controller_fencing_token"]
                    )
                    return None
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
            completed_retirements = Repository.completed_retirement_actions(conn, plan_id)
        if completed_retirements:
            if not getattr(self.target, "source_retirement_preparation_enabled", False):
                return None
            for action in completed_retirements:
                if not self._fence_completed_retirement(action, plan_id, pending_token):
                    return None
        for action in unresolved:
            if action["action_type"] == "VERIFY_NODE_DRAIN":
                if not getattr(self.target, "supports_workload_execution", False):
                    return None
                payload = action.get("payload")
                cordon_node = getattr(self.target, "cordon_source_node", None)
                if not isinstance(payload, dict) or not callable(cordon_node):
                    return None
                node_payload = {
                    **payload,
                    "operation_id": payload.get("cordon_operation_id", payload.get("operation_id")),
                }
                target_ref = f"node:{payload.get('source_node_id')}:drain"
                try:
                    with self.repository.transaction() as conn:
                        Repository.record_execution_fence(
                            conn,
                            plan_id,
                            action["intent_id"],
                            self.controller_id,
                            pending_token,
                            target_ref,
                            {
                                "state": "PENDING",
                                "token": pending_token,
                                "precondition": {"uid": payload.get("source_node_uid")},
                            },
                        )
                    cordon_observation = cordon_node(plan_id, node_payload, pending_token)
                    if not self._node_cordon_matches(
                        cordon_observation, plan_id, node_payload, pending_token
                    ):
                        return None
                    with self.repository.transaction() as conn:
                        Repository.record_execution_fence(
                            conn,
                            plan_id,
                            action["intent_id"],
                            self.controller_id,
                            pending_token,
                            target_ref,
                            {
                                "state": "COMPLETE",
                                "token": pending_token,
                                "precondition": {"uid": payload.get("source_node_uid")},
                                "observation": cordon_observation,
                            },
                        )
                except ActionBlocked as error:
                    LOG.error(
                        "node drain takeover blocked",
                        extra={"plan_id": plan_id, "reason": error.reason},
                    )
                    return None
                except TargetError:
                    return None
                continue
            if action["action_type"] == "CORDON_NODE":
                if not getattr(self.target, "supports_workload_execution", False):
                    return None
                payload = action.get("payload")
                if not isinstance(payload, dict) or not callable(
                    getattr(self.target, "cordon_source_node", None)
                ):
                    return None
                with self.repository.transaction() as conn:
                    saved_fence = Repository.execution_fence_receipt(
                        conn, plan_id, action["intent_id"], pending_token
                    )
                if saved_fence is not None and saved_fence.get("state") == "COMPLETE":
                    if not self._node_cordon_matches(
                        saved_fence.get("observation"),
                        plan_id,
                        payload,
                        pending_token,
                    ):
                        return None
                    continue
                with self.repository.transaction() as conn:
                    Repository.record_execution_fence(
                        conn,
                        plan_id,
                        action["intent_id"],
                        self.controller_id,
                        pending_token,
                        f"node:{payload.get('source_node_id')}",
                        {
                            "state": "PENDING",
                            "precondition": {"uid": payload.get("source_node_uid")},
                        },
                    )
                try:
                    result = self.target.cordon_source_node(plan_id, payload, pending_token)
                except TargetError:
                    return None
                if not self._node_cordon_matches(result, plan_id, payload, pending_token):
                    return None
                with self.repository.transaction() as conn:
                    Repository.record_execution_fence(
                        conn,
                        plan_id,
                        action["intent_id"],
                        self.controller_id,
                        pending_token,
                        f"node:{payload.get('source_node_id')}",
                        {
                            "state": "COMPLETE",
                            "token": pending_token,
                            "precondition": {
                                "uid": payload.get("source_node_uid"),
                            },
                            "observation": result,
                        },
                    )
                continue
            if action["action_type"] == "PREPARE_SOURCE_RETIREMENT":
                if not getattr(
                    self.target, "source_retirement_preparation_enabled", False
                ) or not getattr(self.target, "supports_workload_execution", False):
                    return None
            elif action["action_type"] != "SET_REPLICA_COUNT":
                return None
            if not action["workload_id"]:
                return None
            prefix = (
                "fleetrightworkload"
                if getattr(self.target, "supports_workload_execution", False)
                else "deployment"
            )
            target_ref = f"{prefix}:{action['workload_id']}"
            with self.repository.transaction() as conn:
                saved_fence = Repository.execution_fence_receipt(
                    conn, plan_id, action["intent_id"], pending_token
                )
            if saved_fence is not None and saved_fence.get("state", "COMPLETE") == "COMPLETE":
                continue
            precondition = saved_fence.get("precondition") if saved_fence else None
            if not isinstance(precondition, dict):
                try:
                    live = self.target.observe_scale(action["workload_id"])
                except TargetNotFound:
                    if getattr(self.target, "supports_workload_execution", False):
                        return None
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
                precondition = {key: live[key] for key in ("uid", "generation", "replicas")}
                with self.repository.transaction() as conn:
                    Repository.record_execution_fence(
                        conn,
                        plan_id,
                        action["intent_id"],
                        self.controller_id,
                        pending_token,
                        target_ref,
                        {"state": "PENDING", "precondition": precondition},
                    )
            recovered_fence = False
            try:
                if getattr(self.target, "supports_workload_execution", False):
                    target = self._fence_workload_target(
                        action, plan_id, pending_token, precondition
                    )
                else:
                    target = self._authorize_target(action, plan_id, pending_token, precondition)
                receipt = target
            except TargetPreconditionFailed:
                try:
                    live = self.target.observe_scale(action["workload_id"])
                except TargetError:
                    return None
                if live.get("uid") != precondition.get("uid"):
                    return None
                live_token = live.get("fencing_token", pending_token)
                recovered_fence = live_token == pending_token
                if live_token > pending_token:
                    return None
                if recovered_fence:
                    if getattr(self.target, "supports_workload_execution", False):
                        try:
                            receipt = self._fence_workload_target(
                                action, plan_id, pending_token, precondition
                            )
                        except TargetError:
                            return None
                    else:
                        receipt = {**live, "fence_precondition": precondition}
                else:
                    precondition = {key: live[key] for key in ("uid", "generation", "replicas")}
                    with self.repository.transaction() as conn:
                        Repository.record_execution_fence(
                            conn,
                            plan_id,
                            action["intent_id"],
                            self.controller_id,
                            pending_token,
                            target_ref,
                            {"state": "PENDING", "precondition": precondition},
                        )
                    try:
                        if getattr(self.target, "supports_workload_execution", False):
                            receipt = self._fence_workload_target(
                                action, plan_id, pending_token, precondition
                            )
                        else:
                            receipt = self._authorize_target(
                                action, plan_id, pending_token, precondition
                            )
                    except TargetError:
                        return None
            except TargetNotFound:
                if getattr(self.target, "supports_workload_execution", False):
                    return None
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
            fence_precondition = receipt.get("fence_precondition", {})
            if getattr(self.target, "supports_workload_execution", False):
                if not self._takeover_fence_matches(
                    action, plan_id, pending_token, receipt, precondition
                ):
                    return None
            elif (
                receipt.get("uid") != precondition.get("uid")
                or (not recovered_fence and receipt.get("replicas") != precondition.get("replicas"))
                or receipt.get("fencing_token") != pending_token
                or any(
                    fence_precondition.get(key) != precondition.get(key)
                    for key in ("uid", "generation", "replicas")
                )
            ):
                return None
            durable_receipt = {
                "state": "COMPLETE",
                "token": pending_token,
                "precondition": precondition,
                "fence_precondition": fence_precondition,
                "generation": receipt.get("generation"),
                "prior_execution_fence": receipt.get("prior_execution_fence"),
                "fence_replayed": receipt.get("fence_replayed", False),
                "operation_applied_before_fence": receipt.get(
                    "operation_applied_before_fence", False
                ),
                "observation": receipt,
            }
            with self.repository.transaction() as conn:
                Repository.record_execution_fence(
                    conn,
                    plan_id,
                    action["intent_id"],
                    self.controller_id,
                    pending_token,
                    target_ref,
                    durable_receipt,
                )
        with self.repository.transaction() as conn:
            token = Repository.claim_execution_controller(
                conn,
                plan_id,
                self.controller_id,
                pending_token,
                self.lease_ttl_ms,
            )
        return token

    @staticmethod
    def _node_cordon_matches(
        observation: object, plan_id: str, payload: dict[str, Any], token: int
    ) -> bool:
        if not isinstance(observation, dict):
            return False
        return (
            observation.get("source_node_id") == payload.get("source_node_id")
            and observation.get("source_node_uid") == payload.get("source_node_uid")
            and observation.get("plan_id") == plan_id
            and observation.get("operation_id") == payload.get("operation_id")
            and observation.get("fencing_token") == token
            and observation.get("unschedulable") is True
        )

    @staticmethod
    def _node_drain_matches(observation: object, plan_id: str, payload: dict[str, Any]) -> bool:
        return (
            ExecutionController._node_drain_observation_matches(observation, plan_id, payload)
            and observation.get("stage") == "NODE_DRAIN_COMPLETE"
        )

    @staticmethod
    def _node_drain_observation_matches(
        observation: object, plan_id: str, payload: dict[str, Any]
    ) -> bool:
        if not isinstance(observation, dict):
            return False
        remaining = observation.get("remaining_source_pod_uids")
        return (
            observation.get("stage") in {"NODE_DRAIN_WAITING", "NODE_DRAIN_COMPLETE"}
            and observation.get("source_node_id") == payload.get("source_node_id")
            and observation.get("source_node_uid") == payload.get("source_node_uid")
            and observation.get("plan_id") == plan_id
            and observation.get("operation_id") == payload.get("cordon_operation_id")
            and observation.get("unschedulable") is True
            and isinstance(remaining, list)
            and all(isinstance(uid, str) and uid for uid in remaining)
            and (
                bool(remaining)
                if observation.get("stage") == "NODE_DRAIN_WAITING"
                else not remaining
            )
            and isinstance(observation.get("exempt_pods"), list)
            and type(observation.get("inventory_pod_count")) is int
        )

    def _fence_completed_retirement(
        self, action: dict[str, Any], plan_id: str, pending_token: int
    ) -> bool:
        """Transfer completed retirement observation to the new target fence before verify."""
        if not getattr(self.target, "supports_workload_execution", False):
            return False
        journal = action.get("execution_result")
        payload = action.get("payload")
        final_result = journal.get("result") if isinstance(journal, dict) else None
        original = final_result.get("observation") if isinstance(final_result, dict) else None
        old_token = original.get("fencing_token") if isinstance(original, dict) else None
        if (
            not isinstance(payload, dict)
            or not isinstance(final_result, dict)
            or final_result.get("stage") != "RETIREMENT_COMPLETE"
            or type(old_token) is not int
            or old_token <= 0
            or not action.get("workload_id")
        ):
            return False

        try:
            with self.repository.transaction() as conn:
                receipt = Repository.execution_fence_receipt(
                    conn, plan_id, action["intent_id"], pending_token
                )
            if receipt is None:
                live = self.target.observe_scale(action["workload_id"])
                old_token = live.get("fencing_token")
                if (
                    live.get("uid") != payload.get("source_owner_uid")
                    or type(old_token) is not int
                    or old_token <= 0
                ):
                    return False
                observed = self.target.observe_source_retirement(
                    action["workload_id"],
                    plan_id,
                    payload,
                    final_result,
                    fencing_token=old_token,
                )
                if observed.get("stage") != "RETIREMENT_COMPLETE":
                    return False
                current = observed.get("observation")
                if not isinstance(current, dict):
                    return False
                precondition = {key: current[key] for key in ("uid", "generation", "replicas")}
                prefix = "fleetrightworkload"
                with self.repository.transaction() as conn:
                    Repository.record_execution_fence(
                        conn,
                        plan_id,
                        action["intent_id"],
                        self.controller_id,
                        pending_token,
                        f"{prefix}:{action['workload_id']}",
                        {"state": "PENDING", "precondition": precondition},
                    )
                receipt = {"state": "PENDING", "precondition": precondition}

            if receipt.get("state") != "COMPLETE":
                precondition = receipt.get("precondition")
                if not isinstance(precondition, dict):
                    return False
                target = self._fence_workload_target(action, plan_id, pending_token, precondition)
                candidate = {
                    "state": "COMPLETE",
                    "token": pending_token,
                    "precondition": precondition,
                    "fence_precondition": target.get("fence_precondition"),
                    "generation": target.get("generation"),
                    "prior_execution_fence": target.get("prior_execution_fence"),
                    "fence_replayed": target.get("fence_replayed", False),
                    "operation_applied_before_fence": target.get(
                        "operation_applied_before_fence", False
                    ),
                    "observation": target,
                }
                if not self._takeover_fence_matches(action, plan_id, pending_token, candidate):
                    return False
                with self.repository.transaction() as conn:
                    Repository.record_execution_fence(
                        conn,
                        plan_id,
                        action["intent_id"],
                        self.controller_id,
                        pending_token,
                        f"fleetrightworkload:{action['workload_id']}",
                        candidate,
                    )

            adopted = self.target.observe_source_retirement(
                action["workload_id"],
                plan_id,
                payload,
                final_result,
                fencing_token=pending_token,
            )
            return adopted.get("stage") in {
                "RETIREMENT_COMPLETE",
                "RETIREMENT_WAITING_FINAL_HEALTH",
            } and (
                adopted.get("stage") == "RETIREMENT_COMPLETE"
                or adopted.get("source_removed_observed") is True
            )
        except (TargetError, KeyError, TypeError, ValueError):
            return False

    def _fence_workload_target(
        self,
        action: dict[str, Any],
        plan_id: str,
        fencing_token: int,
        expected: dict[str, Any],
    ) -> dict[str, Any]:
        payload = action.get("payload", {})
        source_retirement = action.get("action_type") == "PREPARE_SOURCE_RETIREMENT"
        desired = (
            payload.get("surge_desired_replicas" if source_retirement else "proposed_replicas")
            if isinstance(payload, dict)
            else None
        )
        if not isinstance(desired, int):
            raise TargetPreconditionFailed("workload execution target is missing replica intent")
        return self.target.fence_scale(
            action["workload_id"],
            fencing_token,
            expected=expected,
            plan_id=plan_id,
            operation_id=action["intent_id"],
            target_replicas=desired,
            **(
                {
                    "destination_nodes": payload["destination_nodes"],
                    "purpose": "staged-replacement-v1",
                    "minimum_ready_replicas": payload["profile_required_replicas"],
                    "source_replica_id": payload["source_logical_slot_id"],
                    "source_pod_uid": payload["source_pod_uid"],
                }
                if source_retirement
                else {}
            ),
        )

    def _authorize_target(
        self,
        action: dict[str, Any],
        plan_id: str,
        fencing_token: int,
        expected: dict[str, Any],
    ) -> dict[str, Any]:
        if getattr(self.target, "supports_workload_execution", False):
            payload = action.get("payload", {})
            desired = payload.get("proposed_replicas") if isinstance(payload, dict) else None
            if not isinstance(desired, int):
                raise TargetPreconditionFailed(
                    "workload execution target is missing replica intent"
                )
            return self.target.authorize_scale(
                action["workload_id"],
                fencing_token,
                expected=expected,
                plan_id=plan_id,
                operation_id=action["intent_id"],
                target_replicas=desired,
                **(
                    {"destination_nodes": payload["destination_nodes"]}
                    if isinstance(payload, dict) and "destination_nodes" in payload
                    else {}
                ),
            )
        return self.target.fence_scale(action["workload_id"], fencing_token, expected=expected)

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
                if unresolved[0]["action_type"] == "PREPARE_SOURCE_RETIREMENT":
                    self._settle_source_retirement_control_request(plan_id, token, unresolved[0])
                    return
                if unresolved[0]["action_type"] in {"CORDON_NODE", "VERIFY_NODE_DRAIN"}:
                    self._start_node_action(plan_id, token, unresolved[0])
                    return
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
                except ActionBlocked as error:
                    self._fail_safe(plan_id, token, error.reason)
                    return
                except TargetPreconditionFailed as error:
                    self._fail_safe(plan_id, token, str(error))
                    return
                except TargetError:
                    return
                self._transition(plan_id, token, "COMPENSATING", "COMPENSATED", recovery)
            return

        action = candidates[0]
        state = action["execution_state"]
        if action["action_type"] in {"CORDON_NODE", "VERIFY_NODE_DRAIN"}:
            if state in {"NOT_STARTED", "RUNNING", "OUTCOME_UNKNOWN"}:
                self._start_node_action(plan_id, token, action)
            else:
                self._fail_safe(plan_id, token, "node-action-state-invalid")
            return
        if action["action_type"] not in {"SET_REPLICA_COUNT", "PREPARE_SOURCE_RETIREMENT"}:
            self._fail_safe(plan_id, token, "unsupported-action")
            return
        if state == "NOT_STARTED":
            self._start_action(plan_id, token, action, compensating, stop)
        elif state in {"RUNNING", "OUTCOME_UNKNOWN"}:
            if action["action_type"] == "PREPARE_SOURCE_RETIREMENT":
                self._start_source_retirement(plan_id, token, action, self._payload(action), stop)
                return
            if stop is not None and stop.is_set():
                return
            self._reconcile_action(plan_id, token, action, compensating, stop=stop)

    def _verify_and_commit(self, plan_id: str, token: int) -> None:
        with self.repository.transaction() as conn:
            actions = conn.execute(
                "SELECT intent_id, action_type, workload_id, payload, execution_state, "
                "execution_started_at, "
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
            result = action["execution_result"] or {}
            previous = result.get("pre_observation", {})
            completed = result.get("target_observation", {})
            if not isinstance(previous, dict):
                self._fail_safe(plan_id, token, "verification-journal-invalid")
                return
            if action["action_type"] == "CORDON_NODE":
                final_result = result.get("result") if isinstance(result, dict) else None
                cordon_observation = completed
                cordon_token = (
                    final_result.get("fencing_token") if isinstance(final_result, dict) else None
                )
                if (
                    not isinstance(payload, dict)
                    or not isinstance(final_result, dict)
                    or final_result.get("stage") != "NODE_CORDONED"
                    or type(cordon_token) is not int
                    or not self._node_cordon_matches(
                        cordon_observation, plan_id, payload, cordon_token
                    )
                ):
                    self._fail_safe(plan_id, token, "node-cordon-verification-journal-invalid")
                    return
                evidence.append(
                    {"intent_id": action["intent_id"], "node_cordon": cordon_observation}
                )
                continue
            if action["action_type"] == "VERIFY_NODE_DRAIN":
                final_result = result.get("result") if isinstance(result, dict) else None
                if (
                    not isinstance(payload, dict)
                    or not isinstance(final_result, dict)
                    or final_result.get("stage") != "NODE_DRAIN_COMPLETE"
                    or not self._node_drain_matches(completed, plan_id, payload)
                ):
                    self._fail_safe(plan_id, token, "node-drain-verification-journal-invalid")
                    return
                cordon_payload = {
                    **payload,
                    "operation_id": payload.get("cordon_operation_id", payload.get("operation_id")),
                }
                try:
                    # Idempotent: re-adopts the Node fence if a takeover advanced the token.
                    cordon = self.target.cordon_source_node(plan_id, cordon_payload, token)
                    if not self._node_cordon_matches(cordon, plan_id, cordon_payload, token):
                        return
                    observed_drain = self.target.observe_source_node_drain(plan_id, payload, token)
                except ActionBlocked as error:
                    self._fail_safe(plan_id, token, error.reason)
                    return
                except TargetError:
                    return
                if not self._node_drain_matches(observed_drain, plan_id, payload):
                    return
                evidence.append({"intent_id": action["intent_id"], "node_drain": observed_drain})
                continue
            if action["action_type"] == "PREPARE_SOURCE_RETIREMENT":
                final_result = result.get("result") if isinstance(result, dict) else None
                if (
                    not isinstance(payload, dict)
                    or not isinstance(final_result, dict)
                    or final_result.get("stage") != "RETIREMENT_COMPLETE"
                ):
                    self._fail_safe(plan_id, token, "retirement-verification-journal-invalid")
                    return
                try:
                    observed_retirement = self.target.observe_source_retirement(
                        action["workload_id"],
                        plan_id,
                        payload,
                        final_result,
                        fencing_token=token,
                    )
                except TargetNotFound:
                    self._fail_safe(plan_id, token, "target-deleted-during-verification")
                    return
                except TargetError:
                    return
                if observed_retirement.get("stage") != "RETIREMENT_COMPLETE":
                    observation = observed_retirement.get("observation")
                    if (
                        observed_retirement.get("stage") == "RETIREMENT_WAITING_FINAL_HEALTH"
                        and observed_retirement.get("source_removed_observed") is True
                        and isinstance(observation, dict)
                    ):
                        self._compensate_retirement_if_needed(
                            plan_id, token, action, observation, phase="VERIFYING"
                        )
                    return
                observed = observed_retirement.get("observation")
                completion_observation = final_result.get("observation")
                desired = payload.get("final_desired_replicas")
                if (
                    not isinstance(observed, dict)
                    or not isinstance(completion_observation, dict)
                    or type(desired) is not int
                ):
                    self._fail_safe(plan_id, token, "retirement-verification-observation-invalid")
                    return
                generation_matches = observed.get("generation") == completion_observation.get(
                    "generation"
                )
                if completion_observation.get("fencing_token") != token:
                    receipt = self._takeover_fence_receipt(plan_id, action["intent_id"], token)
                    fenced_observation = receipt.get("observation") if receipt else None
                    generation_matches = (
                        receipt is not None
                        and receipt.get("state") == "COMPLETE"
                        and self._takeover_fence_matches(action, plan_id, token, receipt)
                        and isinstance(fenced_observation, dict)
                        and receipt.get("generation") == observed.get("generation")
                        and observed.get("fencing_token") == token
                        and fenced_observation.get("execution_fence")
                        == observed.get("execution_fence")
                    )
                if (
                    observed.get("uid") != previous.get("uid")
                    or observed.get("replicas") != desired
                    or not generation_matches
                ):
                    self._fail_safe(plan_id, token, "retirement-target-changed-during-verification")
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
                    started = action.get("execution_finished_at")
                    timed_out = (
                        started is not None
                        and time.time() - started.timestamp() > self.readiness_timeout_seconds
                    )
                    if fresh_bad_sample or timed_out:
                        self._begin_compensation(plan_id, token, action, observed, "VERIFYING")
                    return
                evidence.append(
                    {
                        "intent_id": action["intent_id"],
                        "target": observed,
                        "health": health,
                        "retirement": observed_retirement,
                    }
                )
                continue
            desired = payload.get("proposed_replicas") if isinstance(payload, dict) else None
            if not isinstance(desired, int):
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
        if new in {"COMMITTED", "CANCELLED", "COMPENSATED", "FAILED_SAFE"} and not (
            self._workload_creates_resolved(plan_id)
        ):
            LOG.error(
                "execution remains active until workload CREATE outcomes are known",
                extra={"plan_id": plan_id, "state": new},
            )
            return
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

    def _workload_creates_resolved(self, plan_id: str) -> bool:
        if not getattr(self.target, "supports_workload_execution", False):
            return True
        with self.repository.transaction() as conn:
            actions = conn.execute(
                "SELECT workload_id, payload, execution_result FROM action_intents "
                "WHERE plan_id = %s AND workload_id IS NOT NULL",
                (plan_id,),
            ).fetchall()
        expected_uids: dict[str, set[str]] = {}
        for action in actions:
            payload = action["payload"] if isinstance(action["payload"], dict) else {}
            precondition = payload.get("target_precondition")
            journal = action["execution_result"]
            pre_observation = journal.get("pre_observation") if isinstance(journal, dict) else None
            uid = payload.get("source_owner_uid")
            if not isinstance(uid, str) and isinstance(precondition, dict):
                uid = precondition.get("uid")
            if not isinstance(uid, str) and isinstance(pre_observation, dict):
                uid = pre_observation.get("uid")
            if not isinstance(uid, str) or not uid:
                return False
            expected_uids.setdefault(action["workload_id"], set()).add(uid)
        for workload_id, uids in expected_uids.items():
            if len(uids) != 1:
                return False
            try:
                observed = self.target.observe_scale(workload_id)
            except TargetError:
                return False
            if (
                observed.get("uid") not in uids
                or "pending_pod_create" not in observed
                or observed.get("pending_pod_create") is not None
                or observed.get("pod_create_history_uncertain") is not False
                or type(observed.get("unverified_pod_creates")) is not int
                or observed.get("unverified_pod_creates") != 0
            ):
                return False
        return True

    @staticmethod
    def _payload(action: dict[str, Any]) -> dict[str, Any]:
        payload = action.get("payload")
        if not isinstance(payload, dict):
            raise TargetPreconditionFailed("action payload is invalid")
        return payload

    def _start_node_action(self, plan_id: str, token: int, action: dict[str, Any]) -> None:
        payload = self._payload(action)
        action_type = action["action_type"]
        if action_type not in {"CORDON_NODE", "VERIFY_NODE_DRAIN"}:
            self._fail_safe(plan_id, token, "unsupported-node-action")
            return
        method = (
            getattr(self.target, "cordon_source_node", None)
            if action_type == "CORDON_NODE"
            else getattr(self.target, "observe_source_node_drain", None)
        )
        if not getattr(self.target, "supports_workload_execution", False) or not callable(method):
            self._fail_safe(plan_id, token, "node-action-target-unsupported")
            return
        state = action["execution_state"]
        if state == "NOT_STARTED":
            stage = (
                "NODE_CORDON_INTENT" if action_type == "CORDON_NODE" else "NODE_DRAIN_VERIFY_INTENT"
            )
            pre_observation = {
                "source_node_id": payload.get("source_node_id"),
                "source_node_uid": payload.get("source_node_uid"),
            }
            if action_type == "CORDON_NODE":
                try:
                    before = self.target.observe_source_node_state(plan_id, payload)
                except ActionBlocked as error:
                    self._finish_action(plan_id, token, action, "FAILED", {}, error.reason)
                    self._fail_safe(plan_id, token, error.reason)
                    return
                except TargetError:
                    return
                pre_observation["unschedulable_before"] = before["unschedulable"]
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
                        pre_observation=pre_observation,
                        result={"stage": stage},
                    )
                action = {**action, **updated}
            except (ControlError, LeaseConflict, StaleState):
                return
        try:
            if action_type == "CORDON_NODE":
                result = method(plan_id, payload, token)
                if not self._node_cordon_matches(result, plan_id, payload, token):
                    raise ActionBlocked("DRAIN_NODE_CORDON_POSTCONDITION_FAILED")
                self._finish_action(
                    plan_id,
                    token,
                    action,
                    "SUCCEEDED",
                    result,
                    result={
                        "stage": "NODE_CORDONED",
                        "source_node_uid": payload["source_node_uid"],
                        "fencing_token": token,
                    },
                )
                return

            cordon_payload = {
                **payload,
                "operation_id": payload.get("cordon_operation_id", payload.get("operation_id")),
            }
            cordon_observation = self.target.cordon_source_node(plan_id, cordon_payload, token)
            if not self._node_cordon_matches(cordon_observation, plan_id, cordon_payload, token):
                raise ActionBlocked("DRAIN_NODE_CORDON_POSTCONDITION_FAILED")
            observation = method(plan_id, payload, token)
            if observation.get("stage") == "NODE_DRAIN_WAITING":
                if state == "RUNNING":
                    with self.repository.transaction() as conn:
                        Repository.record_action_progress(
                            conn,
                            plan_id,
                            action["intent_id"],
                            self.controller_id,
                            token,
                            observation=observation,
                            result={
                                "stage": "NODE_DRAIN_WAITING",
                                "remaining_source_pod_uids": observation.get(
                                    "remaining_source_pod_uids"
                                ),
                            },
                        )
                return
            if not self._node_drain_matches(observation, plan_id, payload):
                raise ActionBlocked("DRAIN_NODE_FINAL_OBSERVATION_INVALID")
            self._finish_action(
                plan_id,
                token,
                action,
                "SUCCEEDED",
                observation,
                result={
                    "stage": "NODE_DRAIN_COMPLETE",
                    "source_node_uid": payload["source_node_uid"],
                    "exempt_pods": observation["exempt_pods"],
                    "inventory_pod_count": observation["inventory_pod_count"],
                },
            )
        except OutcomeUnknown as error:
            if action_type == "VERIFY_NODE_DRAIN":
                # Observation is read-only and the re-cordon is idempotent and fenced.
                self._record_node_drain_observation_error(plan_id, token, action, error)
            else:
                self._mark_unknown(plan_id, token, action, "node-cordon-response-unknown")
        except ActionBlocked as error:
            self._finish_action(plan_id, token, action, "FAILED", {}, error.reason)
            self._fail_safe(plan_id, token, error.reason)
        except TargetPreconditionFailed as error:
            # A resourceVersion CAS conflict proves the cordon patch did not commit.
            if action_type == "VERIFY_NODE_DRAIN":
                self._record_node_drain_observation_error(plan_id, token, action, error)
        except TargetError as error:
            if action_type == "CORDON_NODE":
                self._mark_unknown(plan_id, token, action, "node-cordon-outcome-unknown")
            else:
                self._record_node_drain_observation_error(plan_id, token, action, error)

    def _record_node_drain_observation_error(
        self, plan_id: str, token: int, action: dict[str, Any], error: Exception
    ) -> None:
        """Journal each failed drain observation; a persistent failure must fail safe."""
        progress = (action.get("execution_result") or {}).get("progress", [])
        failures = sum(
            1
            for item in progress
            if isinstance(item, dict)
            and (item.get("result") or {}).get("stage") == "NODE_DRAIN_OBSERVATION_ERROR"
        )
        if failures >= NODE_DRAIN_OBSERVATION_ERROR_LIMIT:
            self._finish_action(
                plan_id, token, action, "FAILED", {}, "node-drain-observation-failed"
            )
            self._fail_safe(plan_id, token, "node-drain-observation-failed")
            return
        try:
            with self.repository.transaction() as conn:
                Repository.record_action_progress(
                    conn,
                    plan_id,
                    action["intent_id"],
                    self.controller_id,
                    token,
                    observation={"error": str(error)[:300]},
                    result={"stage": "NODE_DRAIN_OBSERVATION_ERROR"},
                )
        except (ControlError, LeaseConflict, StaleState):
            return

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
        if action["action_type"] == "PREPARE_SOURCE_RETIREMENT":
            self._start_source_retirement(plan_id, token, action, payload, stop)
            return
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

    def _start_source_retirement(
        self,
        plan_id: str,
        token: int,
        action: dict[str, Any],
        payload: dict[str, Any],
        stop: threading.Event | None,
    ) -> None:
        workload = action.get("workload_id")
        prepare = getattr(self.target, "prepare_source_retirement", None)
        if (
            not isinstance(workload, str)
            or not callable(prepare)
            or payload.get("operation_id") != action.get("intent_id")
            or not getattr(self.target, "source_retirement_preparation_enabled", False)
        ):
            self._fail_safe(plan_id, token, "SOURCE_RETIREMENT_TARGET_UNAVAILABLE")
            return
        if stop is not None and stop.is_set():
            return
        try:
            observed = self.target.observe_scale(workload)
        except TargetNotFound:
            if getattr(self.target, "supports_workload_execution", False):
                return
            if self._retirement_may_have_dispatched_eviction(action):
                return
            if action["execution_state"] in {"RUNNING", "OUTCOME_UNKNOWN"}:
                self._finish_action(
                    plan_id,
                    token,
                    action,
                    "FAILED",
                    {"not_found": True},
                    "source-workload-not-found",
                    result={"stage": "STALE", "reason": "source-workload-not-found"},
                )
            self._fail_safe(plan_id, token, "source-workload-not-found")
            return
        except TargetError:
            return
        state = action["execution_state"]
        execution_result = action.get("execution_result") or {}
        history = execution_result.get("progress", []) if isinstance(execution_result, dict) else []
        prior_result = next(
            (
                item["result"]
                for item in reversed(history)
                if isinstance(item, dict)
                and isinstance(item.get("result"), dict)
                and item["result"].get("stage")
                not in {"RETIREMENT_REQUESTED", "RETIREMENT_RECHECKED"}
            ),
            None,
        )
        prior_stage = prior_result.get("stage") if isinstance(prior_result, dict) else None
        if state == "NOT_STARTED":
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
                        result={"stage": "RETIREMENT_REQUESTED"},
                    )
            except (ControlError, LeaseConflict, StaleState):
                return
            action = {**action, **updated}
            progress_result = {"stage": "RETIREMENT_REQUESTED"}
        elif state == "OUTCOME_UNKNOWN":
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
                        error="read-only-retirement-recheck",
                    )
            except (ControlError, LeaseConflict, StaleState):
                return
            action = {**action, **updated}
            progress_result = {"stage": "RETIREMENT_RECHECKED"}
        elif state == "RUNNING":
            progress_result = {"stage": "RETIREMENT_RECHECKED"}
        else:
            return
        eviction_attempt = False
        post_source_removal_observation: dict[str, Any] | None = None
        try:
            settled_retirement_stages = {
                "EVICTION_INTENT",
                "EVICTION_ACCEPTED",
                "EVICTION_IN_PROGRESS",
                "EVICTION_OUTCOME_UNKNOWN",
                "EVICTION_SOURCE_REMOVED",
                "RETIREMENT_WAITING_FINAL_HEALTH",
            }
            if not (state == "RUNNING" and prior_stage in settled_retirement_stages):
                with self.repository.transaction() as conn:
                    progress_action = Repository.record_action_progress(
                        conn,
                        plan_id,
                        action["intent_id"],
                        self.controller_id,
                        token,
                        observation=observed,
                        result=progress_result,
                    )
                action = {**action, **progress_action}
            with self.repository.transaction() as conn:
                Repository.authorize_action(
                    conn, plan_id, action["intent_id"], self.controller_id, token
                )
                self._assert_plan_fresh(conn, plan_id)
            current = self.target.observe_scale(workload)
            if current.get("uid") != observed.get("uid") or current.get(
                "generation"
            ) != observed.get("generation"):
                raise TargetPreconditionFailed("source-workload-identity-changed")
            resumable_eviction = prior_stage in {
                "READY_FOR_SAFE_EVICTION",
                "EVICTION_INTENT",
                "EVICTION_ACCEPTED",
                "EVICTION_IN_PROGRESS",
                "EVICTION_OUTCOME_UNKNOWN",
                "EVICTION_SOURCE_REMOVED",
                "RETIREMENT_WAITING_FINAL_HEALTH",
            } or (
                prior_stage in {"PDB_BLOCKED", "PDB_STATUS_PENDING"}
                and prior_result.get("source_retirement_intent_recorded") is True
            )
            if resumable_eviction:
                if not getattr(self.target, "supports_workload_execution", False):
                    raise ActionBlocked("SOURCE_RETIREMENT_TARGET_UNAVAILABLE")
                prepared = dict(prior_result)
                observed_retirement = self.target.observe_source_retirement(
                    workload,
                    plan_id,
                    payload,
                    prepared,
                    fencing_token=token,
                )
                stage = observed_retirement.get("stage")
                if stage == "RETIREMENT_COMPLETE":
                    completed = observed_retirement.get("observation")
                    if not isinstance(completed, dict):
                        raise TargetError("retirement completion has no target observation")
                    self._finish_action(
                        plan_id,
                        token,
                        action,
                        "SUCCEEDED",
                        completed,
                        result=observed_retirement,
                    )
                    return
                if stage == "EVICTION_RETRY_SAFE":
                    health = self._health_observation(action)
                    required_ready = max(
                        current.get("availability_floor", 0),
                        payload.get("profile_required_replicas", 0),
                    )
                    if (
                        not self._capacity_ready(current)
                        or current.get("ready_replicas", 0)
                        < payload.get("surge_desired_replicas", 0)
                        or current.get("available_replicas", 0) < required_ready
                        or health is None
                        or health.get("ready") is not True
                        or health.get("fresh_sample") is not True
                        or health.get("healthy") is not True
                    ):
                        self._compensate_retirement_if_needed(plan_id, token, action, current)
                        return
                    prepared = dict(observed_retirement)
                    prepared["stage"] = "READY_FOR_SAFE_EVICTION"
                    intent = {
                        **prepared,
                        "stage": "EVICTION_INTENT",
                        "fencing_token": token,
                    }

                    def journal_eviction_intent() -> bool:
                        nonlocal eviction_attempt
                        if stop is not None and stop.is_set():
                            return False
                        with self.repository.transaction() as conn:
                            Repository.record_action_progress(
                                conn,
                                plan_id,
                                action["intent_id"],
                                self.controller_id,
                                token,
                                observation=current,
                                result=intent,
                            )
                            Repository.authorize_action(
                                conn, plan_id, action["intent_id"], self.controller_id, token
                            )
                            self._assert_plan_fresh(conn, plan_id)
                        eviction_attempt = True
                        return True

                    result = self.target.evict_source(
                        workload,
                        plan_id,
                        payload,
                        prepared,
                        fencing_token=token,
                        before_dispatch=journal_eviction_intent,
                    )
                    if result.get("stage") == "EVICTION_ACCEPTED":
                        result = {**prepared, **result, "fencing_token": token}
                else:
                    result = observed_retirement
                    if (
                        stage == "RETIREMENT_WAITING_FINAL_HEALTH"
                        and observed_retirement.get("source_removed_observed") is True
                    ):
                        post_source_removal_observation = observed_retirement.get("observation")
                        if not isinstance(post_source_removal_observation, dict):
                            raise TargetError("post-eviction wait has no target observation")
            else:
                result = (
                    prepare(workload, plan_id, payload, fencing_token=token)
                    if getattr(self.target, "supports_workload_execution", False)
                    else prepare(workload, plan_id, payload)
                )
        except (ControlError, LeaseConflict, StaleState):
            return
        except ActionBlocked as error:
            if eviction_attempt or self._retirement_may_have_dispatched_eviction(action):
                self._mark_unknown(plan_id, token, action, error.reason)
                return
            try:
                current = self.target.observe_scale(workload)
            except TargetError:
                self._mark_unknown(plan_id, token, action, error.reason)
                return
            self._finish_action(
                plan_id,
                token,
                action,
                "FAILED",
                current,
                error.reason,
                result={
                    "stage": "BLOCKED",
                    "blocked_at": (
                        "VALIDATE_CREATE_PERSISTENCE_FENCE"
                        if error.reason == "POD_CREATE_PERSISTENCE_FENCE_UNPROVEN"
                        else "VALIDATE_SOURCE_IDENTITY"
                        if error.reason.startswith("SOURCE_")
                        else "VALIDATE_REPLACEMENT"
                        if error.reason.startswith("REPLACEMENT_")
                        else "RECONCILE_OUTSTANDING_CREATES"
                        if error.reason.startswith("POD_CREATE_")
                        or error.reason.startswith("UNSUPPORTED_POD_CREATE_")
                        else "VALIDATE_CREATE_PERSISTENCE_FENCE"
                    ),
                    "reason": error.reason,
                },
            )
            self._fail_safe(plan_id, token, error.reason)
            return
        except TargetPreconditionFailed as error:
            if (
                not eviction_attempt
                and str(error) == "target resource version changed"
                and not self._retirement_may_have_dispatched_eviction(action)
            ):
                LOG.info(
                    "retirement status compare-and-swap changed; retrying after fresh observation",
                    extra={"plan_id": plan_id, "action_id": action["intent_id"]},
                )
                return
            if eviction_attempt or self._retirement_may_have_dispatched_eviction(action):
                self._mark_unknown(plan_id, token, action, "eviction-precondition-needs-reconcile")
                return
            try:
                current = self.target.observe_scale(workload)
            except TargetError:
                self._mark_unknown(plan_id, token, action, str(error))
                return
            self._finish_action(plan_id, token, action, "FAILED", current, str(error))
            self._fail_safe(plan_id, token, str(error))
            return
        except TargetError:
            self._mark_unknown(plan_id, token, action, "retirement-target-outcome-unknown")
            return
        if not isinstance(result, dict):
            self._mark_unknown(plan_id, token, action, "retirement-target-outcome-invalid")
            return
        if result.get("stage") in {
            "REPLACEMENT_REQUESTED",
            "REPLACEMENT_CREATE_PENDING",
            "REPLACEMENT_SCHEDULING",
            "REPLACEMENT_WAITING_READY",
            "REPLACEMENT_WARMING",
            "REPLACEMENT_CAPACITY_PENDING",
            "READY_FOR_SAFE_EVICTION",
            "PDB_STATUS_PENDING",
            "PDB_BLOCKED",
            "EVICTION_INTENT",
            "EVICTION_ACCEPTED",
            "EVICTION_IN_PROGRESS",
            "EVICTION_OUTCOME_UNKNOWN",
            "EVICTION_SOURCE_REMOVED",
            "RETIREMENT_WAITING_FINAL_HEALTH",
        }:
            progress = action.get("execution_result") or {}
            history = progress.get("progress", []) if isinstance(progress, dict) else []
            last = history[-1].get("result") if history and isinstance(history[-1], dict) else None
            if last != result:
                current = post_source_removal_observation or self.target.observe_scale(workload)
                with self.repository.transaction() as conn:
                    progress_action = Repository.record_action_progress(
                        conn,
                        plan_id,
                        action["intent_id"],
                        self.controller_id,
                        token,
                        observation=current,
                        result=result,
                    )
                action = {**action, **progress_action}
            if post_source_removal_observation is not None:
                self._compensate_retirement_if_needed(
                    plan_id, token, action, post_source_removal_observation
                )
            return
        self._mark_unknown(plan_id, token, action, "retirement-preparation-result-unqualified")

    def _settle_source_retirement_control_request(
        self, plan_id: str, token: int, action: dict[str, Any]
    ) -> None:
        if not (
            getattr(self.target, "source_retirement_preparation_enabled", False)
            and getattr(self.target, "supports_workload_execution", False)
        ):
            return
        receipt = self._takeover_fence_receipt(plan_id, action["intent_id"], token)
        if receipt is None or not self._takeover_fence_matches(action, plan_id, token, receipt):
            return
        execution_result = action.get("execution_result") or {}
        history = execution_result.get("progress", []) if isinstance(execution_result, dict) else []
        prepared = None
        for item in reversed(history):
            result = item.get("result") if isinstance(item, dict) else None
            if not isinstance(result, dict) or result.get("stage") == "RETIREMENT_RECHECKED":
                continue
            prepared = result
            break
        if not isinstance(prepared, dict):
            return
        stage = prepared.get("stage")
        pre_eviction_stages = {
            "REPLACEMENT_REQUESTED",
            "REPLACEMENT_CREATE_PENDING",
            "REPLACEMENT_SCHEDULING",
            "REPLACEMENT_WAITING_READY",
            "REPLACEMENT_WARMING",
            "REPLACEMENT_CAPACITY_PENDING",
            "PDB_STATUS_PENDING",
            "PDB_BLOCKED",
        }
        if stage == "RETIREMENT_REQUESTED" or (
            isinstance(stage, str)
            and stage in pre_eviction_stages
            and prepared.get("source_retirement_intent_recorded") is not True
        ):
            try:
                observed = self.target.observe_scale(action["workload_id"])
            except TargetError:
                return
            payload = self._payload(action)
            fence_observation = receipt.get("observation", {})
            request = observed.get("execution_request")
            execution = observed.get("execution")
            retired_slots = observed.get("retired_replica_slots")
            source_getter = getattr(self.target, "get_pod", None)
            source_name = payload.get("source_pod_name")
            if not callable(source_getter) or not isinstance(source_name, str):
                return
            try:
                source = source_getter(source_name)
            except TargetError:
                return
            source_metadata = source.get("metadata", {}) if isinstance(source, dict) else {}
            source_spec = source.get("spec", {}) if isinstance(source, dict) else {}
            source_is_current = (
                isinstance(source_metadata, dict)
                and source_metadata.get("uid") == payload.get("source_pod_uid")
                and source_metadata.get("deletionTimestamp") is None
                and isinstance(source_spec, dict)
                and source_spec.get("nodeName") == payload.get("source_node_id")
            )
            creates_resolved = (
                observed.get("pending_pod_create") is None
                and observed.get("pod_create_history_uncertain") is False
                and type(observed.get("unverified_pod_creates")) is int
                and observed.get("unverified_pod_creates") == 0
            )
            previous_request_resolved = request is None or (
                isinstance(request, dict)
                and isinstance(execution, dict)
                and execution.get("phase") == "APPLIED"
                and (request.get("planID"), request.get("operationID"))
                != (plan_id, action["intent_id"])
            )
            same_operation_applied = (
                isinstance(request, dict)
                and isinstance(execution, dict)
                and (request.get("planID"), request.get("operationID"))
                == (plan_id, action["intent_id"])
                and request.get("targetReplicas") == payload.get("surge_desired_replicas")
                and request.get("destinationNodes") == payload.get("destination_nodes")
                and request.get("purpose") == "staged-replacement-v1"
                and execution.get("phase") == "APPLIED"
            )
            pre_dispatch_request_resolved = (
                previous_request_resolved
                and observed.get("replicas") == payload.get("baseline_replicas")
            ) or (
                same_operation_applied
                and observed.get("replicas") == payload.get("surge_desired_replicas")
            )
            prepared_request_resolved = same_operation_applied and observed.get(
                "replicas"
            ) == payload.get("surge_desired_replicas")
            if (
                payload.get("source_owner_uid") != observed.get("uid")
                or observed.get("uid") != fence_observation.get("uid")
                or observed.get("generation") != receipt.get("generation")
                or observed.get("fencing_token") != token
                or observed.get("execution_fence") != fence_observation.get("execution_fence")
                or not source_is_current
                or not isinstance(retired_slots, list)
                or any(
                    isinstance(slot, dict)
                    and (
                        slot.get("replicaID") == payload.get("source_logical_slot_id")
                        or slot.get("podUID") == payload.get("source_pod_uid")
                    )
                    for slot in retired_slots
                )
                or not creates_resolved
                or not (
                    pre_dispatch_request_resolved
                    if stage == "RETIREMENT_REQUESTED"
                    else prepared_request_resolved
                )
            ):
                return
            self._finish_action(
                plan_id,
                token,
                action,
                "FAILED",
                observed,
                "source-retirement-stopped-before-target-dispatch",
                result={
                    "stage": "BLOCKED",
                    "blocked_at": "CONTROL_REQUESTED",
                    "reason": "source-retirement-not-dispatched",
                },
            )
            return
        try:
            retirement = self.target.observe_source_retirement(
                action["workload_id"],
                plan_id,
                self._payload(action),
                prepared,
                fencing_token=token,
            )
        except TargetNotFound:
            return
        except TargetError:
            return
        stage = retirement.get("stage")
        if stage == "RETIREMENT_COMPLETE":
            observed = retirement.get("observation")
            if isinstance(observed, dict):
                self._finish_action(
                    plan_id,
                    token,
                    action,
                    "SUCCEEDED",
                    observed,
                    result=retirement,
                )
            return
        if stage not in {"EVICTION_RETRY_SAFE"}:
            if action["execution_state"] == "RUNNING":
                self._mark_unknown(
                    plan_id, token, action, "eviction-outcome-needs-read-only-reconciliation"
                )
            return
        try:
            observed = self.target.observe_scale(action["workload_id"])
        except TargetError:
            return
        fence_observation = receipt.get("observation", {})
        if (
            observed.get("uid") != fence_observation.get("uid")
            or observed.get("generation") != receipt.get("generation")
            or observed.get("fencing_token") != token
            or observed.get("execution_fence") != fence_observation.get("execution_fence")
        ):
            return
        self._finish_action(
            plan_id,
            token,
            action,
            "FAILED",
            observed,
            "source-retirement-stopped-for-control-request",
            result={
                "stage": "BLOCKED",
                "blocked_at": "CONTROL_REQUESTED",
                "reason": "source-retirement-preparation-stopped",
            },
        )

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
            takeover_receipt = self._takeover_fence_receipt(
                plan_id,
                action["intent_id"],
                token,
                after_target_token=(
                    last_dispatch.get("target_fencing_token")
                    if isinstance(last_dispatch, dict)
                    else None
                ),
            )
            fence = observed.get("execution_fence")
            fence_observation = takeover_receipt.get("observation", {}) if takeover_receipt else {}
            fence_precondition = (
                takeover_receipt.get("fence_precondition", {}) if takeover_receipt else {}
            )
            recovered_after_takeover = (
                getattr(self.target, "supports_workload_execution", False)
                and takeover_receipt is not None
                and takeover_receipt.get("operation_applied_before_fence") is True
                and self._takeover_fence_matches(action, plan_id, token, takeover_receipt)
                and isinstance(fence, dict)
                and fence.get("generation") == takeover_receipt.get("token")
                and observed.get("fencing_token") == takeover_receipt.get("token")
                and observed.get("generation") == takeover_receipt.get("generation")
                and isinstance(dispatch_response_observation, dict)
                and fence_precondition.get("generation", -1)
                >= dispatch_response_observation.get("generation", 0)
                and isinstance(fence_observation, dict)
                and fence_observation.get("execution_fence") == fence
                and fence_observation.get("generation") == observed.get("generation")
            )
            if not recovered_after_takeover:
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
        if state == "OUTCOME_UNKNOWN" and observed.get("generation_tracks_spec") is True:
            dispatch_observation = (
                progress[dispatch_index].get("observation")
                if dispatch_index is not None and isinstance(progress, list)
                else pre
            )
            expected_generation = (
                dispatch_observation.get("generation")
                if isinstance(dispatch_observation, dict)
                else None
            )
            takeover_receipt = self._takeover_fence_receipt(
                plan_id,
                action["intent_id"],
                token,
                after_target_token=(
                    last_dispatch.get("target_fencing_token")
                    if isinstance(last_dispatch, dict)
                    else None
                ),
            )
            if getattr(self.target, "supports_workload_execution", False):
                request = observed.get("execution_request")
                if not isinstance(last_dispatch, dict):
                    self._fail_safe(
                        plan_id, token, "target-generation-changed-during-unknown-action"
                    )
                    return
                fence = observed.get("execution_fence")
                if takeover_receipt is None:
                    dispatch_token = last_dispatch.get("target_fencing_token")
                    if current == dispatch_target:
                        base_generation = pre.get("generation")
                        generation_matches = (
                            isinstance(expected_generation, int)
                            and observed.get("generation") == expected_generation + 1
                            and observed.get("fencing_token") == dispatch_token
                            and isinstance(request, dict)
                            and request.get("planID") == plan_id
                            and request.get("operationID") == action["intent_id"]
                            and request.get("expectedReplicas") == dispatch_expected
                            and request.get("targetReplicas") == dispatch_target
                            and request.get("generation") == dispatch_token
                            and isinstance(fence, dict)
                            and fence.get("planID") == plan_id
                            and fence.get("operationID") == action["intent_id"]
                            and fence.get("generation") == dispatch_token
                            and fence.get("expectedGeneration") == base_generation
                            and fence.get("expectedReplicas") == dispatch_expected
                            and fence.get("fencedGeneration") == observed.get("generation")
                        )
                    else:
                        generation_matches = (
                            current == dispatch_expected
                            and isinstance(expected_generation, int)
                            and observed.get("generation") == expected_generation
                        )
                else:
                    fence_observation = takeover_receipt.get("observation", {})
                    fence_matches = (
                        self._takeover_fence_matches(action, plan_id, token, takeover_receipt)
                        and isinstance(fence, dict)
                        and isinstance(fence_observation, dict)
                        and fence_observation.get("execution_fence") == fence
                        and fence.get("generation") == takeover_receipt.get("token")
                        and observed.get("fencing_token") == takeover_receipt.get("token")
                        and observed.get("generation") == takeover_receipt.get("generation")
                        and fence_observation.get("generation") == observed.get("generation")
                    )
                    generation_matches = fence_matches and (
                        (
                            current == dispatch_target
                            and takeover_receipt.get("operation_applied_before_fence") is True
                        )
                        or (
                            current == dispatch_expected
                            and takeover_receipt.get("operation_applied_before_fence") is False
                        )
                    )
                if not generation_matches:
                    self._fail_safe(
                        plan_id, token, "target-generation-changed-during-unknown-action"
                    )
                    return
            elif isinstance(expected_generation, int) and current in {
                dispatch_expected,
                dispatch_target,
            }:
                if current == dispatch_target:
                    expected_generation += 1
                if takeover_receipt is None:
                    generation_matches = observed.get("generation") == expected_generation
                else:
                    fence_precondition = takeover_receipt.get("fence_precondition", {})
                    fence_observation = takeover_receipt.get("observation", {})
                    generation_matches = (
                        isinstance(fence_precondition, dict)
                        and fence_precondition.get("generation") == expected_generation
                        and takeover_receipt.get("generation") == expected_generation + 1
                        and observed.get("generation")
                        == takeover_receipt.get(
                            "generation",
                            fence_observation.get("generation")
                            if isinstance(fence_observation, dict)
                            else None,
                        )
                    )
                if not generation_matches:
                    self._fail_safe(
                        plan_id, token, "target-generation-changed-during-unknown-action"
                    )
                    return
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
            takeover_receipt = self._takeover_fence_receipt(
                plan_id,
                action["intent_id"],
                token,
                after_target_token=(
                    last_dispatch.get("target_fencing_token")
                    if isinstance(last_dispatch, dict)
                    else None
                ),
            )
            if takeover_receipt is None:
                self._yield_for_target_fence(plan_id, token, action["intent_id"])
                return
            if current == dispatch_expected and not allow_dispatch:
                self._finish_action(
                    plan_id,
                    token,
                    action,
                    "FAILED",
                    observed,
                    "control-request-before-apply",
                    result={"target_fence": takeover_receipt},
                )
                return
            if current == dispatch_target and not allow_dispatch:
                if getattr(
                    self.target, "supports_workload_execution", False
                ) and not self._capacity_ready(observed):
                    return
                applied_final_target = current == desired
                self._finish_action(
                    plan_id,
                    token,
                    action,
                    "SUCCEEDED" if applied_final_target else "FAILED",
                    observed,
                    None if applied_final_target else "control-request-after-partial-step",
                    result={
                        "reconciled_applied_step": True,
                        "target_fence": takeover_receipt,
                    },
                )
                return
            if (
                current == dispatch_expected
                and action.get("attempt_number", 1) >= self.max_attempts
            ):
                self._finish_action(
                    plan_id,
                    token,
                    action,
                    "FAILED",
                    observed,
                    "bounded-retry-exhausted",
                    result={"target_fence": takeover_receipt},
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
                            result={
                                "reconciled_by_observation": True,
                                "target_fence": takeover_receipt,
                            },
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
                    if dispatch_response_recorded and current == desired:
                        self._finish_action(
                            plan_id,
                            token,
                            action,
                            "FAILED",
                            observed,
                            "replacement-readiness-timeout",
                        )
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
                    if dispatch_response_recorded and current == desired:
                        self._finish_action(
                            plan_id,
                            token,
                            action,
                            "FAILED",
                            observed,
                            "replacement-warmup-timeout",
                        )
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
        if observed.get("workload_execution") is True:
            request = observed.get("execution_request")
            execution = observed.get("execution")
            if not (
                observed.get("reconciled") is True
                and observed_generation == generation
                and observed.get("observed_replicas") == replicas
                and isinstance(request, dict)
                and isinstance(execution, dict)
                and request.get("targetReplicas") == replicas
                and execution.get("phase") == "APPLIED"
                and all(
                    execution.get(key) == request.get(key)
                    for key in ("planID", "operationID", "generation", "targetReplicas")
                )
            ):
                return False
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
                workload_target = getattr(self.target, "supports_workload_execution", False)
                if workload_target:
                    observed = live
                else:
                    fenced = self.target.fence_scale(workload, target_token, expected=live)
                    fence_precondition = fenced.get("fence_precondition", {})
                    if (
                        fenced.get("uid") != live["uid"]
                        or fence_precondition.get("uid") != live["uid"]
                        or fence_precondition.get("generation") != live["generation"]
                        or fence_precondition.get("replicas") != expected
                        or fenced.get("replicas") != expected
                    ):
                        raise TargetPreconditionFailed("target changed while publishing fence")
                    live = fenced
                    observed = fenced
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
                if getattr(self.target, "supports_workload_execution", False):
                    live = self._authorize_target(
                        action,
                        plan_id,
                        target_token,
                        live,
                    )
                    result = self.target.set_replicas(
                        workload,
                        target_replicas,
                        live,
                        expected_replicas=expected,
                        fencing_token=target_token,
                        plan_id=plan_id,
                        operation_id=action["intent_id"],
                    )
                else:
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

    def _takeover_fence_receipt(
        self,
        plan_id: str,
        intent_id: str,
        token: int,
        *,
        after_target_token: int | None = None,
    ) -> dict[str, Any] | None:
        with self.repository.transaction() as conn:
            receipt = Repository.execution_fence_receipt(conn, plan_id, intent_id, token)
        if receipt is not None and receipt.get("state", "COMPLETE") != "COMPLETE":
            return None
        if (
            receipt is not None
            and after_target_token is not None
            and receipt.get("token", -1) < after_target_token
        ):
            return None
        return receipt if isinstance(receipt, dict) else None

    @staticmethod
    def _latest_dispatch(action: dict[str, Any]) -> dict[str, Any] | None:
        journal = action.get("execution_result")
        progress = journal.get("progress") if isinstance(journal, dict) else None
        if not isinstance(progress, list):
            return None
        return next(
            (
                item["result"]
                for item in reversed(progress)
                if isinstance(item, dict)
                and isinstance(item.get("result"), dict)
                and "dispatch_target_replicas" in item["result"]
            ),
            None,
        )

    @classmethod
    def _takeover_fence_matches(
        cls,
        action: dict[str, Any],
        plan_id: str,
        token: int,
        receipt: dict[str, Any],
        expected_precondition: dict[str, Any] | None = None,
    ) -> bool:
        journal = action.get("execution_result")
        base = journal.get("pre_observation") if isinstance(journal, dict) else None
        target = receipt.get("observation", receipt)
        fence_precondition = receipt.get("fence_precondition")
        saved_precondition = receipt.get("precondition", expected_precondition)
        fence = target.get("execution_fence") if isinstance(target, dict) else None
        request = target.get("execution_request") if isinstance(target, dict) else None
        prior_fence = receipt.get("prior_execution_fence")
        dispatch = cls._latest_dispatch(action)
        payload = action.get("payload")
        source_retirement = action.get("action_type") == "PREPARE_SOURCE_RETIREMENT"
        if not all(
            isinstance(item, dict)
            for item in (base, fence_precondition, saved_precondition, fence, payload)
        ):
            return False
        base_generation = base.get("generation")
        base_replicas = base.get("replicas")
        desired = payload.get(
            "surge_desired_replicas" if source_retirement else "proposed_replicas"
        )
        operation_applied = receipt.get("operation_applied_before_fence") is True
        if (
            type(base_generation) is not int
            or type(base_replicas) is not int
            or type(desired) is not int
            or fence_precondition.get("uid") != base.get("uid")
            or fence_precondition.get("generation") != saved_precondition.get("generation")
            or fence_precondition.get("replicas") != saved_precondition.get("replicas")
            or fence_precondition.get("uid") != saved_precondition.get("uid")
            or receipt.get("generation") != fence_precondition.get("generation", -2) + 1
            or target.get("uid") != base.get("uid")
            or target.get("generation") != receipt.get("generation")
            or target.get("fencing_token") != token
            or receipt.get("fencing_token", token) != token
            or fence.get("planID") != plan_id
            or fence.get("operationID") != action.get("intent_id")
            or fence.get("generation") != token
            or fence.get("expectedGeneration") != base_generation
            or fence.get("expectedReplicas") != base_replicas
            or fence.get("fencedGeneration") != receipt.get("generation")
        ):
            return False
        if operation_applied:
            if not isinstance(request, dict):
                return False
            if source_retirement:
                if (
                    request.get("planID") != plan_id
                    or request.get("operationID") != action.get("intent_id")
                    or request.get("expectedReplicas") != payload.get("baseline_replicas")
                    or request.get("targetReplicas") != desired
                    or request.get("destinationNodes") != payload.get("destination_nodes")
                    or request.get("purpose") != "staged-replacement-v1"
                    or request.get("generation", token) > token
                    or fence_precondition.get("replicas") != desired
                    or target.get("replicas") != desired
                ):
                    return False
            elif (
                not isinstance(dispatch, dict)
                or request.get("planID") != plan_id
                or request.get("operationID") != action.get("intent_id")
                or request.get("expectedReplicas") != dispatch.get("dispatch_expected_replicas")
                or request.get("targetReplicas") != dispatch.get("dispatch_target_replicas")
                or request.get("generation") != dispatch.get("target_fencing_token")
                or request.get("targetReplicas") != desired
                or fence_precondition.get("replicas") != desired
                or target.get("replicas") != desired
            ):
                return False
        elif (
            fence_precondition.get("replicas") != base_replicas
            or target.get("replicas") != base_replicas
            or (
                isinstance(request, dict)
                and request.get("planID") == plan_id
                and request.get("operationID") == action.get("intent_id")
            )
        ):
            return False

        if receipt.get("fence_replayed") is True:
            return isinstance(saved_precondition, dict)
        prior_same_operation = (
            isinstance(prior_fence, dict)
            and prior_fence.get("planID") == plan_id
            and prior_fence.get("operationID") == action.get("intent_id")
            and prior_fence.get("expectedGeneration") == base_generation
            and prior_fence.get("expectedReplicas") == base_replicas
            and prior_fence.get("generation", token) < token
            and prior_fence.get("fencedGeneration") == fence_precondition.get("generation")
        )
        if prior_same_operation:
            return True
        return fence_precondition.get("generation") == base_generation + int(operation_applied)

    def _yield_for_target_fence(self, plan_id: str, token: int, intent_id: str) -> None:
        try:
            with self.repository.transaction() as conn:
                Repository.expire_execution_controller(conn, plan_id, self.controller_id, token)
        except LeaseConflict:
            return
        LOG.warning(
            "controller yielded unresolved action for a new fencing token",
            extra={
                "plan_id": plan_id,
                "action_id": intent_id,
                "controller_id": self.controller_id,
                "fencing_token": token,
            },
        )

    def _finish_action(
        self,
        plan_id: str,
        token: int,
        action: dict[str, Any],
        state: str,
        observation: dict[str, Any],
        error: str | None = None,
        result: dict[str, Any] | None = None,
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
                    result=result,
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
        compensation_target = original
        compensation_payload: dict[str, Any] = {
            "current_replicas": observation["replicas"],
            "proposed_replicas": compensation_target,
            "target_precondition": {
                key: observation[key]
                for key in ("uid", "generation", "replicas")
                if key in observation
            },
            "health": payload.get("health"),
        }
        if action.get("action_type") == "PREPARE_SOURCE_RETIREMENT":
            recovery_target = payload.get("n_minus_1_recovery_target_replicas")
            maximum_live = payload.get("maximum_live_replicas")
            baseline = payload.get("baseline_replicas")
            maximum_surge = payload.get("maximum_temporary_surge_replicas")
            compensation_target = max(
                observation.get("replicas", 0) + 1,
                recovery_target if type(recovery_target) is int else 0,
            )
            recovery_nodes = payload.get("recovery_destination_nodes") or payload.get(
                "destination_nodes"
            )
            if (
                type(observation.get("replicas")) is not int
                or type(maximum_live) is not int
                or type(baseline) is not int
                or type(maximum_surge) is not int
                or type(recovery_target) is not int
                or not isinstance(recovery_nodes, list)
                or not recovery_nodes
                or compensation_target > maximum_live
                or compensation_target > baseline + maximum_surge
                or recovery_target > maximum_live
            ):
                self._fail_safe(plan_id, token, "approved-capacity-compensation-unavailable")
                return
            compensation_payload.update(
                proposed_replicas=compensation_target,
                destination_nodes=recovery_nodes,
                topology_key=payload.get("topology_key", ""),
                minimum_topology_domains=payload.get("minimum_topology_domains", 0),
                maximum_topology_skew=payload.get("maximum_topology_skew"),
            )
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
                    compensation_payload,
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
            unresolved = [
                action
                for action in Repository.incomplete_actions(conn, plan_id)
                if action["execution_state"] in {"RUNNING", "OUTCOME_UNKNOWN"}
            ]
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
            if getattr(self.target, "supports_workload_execution", False) and (
                "pending_pod_create" not in observed
                or observed.get("pending_pod_create") is not None
                or observed.get("pod_create_history_uncertain") is not False
                or type(observed.get("unverified_pod_creates")) is not int
                or observed.get("unverified_pod_creates") != 0
            ):
                LOG.error(
                    "execution remains active until Pod CREATE outcomes are known",
                    extra={"plan_id": plan_id, "action_id": action["intent_id"], "reason": reason},
                )
                return
            dispatch = self._latest_dispatch(action)
            target_fence = self._takeover_fence_receipt(
                plan_id,
                action["intent_id"],
                token,
                after_target_token=(
                    dispatch.get("target_fencing_token") if isinstance(dispatch, dict) else None
                ),
            )
            fence_observation = target_fence.get("observation", {}) if target_fence else {}
            fence_matches_live = (
                target_fence is not None
                and observed.get("uid") == fence_observation.get("uid")
                and observed.get("generation") == target_fence.get("generation")
                and observed.get("fencing_token") == target_fence.get("token")
                and (
                    not getattr(self.target, "supports_workload_execution", False)
                    or (
                        self._takeover_fence_matches(action, plan_id, token, target_fence)
                        and observed.get("execution_fence")
                        == fence_observation.get("execution_fence")
                    )
                )
            )
            if not fence_matches_live:
                LOG.error(
                    "execution yielded until its unresolved target can be fenced",
                    extra={"plan_id": plan_id, "action_id": action["intent_id"], "reason": reason},
                )
                self._yield_for_target_fence(plan_id, token, action["intent_id"])
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
                        result={"target_fence": target_fence},
                        error=reason,
                    )
        if not self._workload_creates_resolved(plan_id):
            LOG.error(
                "execution remains active until workload identity and CREATE outcomes are known",
                extra={"plan_id": plan_id, "reason": reason},
            )
            return
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
                "SELECT intent_id, workload_id, payload FROM action_intents WHERE plan_id = %s "
                "AND compensates_intent_id IS NOT NULL ORDER BY action_sequence",
                (plan_id,),
            ).fetchall()
        observations = []
        for action in actions:
            observed = self.target.observe_scale(action["workload_id"])
            payload = action["payload"]
            precondition = payload.get("target_precondition")
            if (
                not isinstance(precondition, dict)
                or observed.get("uid") != precondition.get("uid")
                or observed.get("replicas") != payload.get("proposed_replicas")
                or not self._capacity_ready(observed)
            ):
                raise TargetUnavailable("compensation target is not ready")
            if getattr(self.target, "supports_workload_execution", False):
                verify = getattr(self.target, "verify_recovery", None)
                if not callable(verify):
                    raise ActionBlocked("RECOVERY_TARGET_VERIFICATION_UNAVAILABLE")
                verify(plan_id, action, observed)
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
