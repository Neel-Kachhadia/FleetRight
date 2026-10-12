from __future__ import annotations

from contextlib import nullcontext
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fleetright.control import Repository
from fleetright.reconciler import ExecutionController


@pytest.mark.parametrize(
    "action_type", ["EVICT_SOURCE", "MOVE_REPLICA", "DRAIN_NODE", "DEACTIVATE_NODE"]
)
@pytest.mark.parametrize("compensating", [False, True])
def test_destructive_journal_action_never_reaches_target(
    monkeypatch: pytest.MonkeyPatch, action_type: str, compensating: bool
) -> None:
    target = Mock()
    repository = Mock()
    controller = ExecutionController(repository, target, Mock(), "controller-b")
    fail_safe = Mock()
    monkeypatch.setattr(controller, "_fail_safe", fail_safe)
    action = {
        "intent_id": "source-retirement",
        "action_type": action_type,
        "workload_id": "cpu-api",
        "payload": {
            "source_pod_uid": "source-uid",
            "source_node_uid": "node-uid",
            "current_replicas": 3,
            "proposed_replicas": 2,
        },
    }

    controller._start_action("plan", 12, action, compensating)

    fail_safe.assert_called_once_with("plan", 12, "unsupported-action")
    assert target.mock_calls == []
    assert repository.mock_calls == []


@pytest.mark.parametrize("state", ["RUNNING", "OUTCOME_UNKNOWN"])
@pytest.mark.parametrize(
    "action_type", ["EVICT_SOURCE", "MOVE_REPLICA", "DRAIN_NODE", "DEACTIVATE_NODE"]
)
def test_takeover_does_not_resolve_unknown_destructive_action_with_scale_fence(
    monkeypatch: pytest.MonkeyPatch, state: str, action_type: str
) -> None:
    target = Mock()
    repository = SimpleNamespace(transaction=lambda: nullcontext(object()))
    controller = ExecutionController(repository, target, Mock(), "controller-b")
    action = {
        "intent_id": "source-retirement",
        "action_type": action_type,
        "workload_id": "cpu-api",
        "execution_state": state,
        "execution_result": {
            "pre_observation": {"uid": "workload-uid", "resource_version": "17"},
            "source_pod_uid": "source-uid",
        },
    }
    before = deepcopy(action)
    prepare = Mock(return_value=12)
    monkeypatch.setattr(Repository, "prepare_execution_takeover", prepare)
    monkeypatch.setattr(Repository, "incomplete_actions", Mock(return_value=[action]))
    monkeypatch.setattr(Repository, "completed_retirement_actions", Mock(return_value=[]))
    scale_receipt = Mock(
        return_value={
            "state": "COMPLETE",
            "token": 12,
            "observation": {"uid": "workload-uid", "resource_version": "18"},
        }
    )
    monkeypatch.setattr(Repository, "execution_fence_receipt", scale_receipt)
    claim = Mock(return_value=12)
    monkeypatch.setattr(Repository, "claim_execution_controller", claim)
    record = Mock()
    monkeypatch.setattr(Repository, "record_execution_fence", record)

    token = controller._claim(
        {
            "plan_id": "plan",
            "controller_id": "controller-a",
            "controller_lease_expired": True,
            "controller_fencing_token": 11,
        }
    )

    assert token is None
    prepare.assert_called_once()
    scale_receipt.assert_not_called()
    claim.assert_not_called()
    record.assert_not_called()
    assert target.mock_calls == []
    assert action == before


@pytest.mark.parametrize("state", ["RUNNING", "OUTCOME_UNKNOWN"])
@pytest.mark.parametrize(
    "action_type", ["EVICT_SOURCE", "MOVE_REPLICA", "DRAIN_NODE", "DEACTIVATE_NODE"]
)
def test_current_owner_does_not_renew_unsupported_unknown_action(
    monkeypatch: pytest.MonkeyPatch, state: str, action_type: str
) -> None:
    target = Mock()
    repository = SimpleNamespace(transaction=lambda: nullcontext(object()))
    controller = ExecutionController(repository, target, Mock(), "controller-a")
    action = {
        "intent_id": "source-retirement",
        "action_type": action_type,
        "execution_state": state,
    }
    monkeypatch.setattr(Repository, "incomplete_actions", Mock(return_value=[action]))
    renew = Mock(return_value=True)
    monkeypatch.setattr(Repository, "renew_execution_controller", renew)

    token = controller._claim(
        {
            "plan_id": "plan",
            "controller_id": "controller-a",
            "controller_lease_expired": False,
            "controller_fencing_token": 11,
        }
    )

    assert token is None
    renew.assert_not_called()
    assert target.mock_calls == []


@pytest.mark.parametrize("state", ["RUNNING", "OUTCOME_UNKNOWN"])
def test_reconcile_never_sends_unsupported_inflight_action_to_scale_reconciler(
    monkeypatch: pytest.MonkeyPatch, state: str
) -> None:
    target = Mock()
    repository = SimpleNamespace(transaction=lambda: nullcontext(object()))
    controller = ExecutionController(repository, target, Mock(), "controller-a")
    action = {
        "intent_id": "source-retirement",
        "action_type": "EVICT_SOURCE",
        "execution_state": state,
        "compensates_intent_id": None,
    }
    monkeypatch.setattr(
        Repository,
        "assert_execution_controller",
        Mock(
            return_value={
                "status": "EXECUTING",
                "pause_requested": False,
                "cancel_requested": False,
            }
        ),
    )
    monkeypatch.setattr(Repository, "incomplete_actions", Mock(return_value=[action]))
    fail_safe = Mock()
    monkeypatch.setattr(controller, "_fail_safe", fail_safe)

    controller._reconcile("plan", 11, None)

    fail_safe.assert_called_once_with("plan", 11, "unsupported-action")
    assert target.mock_calls == []
