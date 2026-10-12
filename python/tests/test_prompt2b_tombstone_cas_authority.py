from __future__ import annotations

import json
from copy import deepcopy
from typing import Any

import pytest
from fleetright.execution import (
    ActionBlocked,
    FleetRightWorkloadTarget,
    TargetPreconditionFailed,
)
from fleetright.workload_controller import (
    POD_CREATE_OPERATION_ANNOTATION,
    POD_CREATE_PLAN_ANNOTATION,
    ApiError,
    _pod_create_intent,
    _write_pending_pod_create,
    _write_status,
)
from test_pod_admission import EvictionAPI


@pytest.mark.parametrize(
    "competing_write",
    ["reconciled_status", "replacement_unready", "create_reservation", "same_tombstone"],
)
def test_tombstone_cas_retry_revalidates_current_target_state(
    monkeypatch: pytest.MonkeyPatch, competing_write: str
) -> None:
    api = EvictionAPI()
    api.workload["status"].pop("retiredReplicaSlots")
    source_record = api.workload["status"]["authorizedPodCreates"][0]
    source_record.update(planID="bootstrap", operationID="bootstrap-1-0")
    api.source["metadata"]["annotations"].update(
        {
            POD_CREATE_PLAN_ANNOTATION: source_record["planID"],
            POD_CREATE_OPERATION_ANNOTATION: source_record["operationID"],
        }
    )
    target = FleetRightWorkloadTarget(namespace="fleetright-v2")
    payload = {
        "operation_id": "operation-1",
        "source_owner_uid": "workload-uid",
        "source_pod_name": api.source["metadata"]["name"],
        "source_pod_uid": "source-uid",
        "source_node_id": "worker-a",
        "source_node_uid": "node-a-uid",
        "source_logical_slot_id": "api-0",
        "workload_generation": 1,
        "baseline_replicas": 1,
        "surge_desired_replicas": 2,
        "final_desired_replicas": 2,
        "maximum_live_replicas": 2,
        "maximum_temporary_surge_replicas": 1,
        "profile_required_replicas": 1,
        "readiness_stabilization_ms": 0,
        "destination_nodes": [{"name": "worker-b", "uid": "node-b-uid"}],
    }
    injected = False
    conflicts = 0

    def read_or_patch(*args: str) -> dict[str, Any]:
        nonlocal injected, conflicts
        if args[:2] == ("get", "fleetrightworkload"):
            return deepcopy(api.workload)
        if args[:2] == ("get", "pod"):
            pod = api.source if args[2] == payload["source_pod_name"] else api.replacement
            return deepcopy(pod)
        if args[:2] == ("get", "node"):
            return deepcopy(next(node for node in api.nodes if node["metadata"]["name"] == args[2]))
        if args[:2] == ("get", "pdb"):
            return deepcopy(api.pdb)
        assert args[:2] == ("patch", "fleetrightworkload")
        assert "--subresource=status" in args
        patch = json.loads(args[args.index("-p") + 1])
        assert "retiredReplicaSlots" in patch["status"]
        if not injected:
            injected = True
            current = deepcopy(api.workload)
            if competing_write == "same_tombstone":
                api.patch_status("api", current["metadata"]["resourceVersion"], patch["status"])
            elif competing_write == "create_reservation":
                intent = _pod_create_intent(current, 2)
                intent["operationID"] = "another-create-operation"
                _write_pending_pod_create(api, current, intent)
            else:
                if competing_write == "replacement_unready":
                    api.replacement["status"]["conditions"][0]["status"] = "False"
                _write_status(
                    api,
                    current,
                    [api.source, api.replacement],
                    "Reconciled",
                    "workload reconciled from API state",
                )
        try:
            return api.patch_status("api", patch["metadata"]["resourceVersion"], patch["status"])
        except ApiError as error:
            assert error.status == 409
            conflicts += 1
            raise TargetPreconditionFailed("target resource version changed") from error

    monkeypatch.setattr(target, "_json", read_or_patch)
    monkeypatch.setattr(target, "_require_fencing_schema", lambda: None)
    monkeypatch.setattr(
        target, "_kubectl", lambda *_args, **_kwargs: pytest.fail("Eviction must not be dispatched")
    )

    with pytest.raises(TargetPreconditionFailed, match="target resource version changed"):
        target.prepare_source_retirement("api", "plan-1", payload, fencing_token=41)

    assert conflicts == 1
    assert api.source["metadata"]["uid"] == "source-uid"
    assert api.source["metadata"].get("deletionTimestamp") is None
    if competing_write != "same_tombstone":
        assert api.workload["status"].get("retiredReplicaSlots", []) == []

    if competing_write == "create_reservation":
        with pytest.raises(ActionBlocked, match="POD_CREATE_OUTCOME_UNKNOWN"):
            target.prepare_source_retirement("api", "plan-1", payload, fencing_token=41)
        pending = api.workload["status"]["pendingPodCreate"]
        assert pending["operationID"] == "another-create-operation"
        assert api.workload["status"].get("retiredReplicaSlots", []) == []
    else:
        result = target.prepare_source_retirement("api", "plan-1", payload, fencing_token=41)
        if competing_write == "replacement_unready":
            assert result["stage"] == "REPLACEMENT_WAITING_READY"
            assert api.workload["status"].get("retiredReplicaSlots", []) == []
        else:
            assert result["stage"] == "READY_FOR_SAFE_EVICTION"
            assert result["source_retirement_intent_recorded"] is True
            tombstones = api.workload["status"]["retiredReplicaSlots"]
            assert len(tombstones) == 1
            assert tombstones[0]["podUID"] == "source-uid"
            assert tombstones[0]["replacementPodUID"] == "replacement-uid"
            assert tombstones[0]["readinessStabilizationMs"] == 0
    assert conflicts == 1
