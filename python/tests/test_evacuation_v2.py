from __future__ import annotations

from copy import deepcopy

from fleetright import api as api_module
from fleetright.contracts import decode_text, encode_text
from fleetright.scenario import oracle_snapshot_fields


def _v2_snapshot(now_ms: int | None = None) -> tuple[dict[str, object], bytes, dict[str, object]]:
    fields = oracle_snapshot_fields()
    now_ms = int(fields["evaluation_time_ms"]) if now_ms is None else now_ms
    fields.update(
        observed_at_ms=now_ms - 1_000,
        expires_at_ms=now_ms + 3_600_000,
        evaluation_time_ms=now_ms,
    )
    workload = fields["workloads"][0]
    fields["capacity_profiles"][0]["allocation"] = deepcopy(workload["current_resources"])
    for profile in fields["capacity_profiles"]:
        profile.update(generated_at_ms=now_ms - 1_000, valid_until_ms=now_ms + 3_600_000)
    for index in range(3, 5):
        node = deepcopy(fields["nodes"][0])
        node["node_id"] = f"extra-node-{index}"
        node["generation"] = 1
        fields["nodes"].append(node)

    inventory = {
        "cluster_uid": "cluster-python-test",
        "namespace": "default",
        "inventory_observed_at_ms": now_ms - 1_000,
        "controllers": [
            {
                "workload_id": workload["workload_id"],
                "namespace": "default",
                "deployment_name": "api",
                "deployment_uid": "deployment-uid",
                "deployment_resource_version": "deployment-rv",
                "deployment_generation": 1,
                "replicaset_name": "api-1",
                "replicaset_uid": "replicaset-uid",
                "replicaset_resource_version": "replicaset-rv",
                "pod_template_sha256": "template-digest",
                "desired_replicas": len(fields["current_placements"]),
                "active_replicaset_count": 1,
                "pdb_allowed_disruptions": 1,
                "pdb_resource_version": "pdb-rv",
                "observed_at_ms": now_ms - 1_000,
            }
        ],
        "pods": [
            {
                **placement,
                "pod_name": placement["replica_id"],
                "pod_uid": f"{placement['replica_id']}-uid",
                "controller_uid": "replicaset-uid",
                "ready": True,
                "terminating": False,
            }
            for placement in fields["current_placements"]
        ],
        "nodes": [
            {
                "node_id": node["node_id"],
                "node_uid": f"{node['node_id']}-uid",
                "resource_version": f"{node['node_id']}-rv",
            }
            for node in fields["nodes"]
        ],
    }
    request_snapshot = {
        **fields,
        "plan_schema_version": 2,
        "evacuation_v2": inventory,
    }
    normalized_source, payload, normalized = api_module._validate_snapshot(request_snapshot, now_ms)
    assert normalized_source["snapshot_id"] == fields["snapshot_id"]
    return normalized_source, payload, normalized


def test_v2_snapshot_hash_round_trip_and_cpp_planner_verifier() -> None:
    source, payload, normalized = _v2_snapshot()
    text = decode_text(
        "fleetright.v2.EvacuationSnapshot", "fleetright/v2/evacuation.proto", payload
    )
    assert (
        encode_text("fleetright.v2.EvacuationSnapshot", "fleetright/v2/evacuation.proto", text)
        == payload
    )
    assert normalized["evacuation_snapshot_sha256"]

    plan, verification, plan_text, verification_text, summary = api_module._run_planner(
        payload, plan_schema_version=2
    )
    assert plan
    assert verification
    assert summary["schema_version"] == 2
    assert summary["feasible"] is True
    assert summary["verification_accepted"] is True
    assert api_module._proto_scalar(verification_text, "execution_admitted", False) is False
    assert summary["snapshot_id"] == source["snapshot_id"]

    admission = api_module._execution_admission_reasons(
        summary, source["workloads"], "kubernetes-workload"
    )
    assert "UNSUPPORTED_EXECUTION_PROTOCOL_V2" in admission
    assert "UNSUPPORTED_EXECUTION_ACTION_MOVE_REPLICA" in admission
    assert "UNSUPPORTED_EXECUTION_ACTION_DEACTIVATE_NODE" in admission

    tampered_text = plan_text.replace("maximum_live_replicas: 5", "maximum_live_replicas: 99", 1)
    assert tampered_text != plan_text
    tampered_plan = encode_text(
        "fleetright.v2.EvacuationPlan", "fleetright/v2/evacuation.proto", tampered_text
    )
    accepted, _, _ = api_module._run_verifier(payload, tampered_plan, plan_schema_version=2)
    assert accepted is False


def test_unknown_execution_protocol_is_not_admitted() -> None:
    summary = {
        "schema_version": 9,
        "deactivated_node_ids": [],
        "evacuation_count": 0,
        "decisions": [],
    }
    assert api_module._execution_admission_reasons(summary, [], "kubernetes-workload") == [
        "UNSUPPORTED_EXECUTION_PROTOCOL_VERSION"
    ]
    assert api_module._execution_admission_reasons(
        {**summary, "schema_version": True}, [], "kubernetes-workload"
    ) == ["UNSUPPORTED_EXECUTION_PROTOCOL_VERSION"]
