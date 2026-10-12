from __future__ import annotations

from copy import deepcopy

import pytest
from fleetright import api as api_module
from fleetright.contracts import decode_text, encode_text
from fleetright.scenario import oracle_snapshot_fields


def _v2_snapshot(
    now_ms: int | None = None,
    *,
    owner_contract_version: int = 1,
    owner_kind: str = "FleetRightWorkload",
    owner_uid: str = "workload-uid",
    pod_owner_uid: str = "workload-uid",
    controller_observed_at_ms: int | None = None,
    include_legacy_owner: bool = False,
) -> tuple[dict[str, object], bytes, dict[str, object]]:
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
        "owner_contract_version": owner_contract_version,
        "controllers": [
            {
                "workload_id": workload["workload_id"],
                "namespace": "default",
                "owner_kind": owner_kind,
                "owner_name": workload["workload_id"],
                "owner_uid": owner_uid,
                "owner_resource_version": "workload-rv",
                "owner_generation": 1,
                "controller_type": "fleetright-workload-controller/v1",
                "execution_protocol": "monotonic-scale-up-v1",
                "pod_template_sha256": "template-digest",
                "desired_replicas": len(fields["current_placements"]),
                "pdb_allowed_disruptions": 1,
                "pdb_resource_version": "pdb-rv",
                "observed_at_ms": (
                    now_ms - 1_000
                    if controller_observed_at_ms is None
                    else controller_observed_at_ms
                ),
                **({"replicaset_uid": "legacy-replicaset-uid"} if include_legacy_owner else {}),
            }
        ],
        "pods": [
            {
                **placement,
                "pod_name": placement["replica_id"],
                "pod_uid": f"{placement['replica_id']}-uid",
                "owner_kind": owner_kind,
                "owner_name": workload["workload_id"],
                "owner_uid": pod_owner_uid,
                "logical_slot_id": placement["replica_id"],
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


@pytest.mark.parametrize(
    ("kwargs", "detail"),
    [
        ({"owner_contract_version": 2}, "unsupported-v2-owner-contract"),
        ({"owner_kind": "Deployment"}, "unsupported-v2-controller-state"),
        (
            {"owner_uid": "new-workload-uid", "pod_owner_uid": "old-workload-uid"},
            "v2-pod-inventory-mismatch",
        ),
        ({"include_legacy_owner": True}, "unknown-fields-in-v2-controller-identity"),
    ],
)
def test_v2_owner_contract_rejects_legacy_or_mismatched_identity(
    kwargs: dict[str, object], detail: str
) -> None:
    with pytest.raises(api_module.HTTPException) as error:
        _v2_snapshot(**kwargs)
    assert error.value.status_code == 422
    assert error.value.detail == detail


def test_v2_inventory_rejects_stale_controller_observation() -> None:
    fields = oracle_snapshot_fields()
    now_ms = int(fields["evaluation_time_ms"])
    maximum_age_ms = int(fields["policy"]["maximum_snapshot_age_ms"])
    with pytest.raises(api_module.HTTPException) as error:
        _v2_snapshot(
            now_ms,
            controller_observed_at_ms=now_ms - maximum_age_ms - 1,
        )
    assert error.value.status_code == 409
    assert error.value.detail == "v2-controller-observation-stale"


def test_v2_inventory_rejects_duplicate_controller_owner_uid() -> None:
    fields = oracle_snapshot_fields()
    now_ms = int(fields["evaluation_time_ms"])
    _, _, normalized = _v2_snapshot(now_ms)
    inventory = deepcopy(normalized["evacuation_v2"])
    existing_placements = list(fields["current_placements"])
    fields["current_placements"] = existing_placements + [
        {**placement, "workload_id": "worker"} for placement in existing_placements
    ]
    inventory["controllers"].append(
        {**inventory["controllers"][0], "workload_id": "worker", "owner_name": "worker"}
    )

    with pytest.raises(api_module.HTTPException) as error:
        api_module._validate_evacuation_inventory(inventory, fields, now_ms)
    assert error.value.status_code == 422
    assert error.value.detail == "duplicate-v2-controller-owner-uid"


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
    assert admission == []
    assert summary["evacuations"][0]["source_pod_uid"]
    assert summary["evacuations"][0]["source_node_uid"]
    operation = summary["evacuations"][0]
    proto_operation = api_module._proto_blocks(plan_text, "operations")[0]
    assert operation["baseline_replicas"] == api_module._proto_scalar(
        proto_operation, "baseline_replicas", 0
    )
    assert operation["maximum_temporary_surge_replicas"] == api_module._proto_scalar(
        proto_operation, "maximum_temporary_surge_replicas", 0
    )
    assert operation["destination_node_ids"] == api_module._proto_scalars(
        api_module._proto_blocks(proto_operation, "destinations")[0], "allowed_node_ids"
    )
    assert operation["permitted_intermediate_states"]
    assert operation["actions"]

    unsupported = api_module._execution_admission_reasons(
        {**summary, "evacuation_count": 2}, source["workloads"], "kubernetes-workload"
    )
    assert unsupported == ["UNSUPPORTED_SOURCE_RETIREMENT_PREPARATION"]

    tampered_text = plan_text.replace("maximum_live_replicas: 5", "maximum_live_replicas: 99", 1)
    assert tampered_text != plan_text
    tampered_plan = encode_text(
        "fleetright.v2.EvacuationPlan", "fleetright/v2/evacuation.proto", tampered_text
    )
    accepted, _, _ = api_module._run_verifier(payload, tampered_plan, plan_schema_version=2)
    assert accepted is False


def test_v2_four_workload_node_drain_plan_reaches_execution_admission(monkeypatch) -> None:
    fields, _, _ = _v2_snapshot()
    now_ms = int(fields["evaluation_time_ms"])
    node_ids = [node["node_id"] for node in fields["nodes"]]
    base = fields["workloads"][0]
    base["current_resources"] = {"cpu_millicores": 100, "memory_mib": 128, "network_mbps": 0}
    workloads, placements, profiles, controllers = [], [], [], []
    pods = []
    for index in range(4):
        workload_id = "cpu-api" if index == 0 else f"cpu-api-{index}"
        workload = deepcopy(base)
        workload.update(workload_id=workload_id, min_replicas=2, availability_floor=1)
        workloads.append(workload)

        profile = deepcopy(fields["capacity_profiles"][0])
        profile.update(
            workload_id=workload_id,
            profile_id=f"{workload_id}-profile",
            allocation=deepcopy(workload["current_resources"]),
        )
        profiles.append(profile)
        for replica_index, node_id in enumerate(node_ids[:2]):
            placements.append(
                {
                    "workload_id": workload_id,
                    "replica_id": f"{workload_id}-{replica_index}",
                    "node_id": node_id,
                }
            )
        controllers.append(
            {
                "workload_id": workload_id,
                "namespace": "default",
                "owner_kind": "FleetRightWorkload",
                "owner_name": workload_id,
                "owner_uid": f"{workload_id}-owner",
                "owner_resource_version": "workload-rv",
                "owner_generation": 1,
                "controller_type": "fleetright-workload-controller/v1",
                "execution_protocol": "monotonic-scale-up-v1",
                "pod_template_sha256": "template-digest",
                "desired_replicas": 2,
                "pdb_allowed_disruptions": 1,
                "pdb_resource_version": "pdb-rv",
                "observed_at_ms": now_ms - 1_000,
            }
        )

    for placement in placements:
        workload_id, replica_id = placement["workload_id"], placement["replica_id"]
        pods.append(
            {
                **placement,
                "pod_name": replica_id,
                "pod_uid": f"{replica_id}-uid",
                "owner_kind": "FleetRightWorkload",
                "owner_name": workload_id,
                "owner_uid": f"{workload_id}-owner",
                "logical_slot_id": replica_id,
                "ready": True,
                "terminating": False,
            }
        )

    fields["workloads"], fields["current_placements"], fields["capacity_profiles"] = (
        workloads,
        placements,
        profiles,
    )
    for index, node in enumerate(fields["nodes"]):
        node["labels"] = [("zone", f"zone-{index}"), ("rack", f"rack-{index}")]
    fields["evacuation_v2"].update(
        controllers=controllers,
        pods=pods,
        nodes=[
            {"node_id": node_id, "node_uid": f"{node_id}-uid", "resource_version": f"{node_id}-rv"}
            for node_id in node_ids
        ],
        inventory_observed_at_ms=now_ms - 1_000,
    )
    monkeypatch.setenv(
        "FLEETRIGHT_KUBERNETES_WORKLOADS", ",".join(w["workload_id"] for w in workloads)
    )

    source, payload, _ = api_module._validate_snapshot(fields, now_ms)
    _, _, _, _, summary = api_module._run_planner(payload, plan_schema_version=2)
    assert summary["feasible"] is True
    assert summary["verification_accepted"] is True
    assert summary["mode"] == 1
    assert [item["workload_id"] for item in summary["evacuations"]] == [
        item["workload_id"] for item in workloads
    ]
    assert {item["source_node_id"] for item in summary["evacuations"]} == {node_ids[0]}
    assert summary["plan_actions"][0]["stage"] == 7
    assert summary["plan_actions"][1]["stage"] == 8
    assert (
        api_module._execution_admission_reasons(summary, source["workloads"], "kubernetes-workload")
        == []
    )


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
