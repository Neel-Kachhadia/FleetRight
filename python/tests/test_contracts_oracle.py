from __future__ import annotations

import subprocess
from pathlib import Path

from fleetright.contracts import decode_text, encode_profile, encode_snapshot
from fleetright.experiment import _field
from fleetright.oracle import exact_single_workload
from fleetright.scenario import demo_snapshot_fields, oracle_snapshot_fields
from fleetright.traces import generate_trace
from fleetright.units import multiply_ppm_floor

ROOT = Path(__file__).resolve().parents[2]


def test_versioned_snapshot_hash_and_protobuf_text_round_trip() -> None:
    payload, fields = encode_snapshot(demo_snapshot_fields())
    text = decode_text("fleetright.v1.OptimizationSnapshot", "fleetright/v1/model.proto", payload)
    repeated = subprocess.run(
        [
            "protoc",
            f"--proto_path={ROOT / 'proto'}",
            "--encode=fleetright.v1.OptimizationSnapshot",
            "fleetright/v1/model.proto",
        ],
        input=text.encode(),
        capture_output=True,
        check=True,
        cwd=ROOT,
    )
    assert repeated.stdout == payload
    assert fields["content_sha256"]
    assert fields["policy"]["planning_horizon_seconds"] == 3600


def test_exact_small_oracle_finds_cheapest_feasible_node_subset() -> None:
    result = exact_single_workload(oracle_snapshot_fields())
    assert result.replicas == 2
    assert result.objective_micros_per_hour == 100_000
    assert len(result.node_ids) == 2
    assert result.scenarios_examined == 3


def test_trace_serialization_is_canonical_json_data() -> None:
    trace = generate_trace("correlated_spike", 9)
    assert trace.as_dict()["content_sha256"] == trace.content_sha256
    assert len(trace.workloads) == 3


def test_profile_contract_retains_raw_error_count_and_hashes_artifact() -> None:
    fields = demo_snapshot_fields()
    fields["capacity_profiles"][0]["safe_points"][0]["error_count"] = 3
    payload, finalized = encode_profile(fields["capacity_profiles"][0])
    text = decode_text("fleetright.v1.CapacityProfile", "fleetright/v1/model.proto", payload)
    assert "error_count: 3" in text
    assert finalized["profile_sha256"]


def test_ppm_capacity_factors_round_down_once() -> None:
    assert multiply_ppm_floor(5, 999_999, 999_999) == 4
    assert multiply_ppm_floor(5, 999_999, 999_999) > 5 * 999_999 // 1_000_000 * 999_999 // 1_000_000


def test_proto3_omitted_false_default_is_read_as_false() -> None:
    assert _field('plan_id: "plan-1"', "feasible", "false") == "false"
