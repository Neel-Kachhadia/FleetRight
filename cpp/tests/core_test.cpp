#include "fleetright/evacuation_v2.hpp"
#include "fleetright/hash.hpp"
#include "fleetright/planner.hpp"
#include "fleetright/units.hpp"
#include "fleetright/v2_contract_utils.hpp"
#include "fleetright/verifier.hpp"

#include <google/protobuf/unknown_field_set.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <functional>
#include <limits>
#include <numeric>
#include <random>
#include <string>
#include <vector>

namespace {
using fleetright::v1::OptimizationSnapshot;

void SetResource(fleetright::v1::ResourceVector *resource, std::int64_t cpu, std::int64_t memory,
                 std::int64_t network) {
  resource->set_cpu_millicores(cpu);
  resource->set_memory_mib(memory);
  resource->set_network_mbps(network);
}

OptimizationSnapshot Fixture() {
  OptimizationSnapshot snapshot;
  snapshot.set_schema_version(1);
  snapshot.set_snapshot_id("snapshot-test-001");
  snapshot.set_generation(7);
  snapshot.set_observed_at_ms(900);
  snapshot.set_expires_at_ms(2'000);
  snapshot.set_evaluation_time_ms(1'000);

  auto *policy = snapshot.mutable_policy();
  policy->set_schema_version(1);
  policy->set_headroom_ppm(0);
  policy->set_minimum_profile_confidence_ppm(950'000);
  policy->set_maximum_profile_age_ms(10'000);
  policy->set_maximum_snapshot_age_ms(10'000);
  policy->set_maximum_plan_actions(100);
  policy->set_movement_cost_micros(100);
  policy->set_minimum_improvement_ppm(1);
  policy->set_maximum_local_search_iterations(100);
  policy->set_allow_synthetic_profiles(true);
  policy->set_planning_horizon_seconds(3'600);

  auto *catalog = snapshot.mutable_machine_catalog();
  catalog->set_schema_version(1);
  catalog->set_catalog_version("fixture-1");
  auto *machine = catalog->add_machines();
  machine->set_machine_type("small");
  SetResource(machine->mutable_allocatable(), 2'000, 4'096, 1'000);
  machine->set_hourly_cost_micros(50'000);
  auto catalog_copy = *catalog;
  catalog_copy.clear_catalog_sha256();
  catalog->set_catalog_sha256(fleetright::MessageSha256(catalog_copy));

  auto *workload = snapshot.add_workloads();
  workload->set_workload_id("api");
  workload->set_priority(10);
  workload->set_min_replicas(2);
  workload->set_max_replicas(8);
  workload->set_availability_floor(2);
  workload->set_demand_qps_milli(700'000);
  workload->set_concurrency_bound(180);
  workload->set_latency_slo_ms(120);
  workload->set_error_slo_ppm(1'000);
  workload->set_startup_ms(500);
  workload->set_readiness_stabilization_ms(100);
  workload->set_failure_tolerance(fleetright::v1::FAILURE_TOLERANCE_NONE);
  SetResource(workload->mutable_current_resources(), 1'000, 1'024, 100);
  workload->set_image_digest("sha256:build-a");
  workload->set_request_mix_id("mix-a");
  workload->set_runtime_config_class("default");
  workload->set_platform_class("x86_64-linux");
  workload->set_profile_confidence_factor_ppm(1'000'000);
  workload->set_model_accuracy_factor_ppm(1'000'000);
  workload->set_degraded_cluster_factor_ppm(1'000'000);
  workload->set_max_throttle_ppm(50'000);
  workload->set_max_memory_pressure_ppm(100'000);
  workload->set_max_scale_down_fraction_ppm(500'000);
  workload->set_max_moves(2);

  for (int i = 0; i < 4; ++i) {
    auto *node = snapshot.add_nodes();
    node->set_node_id("node-" + std::to_string(i));
    node->set_machine_type("small");
    SetResource(node->mutable_allocatable(), 2'000, 4'096, 1'000);
    SetResource(node->mutable_reserved(), 0, 0, 0);
    node->set_hourly_cost_micros(50'000);
    node->set_health(fleetright::v1::NODE_HEALTH_HEALTHY);
    node->set_active(true);
    node->set_generation(4);
  }
  for (int i = 0; i < 3; ++i) {
    auto *placement = snapshot.add_current_placements();
    placement->set_workload_id("api");
    placement->set_replica_id("api-" + std::to_string(i));
    placement->set_node_id("node-" + std::to_string(i));
  }

  auto *profile = snapshot.add_capacity_profiles();
  profile->set_schema_version(1);
  profile->set_profile_id("api-small-v1");
  profile->set_profile_version("1");
  profile->set_workload_id("api");
  profile->set_image_digest("sha256:build-a");
  profile->set_request_mix_id("mix-a");
  profile->set_runtime_config_class("default");
  profile->set_platform_class("x86_64-linux");
  SetResource(profile->mutable_allocation(), 1'000, 1'024, 100);
  profile->set_generated_at_ms(800);
  profile->set_valid_until_ms(9'000);
  profile->set_confidence_ppm(980'000);
  profile->set_conservative_haircut_ppm(100'000);
  profile->set_measurement_kind("synthetic_test_fixture");
  auto *point = profile->add_safe_points();
  point->set_safe_qps_milli(500'000);
  point->set_safe_concurrency(100);
  point->set_p99_upper_ms(100);
  point->set_error_rate_upper_ppm(200);
  point->set_queue_growth_upper_milli(0);
  point->set_memory_pressure_ppm(20'000);
  point->set_cpu_throttling_ppm(10'000);
  point->set_request_count(50'000);
  point->set_repeated_run_count(3);
  point->set_threshold_successes(49'900);
  point->set_threshold_trials(50'000);
  point->set_latency_success_lower_ppm(995'000);
  point->set_error_rate_wilson_upper_ppm(300);
  point->set_run_dispersion_ppm(10'000);
  point->set_variance_ppm(500);
  point->set_histogram_config_id("classic-v1");
  point->set_stable_across_runs(true);
  auto profile_copy = *profile;
  profile_copy.clear_profile_sha256();
  profile->set_profile_sha256(fleetright::MessageSha256(profile_copy));

  auto snapshot_copy = snapshot;
  snapshot_copy.clear_content_sha256();
  snapshot.set_content_sha256(fleetright::MessageSha256(snapshot_copy));
  return snapshot;
}

void SealProfile(fleetright::v1::CapacityProfile *profile) {
  auto copy = *profile;
  copy.clear_profile_sha256();
  profile->set_profile_sha256(fleetright::MessageSha256(copy));
}

void SealSnapshot(OptimizationSnapshot *snapshot) {
  for (auto &profile : *snapshot->mutable_capacity_profiles())
    SealProfile(&profile);
  auto *catalog = snapshot->mutable_machine_catalog();
  auto catalog_copy = *catalog;
  catalog_copy.clear_catalog_sha256();
  catalog->set_catalog_sha256(fleetright::MessageSha256(catalog_copy));
  auto copy = *snapshot;
  copy.clear_content_sha256();
  snapshot->set_content_sha256(fleetright::MessageSha256(copy));
}

void SealPlan(fleetright::v1::OptimizationPlan *plan) {
  plan->clear_plan_id();
  plan->set_plan_id(fleetright::MessageSha256(*plan));
}

void SealEvacuationSnapshot(fleetright::v2::EvacuationSnapshot *snapshot) {
  auto *model = snapshot->mutable_model_snapshot();
  auto model_copy = *model;
  model_copy.clear_content_sha256();
  model->set_content_sha256(fleetright::MessageSha256(model_copy));
  snapshot->clear_content_sha256();
  snapshot->set_content_sha256(fleetright::EvacuationSnapshotSha256(*snapshot));
}

fleetright::v2::EvacuationSnapshot EvacuationFixture() {
  fleetright::v2::EvacuationSnapshot snapshot;
  snapshot.set_schema_version(2);
  snapshot.set_owner_contract_version(1);
  snapshot.set_cluster_uid("cluster-uid");
  snapshot.set_namespace_("test");
  snapshot.set_inventory_observed_at_ms(1'000);
  *snapshot.mutable_model_snapshot() = Fixture();
  auto *model = snapshot.mutable_model_snapshot();
  for (int i = 0; i < 2; ++i) {
    auto *node = model->add_nodes();
    node->set_node_id("node-" + std::to_string(i + 4));
    node->set_machine_type("small");
    SetResource(node->mutable_allocatable(), 2'000, 4'096, 1'000);
    SetResource(node->mutable_reserved(), 0, 0, 0);
    node->set_hourly_cost_micros(50'000);
    node->set_health(fleetright::v1::NODE_HEALTH_HEALTHY);
    node->set_active(true);
    node->set_generation(4);
  }
  for (int i = 0; i < model->nodes_size(); ++i) {
    auto *label = model->mutable_nodes(i)->add_labels();
    label->set_key("zone");
    label->set_value("zone-" + std::to_string(i));

    auto *node = snapshot.add_nodes();
    node->set_node_id(model->nodes(i).node_id());
    node->set_node_uid("node-uid-" + std::to_string(i));
    node->set_resource_version("node-rv-" + std::to_string(i));
  }
  auto *controller = snapshot.add_controllers();
  controller->set_workload_id("api");
  controller->set_namespace_("test");
  controller->set_owner_kind("FleetRightWorkload");
  controller->set_owner_name("api");
  controller->set_owner_uid("workload-uid");
  controller->set_owner_resource_version("workload-rv");
  controller->set_owner_generation(1);
  controller->set_controller_type("fleetright-workload-controller/v1");
  controller->set_execution_protocol("monotonic-scale-up-v1");
  controller->set_pod_template_sha256("template-sha256");
  controller->set_desired_replicas(3);
  controller->set_pdb_allowed_disruptions(1);
  controller->set_pdb_resource_version("pdb-rv");
  controller->set_observed_at_ms(1'000);
  for (const auto &placement : model->current_placements()) {
    auto *pod = snapshot.add_pods();
    pod->set_workload_id(placement.workload_id());
    pod->set_replica_id(placement.replica_id());
    pod->set_node_id(placement.node_id());
    pod->set_pod_name(placement.replica_id());
    pod->set_pod_uid(placement.replica_id() + "-uid");
    pod->set_owner_kind("FleetRightWorkload");
    pod->set_owner_name("api");
    pod->set_owner_uid("workload-uid");
    pod->set_logical_slot_id(placement.replica_id());
    pod->set_ready(true);
    pod->set_terminating(false);
  }
  SealEvacuationSnapshot(&snapshot);
  return snapshot;
}

fleetright::v2::EvacuationPlan
FeasibleEvacuationPlan(const fleetright::v2::EvacuationSnapshot &snapshot) {
  auto plan = fleetright::PlanEvacuationV2(snapshot);
  EXPECT_TRUE(plan.feasible()) << plan.infeasible_reason();
  return plan;
}

fleetright::v2::EvacuationSnapshot DrainFixture(int count = 4) {
  auto snapshot = EvacuationFixture();
  auto *model = snapshot.mutable_model_snapshot();
  const auto workload_template = model->workloads(0);
  const auto profile_template = model->capacity_profiles(0);
  const auto controller_template = snapshot.controllers(0);
  model->mutable_nodes()->DeleteSubrange(4, 2);
  snapshot.mutable_nodes()->DeleteSubrange(4, 2);
  for (auto &node : *model->mutable_nodes()) {
    auto *label = node.add_labels();
    label->set_key("kubernetes.io/hostname");
    label->set_value(node.node_id());
  }
  model->clear_workloads();
  model->clear_capacity_profiles();
  model->clear_current_placements();
  snapshot.clear_controllers();
  snapshot.clear_pods();
  for (int i = 0; i < count; ++i) {
    const auto id = "drain-" + std::to_string(i);
    auto *workload = model->add_workloads();
    *workload = workload_template;
    workload->set_workload_id(id);
    workload->set_min_replicas(1);
    workload->set_availability_floor(1);
    workload->set_demand_qps_milli(100'000);
    workload->set_concurrency_bound(10);
    workload->set_topology_key("kubernetes.io/hostname");
    workload->set_min_topology_domains(1);
    workload->set_max_topology_skew(1);
    SetResource(workload->mutable_current_resources(), 200, 256, 10);
    auto *profile = model->add_capacity_profiles();
    *profile = profile_template;
    profile->set_workload_id(id);
    profile->set_profile_id(id + "-profile");
    *profile->mutable_allocation() = workload->current_resources();
    SealProfile(profile);
    auto *controller = snapshot.add_controllers();
    *controller = controller_template;
    controller->set_workload_id(id);
    controller->set_owner_name(id);
    controller->set_owner_uid(id + "-owner-uid");
    controller->set_desired_replicas(1);
    auto *placement = model->add_current_placements();
    placement->set_workload_id(id);
    placement->set_replica_id(id + "-0");
    placement->set_node_id("node-0");
    auto *pod = snapshot.add_pods();
    pod->set_workload_id(id);
    pod->set_replica_id(placement->replica_id());
    pod->set_node_id("node-0");
    pod->set_pod_name(placement->replica_id());
    pod->set_pod_uid(id + "-source-uid");
    pod->set_owner_kind("FleetRightWorkload");
    pod->set_owner_name(id);
    pod->set_owner_uid(controller->owner_uid());
    pod->set_logical_slot_id(placement->replica_id());
    pod->set_ready(true);
  }
  SealEvacuationSnapshot(&snapshot);
  return snapshot;
}

fleetright::v2::EvacuationSnapshot DrainFixtureWithHardAffinityDomains() {
  auto snapshot = DrainFixture();
  auto *model = snapshot.mutable_model_snapshot();
  const auto node_template = model->nodes(2);
  auto *extra_node = model->add_nodes();
  *extra_node = node_template;
  extra_node->set_node_id("node-4");
  for (auto &label : *extra_node->mutable_labels())
    if (label.key() == "kubernetes.io/hostname")
      label.set_value("node-4");
  auto *extra_identity = snapshot.add_nodes();
  *extra_identity = snapshot.nodes(2);
  extra_identity->set_node_id("node-4");
  extra_identity->set_node_uid("node-uid-4");
  extra_identity->set_resource_version("node-rv-4");

  for (int node_index = 0; node_index < model->nodes_size(); ++node_index) {
    auto *node = model->mutable_nodes(node_index);
    for (auto &label : *node->mutable_labels())
      if (label.key() == "zone")
        label.set_value(node_index == 3 ? "zone-0"
                                        : "zone-" + std::to_string(node_index));
  }
  for (int index = 0; index < model->workloads_size(); ++index) {
    auto *workload = model->mutable_workloads(index);
    workload->set_topology_key("zone");
    workload->set_min_topology_domains(2);
    snapshot.mutable_controllers(index)->set_desired_replicas(2);
    auto *placement = model->add_current_placements();
    placement->set_workload_id(workload->workload_id());
    placement->set_replica_id(workload->workload_id() + "-1");
    placement->set_node_id("node-1");
    auto *pod = snapshot.add_pods();
    pod->set_workload_id(workload->workload_id());
    pod->set_replica_id(placement->replica_id());
    pod->set_node_id("node-1");
    pod->set_pod_name(placement->replica_id());
    pod->set_pod_uid(placement->replica_id() + "-uid");
    pod->set_owner_kind("FleetRightWorkload");
    pod->set_owner_name(workload->workload_id());
    pod->set_owner_uid(snapshot.controllers(index).owner_uid());
    pod->set_logical_slot_id(placement->replica_id());
    pod->set_ready(true);
    pod->set_terminating(false);
  }
  SealEvacuationSnapshot(&snapshot);
  return snapshot;
}

void SealEvacuationPlan(fleetright::v2::EvacuationPlan *plan) {
  plan->clear_plan_id();
  plan->set_plan_id(fleetright::EvacuationPlanSha256(*plan));
}
} // namespace

TEST(EvacuationV2Test, AcceptsAConstraintPlanAndReplaysDeterministically) {
  const auto snapshot = EvacuationFixture();
  const auto plan = FeasibleEvacuationPlan(snapshot);
  ASSERT_TRUE(plan.feasible());
  ASSERT_EQ(plan.operations_size(), 1);
  EXPECT_EQ(plan.plan_actions_size(), 0);
  EXPECT_EQ(plan.operations(0).depends_on_plan_action_sequences_size(), 0);
  EXPECT_FALSE(plan.operations(0).deactivate_source_node_after_empty());
  EXPECT_EQ(plan.operations(0).actions_size(), 5);
  EXPECT_EQ(plan.operations(0).destinations().mode(),
            fleetright::v2::DESTINATION_MODE_CONSTRAINT_NODE_SET);
  EXPECT_TRUE(plan.operations(0).destinations().require_hard_pod_anti_affinity());
  EXPECT_EQ(plan.operations(0).maximum_temporary_surge_replicas(), 2U);
  EXPECT_EQ(plan.required_runtime_capabilities_size(), 9);

  const auto result = fleetright::VerifyEvacuationV2(snapshot, plan);
  EXPECT_TRUE(result.accepted()) << result.DebugString();
  EXPECT_FALSE(result.execution_admitted());
  EXPECT_EQ(result.SerializeAsString(),
            fleetright::VerifyEvacuationV2(snapshot, plan).SerializeAsString());
  EXPECT_EQ(plan.SerializeAsString(), fleetright::PlanEvacuationV2(snapshot).SerializeAsString());
}

TEST(EvacuationV2Test, SequentialDrainAcceptsTwoThroughFourDistinctWorkloads) {
  for (int count = 2; count <= 4; ++count) {
    SCOPED_TRACE(count);
    const auto snapshot = DrainFixture(count);
    const auto plan = FeasibleEvacuationPlan(snapshot);
    ASSERT_TRUE(plan.feasible()) << plan.infeasible_reason();
    EXPECT_EQ(plan.mode(), fleetright::v2::EVACUATION_PLAN_MODE_SEQUENTIAL_NODE_DRAIN);
    ASSERT_EQ(plan.operations_size(), count);
    EXPECT_EQ(plan.profile_bindings_size(), count);
    EXPECT_EQ(plan.required_runtime_capabilities_size(), 10);
    ASSERT_EQ(plan.plan_actions_size(), 2);
    EXPECT_EQ(plan.plan_actions(0).sequence(), 0U);
    EXPECT_EQ(plan.plan_actions(0).stage(), fleetright::v2::EVACUATION_STAGE_CORDON_SOURCE_NODE);
    EXPECT_EQ(plan.plan_actions(0).depends_on_sequences_size(), 0);
    EXPECT_EQ(plan.plan_actions(1).sequence(), static_cast<unsigned>(count + 1));
    EXPECT_EQ(plan.plan_actions(1).stage(),
              fleetright::v2::EVACUATION_STAGE_VERIFY_SOURCE_NODE_EMPTY);
    ASSERT_EQ(plan.plan_actions(1).depends_on_sequences_size(), 1);
    EXPECT_EQ(plan.plan_actions(1).depends_on_sequences(0), static_cast<unsigned>(count));
    for (const auto &action : plan.plan_actions()) {
      EXPECT_EQ(action.source_node_id(), "node-0");
      EXPECT_EQ(action.source_node_uid(), "node-uid-0");
    }
    for (int i = 0; i < count; ++i) {
      const auto &operation = plan.operations(i);
      EXPECT_EQ(operation.sequence(), static_cast<unsigned>(i));
      EXPECT_EQ(operation.source_node_id(), "node-0");
      EXPECT_EQ(operation.source_node_uid(), "node-uid-0");
      EXPECT_EQ(operation.baseline_replicas(), 1U);
      EXPECT_EQ(operation.final_desired_replicas(), 2U);
      EXPECT_EQ(operation.maximum_live_replicas(), 3U);
      EXPECT_EQ(operation.maximum_concurrent_disruptions(), 1U);
      EXPECT_TRUE(operation.destinations().require_hard_pod_anti_affinity());
      EXPECT_EQ(operation.destinations().allowed_node_ids_size(), 3);
      EXPECT_EQ(operation.deactivate_source_node_after_empty(), i == count - 1);
      EXPECT_EQ(operation.actions_size(), i == count - 1 ? 6 : 5);
      ASSERT_EQ(operation.depends_on_operation_sequences_size(), i == 0 ? 0 : 1);
      ASSERT_EQ(operation.depends_on_plan_action_sequences_size(), i == 0 ? 1 : 0);
      if (i == 0) {
        EXPECT_EQ(operation.depends_on_plan_action_sequences(0), 0U);
      }
      if (i > 0) {
        EXPECT_EQ(operation.depends_on_operation_sequences(0), static_cast<unsigned>(i - 1));
      }
    }
    const auto result = fleetright::VerifyEvacuationV2(snapshot, plan);
    EXPECT_TRUE(result.accepted()) << result.DebugString();
    EXPECT_FALSE(result.execution_admitted());
    EXPECT_EQ(plan.SerializeAsString(), fleetright::PlanEvacuationV2(snapshot).SerializeAsString());
    EXPECT_EQ(result.SerializeAsString(),
              fleetright::VerifyEvacuationV2(snapshot, plan).SerializeAsString());
  }
}

TEST(EvacuationV2Test, SequentialDrainFiltersDestinationsByHardAffinityDomain) {
  const auto snapshot = DrainFixtureWithHardAffinityDomains();
  const auto plan = FeasibleEvacuationPlan(snapshot);
  ASSERT_EQ(plan.operations_size(), 4);
  for (const auto &operation : plan.operations()) {
    EXPECT_EQ(operation.destinations().allowed_node_ids_size(), 2);
    EXPECT_EQ(operation.destinations().allowed_node_ids(0), "node-2");
    EXPECT_EQ(operation.destinations().allowed_node_ids(1), "node-4");
  }
  EXPECT_TRUE(fleetright::VerifyEvacuationV2(snapshot, plan).accepted());

  auto tampered = plan;
  auto *destinations = tampered.mutable_operations(0)->mutable_destinations();
  destinations->clear_allowed_node_ids();
  destinations->add_allowed_node_ids("node-2");
  destinations->add_allowed_node_ids("node-3");
  destinations->add_allowed_node_ids("node-4");
  SealEvacuationPlan(&tampered);
  EXPECT_FALSE(fleetright::VerifyEvacuationV2(snapshot, tampered).accepted());
}

TEST(EvacuationV2Test, SequentialDrainRejectsUncoveredSourceOccupant) {
  const auto original_snapshot = DrainFixture();
  auto plan = FeasibleEvacuationPlan(original_snapshot);
  const auto final_action = plan.operations(3).actions(5);
  plan.mutable_operations()->RemoveLast();
  plan.mutable_profile_bindings()->RemoveLast();
  auto *last = plan.mutable_operations(2);
  last->set_deactivate_source_node_after_empty(true);
  *last->add_actions() = final_action;
  // Keep the plan-level action envelope valid for three operations so rejection
  // still proves that the fourth source occupant cannot be left uncovered.
  plan.mutable_plan_actions(1)->set_sequence(4);
  plan.mutable_plan_actions(1)->set_depends_on_sequences(0, 3);
  SealEvacuationPlan(&plan);
  EXPECT_FALSE(fleetright::VerifyEvacuationV2(original_snapshot, plan).accepted());

  plan = FeasibleEvacuationPlan(original_snapshot);
  const auto snapshot = DrainFixture(5);
  EXPECT_FALSE(fleetright::PlanEvacuationV2(snapshot).feasible());
  plan.set_snapshot_sha256(snapshot.content_sha256());
  SealEvacuationPlan(&plan);
  EXPECT_FALSE(fleetright::VerifyEvacuationV2(snapshot, plan).accepted());
}

TEST(EvacuationV2Test, SequentialDrainRejectsPlanActionIdentityAndOrderTampering) {
  const auto snapshot = DrainFixture();
  const auto original = FeasibleEvacuationPlan(snapshot);
  for (int variant = 0; variant < 10; ++variant) {
    SCOPED_TRACE(variant);
    auto plan = original;
    switch (variant) {
    case 0:
      plan.clear_plan_actions();
      break;
    case 1:
      plan.mutable_plan_actions()->SwapElements(0, 1);
      break;
    case 2:
      plan.mutable_plan_actions(0)->set_source_node_uid("wrong-node-uid");
      break;
    case 3:
      plan.mutable_plan_actions(1)->set_source_node_id("node-1");
      break;
    case 4:
      plan.mutable_plan_actions(1)->set_depends_on_sequences(0, 1);
      break;
    case 5:
      plan.mutable_plan_actions(0)->set_sequence(1);
      break;
    case 6:
      plan.mutable_plan_actions(1)->set_sequence(4);
      break;
    case 7:
      plan.mutable_operations(0)->clear_depends_on_plan_action_sequences();
      break;
    case 8:
      plan.mutable_plan_actions(0)->set_expected_postcondition("cordon is optional");
      break;
    case 9:
      plan.mutable_required_runtime_capabilities()->RemoveLast();
      break;
    }
    SealEvacuationPlan(&plan);
    EXPECT_FALSE(fleetright::VerifyEvacuationV2(snapshot, plan).accepted());
  }
}

TEST(EvacuationV2Test, SequentialDrainActionBudgetIncludesCordonAndFinalVerification) {
  auto snapshot = DrainFixture();
  snapshot.mutable_model_snapshot()->mutable_policy()->set_maximum_plan_actions(23);
  SealEvacuationSnapshot(&snapshot);
  auto plan = FeasibleEvacuationPlan(snapshot);
  EXPECT_TRUE(fleetright::VerifyEvacuationV2(snapshot, plan).accepted());

  snapshot.mutable_model_snapshot()->mutable_policy()->set_maximum_plan_actions(22);
  SealEvacuationSnapshot(&snapshot);
  EXPECT_FALSE(fleetright::PlanEvacuationV2(snapshot).feasible());
  plan.set_snapshot_sha256(snapshot.content_sha256());
  plan.set_policy_sha256(fleetright::MessageSha256(snapshot.model_snapshot().policy()));
  SealEvacuationPlan(&plan);
  EXPECT_FALSE(fleetright::VerifyEvacuationV2(snapshot, plan).accepted());
}

TEST(EvacuationV2Test, SingleSourceRejectsPlanLevelNodeActionsAndDependencies) {
  const auto snapshot = EvacuationFixture();
  const auto original = FeasibleEvacuationPlan(snapshot);
  const auto batch = FeasibleEvacuationPlan(DrainFixture());
  for (int variant = 0; variant < 4; ++variant) {
    SCOPED_TRACE(variant);
    auto plan = original;
    if (variant < 2)
      *plan.add_plan_actions() = batch.plan_actions(variant);
    else if (variant == 2)
      plan.mutable_operations(0)->add_depends_on_plan_action_sequences(0);
    else {
      plan.mutable_operations(0)->set_deactivate_source_node_after_empty(true);
      *plan.mutable_operations(0)->add_actions() = batch.operations(3).actions(5);
    }
    SealEvacuationPlan(&plan);
    EXPECT_FALSE(fleetright::VerifyEvacuationV2(snapshot, plan).accepted());
  }
}

TEST(EvacuationV2Test, SequentialDrainRejectsCumulativeDestinationOvercapacity) {
  auto snapshot = DrainFixture();
  auto plan = FeasibleEvacuationPlan(snapshot);
  // Every individual operation's two replacements fit 500m, while several
  // earlier completed workloads can consume that same allowed destination.
  SetResource(snapshot.mutable_model_snapshot()->mutable_nodes(1)->mutable_reserved(), 1'500, 0, 0);
  SealEvacuationSnapshot(&snapshot);
  EXPECT_FALSE(fleetright::PlanEvacuationV2(snapshot).feasible());
  plan.set_snapshot_sha256(snapshot.content_sha256());
  SealEvacuationPlan(&plan);
  const auto result = fleetright::VerifyEvacuationV2(snapshot, plan);
  EXPECT_FALSE(result.accepted());
  const auto cumulative = std::find_if(
      result.invariants().begin(), result.invariants().end(), [](const auto &invariant) {
        return invariant.invariant() == "v2_sequential_destination_outcomes_and_final_empty";
      });
  ASSERT_NE(cumulative, result.invariants().end());
  EXPECT_FALSE(cumulative->passed());
}

TEST(EvacuationV2Test, SequentialDrainRejectsOrderAndAuthorityTampering) {
  const auto snapshot = DrainFixture();
  const auto original = FeasibleEvacuationPlan(snapshot);
  for (int variant = 0; variant < 11; ++variant) {
    SCOPED_TRACE(variant);
    auto plan = original;
    switch (variant) {
    case 0:
      plan.mutable_operations()->SwapElements(0, 1);
      break;
    case 1:
      plan.mutable_operations(2)->set_depends_on_operation_sequences(0, 0);
      break;
    case 2:
      plan.mutable_operations(1)->set_source_node_uid("different-node-uid");
      break;
    case 3:
      plan.mutable_operations(1)->set_source_pod_uid("different-pod-uid");
      break;
    case 4:
      plan.set_mode(fleetright::v2::EVACUATION_PLAN_MODE_SINGLE_SOURCE);
      break;
    case 5:
      plan.mutable_operations(0)->set_deactivate_source_node_after_empty(true);
      break;
    case 6:
      plan.mutable_operations(0)->mutable_destinations()->set_require_hard_pod_anti_affinity(false);
      break;
    case 7:
      plan.mutable_operations(0)->mutable_actions()->SwapElements(2, 3);
      break;
    case 8:
      plan.mutable_required_runtime_capabilities()->RemoveLast();
      break;
    case 9:
      plan.mutable_operations(1)->set_workload_id(plan.operations(0).workload_id());
      break;
    case 10:
      plan.set_mode(static_cast<fleetright::v2::EvacuationPlanMode>(99));
      break;
    }
    SealEvacuationPlan(&plan);
    EXPECT_FALSE(fleetright::VerifyEvacuationV2(snapshot, plan).accepted());
  }
}

TEST(EvacuationV2Test, SequentialDrainRejectsTopologyDomainCollocationAndNMinusOne) {
  auto snapshot = DrainFixture();
  auto plan = FeasibleEvacuationPlan(snapshot);
  snapshot.mutable_model_snapshot()->mutable_nodes(2)->mutable_labels(1)->set_value("node-1");
  SealEvacuationSnapshot(&snapshot);
  EXPECT_FALSE(fleetright::PlanEvacuationV2(snapshot).feasible());
  plan.set_snapshot_sha256(snapshot.content_sha256());
  SealEvacuationPlan(&plan);
  EXPECT_FALSE(fleetright::VerifyEvacuationV2(snapshot, plan).accepted());

  snapshot = DrainFixture();
  plan = FeasibleEvacuationPlan(snapshot);
  snapshot.mutable_model_snapshot()->mutable_workloads(0)->set_failure_tolerance(
      fleetright::v1::FAILURE_TOLERANCE_N_MINUS_1);
  SealEvacuationSnapshot(&snapshot);
  EXPECT_FALSE(fleetright::PlanEvacuationV2(snapshot).feasible());
  plan.set_snapshot_sha256(snapshot.content_sha256());
  SealEvacuationPlan(&plan);
  EXPECT_FALSE(fleetright::VerifyEvacuationV2(snapshot, plan).accepted());
}

TEST(EvacuationV2Test, RejectsInsufficientSurgeCapacity) {
  auto snapshot = EvacuationFixture();
  for (int index = 3; index <= 4; ++index)
    SetResource(snapshot.mutable_model_snapshot()->mutable_nodes(index)->mutable_reserved(), 1'500,
                3'500, 500);
  snapshot.mutable_model_snapshot()->mutable_nodes(5)->set_active(false);
  SealEvacuationSnapshot(&snapshot);
  const auto plan = fleetright::PlanEvacuationV2(snapshot);
  EXPECT_FALSE(plan.feasible());
  EXPECT_NE(plan.infeasible_reason().find("capacity"), std::string::npos);
  EXPECT_FALSE(fleetright::VerifyEvacuationV2(snapshot, plan).accepted());
}

TEST(EvacuationV2Test, RejectsUnenforceableExactDestination) {
  const auto snapshot = EvacuationFixture();
  auto plan = FeasibleEvacuationPlan(snapshot);
  auto *destination = plan.mutable_operations(0)->mutable_destinations();
  destination->set_mode(fleetright::v2::DESTINATION_MODE_EXACT_NODE);
  destination->set_exact_destination_node_id(destination->allowed_node_ids(0));
  SealEvacuationPlan(&plan);
  const auto result = fleetright::VerifyEvacuationV2(snapshot, plan);
  EXPECT_FALSE(result.accepted());
}

TEST(EvacuationV2Test, FourNodesCannotReplaceWorkloadWideHardAntiAffinityWithOccupiedNodes) {
  auto snapshot = EvacuationFixture();
  snapshot.mutable_model_snapshot()->mutable_nodes()->DeleteSubrange(4, 2);
  snapshot.mutable_nodes()->DeleteSubrange(4, 2);
  SealEvacuationSnapshot(&snapshot);

  // Three existing replicas leave only one eligible Node. Spare CPU on the occupied
  // Nodes does not make them eligible under the proposed hard Pod anti-affinity.
  const auto plan = fleetright::PlanEvacuationV2(snapshot);
  EXPECT_FALSE(plan.feasible());
  EXPECT_FALSE(fleetright::VerifyEvacuationV2(snapshot, plan).accepted());
}

TEST(EvacuationV2Test, IndependentVerifierRejectsOccupiedHardAntiAffinityDestination) {
  const auto snapshot = EvacuationFixture();
  auto plan = FeasibleEvacuationPlan(snapshot);
  auto *operation = plan.mutable_operations(0);
  operation->mutable_destinations()->clear_allowed_node_ids();
  operation->mutable_destinations()->add_allowed_node_ids("node-1");
  operation->mutable_destinations()->add_allowed_node_ids("node-3");
  operation->clear_recovery_destination_node_ids();
  operation->add_recovery_destination_node_ids("node-1");
  operation->add_recovery_destination_node_ids("node-3");
  SealEvacuationPlan(&plan);

  // A replica plus a replacement fits node-1's resources, but the existing api-1
  // makes this destination incompatible with the plan's hard anti-affinity.
  const auto result = fleetright::VerifyEvacuationV2(snapshot, plan);
  EXPECT_FALSE(result.accepted());
  const auto contract = std::find_if(
      result.invariants().begin(), result.invariants().end(), [](const auto &invariant) {
        return invariant.invariant() == "v2_constraint_contract_and_action_order";
      });
  ASSERT_NE(contract, result.invariants().end());
  EXPECT_FALSE(contract->passed());
}

TEST(EvacuationV2Test, IndependentVerifierRejectsRemovingRequiredHardAntiAffinity) {
  const auto snapshot = EvacuationFixture();
  auto plan = FeasibleEvacuationPlan(snapshot);
  plan.mutable_operations(0)->mutable_destinations()->set_require_hard_pod_anti_affinity(false);
  SealEvacuationPlan(&plan);
  EXPECT_FALSE(fleetright::VerifyEvacuationV2(snapshot, plan).accepted());
}

TEST(EvacuationV2Test, IndependentVerifierRejectsDestinationResourceAndTopologyDrift) {
  auto original = EvacuationFixture();
  auto *workload = original.mutable_model_snapshot()->mutable_workloads(0);
  workload->set_topology_key("zone");
  workload->set_min_topology_domains(2);
  workload->set_max_topology_skew(1);
  SealEvacuationSnapshot(&original);
  const auto original_plan = FeasibleEvacuationPlan(original);
  ASSERT_TRUE(fleetright::VerifyEvacuationV2(original, original_plan).accepted());

  for (const bool resource_drift : {true, false}) {
    SCOPED_TRACE(resource_drift ? "resource capacity" : "topology skew");
    auto snapshot = original;
    auto *node = snapshot.mutable_model_snapshot()->mutable_nodes(4);
    if (resource_drift)
      SetResource(node->mutable_reserved(), 1'500, 0, 0);
    else
      node->mutable_labels(0)->set_value("zone-1");
    SealEvacuationSnapshot(&snapshot);
    EXPECT_FALSE(fleetright::PlanEvacuationV2(snapshot).feasible());

    auto forged = original_plan;
    forged.set_snapshot_sha256(snapshot.content_sha256());
    SealEvacuationPlan(&forged);
    const auto result = fleetright::VerifyEvacuationV2(snapshot, forged);
    EXPECT_FALSE(result.accepted());
    const auto outcomes = std::find_if(
        result.invariants().begin(), result.invariants().end(), [](const auto &invariant) {
          return invariant.invariant() == "v2_all_intermediate_and_final_outcomes";
        });
    ASSERT_NE(outcomes, result.invariants().end());
    EXPECT_FALSE(outcomes->passed());
  }
}

TEST(EvacuationV2Test, RejectsIntermediateAvailabilityAndActionOrderViolations) {
  const auto snapshot = EvacuationFixture();
  auto plan = FeasibleEvacuationPlan(snapshot);
  plan.mutable_operations(0)->mutable_permitted_intermediate_states(1)->set_minimum_ready_replicas(
      1);
  SealEvacuationPlan(&plan);
  EXPECT_FALSE(fleetright::VerifyEvacuationV2(snapshot, plan).accepted());

  plan = FeasibleEvacuationPlan(snapshot);
  auto *actions = plan.mutable_operations(0)->mutable_actions();
  actions->SwapElements(2, 3);
  SealEvacuationPlan(&plan);
  EXPECT_FALSE(fleetright::VerifyEvacuationV2(snapshot, plan).accepted());
}

TEST(EvacuationV2Test, RejectsUnsupportedSchemaUnknownFieldsAndTampering) {
  const auto snapshot = EvacuationFixture();
  auto plan = FeasibleEvacuationPlan(snapshot);
  plan.set_schema_version(7);
  SealEvacuationPlan(&plan);
  EXPECT_FALSE(fleetright::VerifyEvacuationV2(snapshot, plan).accepted());

  plan = FeasibleEvacuationPlan(snapshot);
  plan.GetReflection()->MutableUnknownFields(&plan)->AddVarint(101, 1);
  SealEvacuationPlan(&plan);
  EXPECT_FALSE(fleetright::VerifyEvacuationV2(snapshot, plan).accepted());

  plan = FeasibleEvacuationPlan(snapshot);
  plan.mutable_operations(0)->set_maximum_live_replicas(99);
  EXPECT_FALSE(fleetright::VerifyEvacuationV2(snapshot, plan).accepted());
}

TEST(EvacuationV2Test, RejectsDuplicateNodeUids) {
  auto snapshot = EvacuationFixture();
  auto plan = FeasibleEvacuationPlan(snapshot);
  snapshot.mutable_nodes(1)->set_node_uid(snapshot.nodes(0).node_uid());
  SealEvacuationSnapshot(&snapshot);
  plan.set_snapshot_sha256(snapshot.content_sha256());
  SealEvacuationPlan(&plan);

  EXPECT_FALSE(fleetright::PlanEvacuationV2(snapshot).feasible());
  const auto result = fleetright::VerifyEvacuationV2(snapshot, plan);
  EXPECT_FALSE(result.accepted());
  const auto snapshot_check = std::find_if(
      result.invariants().begin(), result.invariants().end(), [](const auto &invariant) {
        return invariant.invariant() == "v2_snapshot_identity_inventory_and_hash";
      });
  ASSERT_NE(snapshot_check, result.invariants().end());
  EXPECT_FALSE(snapshot_check->passed());
}

TEST(EvacuationV2Test, RejectsControllerNamespaceAndOwnerContractMismatches) {
  auto snapshot = EvacuationFixture();
  snapshot.mutable_controllers(0)->set_namespace_("other-namespace");
  SealEvacuationSnapshot(&snapshot);
  EXPECT_FALSE(fleetright::PlanEvacuationV2(snapshot).feasible());

  snapshot = EvacuationFixture();
  snapshot.mutable_pods(0)->set_owner_uid("different-workload-uid");
  SealEvacuationSnapshot(&snapshot);
  EXPECT_FALSE(fleetright::PlanEvacuationV2(snapshot).feasible());

  snapshot = EvacuationFixture();
  snapshot.mutable_controllers(0)->set_owner_kind("Deployment");
  SealEvacuationSnapshot(&snapshot);
  EXPECT_FALSE(fleetright::PlanEvacuationV2(snapshot).feasible());

  snapshot = EvacuationFixture();
  snapshot.set_owner_contract_version(0);
  SealEvacuationSnapshot(&snapshot);
  EXPECT_FALSE(fleetright::PlanEvacuationV2(snapshot).feasible());

  const std::vector<std::function<void(fleetright::v2::EvacuationSnapshot &)>> legacy_fields = {
      [](auto &value) { value.mutable_controllers(0)->set_deployment_name("legacy"); },
      [](auto &value) { value.mutable_controllers(0)->set_deployment_uid("legacy"); },
      [](auto &value) { value.mutable_controllers(0)->set_deployment_resource_version("legacy"); },
      [](auto &value) { value.mutable_controllers(0)->set_deployment_generation(1); },
      [](auto &value) { value.mutable_controllers(0)->set_replicaset_name("legacy"); },
      [](auto &value) { value.mutable_controllers(0)->set_replicaset_uid("legacy"); },
      [](auto &value) { value.mutable_controllers(0)->set_replicaset_resource_version("legacy"); },
      [](auto &value) { value.mutable_controllers(0)->set_active_replicaset_count(1); },
      [](auto &value) { value.mutable_pods(0)->set_controller_uid("legacy"); },
  };
  for (std::size_t index = 0; index < legacy_fields.size(); ++index) {
    snapshot = EvacuationFixture();
    legacy_fields[index](snapshot);
    SealEvacuationSnapshot(&snapshot);
    EXPECT_FALSE(fleetright::PlanEvacuationV2(snapshot).feasible()) << index;
  }
}

TEST(EvacuationV2Test, IndependentVerifierRejectsOwnerContractDrift) {
  const auto snapshot = EvacuationFixture();
  const auto plan = FeasibleEvacuationPlan(snapshot);

  auto stale_owner = snapshot;
  stale_owner.mutable_controllers(0)->set_owner_uid("replacement-workload-uid");
  for (auto &pod : *stale_owner.mutable_pods())
    pod.set_owner_uid("replacement-workload-uid");
  SealEvacuationSnapshot(&stale_owner);
  auto stale_owner_plan = plan;
  stale_owner_plan.set_snapshot_sha256(stale_owner.content_sha256());
  SealEvacuationPlan(&stale_owner_plan);
  const auto owner_drift = fleetright::VerifyEvacuationV2(stale_owner, stale_owner_plan);
  EXPECT_FALSE(owner_drift.accepted());
  const auto snapshot_check = std::find_if(
      owner_drift.invariants().begin(), owner_drift.invariants().end(), [](const auto &invariant) {
        return invariant.invariant() == "v2_snapshot_identity_inventory_and_hash";
      });
  ASSERT_NE(snapshot_check, owner_drift.invariants().end());
  EXPECT_TRUE(snapshot_check->passed());
  const auto plan_check = std::find_if(
      owner_drift.invariants().begin(), owner_drift.invariants().end(), [](const auto &invariant) {
        return invariant.invariant() == "v2_plan_canonical_binding_and_replay";
      });
  ASSERT_NE(plan_check, owner_drift.invariants().end());
  EXPECT_TRUE(plan_check->passed());
  const auto source_check = std::find_if(
      owner_drift.invariants().begin(), owner_drift.invariants().end(), [](const auto &invariant) {
        return invariant.invariant() == "v2_source_pod_owner_and_node_uid";
      });
  ASSERT_NE(source_check, owner_drift.invariants().end());
  EXPECT_FALSE(source_check->passed());

  auto legacy_owner = snapshot;
  legacy_owner.mutable_controllers(0)->set_replicaset_uid("legacy-replicaset-uid");
  SealEvacuationSnapshot(&legacy_owner);
  auto legacy_owner_plan = plan;
  legacy_owner_plan.set_snapshot_sha256(legacy_owner.content_sha256());
  SealEvacuationPlan(&legacy_owner_plan);
  const auto legacy_result = fleetright::VerifyEvacuationV2(legacy_owner, legacy_owner_plan);
  EXPECT_FALSE(legacy_result.accepted());
  const auto legacy_snapshot_check =
      std::find_if(legacy_result.invariants().begin(), legacy_result.invariants().end(),
                   [](const auto &invariant) {
                     return invariant.invariant() == "v2_snapshot_identity_inventory_and_hash";
                   });
  ASSERT_NE(legacy_snapshot_check, legacy_result.invariants().end());
  EXPECT_FALSE(legacy_snapshot_check->passed());

  auto legacy_operation_plan = plan;
  legacy_operation_plan.mutable_operations(0)->set_source_controller_uid("legacy");
  SealEvacuationPlan(&legacy_operation_plan);
  const auto operation_result = fleetright::VerifyEvacuationV2(snapshot, legacy_operation_plan);
  EXPECT_FALSE(operation_result.accepted());
  const auto operation_source_check =
      std::find_if(operation_result.invariants().begin(), operation_result.invariants().end(),
                   [](const auto &invariant) {
                     return invariant.invariant() == "v2_source_pod_owner_and_node_uid";
                   });
  ASSERT_NE(operation_source_check, operation_result.invariants().end());
  EXPECT_FALSE(operation_source_check->passed());
}

TEST(EvacuationV2Test, RejectsTopologyThatCannotSurviveTheMove) {
  auto snapshot = EvacuationFixture();
  auto *model = snapshot.mutable_model_snapshot();
  auto *workload = model->mutable_workloads(0);
  workload->set_topology_key("zone");
  workload->set_min_topology_domains(4);
  workload->set_max_topology_skew(1);
  model->mutable_nodes(4)->mutable_labels(0)->set_value("zone-3");
  model->mutable_nodes(5)->set_active(false);
  SealEvacuationSnapshot(&snapshot);
  const auto plan = fleetright::PlanEvacuationV2(snapshot);
  EXPECT_FALSE(plan.feasible());
}

TEST(EvacuationV2Test, RejectsInsufficientOccupiedTopologyDomains) {
  auto snapshot = EvacuationFixture();
  auto *model = snapshot.mutable_model_snapshot();
  auto *workload = model->mutable_workloads(0);
  workload->set_min_topology_domains(4);
  workload->set_max_topology_skew(2);
  model->mutable_nodes(4)->mutable_labels(0)->set_value("zone-3");
  SealEvacuationSnapshot(&snapshot);
  EXPECT_FALSE(fleetright::PlanEvacuationV2(snapshot).feasible());
}

TEST(EvacuationV2Test, RejectsNMinusOneFailureDuringAnIntermediateStage) {
  auto snapshot = EvacuationFixture();
  snapshot.mutable_model_snapshot()->mutable_workloads(0)->set_demand_qps_milli(1'400'000);
  SealEvacuationSnapshot(&snapshot);
  const auto no_n1_plan = FeasibleEvacuationPlan(snapshot);

  snapshot.mutable_model_snapshot()->mutable_workloads(0)->set_failure_tolerance(
      fleetright::v1::FAILURE_TOLERANCE_N_MINUS_1);
  SealEvacuationSnapshot(&snapshot);
  EXPECT_FALSE(fleetright::PlanEvacuationV2(snapshot).feasible());

  auto forged = no_n1_plan;
  forged.set_snapshot_sha256(snapshot.content_sha256());
  forged.mutable_operations(0)->set_failure_tolerance(fleetright::v1::FAILURE_TOLERANCE_N_MINUS_1);
  SealEvacuationPlan(&forged);
  EXPECT_FALSE(fleetright::VerifyEvacuationV2(snapshot, forged).accepted());
}

TEST(EvacuationV2Test, RejectsUnsafeNMinusOneRecoveryArrivalOrder) {
  auto snapshot = EvacuationFixture();
  auto *model = snapshot.mutable_model_snapshot();
  auto *workload = model->mutable_workloads(0);
  workload->set_min_replicas(1);
  workload->set_availability_floor(1);
  workload->set_demand_qps_milli(100'000);
  workload->set_concurrency_bound(50);
  workload->set_topology_key("zone");
  workload->set_min_topology_domains(1);
  workload->set_max_topology_skew(2);
  const std::vector<std::string> zones = {"D", "C", "C", "B", "B", "A"};
  for (int index = 0; index < model->nodes_size(); ++index)
    model->mutable_nodes(index)->mutable_labels(0)->set_value(zones[index]);
  for (int index = 6; index < 8; ++index) {
    auto *node = model->add_nodes();
    node->set_node_id("node-" + std::to_string(index));
    node->set_machine_type("small");
    SetResource(node->mutable_allocatable(), 2'000, 4'096, 1'000);
    SetResource(node->mutable_reserved(), 0, 0, 0);
    node->set_hourly_cost_micros(50'000);
    node->set_health(fleetright::v1::NODE_HEALTH_HEALTHY);
    node->set_active(true);
    node->set_generation(4);
    auto *label = node->add_labels();
    label->set_key("zone");
    label->set_value("A");
    auto *identity = snapshot.add_nodes();
    identity->set_node_id(node->node_id());
    identity->set_node_uid(node->node_id() + "-uid");
    identity->set_resource_version(node->node_id() + "-rv");
  }
  SealEvacuationSnapshot(&snapshot);
  const auto no_n1_plan = FeasibleEvacuationPlan(snapshot);
  ASSERT_EQ(no_n1_plan.operations_size(), 1);

  model = snapshot.mutable_model_snapshot();
  workload = model->mutable_workloads(0);
  workload->set_failure_tolerance(fleetright::v1::FAILURE_TOLERANCE_N_MINUS_1);
  SealEvacuationSnapshot(&snapshot);
  EXPECT_FALSE(fleetright::PlanEvacuationV2(snapshot).feasible());
  auto forged = no_n1_plan;
  forged.set_snapshot_sha256(snapshot.content_sha256());
  forged.mutable_operations(0)->set_failure_tolerance(fleetright::v1::FAILURE_TOLERANCE_N_MINUS_1);
  SealEvacuationPlan(&forged);
  EXPECT_FALSE(fleetright::VerifyEvacuationV2(snapshot, forged).accepted());
}

TEST(EvacuationV2Test, RecomputesProfileEvidenceAndRejectsTamperedPodInventory) {
  const auto good_snapshot = EvacuationFixture();
  const auto good_plan = FeasibleEvacuationPlan(good_snapshot);

  auto false_evidence = good_snapshot;
  auto *profile = false_evidence.mutable_model_snapshot()->mutable_capacity_profiles(0);
  profile->mutable_safe_points(0)->set_threshold_successes(0);
  SealProfile(profile);
  SealEvacuationSnapshot(&false_evidence);
  auto profile_plan = good_plan;
  profile_plan.set_snapshot_sha256(false_evidence.content_sha256());
  profile_plan.mutable_profile_bindings(0)->set_profile_sha256(profile->profile_sha256());
  SealEvacuationPlan(&profile_plan);
  EXPECT_FALSE(fleetright::PlanEvacuationV2(false_evidence).feasible());
  EXPECT_FALSE(fleetright::VerifyEvacuationV2(false_evidence, profile_plan).accepted());

  auto duplicated = good_snapshot;
  auto *replacement = duplicated.mutable_pods(2);
  replacement->set_workload_id(duplicated.pods(0).workload_id());
  replacement->set_replica_id(duplicated.pods(0).replica_id());
  replacement->set_node_id(duplicated.pods(0).node_id());
  replacement->set_pod_name("duplicate-source-name");
  replacement->set_pod_uid("duplicate-source-uid");
  SealEvacuationSnapshot(&duplicated);
  auto pod_plan = good_plan;
  pod_plan.set_snapshot_sha256(duplicated.content_sha256());
  SealEvacuationPlan(&pod_plan);
  EXPECT_FALSE(fleetright::PlanEvacuationV2(duplicated).feasible());
  EXPECT_FALSE(fleetright::VerifyEvacuationV2(duplicated, pod_plan).accepted());
}

TEST(EvacuationV2Test, RejectsUnboundReadinessAndPolicyBudgets) {
  const auto snapshot = EvacuationFixture();
  auto plan = FeasibleEvacuationPlan(snapshot);
  plan.mutable_operations(0)->set_readiness_stabilization_ms(0);
  SealEvacuationPlan(&plan);
  EXPECT_FALSE(fleetright::VerifyEvacuationV2(snapshot, plan).accepted());

  plan = FeasibleEvacuationPlan(snapshot);
  plan.mutable_operations(0)->set_workload_id("unknown-workload");
  SealEvacuationPlan(&plan);
  EXPECT_FALSE(fleetright::VerifyEvacuationV2(snapshot, plan).accepted());

  auto no_moves = snapshot;
  no_moves.mutable_model_snapshot()->mutable_workloads(0)->set_max_moves(0);
  SealEvacuationSnapshot(&no_moves);
  EXPECT_FALSE(fleetright::PlanEvacuationV2(no_moves).feasible());

  auto no_actions = snapshot;
  no_actions.mutable_model_snapshot()->mutable_policy()->set_maximum_plan_actions(4);
  SealEvacuationSnapshot(&no_actions);
  EXPECT_FALSE(fleetright::PlanEvacuationV2(no_actions).feasible());
}

TEST(EvacuationV2Test, RequiresNMinusOneRecoveryCapacity) {
  auto snapshot = EvacuationFixture();
  snapshot.mutable_model_snapshot()->mutable_workloads(0)->set_failure_tolerance(
      fleetright::v1::FAILURE_TOLERANCE_N_MINUS_1);
  SealEvacuationSnapshot(&snapshot);
  const auto feasible = fleetright::PlanEvacuationV2(snapshot);
  ASSERT_TRUE(feasible.feasible()) << feasible.DebugString();
  EXPECT_EQ(feasible.operations(0).n_minus_1_failure_domain_key(), "zone");
  EXPECT_TRUE(fleetright::VerifyEvacuationV2(snapshot, feasible).accepted());

  auto missing_failure_key = feasible;
  missing_failure_key.mutable_operations(0)->clear_n_minus_1_failure_domain_key();
  SealEvacuationPlan(&missing_failure_key);
  EXPECT_FALSE(fleetright::VerifyEvacuationV2(snapshot, missing_failure_key).accepted());

  auto insufficient = snapshot;
  SetResource(insufficient.mutable_model_snapshot()->mutable_nodes(5)->mutable_reserved(), 2'000,
              4'096, 1'000);
  SealEvacuationSnapshot(&insufficient);
  EXPECT_FALSE(fleetright::PlanEvacuationV2(insufficient).feasible());
}

TEST(UnitsTest, UsesDistinctUnitTypesAndChecksOverflow) {
  using fleetright::CheckedAdd;
  using fleetright::CpuMillicores;
  using fleetright::MemoryMiB;
  using fleetright::NetworkMbps;
  using fleetright::ResourceVector;
  EXPECT_TRUE((CheckedAdd({CpuMillicores(100), MemoryMiB(200), NetworkMbps(10)},
                          {CpuMillicores(20), MemoryMiB(30), NetworkMbps(5)}))
                  .has_value());
  EXPECT_FALSE((CheckedAdd({CpuMillicores(std::numeric_limits<std::int64_t>::max()), MemoryMiB(0),
                            NetworkMbps(0)},
                           {CpuMillicores(1), MemoryMiB(0), NetworkMbps(0)}))
                   .has_value());
  static_assert(!std::is_same_v<CpuMillicores, MemoryMiB>);
  EXPECT_FALSE(fleetright::Fits(ResourceVector{CpuMillicores(-1), MemoryMiB(0), NetworkMbps(0)},
                                ResourceVector{CpuMillicores(1), MemoryMiB(1), NetworkMbps(1)}));
}

TEST(UnitsTest, CeilingAndPpmFactorsRoundDownOnce) {
  EXPECT_EQ(fleetright::CeilDivide(10, 3), 4U);
  EXPECT_EQ(fleetright::CeilDivide(0, 5), 0U);
  EXPECT_EQ(fleetright::CeilDivide(1, 0), std::numeric_limits<std::uint64_t>::max());
  EXPECT_EQ(fleetright::MultiplyPpmFloor(1'000'000, 950'000, 950'000, 1'000'000),
            std::optional<std::uint64_t>(902'500));
  EXPECT_FALSE(fleetright::MultiplyPpmFloor(1, 0, 1'000'000, 1'000'000));
  EXPECT_FALSE(fleetright::MultiplyPpmFloor(1, 1'000'001, 1, 1));
}

TEST(PlannerVerifierTest, ProducesDeterministicVerifiedLowerCostPlan) {
  const auto snapshot = Fixture();
  const auto first = fleetright::Plan(snapshot);
  const auto second = fleetright::Plan(snapshot);
  EXPECT_TRUE(first.feasible()) << first.infeasible_reason();
  EXPECT_EQ(first.SerializeAsString(), second.SerializeAsString());
  EXPECT_EQ(first.workload_decisions(0).proposed_replicas(), 2U);
  EXPECT_LT(first.proposed_cost_micros_per_hour(), first.current_cost_micros_per_hour());
  EXPECT_EQ(first.current_objective_cost_micros(), first.current_cost_micros_per_hour());
  EXPECT_EQ(first.proposed_objective_cost_micros(), first.proposed_cost_micros_per_hour());
  EXPECT_EQ(first.workload_decisions(0).profile_point_index(), 0U);
  EXPECT_EQ(first.workload_decisions(0).boundary_risk_ppm(), 900'000U);
  const auto result = fleetright::Verify(snapshot, first);
  EXPECT_TRUE(result.accepted());
  EXPECT_FALSE(result.canonical_result_sha256().empty());

  auto invalid = first;
  invalid.mutable_placements(1)->set_node_id("missing-node");
  invalid.clear_plan_id();
  invalid.set_plan_id(fleetright::MessageSha256(invalid));
  const auto rejected = fleetright::Verify(snapshot, invalid);
  EXPECT_FALSE(rejected.accepted());
}

TEST(PlannerVerifierTest, RejectsAPlanWhoseRiskPenaltyErasesItsSavings) {
  auto snapshot = Fixture();
  snapshot.mutable_policy()->set_boundary_cost_micros_per_workload(1'000'000);
  SealSnapshot(&snapshot);
  const auto plan = fleetright::Plan(snapshot);
  ASSERT_TRUE(plan.feasible());
  EXPECT_EQ(plan.action_count(), 0U);
  EXPECT_EQ(plan.workload_decisions(0).proposed_replicas(), 3U);
  ASSERT_EQ(plan.rejection_codes_size(), 1);
  EXPECT_EQ(plan.rejection_codes(0), fleetright::v1::REJECTION_CODE_INSUFFICIENT_IMPROVEMENT);
  EXPECT_TRUE(fleetright::Verify(snapshot, plan).accepted());
}

TEST(PlannerVerifierTest, MinimumImprovementCannotSuppressRequiredCapacityRepair) {
  auto snapshot = Fixture();
  snapshot.mutable_workloads(0)->set_demand_qps_milli(2'000'000);
  snapshot.mutable_policy()->set_movement_cost_micros(0);
  snapshot.mutable_policy()->set_action_churn_cost_micros(0);
  snapshot.mutable_policy()->set_uncertainty_cost_micros_per_replica(0);
  snapshot.mutable_policy()->set_boundary_cost_micros_per_workload(0);
  SealSnapshot(&snapshot);

  const auto plan = fleetright::Plan(snapshot);
  ASSERT_TRUE(plan.feasible()) << plan.infeasible_reason();
  ASSERT_EQ(plan.workload_decisions_size(), 1);
  EXPECT_EQ(plan.workload_decisions(0).current_replicas(), 3U);
  EXPECT_EQ(plan.workload_decisions(0).proposed_replicas(), 4U);
  EXPECT_GT(plan.action_count(), 0U);
  EXPECT_EQ(plan.proposed_objective_cost_micros(), plan.current_objective_cost_micros());
  EXPECT_TRUE(fleetright::Verify(snapshot, plan).accepted());
}

TEST(PlannerVerifierTest, SelectsAProfileWithLowerWeightedObjective) {
  auto snapshot = Fixture();
  auto larger_profile = snapshot.capacity_profiles(0);
  larger_profile.set_profile_id("api-large-v1");
  SetResource(larger_profile.mutable_allocation(), 1'500, 1'536, 150);
  auto *point = larger_profile.mutable_safe_points(0);
  point->set_safe_qps_milli(1'000'000);
  point->set_safe_concurrency(200);
  SealProfile(&larger_profile);
  *snapshot.add_capacity_profiles() = larger_profile;
  snapshot.mutable_policy()->set_boundary_cost_micros_per_workload(1'000'000);
  SealSnapshot(&snapshot);

  const auto plan = fleetright::Plan(snapshot);
  ASSERT_TRUE(plan.feasible()) << plan.infeasible_reason();
  ASSERT_EQ(plan.workload_decisions_size(), 1);
  EXPECT_EQ(plan.workload_decisions(0).capacity_profile_id(), "api-large-v1");
  EXPECT_EQ(plan.workload_decisions(0).proposed_replicas(), 2U);
  EXPECT_EQ(plan.proposed_objective_cost_micros(), 550'000U);
  const auto verification = fleetright::Verify(snapshot, plan);
  EXPECT_TRUE(verification.accepted());
}

TEST(PlannerVerifierTest, CurrentRiskUsesOnlyEvidenceWithinReplicaBounds) {
  auto snapshot = Fixture();
  snapshot.mutable_workloads(0)->set_max_replicas(2);
  snapshot.mutable_workloads(0)->set_demand_qps_milli(1'200'000);
  snapshot.mutable_policy()->set_boundary_cost_micros_per_workload(1'000'000);
  SealSnapshot(&snapshot);

  const auto plan = fleetright::Plan(snapshot);
  ASSERT_TRUE(plan.feasible()) << plan.infeasible_reason();
  ASSERT_EQ(plan.action_count(), 0U);
  EXPECT_EQ(plan.current_boundary_cost_micros(), 1'000'000U);
  const auto verification = fleetright::Verify(snapshot, plan);
  EXPECT_FALSE(verification.accepted());
  bool objective_recomputed = false;
  for (const auto &invariant : verification.invariants()) {
    if (invariant.invariant() == "weighted_objective_and_minimum_improvement")
      objective_recomputed = invariant.passed();
  }
  EXPECT_TRUE(objective_recomputed);
}

TEST(PlannerVerifierTest, RejectsObjectivePenaltyOverflowWithoutWrapping) {
  auto snapshot = Fixture();
  snapshot.mutable_policy()->set_action_churn_cost_micros(
      std::numeric_limits<std::uint64_t>::max());
  SealSnapshot(&snapshot);
  const auto plan = fleetright::Plan(snapshot);
  EXPECT_FALSE(plan.feasible());
  EXPECT_EQ(plan.infeasible_reason(),
            "objective arithmetic overflow or action accounting mismatch");
}

TEST(PlannerVerifierTest, RejectsUnrepresentableHourlyNodeCostTotals) {
  auto snapshot = Fixture();
  const auto hourly_cost = std::numeric_limits<std::uint64_t>::max() / 3;
  snapshot.mutable_policy()->set_planning_horizon_seconds(1);
  snapshot.mutable_machine_catalog()->mutable_machines(0)->set_hourly_cost_micros(hourly_cost);
  for (auto &node : *snapshot.mutable_nodes())
    node.set_hourly_cost_micros(hourly_cost);
  SealSnapshot(&snapshot);

  const auto plan = fleetright::Plan(snapshot);
  EXPECT_FALSE(plan.feasible());
  EXPECT_EQ(plan.infeasible_reason(), "node cost accumulation overflow");

  auto forged = plan;
  forged.set_feasible(true);
  forged.set_current_cost_micros_per_hour(std::numeric_limits<std::uint64_t>::max());
  forged.set_proposed_cost_micros_per_hour(std::numeric_limits<std::uint64_t>::max());
  SealPlan(&forged);
  const auto verification = fleetright::Verify(snapshot, forged);
  EXPECT_FALSE(verification.accepted());
  bool cost_rejected = false;
  for (const auto &invariant : verification.invariants()) {
    if (invariant.invariant() == "cost_catalog_consistency")
      cost_rejected = !invariant.passed();
  }
  EXPECT_TRUE(cost_rejected);
}

TEST(VerifierAdversarialTest, BindsObjectiveRiskToSelectedProfilePoint) {
  const auto snapshot = Fixture();
  auto plan = fleetright::Plan(snapshot);
  ASSERT_TRUE(plan.feasible());
  plan.mutable_workload_decisions(0)->set_boundary_risk_ppm(1);
  SealPlan(&plan);
  const auto result = fleetright::Verify(snapshot, plan);
  EXPECT_FALSE(result.accepted());
}

TEST(PlannerVerifierTest, LowConfidenceProfileProducesVerifiedNoOp) {
  auto snapshot = Fixture();
  snapshot.mutable_capacity_profiles(0)->mutable_safe_points(0)->set_latency_success_lower_ppm(
      970'000);
  SealSnapshot(&snapshot);
  const auto plan = fleetright::Plan(snapshot);
  ASSERT_TRUE(plan.feasible());
  EXPECT_EQ(plan.action_count(), 0U);
  EXPECT_EQ(plan.workload_decisions(0).proposed_replicas(), 3U);
  EXPECT_TRUE(fleetright::Verify(snapshot, plan).accepted());
}

TEST(VerifierAdversarialTest, BindsReportedCurrentReplicasToSnapshotBeforeScaleIn) {
  auto snapshot = Fixture();
  snapshot.clear_capacity_profiles();
  SealSnapshot(&snapshot);
  auto plan = fleetright::Plan(snapshot);
  ASSERT_TRUE(plan.feasible());
  ASSERT_EQ(plan.placements_size(), 3);
  auto *decision = plan.mutable_workload_decisions(0);
  decision->set_current_replicas(2);
  decision->set_proposed_replicas(2);
  plan.mutable_placements()->RemoveLast();
  plan.set_action_count(0);
  SealPlan(&plan);
  const auto result = fleetright::Verify(snapshot, plan);
  EXPECT_FALSE(result.accepted());
}

TEST(PlannerVerifierTest, RetainsNMinusOneRecoveryCapacityOnAnActiveNode) {
  auto snapshot = Fixture();
  snapshot.mutable_nodes()->RemoveLast();
  auto *workload = snapshot.mutable_workloads(0);
  workload->set_min_replicas(1);
  workload->set_availability_floor(1);
  workload->set_failure_tolerance(fleetright::v1::FAILURE_TOLERANCE_N_MINUS_1);
  workload->set_demand_qps_milli(100'000);
  workload->set_concurrency_bound(50);
  for (int index = 0; index < snapshot.nodes_size(); ++index) {
    auto *label = snapshot.mutable_nodes(index)->add_labels();
    label->set_key("zone");
    label->set_value("zone-" + std::to_string(index));
  }
  SealSnapshot(&snapshot);
  const auto plan = fleetright::Plan(snapshot);
  ASSERT_TRUE(plan.feasible());
  EXPECT_EQ(plan.workload_decisions(0).proposed_replicas(), 2U);
  EXPECT_GT(plan.action_count(), 0U);
  EXPECT_EQ(plan.deactivated_node_ids_size(), 0);
  const auto verification = fleetright::Verify(snapshot, plan);
  EXPECT_TRUE(verification.accepted()) << plan.DebugString() << verification.DebugString();
}

TEST(PlannerPropertyTest, RequiredReplicasNeverDecreaseAsDemandRises) {
  auto snapshot = Fixture();
  std::uint32_t previous = 0;
  for (const std::uint64_t demand :
       {500'000ULL, 700'000ULL, 900'000ULL, 1'000'000ULL, 1'200'000ULL, 1'600'000ULL}) {
    snapshot.mutable_workloads(0)->set_demand_qps_milli(demand);
    SealSnapshot(&snapshot);
    const auto plan = fleetright::Plan(snapshot);
    ASSERT_TRUE(plan.feasible()) << plan.infeasible_reason();
    const auto replicas = plan.workload_decisions(0).proposed_replicas();
    EXPECT_GE(replicas, previous);
    previous = replicas;
  }
}

TEST(PlannerPropertyTest, FeasiblePlansAreVerifiedAndRespectTheWorkloadMoveBudget) {
  // Regression: the planner once moved replicas onto a node still hosting a sibling replica that
  // the same plan removes, and ignored the per-workload move budget; the verifier rejected both.
  std::minstd_rand random(0xC0FFEEU);
  std::uint32_t feasible_plans = 0;
  for (std::uint32_t trial = 0; trial < 96; ++trial) {
    auto snapshot = Fixture();
    const auto extra_nodes = random() % 5;
    for (std::uint32_t extra = 0; extra < extra_nodes; ++extra) {
      auto *node = snapshot.add_nodes();
      *node = snapshot.nodes(0);
      node->set_node_id("node-extra-" + std::to_string(extra));
    }
    // Mixed machine types make consolidation onto cheaper nodes (and thus moves) attractive.
    auto *catalog = snapshot.mutable_machine_catalog();
    for (const auto &[type, cpu, memory, network, price] :
         {std::tuple{"medium", 4'000, 8'192, 2'000, 90'000},
          std::tuple{"large", 8'000, 16'384, 4'000, 170'000}}) {
      auto *machine = catalog->add_machines();
      machine->set_machine_type(type);
      SetResource(machine->mutable_allocatable(), cpu, memory, network);
      machine->set_hourly_cost_micros(price);
    }
    for (auto &node : *snapshot.mutable_nodes()) {
      const auto &machine = catalog->machines(static_cast<int>(random() % 3));
      node.set_machine_type(machine.machine_type());
      *node.mutable_allocatable() = machine.allocatable();
      node.set_hourly_cost_micros(machine.hourly_cost_micros());
    }
    const std::uint32_t budgets[] = {0, 1, 2, 100};
    const auto budget = budgets[random() % 4];
    auto *workload = snapshot.mutable_workloads(0);
    workload->set_max_moves(budget);
    workload->set_demand_qps_milli(100'000 + random() % 1'200'000);
    workload->set_concurrency_bound(20 + random() % 150);
    std::vector<int> order(static_cast<std::size_t>(snapshot.nodes_size()));
    std::iota(order.begin(), order.end(), 0);
    std::shuffle(order.begin(), order.end(), random);
    for (int i = 0; i < snapshot.current_placements_size(); ++i)
      snapshot.mutable_current_placements(i)->set_node_id(
          snapshot.nodes(order[static_cast<std::size_t>(i)]).node_id());
    SealSnapshot(&snapshot);
    const auto plan = fleetright::Plan(snapshot);
    if (!plan.feasible())
      continue;
    ++feasible_plans;
    EXPECT_LE(static_cast<std::uint32_t>(plan.evacuation_steps_size()), budget) << "trial " << trial;
    const auto verification = fleetright::Verify(snapshot, plan);
    EXPECT_TRUE(verification.accepted())
        << "trial " << trial << "\n" << plan.DebugString() << verification.DebugString();
  }
  EXPECT_GE(feasible_plans, 48U);
}

TEST(PlannerPropertyTest, RandomizedNMinusOneTopologiesRemainIndependentlyFeasible) {
  std::minstd_rand random(0xF1EE7U);
  for (std::uint32_t trial = 0; trial < 32; ++trial) {
    auto snapshot = Fixture();
    const auto extra_nodes = random() % 3;
    for (std::uint32_t extra = 0; extra < extra_nodes; ++extra) {
      auto *node = snapshot.add_nodes();
      *node = snapshot.nodes(2 + static_cast<int>(extra));
      node->set_node_id("node-extra-" + std::to_string(trial) + "-" + std::to_string(extra));
    }
    auto *workload = snapshot.mutable_workloads(0);
    workload->set_min_replicas(1);
    workload->set_availability_floor(1);
    workload->set_failure_tolerance(fleetright::v1::FAILURE_TOLERANCE_N_MINUS_1);
    workload->set_demand_qps_milli(100'000);
    workload->set_concurrency_bound(50);
    for (int index = 0; index < snapshot.nodes_size(); ++index) {
      auto *label = snapshot.mutable_nodes(index)->add_labels();
      label->set_key("zone");
      if (index == 0 || index == 2) {
        label->set_value("zone-a");
      } else if (index == 1 || index == 3) {
        label->set_value("zone-b");
      } else {
        label->set_value(random() % 2 == 0 ? "zone-a" : "zone-b");
      }
    }
    SealSnapshot(&snapshot);
    const auto plan = fleetright::Plan(snapshot);
    ASSERT_TRUE(plan.feasible()) << "trial " << trial << ": " << plan.infeasible_reason();
    const auto verification = fleetright::Verify(snapshot, plan);
    EXPECT_TRUE(verification.accepted()) << "trial " << trial << "\n"
                                         << plan.DebugString() << verification.DebugString();
  }
}

TEST(VerifierAdversarialTest, RejectsEvacuationThatOverfillsIntermediateDestination) {
  auto snapshot = Fixture();
  auto *workload = snapshot.mutable_workloads(0);
  workload->set_min_replicas(3);
  workload->set_availability_floor(3);
  SetResource(workload->mutable_current_resources(), 1'500, 1'024, 100);
  SetResource(snapshot.mutable_capacity_profiles(0)->mutable_allocation(), 1'500, 1'024, 100);
  SealSnapshot(&snapshot);

  auto plan = fleetright::Plan(snapshot);
  ASSERT_TRUE(plan.feasible()) << plan.infeasible_reason();
  ASSERT_EQ(plan.placements_size(), 3);
  for (auto &placement : *plan.mutable_placements()) {
    if (placement.replica_id() == "api-0")
      placement.set_node_id("node-1");
    else if (placement.replica_id() == "api-1")
      placement.set_node_id("node-0");
  }
  plan.clear_evacuation_steps();
  auto *first = plan.add_evacuation_steps();
  first->set_sequence(0);
  first->set_workload_id("api");
  first->set_replica_id("api-0");
  first->set_source_node_id("node-0");
  first->set_destination_node_id("node-1");
  auto *second = plan.add_evacuation_steps();
  second->set_sequence(1);
  second->set_workload_id("api");
  second->set_replica_id("api-1");
  second->set_source_node_id("node-1");
  second->set_destination_node_id("node-0");
  plan.set_action_count(plan.action_count() + 2);
  plan.set_proposed_movement_cost_micros(plan.proposed_movement_cost_micros() + 200);
  plan.set_proposed_objective_cost_micros(plan.proposed_objective_cost_micros() + 200);
  SealPlan(&plan);

  const auto result = fleetright::Verify(snapshot, plan);
  EXPECT_FALSE(result.accepted());
  const auto check = std::find_if(
      result.invariants().begin(), result.invariants().end(), [](const auto &invariant) {
        return invariant.invariant() == "eviction_recreation_intermediate_states";
      });
  ASSERT_NE(check, result.invariants().end());
  EXPECT_FALSE(check->passed());
}

TEST(VerifierAdversarialTest, RejectsNMinusOneRecoveryOnADeactivatedNode) {
  auto snapshot = Fixture();
  snapshot.mutable_nodes()->RemoveLast();
  auto *workload = snapshot.mutable_workloads(0);
  workload->set_min_replicas(1);
  workload->set_availability_floor(1);
  workload->set_failure_tolerance(fleetright::v1::FAILURE_TOLERANCE_N_MINUS_1);
  workload->set_demand_qps_milli(100'000);
  workload->set_concurrency_bound(50);
  for (int index = 0; index < snapshot.nodes_size(); ++index) {
    auto *label = snapshot.mutable_nodes(index)->add_labels();
    label->set_key("zone");
    label->set_value("zone-" + std::to_string(index));
  }
  SealSnapshot(&snapshot);
  auto plan = fleetright::Plan(snapshot);
  ASSERT_TRUE(plan.feasible());
  ASSERT_EQ(plan.deactivated_node_ids_size(), 0);
  plan.add_deactivated_node_ids("node-2");
  plan.set_proposed_cost_micros_per_hour(100'000);
  plan.set_action_count(plan.action_count() + 1);
  SealPlan(&plan);
  const auto result = fleetright::Verify(snapshot, plan);
  EXPECT_FALSE(result.accepted());
  const auto n1 = std::find_if(result.invariants().begin(), result.invariants().end(),
                               [](const auto &invariant) {
                                 return invariant.invariant() == "n_minus_1_serving_and_recovery";
                               });
  ASSERT_NE(n1, result.invariants().end());
  EXPECT_FALSE(n1->passed());
}

TEST(PlannerProfileTest, InconsistentTailCountsCannotEnableScaleIn) {
  auto snapshot = Fixture();
  auto *point = snapshot.mutable_capacity_profiles(0)->mutable_safe_points(0);
  point->set_threshold_successes(0);
  point->set_latency_success_lower_ppm(995'000);
  SealSnapshot(&snapshot);
  const auto plan = fleetright::Plan(snapshot);
  ASSERT_TRUE(plan.feasible());
  EXPECT_EQ(plan.action_count(), 0U);
  EXPECT_EQ(plan.workload_decisions(0).proposed_replicas(), 3U);
  EXPECT_TRUE(fleetright::Verify(snapshot, plan).accepted());
}

TEST(PlannerProfileTest, UsesTheSafePointThatSupportsTheDemandBound) {
  auto snapshot = Fixture();
  auto *profile = snapshot.mutable_capacity_profiles(0);
  auto *weak = profile->mutable_safe_points(0);
  weak->set_safe_qps_milli(1'000);
  weak->set_safe_concurrency(1);
  auto *strong = profile->add_safe_points();
  *strong = *weak;
  strong->set_safe_qps_milli(500'000);
  strong->set_safe_concurrency(100);
  SealSnapshot(&snapshot);
  const auto plan = fleetright::Plan(snapshot);
  ASSERT_TRUE(plan.feasible()) << plan.infeasible_reason();
  EXPECT_EQ(plan.workload_decisions(0).proposed_replicas(), 2U);
  EXPECT_TRUE(fleetright::Verify(snapshot, plan).accepted());
}

TEST(PlannerProfileTest, WideShapeScoreDoesNotWrapIntoPreferredCandidate) {
  auto snapshot = Fixture();
  auto *huge = snapshot.add_capacity_profiles();
  *huge = snapshot.capacity_profiles(0);
  huge->set_profile_id("api-huge-shape-v1");
  huge->mutable_allocation()->set_cpu_millicores(18'446'744'073'710LL);
  huge->mutable_safe_points(0)->set_safe_qps_milli(900'000);
  huge->mutable_safe_points(0)->set_safe_concurrency(200);
  SealSnapshot(&snapshot);
  const auto plan = fleetright::Plan(snapshot);
  ASSERT_TRUE(plan.feasible());
  EXPECT_EQ(plan.workload_decisions(0).capacity_profile_id(), "api-small-v1");
}

TEST(VerifierAdversarialTest, RejectsUnknownFailureToleranceAndCatalogPriceMismatch) {
  auto invalid_enum = Fixture();
  invalid_enum.mutable_workloads(0)->set_failure_tolerance(
      static_cast<fleetright::v1::FailureTolerance>(3));
  SealSnapshot(&invalid_enum);
  EXPECT_FALSE(fleetright::Plan(invalid_enum).feasible());

  auto wrong_price = Fixture();
  wrong_price.mutable_nodes(0)->set_hourly_cost_micros(1);
  SealSnapshot(&wrong_price);
  EXPECT_FALSE(fleetright::Plan(wrong_price).feasible());
}

TEST(VerifierAdversarialTest, RejectsUnknownSnapshotRootFields) {
  auto snapshot = Fixture();
  SealSnapshot(&snapshot);
  const auto plan = fleetright::Plan(snapshot);
  ASSERT_TRUE(plan.feasible());

  snapshot.GetReflection()->MutableUnknownFields(&snapshot)->AddVarint(99, 7);
  const auto result = fleetright::Verify(snapshot, plan);
  EXPECT_FALSE(result.accepted());
  EXPECT_FALSE(result.invariants(0).passed());
}

TEST(VerifierAdversarialTest, EnforcesPerWorkloadMoveBudgetWithoutScaleDown) {
  auto snapshot = Fixture();
  snapshot.mutable_workloads(0)->set_max_moves(1);
  snapshot.mutable_workloads(0)->set_max_scale_down_fraction_ppm(0);
  SealSnapshot(&snapshot);
  auto plan = fleetright::Plan(snapshot);
  ASSERT_TRUE(plan.feasible());
  auto *decision = plan.mutable_workload_decisions(0);
  decision->set_proposed_replicas(3);
  plan.clear_placements();
  const std::vector<std::pair<std::string, std::string>> targets = {
      {"api-0", "node-03"}, {"api-1", "node-00"}, {"api-2", "node-02"}};
  for (const auto &[replica, node] : targets) {
    auto *placement = plan.add_placements();
    placement->set_workload_id("api");
    placement->set_replica_id(replica);
    placement->set_node_id(node);
  }
  auto *first = plan.add_evacuation_steps();
  first->set_sequence(0);
  first->set_workload_id("api");
  first->set_replica_id("api-0");
  first->set_source_node_id("node-00");
  first->set_destination_node_id("node-03");
  auto *second = plan.add_evacuation_steps();
  second->set_sequence(1);
  second->set_workload_id("api");
  second->set_replica_id("api-1");
  second->set_source_node_id("node-01");
  second->set_destination_node_id("node-00");
  plan.add_deactivated_node_ids("node-01");
  plan.set_proposed_cost_micros_per_hour(150'000);
  plan.set_action_count(2);
  SealPlan(&plan);
  const auto result = fleetright::Verify(snapshot, plan);
  EXPECT_FALSE(result.accepted());
}
