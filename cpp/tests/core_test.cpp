#include "fleetright/hash.hpp"
#include "fleetright/planner.hpp"
#include "fleetright/units.hpp"
#include "fleetright/verifier.hpp"

#include <google/protobuf/unknown_field_set.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <random>
#include <string>

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
} // namespace

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
