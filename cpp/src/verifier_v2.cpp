#include "fleetright/evacuation_v2.hpp"

#include "fleetright/hash.hpp"
#include "fleetright/planner.hpp"
#include "fleetright/units.hpp"
#include "fleetright/v2_contract_utils.hpp"
#include "fleetright/verifier.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <vector>

namespace fleetright {
namespace {
using v1::OptimizationSnapshot;
using v1::ReplicaPlacement;
using v1::ResourceVector;
using v1::WorkloadSpec;
using v2::EvacuationOperation;
using v2::EvacuationPlan;
using v2::EvacuationSnapshot;
using v2::PodIdentity;
using ProtoResource = v1::ResourceVector;
constexpr std::uint64_t kPpm = 1'000'000;
constexpr std::uint64_t kMaxProfileTrials = 1'000'000'000'000ULL;
constexpr std::uint32_t kMinimumTailPpm = 990'000;
constexpr std::size_t kMaxV2Nodes = 64;
constexpr std::size_t kMaxV2Workloads = 100;
constexpr std::size_t kMaxV2Profiles = 10'000;
constexpr std::size_t kMaxV2Placements = 10'000;
constexpr std::size_t kMaxProofOutcomes = 250'000;

void AddInvariant(v2::VerificationResult *result, const std::string &name, bool passed,
                  const std::string &detail) {
  auto *entry = result->add_invariants();
  entry->set_invariant(name);
  entry->set_passed(passed);
  entry->set_detail(detail);
  if (!passed)
    result->set_accepted(false);
}

std::string ReplicaKey(const std::string &workload, const std::string &replica) {
  return workload + "\x1f" + replica;
}

bool SameResource(const ProtoResource &left, const ProtoResource &right) {
  return left.cpu_millicores() == right.cpu_millicores() &&
         left.memory_mib() == right.memory_mib() && left.network_mbps() == right.network_mbps();
}

bool ResourceNonNegative(const ProtoResource &value) {
  return value.cpu_millicores() >= 0 && value.memory_mib() >= 0 && value.network_mbps() >= 0;
}

bool AddChecked(const ProtoResource &left, const ProtoResource &right, ProtoResource *out) {
  if (!ResourceNonNegative(left) || !ResourceNonNegative(right) ||
      left.cpu_millicores() > std::numeric_limits<std::int64_t>::max() - right.cpu_millicores() ||
      left.memory_mib() > std::numeric_limits<std::int64_t>::max() - right.memory_mib() ||
      left.network_mbps() > std::numeric_limits<std::int64_t>::max() - right.network_mbps())
    return false;
  out->set_cpu_millicores(left.cpu_millicores() + right.cpu_millicores());
  out->set_memory_mib(left.memory_mib() + right.memory_mib());
  out->set_network_mbps(left.network_mbps() + right.network_mbps());
  return true;
}

std::optional<ProtoResource> LimitForNode(const v1::NodeState &node, std::uint32_t headroom_ppm) {
  if (!ResourceNonNegative(node.allocatable()) || !ResourceNonNegative(node.reserved()) ||
      headroom_ppm > kPpm)
    return std::nullopt;
  ProtoResource limit;
  const auto part = [headroom_ppm](std::int64_t alloc, std::int64_t reserved, std::int64_t *value) {
    if (alloc < 0 || reserved < 0)
      return false;
    __extension__ using UInt128 = unsigned __int128;
    const UInt128 scaled = static_cast<UInt128>(alloc) * (kPpm - headroom_ppm) / kPpm;
    if (scaled > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) ||
        static_cast<std::int64_t>(scaled) < reserved)
      return false;
    *value = static_cast<std::int64_t>(scaled) - reserved;
    return true;
  };
  std::int64_t cpu = 0;
  std::int64_t memory = 0;
  std::int64_t network = 0;
  if (!part(node.allocatable().cpu_millicores(), node.reserved().cpu_millicores(), &cpu) ||
      !part(node.allocatable().memory_mib(), node.reserved().memory_mib(), &memory) ||
      !part(node.allocatable().network_mbps(), node.reserved().network_mbps(), &network))
    return std::nullopt;
  limit.set_cpu_millicores(cpu);
  limit.set_memory_mib(memory);
  limit.set_network_mbps(network);
  return limit;
}

bool Within(const ProtoResource &used, const ProtoResource &limit) {
  return ResourceNonNegative(used) && ResourceNonNegative(limit) &&
         used.cpu_millicores() <= limit.cpu_millicores() &&
         used.memory_mib() <= limit.memory_mib() && used.network_mbps() <= limit.network_mbps();
}

const WorkloadSpec *WorkloadById(const OptimizationSnapshot &snapshot, const std::string &id) {
  for (const auto &workload : snapshot.workloads())
    if (workload.workload_id() == id)
      return &workload;
  return nullptr;
}

const v1::NodeState *NodeById(const OptimizationSnapshot &snapshot, const std::string &id) {
  for (const auto &node : snapshot.nodes())
    if (node.node_id() == id)
      return &node;
  return nullptr;
}

const v2::NodeIdentity *NodeIdentityById(const EvacuationSnapshot &snapshot,
                                         const std::string &id) {
  for (const auto &node : snapshot.nodes())
    if (node.node_id() == id)
      return &node;
  return nullptr;
}

const PodIdentity *PodByKey(const EvacuationSnapshot &snapshot, const std::string &workload,
                            const std::string &replica) {
  for (const auto &pod : snapshot.pods())
    if (pod.workload_id() == workload && pod.replica_id() == replica)
      return &pod;
  return nullptr;
}

const v2::ControllerIdentity *ControllerByWorkload(const EvacuationSnapshot &snapshot,
                                                   const std::string &workload) {
  for (const auto &controller : snapshot.controllers())
    if (controller.workload_id() == workload)
      return &controller;
  return nullptr;
}

std::string LabelValue(const v1::NodeState &node, const std::string &key) {
  for (const auto &label : node.labels())
    if (label.key() == key)
      return label.value();
  return {};
}

bool TaintsAllowed(const WorkloadSpec &workload, const v1::NodeState &node) {
  for (const auto &taint : node.taints())
    if (std::find(workload.tolerated_taint_keys().begin(), workload.tolerated_taint_keys().end(),
                  taint.key()) == workload.tolerated_taint_keys().end())
      return false;
  return true;
}

std::optional<std::uint32_t> WilsonLowerBoundPpm(std::uint64_t successes, std::uint64_t trials) {
  if (trials == 0 || trials > kMaxProfileTrials || successes > trials)
    return std::nullopt;
  constexpr long double z = 1.96L;
  const long double n = static_cast<long double>(trials);
  const long double p = static_cast<long double>(successes) / n;
  const long double z2 = z * z;
  const long double center = p + z2 / (2.0L * n);
  const long double radius = z * std::sqrt((p * (1.0L - p) + z2 / (4.0L * n)) / n);
  const long double lower = std::max(0.0L, (center - radius) / (1.0L + z2 / n));
  return static_cast<std::uint32_t>(std::floor(lower * kPpm));
}

std::optional<std::uint32_t> WilsonUpperBoundPpm(std::uint64_t failures, std::uint64_t trials) {
  if (trials == 0 || trials > kMaxProfileTrials || failures > trials)
    return std::nullopt;
  constexpr long double z = 1.96L;
  const long double n = static_cast<long double>(trials);
  const long double p = static_cast<long double>(failures) / n;
  const long double z2 = z * z;
  const long double center = p + z2 / (2.0L * n);
  const long double radius = z * std::sqrt((p * (1.0L - p) + z2 / (4.0L * n)) / n);
  const long double upper = std::min(1.0L, (center + radius) / (1.0L + z2 / n));
  return static_cast<std::uint32_t>(std::ceil(upper * kPpm));
}

bool PointEvidenceSafe(const v1::CapacityPoint &point, const WorkloadSpec &workload) {
  if (point.request_count() == 0 || point.request_count() > kMaxProfileTrials ||
      point.threshold_trials() != point.request_count() ||
      point.threshold_successes() > point.threshold_trials() ||
      point.error_count() > point.request_count() || point.latency_success_lower_ppm() > kPpm ||
      point.error_rate_upper_ppm() > kPpm || point.error_rate_wilson_upper_ppm() > kPpm ||
      point.memory_pressure_ppm() > kPpm || point.cpu_throttling_ppm() > kPpm ||
      point.run_dispersion_ppm() > kPpm || point.repeated_run_count() > 10'000)
    return false;
  const auto latency = WilsonLowerBoundPpm(point.threshold_successes(), point.threshold_trials());
  const auto errors = WilsonUpperBoundPpm(point.error_count(), point.request_count());
  if (!latency || !errors || point.latency_success_lower_ppm() > *latency ||
      point.error_rate_wilson_upper_ppm() < *errors)
    return false;
  __extension__ using UInt128 = unsigned __int128;
  const UInt128 raw_error_ppm =
      (static_cast<UInt128>(point.error_count()) * kPpm + point.request_count() - 1) /
      point.request_count();
  return raw_error_ppm <= point.error_rate_upper_ppm() &&
         point.latency_success_lower_ppm() >= kMinimumTailPpm &&
         point.error_rate_upper_ppm() <= workload.error_slo_ppm() &&
         point.error_rate_wilson_upper_ppm() <= workload.error_slo_ppm() &&
         point.p99_upper_ms() <= workload.latency_slo_ms() &&
         point.queue_growth_upper_milli() <= 0 &&
         point.memory_pressure_ppm() <= workload.max_memory_pressure_ppm() &&
         point.cpu_throttling_ppm() <= workload.max_throttle_ppm() && point.stable_across_runs() &&
         point.run_dispersion_ppm() <= 250'000U;
}

bool ProfileDigestMatches(const v1::CapacityProfile &profile) {
  auto copy = profile;
  const auto digest = copy.profile_sha256();
  copy.clear_profile_sha256();
  return !digest.empty() && MessageSha256(copy) == digest;
}

struct ProfileCapacity {
  const v1::CapacityProfile *profile = nullptr;
  std::uint32_t point_index = 0;
  std::uint64_t qps_per_replica = 0;
  std::uint64_t concurrency_per_replica = 0;
  std::uint32_t required_replicas = 0;
};

std::optional<ProfileCapacity> RecomputeProfileCapacity(const OptimizationSnapshot &snapshot,
                                                        const WorkloadSpec &workload,
                                                        const std::string &profile_id) {
  const v1::CapacityProfile *selected = nullptr;
  for (const auto &profile : snapshot.capacity_profiles())
    if (profile.profile_id() == profile_id) {
      if (selected != nullptr)
        return std::nullopt;
      selected = &profile;
    }
  if (selected == nullptr || selected->schema_version() != 1 ||
      selected->workload_id() != workload.workload_id() ||
      !SameResource(selected->allocation(), workload.current_resources()) ||
      selected->image_digest() != workload.image_digest() ||
      selected->request_mix_id() != workload.request_mix_id() ||
      selected->runtime_config_class() != workload.runtime_config_class() ||
      selected->platform_class() != workload.platform_class() || !ProfileDigestMatches(*selected) ||
      selected->generated_at_ms() > snapshot.evaluation_time_ms() ||
      selected->valid_until_ms() <= snapshot.evaluation_time_ms() ||
      snapshot.evaluation_time_ms() - selected->generated_at_ms() >
          snapshot.policy().maximum_profile_age_ms() ||
      selected->confidence_ppm() < snapshot.policy().minimum_profile_confidence_ppm() ||
      selected->confidence_ppm() > kPpm || selected->conservative_haircut_ppm() > kPpm ||
      !snapshot.policy().allow_synthetic_profiles() ||
      !selected->measurement_kind().starts_with("synthetic_"))
    return std::nullopt;

  std::optional<ProfileCapacity> best;
  for (int i = 0; i < selected->safe_points_size(); ++i) {
    const auto &point = selected->safe_points(i);
    if (!PointEvidenceSafe(point, workload) ||
        selected->confidence_ppm() > point.latency_success_lower_ppm())
      continue;
    const auto qps = MultiplyPpmFloor(
        point.safe_qps_milli(), workload.profile_confidence_factor_ppm(),
        workload.model_accuracy_factor_ppm(), workload.degraded_cluster_factor_ppm());
    const auto concurrency = MultiplyPpmFloor(
        point.safe_concurrency(), workload.profile_confidence_factor_ppm(),
        workload.model_accuracy_factor_ppm(), workload.degraded_cluster_factor_ppm());
    if (!qps || !concurrency || *qps == 0 || *concurrency == 0)
      continue;
    const std::uint64_t required =
        std::max({CeilDivide(workload.demand_qps_milli(), *qps),
                  CeilDivide(workload.concurrency_bound(), *concurrency),
                  static_cast<std::uint64_t>(workload.min_replicas()),
                  static_cast<std::uint64_t>(workload.availability_floor())});
    if (required > std::numeric_limits<std::uint32_t>::max())
      continue;
    ProfileCapacity candidate{selected, static_cast<std::uint32_t>(i), *qps, *concurrency,
                              static_cast<std::uint32_t>(required)};
    if (!best ||
        std::tie(candidate.required_replicas, candidate.profile->profile_id(),
                 candidate.point_index) <
            std::tie(best->required_replicas, best->profile->profile_id(), best->point_index))
      best = candidate;
  }
  return best;
}

std::uint32_t CountFor(const std::vector<ReplicaPlacement> &pods, const std::string &workload) {
  return static_cast<std::uint32_t>(std::count_if(
      pods.begin(), pods.end(), [&](const auto &pod) { return pod.workload_id() == workload; }));
}

bool ResourceStateValid(const EvacuationSnapshot &snapshot,
                        const std::vector<ReplicaPlacement> &pods,
                        const std::set<std::string> &inactive_nodes) {
  const auto &model = snapshot.model_snapshot();
  std::map<std::string, ProtoResource> usage;
  std::map<std::string, ProtoResource> limits;
  for (const auto &node : model.nodes()) {
    if (!node.active() || node.health() != v1::NODE_HEALTH_HEALTHY ||
        inactive_nodes.contains(node.node_id()))
      continue;
    const auto limit = LimitForNode(node, model.policy().headroom_ppm());
    if (!limit)
      return false;
    limits.emplace(node.node_id(), *limit);
  }
  for (const auto &pod : pods) {
    const auto *workload = WorkloadById(model, pod.workload_id());
    const auto limit = limits.find(pod.node_id());
    const auto *node = NodeById(model, pod.node_id());
    if (workload == nullptr || node == nullptr || limit == limits.end() ||
        !TaintsAllowed(*workload, *node))
      return false;
    ProtoResource next;
    if (!AddChecked(usage[pod.node_id()], workload->current_resources(), &next) ||
        !Within(next, limit->second))
      return false;
    usage[pod.node_id()] = std::move(next);
  }
  return true;
}

bool TopologyStateValid(const EvacuationSnapshot &snapshot, const WorkloadSpec &workload,
                        const std::vector<ReplicaPlacement> &pods,
                        const std::set<std::string> &inactive_nodes) {
  if (workload.min_topology_domains() == 0)
    return true;
  std::map<std::string, std::uint32_t> domains;
  for (const auto &node : snapshot.model_snapshot().nodes()) {
    if (!node.active() || node.health() != v1::NODE_HEALTH_HEALTHY ||
        inactive_nodes.contains(node.node_id()) || !TaintsAllowed(workload, node))
      continue;
    const auto domain = LabelValue(node, workload.topology_key());
    if (domain.empty())
      return false;
    domains.try_emplace(domain, 0U);
  }
  if (domains.size() < workload.min_topology_domains())
    return false;
  for (const auto &pod : pods) {
    if (pod.workload_id() != workload.workload_id() || inactive_nodes.contains(pod.node_id()))
      continue;
    const auto *node = NodeById(snapshot.model_snapshot(), pod.node_id());
    if (node == nullptr)
      return false;
    const auto value = LabelValue(*node, workload.topology_key());
    const auto found = domains.find(value);
    if (value.empty() || found == domains.end() || found->second == UINT32_MAX)
      return false;
    ++found->second;
  }
  const auto occupied = std::count_if(domains.begin(), domains.end(),
                                      [](const auto &item) { return item.second != 0; });
  if (occupied < workload.min_topology_domains())
    return false;
  const auto [minimum, maximum] =
      std::minmax_element(domains.begin(), domains.end(), [](const auto &left, const auto &right) {
        return left.second < right.second;
      });
  return maximum->second - minimum->second <= workload.max_topology_skew();
}

std::vector<ReplicaPlacement> AddPod(const std::vector<ReplicaPlacement> &pods,
                                     const std::string &workload, const std::string &id,
                                     const std::string &node) {
  auto result = pods;
  ReplicaPlacement pod;
  pod.set_workload_id(workload);
  pod.set_replica_id(id);
  pod.set_node_id(node);
  result.push_back(std::move(pod));
  return result;
}

std::vector<ReplicaPlacement> RemoveSource(const std::vector<ReplicaPlacement> &pods,
                                           const std::string &workload,
                                           const std::string &replica) {
  std::vector<ReplicaPlacement> result;
  for (const auto &pod : pods)
    if (pod.workload_id() != workload || pod.replica_id() != replica)
      result.push_back(pod);
  return result;
}

bool TopologyAndCapacity(const EvacuationSnapshot &snapshot, const WorkloadSpec &workload,
                         const std::vector<ReplicaPlacement> &pods,
                         const std::set<std::string> &inactive) {
  return ResourceStateValid(snapshot, pods, inactive) &&
         TopologyStateValid(snapshot, workload, pods, inactive);
}

bool StateCountsSafe(const EvacuationOperation &operation, const ProfileCapacity &profile,
                     std::uint32_t ready, std::uint32_t live) {
  const auto required =
      std::max(operation.availability_floor_replicas(), profile.required_replicas);
  if (ready < required || live > operation.maximum_live_replicas())
    return false;
  return true;
}

bool DependenciesValid(const EvacuationOperation &operation) {
  const std::vector<v2::EvacuationStage> stages = {
      v2::EVACUATION_STAGE_APPLY_DESTINATION_CONSTRAINT,
      v2::EVACUATION_STAGE_SET_SURGE_REPLICAS,
      v2::EVACUATION_STAGE_WAIT_REPLACEMENT_READY,
      v2::EVACUATION_STAGE_EVICT_SOURCE_UID,
      v2::EVACUATION_STAGE_WAIT_SOURCE_ABSENT,
      v2::EVACUATION_STAGE_DEACTIVATE_SOURCE_NODE};
  const std::vector<std::string> postconditions = {
      "approved node set and hard anti-affinity are active",
      "desired replicas equal baseline plus one",
      "one replacement is stably ready for the configured duration",
      "the named source Pod UID is evicted through the observed PDB",
      "source UID is absent and desired replicas are stably ready at baseline plus one",
      "source node has no remaining workload Pods"};
  if (operation.actions_size() != static_cast<int>(stages.size()))
    return false;
  for (std::size_t i = 0; i < static_cast<std::size_t>(operation.actions_size()); ++i) {
    const auto &action = operation.actions(static_cast<int>(i));
    if (action.sequence() != static_cast<std::uint32_t>(i) ||
        action.action_id() != "evacuation-" + std::to_string(i) || action.stage() != stages[i] ||
        action.expected_postcondition() != postconditions[i] ||
        action.on_failure() != v2::FAILURE_BEHAVIOR_FAIL_CLOSED_KEEP_SURGE ||
        !v2::EvacuationStage_IsValid(static_cast<int>(action.stage())))
      return false;
    if (i == 0) {
      if (action.depends_on_sequences_size() != 0)
        return false;
    } else if (action.depends_on_sequences_size() != 1 ||
               action.depends_on_sequences(0) != static_cast<std::uint32_t>(i - 1)) {
      return false;
    }
  }
  return true;
}

bool IntermediateContractValid(const EvacuationOperation &operation, std::uint32_t count,
                               const ProfileCapacity &profile) {
  if (operation.permitted_intermediate_states_size() != 5)
    return false;
  const std::uint32_t required =
      std::max(operation.availability_floor_replicas(), profile.required_replicas);
  const std::vector<std::tuple<v2::IntermediateStateKind, std::uint32_t, std::uint32_t, bool, bool>>
      expected = {
          {v2::INTERMEDIATE_STATE_BASELINE, count, count, true, true},
          {v2::INTERMEDIATE_STATE_FIRST_REPLACEMENT_PENDING, count, count + 1, true, true},
          {v2::INTERMEDIATE_STATE_FIRST_REPLACEMENT_READY, std::max(count + 1, required), count + 1,
           true, true},
          {v2::INTERMEDIATE_STATE_SOURCE_TERMINATING_OVERLAP, count, count + 2, true, false},
          {v2::INTERMEDIATE_STATE_FINAL, std::max(count + 1, required), count + 1, false, false}};
  for (std::size_t i = 0;
       i < static_cast<std::size_t>(operation.permitted_intermediate_states_size()); ++i) {
    const auto &state = operation.permitted_intermediate_states(static_cast<int>(i));
    const auto &[kind, ready, live, consumes, available] = expected[i];
    if (state.kind() != kind || state.minimum_ready_replicas() != ready ||
        state.maximum_live_replicas() != live ||
        state.source_pod_consumes_resources() != consumes ||
        state.source_pod_counts_as_available() != available)
      return false;
  }
  return true;
}

bool CheckPlacementOutcomes(const EvacuationSnapshot &snapshot,
                            const EvacuationOperation &operation, const WorkloadSpec &workload,
                            const ProfileCapacity &profile,
                            const std::vector<ReplicaPlacement> &baseline,
                            std::size_t *outcome_count) {
  const std::vector<std::string> destinations(operation.destinations().allowed_node_ids().begin(),
                                              operation.destinations().allowed_node_ids().end());
  const auto count = CountFor(baseline, workload.workload_id());
  if (destinations.size() < 2 || count == 0)
    return false;
  const std::uint64_t required_ready =
      std::max(operation.availability_floor_replicas(), profile.required_replicas);
  if (count < required_ready)
    return false;
  for (const auto &first_node : destinations) {
    ++*outcome_count;
    auto pending = AddPod(baseline, workload.workload_id(), "__v2_first_pending", first_node);
    if (!StateCountsSafe(operation, profile, count, count + 1) ||
        !TopologyAndCapacity(snapshot, workload, pending, {}))
      return false;
    if (!StateCountsSafe(operation, profile, count + 1, count + 1))
      return false;
  }
  for (const auto &first_node : destinations) {
    for (const auto &second_node : destinations) {
      if (first_node == second_node && operation.destinations().require_hard_pod_anti_affinity())
        continue;
      ++*outcome_count;
      if (*outcome_count > kMaxProofOutcomes)
        return false;
      auto overlap = AddPod(baseline, workload.workload_id(), "__v2_first_ready", first_node);
      overlap = AddPod(overlap, workload.workload_id(), "__v2_second_pending", second_node);
      if (!StateCountsSafe(operation, profile, count, count + 2) ||
          !TopologyAndCapacity(snapshot, workload, overlap, {}))
        return false;
      auto final = RemoveSource(overlap, workload.workload_id(), operation.replica_id());
      if (!StateCountsSafe(operation, profile, count + 1, count + 1) ||
          !TopologyAndCapacity(snapshot, workload, final, {operation.source_node_id()}) ||
          CountFor(final, workload.workload_id()) != count + 1)
        return false;
    }
  }
  return true;
}

bool ValidateRecoveryOutcomes(const EvacuationSnapshot &snapshot,
                              const EvacuationOperation &operation, const WorkloadSpec &workload,
                              const ProfileCapacity &profile,
                              const std::vector<ReplicaPlacement> &final_state,
                              const std::string &failed_key, const std::string &failed_domain,
                              std::size_t *outcome_count) {
  const auto &model = snapshot.model_snapshot();
  std::set<std::string> failed_nodes{operation.source_node_id()};
  for (const auto &node : model.nodes())
    if (node.active() && node.health() == v1::NODE_HEALTH_HEALTHY &&
        LabelValue(node, failed_key) == failed_domain)
      failed_nodes.insert(node.node_id());
  std::vector<ReplicaPlacement> survivors;
  for (const auto &pod : final_state)
    if (!failed_nodes.contains(pod.node_id()))
      survivors.push_back(pod);
  const auto serving = CountFor(survivors, workload.workload_id());
  const auto required =
      std::max(operation.availability_floor_replicas(), profile.required_replicas);
  if (serving < required)
    return false;
  const auto target = operation.n_minus_1_recovery_target_replicas();
  if (target < serving)
    return false;
  const std::uint32_t missing = target - serving;
  if (missing > operation.maximum_temporary_surge_replicas())
    return false;
  if (missing == 0)
    return true;
  std::vector<std::string> candidates;
  for (const auto &node_id : operation.recovery_destination_node_ids()) {
    if (failed_nodes.contains(node_id))
      continue;
    const auto *node = NodeById(model, node_id);
    if (node == nullptr || !node->active() || node->health() != v1::NODE_HEALTH_HEALTHY ||
        !TaintsAllowed(workload, *node))
      return false;
    const bool already_present =
        std::any_of(survivors.begin(), survivors.end(), [&](const auto &p) {
          return p.workload_id() == workload.workload_id() && p.node_id() == node_id;
        });
    if (!already_present)
      candidates.push_back(node_id);
  }
  std::sort(candidates.begin(), candidates.end());
  if (candidates.size() < missing)
    return false;
  bool all_safe = true;
  std::vector<std::string> chosen;
  const auto enumerate = [&](const auto &self) -> void {
    if (!all_safe)
      return;
    if (chosen.size() == missing) {
      ++*outcome_count;
      if (*outcome_count > kMaxProofOutcomes) {
        all_safe = false;
        return;
      }
      auto recovered = survivors;
      for (std::size_t i = 0; i < chosen.size(); ++i) {
        recovered = AddPod(recovered, workload.workload_id(), "__v2_recovery_" + std::to_string(i),
                           chosen[i]);
        if (CountFor(recovered, workload.workload_id()) < required ||
            !TopologyAndCapacity(snapshot, workload, recovered, failed_nodes))
          all_safe = false;
      }
      if (CountFor(recovered, workload.workload_id()) < target ||
          !TopologyAndCapacity(snapshot, workload, recovered, failed_nodes))
        all_safe = false;
      return;
    }
    for (const auto &candidate : candidates) {
      if (std::find(chosen.begin(), chosen.end(), candidate) != chosen.end())
        continue;
      chosen.push_back(candidate);
      self(self);
      chosen.pop_back();
    }
  };
  enumerate(enumerate);
  return all_safe;
}

bool IntermediateNMinusOneStateSafe(const EvacuationSnapshot &snapshot,
                                    const WorkloadSpec &workload, std::uint32_t required,
                                    const std::vector<ReplicaPlacement> &state,
                                    const std::set<std::string> &pending,
                                    const std::string &source_replica, bool source_available,
                                    const std::string &failure_key,
                                    const std::string &failure_domain, std::size_t *outcome_count) {
  std::set<std::string> failed_nodes;
  for (const auto &node : snapshot.model_snapshot().nodes())
    if (node.active() && node.health() == v1::NODE_HEALTH_HEALTHY &&
        LabelValue(node, failure_key) == failure_domain)
      failed_nodes.insert(node.node_id());
  std::vector<ReplicaPlacement> survivors;
  std::uint32_t ready = 0;
  for (const auto &pod : state) {
    if (failed_nodes.contains(pod.node_id()))
      continue;
    survivors.push_back(pod);
    if (pod.workload_id() == workload.workload_id() && !pending.contains(pod.replica_id()) &&
        (pod.replica_id() != source_replica || source_available))
      ++ready;
  }
  ++*outcome_count;
  return *outcome_count <= kMaxProofOutcomes && ready >= required &&
         ResourceStateValid(snapshot, survivors, failed_nodes) &&
         TopologyStateValid(snapshot, workload, survivors, failed_nodes);
}

bool NMinusOneRecoverySafe(const EvacuationSnapshot &snapshot, const EvacuationOperation &operation,
                           const WorkloadSpec &workload, const ProfileCapacity &profile,
                           const std::vector<ReplicaPlacement> &baseline,
                           std::size_t *outcome_count) {
  std::size_t n1_workloads = 0;
  for (const auto &item : snapshot.model_snapshot().workloads())
    if (item.failure_tolerance() == v1::FAILURE_TOLERANCE_N_MINUS_1)
      ++n1_workloads;
  if (n1_workloads == 0)
    return workload.failure_tolerance() != v1::FAILURE_TOLERANCE_N_MINUS_1;
  if (n1_workloads != 1 || workload.failure_tolerance() != v1::FAILURE_TOLERANCE_N_MINUS_1)
    return false;
  const std::string &key = operation.n_minus_1_failure_domain_key();
  if (key.empty())
    return false;
  std::set<std::string> failure_domains;
  for (const auto &node : snapshot.model_snapshot().nodes()) {
    if (!node.active() || node.health() != v1::NODE_HEALTH_HEALTHY ||
        node.node_id() == operation.source_node_id())
      continue;
    const auto value = LabelValue(node, key);
    if (value.empty())
      return false;
    failure_domains.insert(value);
  }
  if (failure_domains.size() < 2)
    return false;
  std::set<std::string> stage_failure_domains;
  for (const auto &node : snapshot.model_snapshot().nodes()) {
    if (!node.active() || node.health() != v1::NODE_HEALTH_HEALTHY)
      continue;
    const auto domain = LabelValue(node, key);
    if (domain.empty())
      return false;
    stage_failure_domains.insert(domain);
  }
  if (stage_failure_domains.size() < 2)
    return false;
  std::vector<std::string> destinations(operation.destinations().allowed_node_ids().begin(),
                                        operation.destinations().allowed_node_ids().end());
  const auto required =
      std::max(operation.availability_floor_replicas(), profile.required_replicas);
  for (const auto &first : destinations) {
    const auto pending = AddPod(baseline, workload.workload_id(), "__v2_stage_pending", first);
    const auto ready_state = AddPod(baseline, workload.workload_id(), "__v2_stage_ready", first);
    for (const auto &domain : stage_failure_domains)
      if (!IntermediateNMinusOneStateSafe(snapshot, workload, required, pending,
                                          {"__v2_stage_pending"}, operation.replica_id(), true, key,
                                          domain, outcome_count) ||
          !IntermediateNMinusOneStateSafe(snapshot, workload, required, ready_state, {},
                                          operation.replica_id(), true, key, domain, outcome_count))
        return false;
    for (const auto &second : destinations) {
      if (first == second)
        continue;
      auto overlap =
          AddPod(ready_state, workload.workload_id(), "__v2_stage_second_pending", second);
      auto final = RemoveSource(baseline, workload.workload_id(), operation.replica_id());
      final = AddPod(final, workload.workload_id(), "__v2_stage_final_ready_a", first);
      final = AddPod(final, workload.workload_id(), "__v2_stage_final_ready_b", second);
      for (const auto &domain : stage_failure_domains)
        if (!IntermediateNMinusOneStateSafe(snapshot, workload, required, overlap,
                                            {"__v2_stage_second_pending"}, operation.replica_id(),
                                            false, key, domain, outcome_count) ||
            !IntermediateNMinusOneStateSafe(snapshot, workload, required, final, {},
                                            operation.replica_id(), false, key, domain,
                                            outcome_count))
          return false;
    }
  }
  for (std::size_t i = 0; i < destinations.size(); ++i) {
    for (std::size_t j = i + 1; j < destinations.size(); ++j) {
      auto final = RemoveSource(baseline, workload.workload_id(), operation.replica_id());
      final = AddPod(final, workload.workload_id(), "__v2_final_a", destinations[i]);
      final = AddPod(final, workload.workload_id(), "__v2_final_b", destinations[j]);
      for (const auto &domain : failure_domains)
        if (!ValidateRecoveryOutcomes(snapshot, operation, workload, profile, final, key, domain,
                                      outcome_count))
          return false;
    }
  }
  return true;
}

bool ExactStages(const EvacuationOperation &operation, std::uint32_t count,
                 const ProfileCapacity &profile) {
  if (!DependenciesValid(operation) ||
      operation.on_failure() != v2::FAILURE_BEHAVIOR_FAIL_CLOSED_KEEP_SURGE ||
      !IntermediateContractValid(operation, count, profile))
    return false;
  const auto min_ready =
      std::max(operation.availability_floor_replicas(), profile.required_replicas);
  if (count < min_ready || operation.surge_desired_replicas() != count + 1 ||
      operation.final_desired_replicas() != count + 1 ||
      operation.maximum_live_replicas() != count + 2 ||
      operation.maximum_temporary_surge_replicas() != 2 ||
      operation.required_pdb_disruptions() != 1 ||
      operation.maximum_concurrent_disruptions() != 1 ||
      operation.profile_required_replicas() != profile.required_replicas ||
      operation.n_minus_1_recovery_target_replicas() != count + 1 ||
      operation.readiness_timeout_ms() == 0)
    return false;
  return true;
}

bool SnapshotIdentityValid(const EvacuationSnapshot &snapshot) {
  const auto &model = snapshot.model_snapshot();
  if (snapshot.schema_version() != 2 || snapshot.cluster_uid().empty() ||
      snapshot.namespace_().empty() || model.schema_version() != 1 || model.snapshot_id().empty() ||
      model.policy().schema_version() != 1 || model.machine_catalog().schema_version() != 1 ||
      model.nodes_size() == 0 || model.nodes_size() > static_cast<int>(kMaxV2Nodes) ||
      model.workloads_size() == 0 || model.workloads_size() > static_cast<int>(kMaxV2Workloads) ||
      model.capacity_profiles_size() > static_cast<int>(kMaxV2Profiles) ||
      model.current_placements_size() > static_cast<int>(kMaxV2Placements) ||
      model.evaluation_time_ms() < model.observed_at_ms() ||
      model.expires_at_ms() <= model.evaluation_time_ms() ||
      model.evaluation_time_ms() - model.observed_at_ms() >
          model.policy().maximum_snapshot_age_ms() ||
      snapshot.inventory_observed_at_ms() > model.evaluation_time_ms() ||
      model.evaluation_time_ms() - snapshot.inventory_observed_at_ms() >
          model.policy().maximum_snapshot_age_ms() ||
      HasUnknownFieldsDeep(snapshot))
    return false;
  auto outer = snapshot;
  const auto outer_hash = outer.content_sha256();
  outer.clear_content_sha256();
  if (outer_hash.empty() || EvacuationSnapshotSha256(outer) != outer_hash)
    return false;
  auto inner = model;
  const auto inner_hash = inner.content_sha256();
  inner.clear_content_sha256();
  if (inner_hash.empty() || MessageSha256(inner) != inner_hash)
    return false;
  auto catalog = model.machine_catalog();
  const auto catalog_hash = catalog.catalog_sha256();
  catalog.clear_catalog_sha256();
  if (catalog_hash.empty() || MessageSha256(catalog) != catalog_hash)
    return false;

  std::set<std::string> nodes;
  std::map<std::string, const v1::MachineType *> machines;
  for (const auto &machine : model.machine_catalog().machines())
    if (machine.machine_type().empty() ||
        !machines.emplace(machine.machine_type(), &machine).second)
      return false;
  for (const auto &node : model.nodes()) {
    const auto machine = machines.find(node.machine_type());
    if (node.node_id().empty() || !nodes.insert(node.node_id()).second ||
        machine == machines.end() || !ResourceNonNegative(node.allocatable()) ||
        !ResourceNonNegative(node.reserved()) ||
        !SameResource(node.allocatable(), machine->second->allocatable()) ||
        node.hourly_cost_micros() != machine->second->hourly_cost_micros() ||
        !v1::NodeHealth_IsValid(node.health()) || node.health() == v1::NODE_HEALTH_UNSPECIFIED ||
        !LimitForNode(node, model.policy().headroom_ppm()))
      return false;
    std::set<std::string> label_keys;
    for (const auto &label : node.labels())
      if (label.key().empty() || !label_keys.insert(label.key()).second)
        return false;
  }
  if (snapshot.nodes_size() != model.nodes_size() ||
      snapshot.pods_size() != model.current_placements_size())
    return false;
  std::set<std::string> node_ids;
  std::set<std::string> node_uids;
  for (const auto &node : snapshot.nodes())
    if (node.node_id().empty() || node.node_uid().empty() || node.resource_version().empty() ||
        !nodes.contains(node.node_id()) || !node_ids.insert(node.node_id()).second ||
        !node_uids.insert(node.node_uid()).second)
      return false;

  std::set<std::string> workloads;
  for (const auto &workload : model.workloads())
    if (workload.workload_id().empty() || !workloads.insert(workload.workload_id()).second ||
        workload.min_replicas() > workload.max_replicas() ||
        workload.availability_floor() > workload.max_replicas() ||
        !ResourceNonNegative(workload.current_resources()) || workload.error_slo_ppm() > kPpm ||
        workload.latency_slo_ms() == 0 ||
        workload.failure_tolerance() == v1::FAILURE_TOLERANCE_UNSPECIFIED ||
        !v1::FailureTolerance_IsValid(static_cast<int>(workload.failure_tolerance())) ||
        workload.profile_confidence_factor_ppm() == 0 ||
        workload.model_accuracy_factor_ppm() == 0 || workload.degraded_cluster_factor_ppm() == 0 ||
        workload.profile_confidence_factor_ppm() > kPpm ||
        workload.model_accuracy_factor_ppm() > kPpm ||
        workload.degraded_cluster_factor_ppm() > kPpm || workload.max_throttle_ppm() > kPpm ||
        workload.max_memory_pressure_ppm() > kPpm ||
        (workload.min_topology_domains() > 0 && workload.topology_key().empty()))
      return false;

  std::set<std::string> replica_ids;
  std::set<std::string> pod_uids;
  std::set<std::string> pod_names;
  std::set<std::string> pod_replica_ids;
  for (const auto &placement : model.current_placements()) {
    if (placement.workload_id().empty() || placement.replica_id().empty() ||
        !workloads.contains(placement.workload_id()) || !nodes.contains(placement.node_id()) ||
        !replica_ids.insert(ReplicaKey(placement.workload_id(), placement.replica_id())).second)
      return false;
  }
  for (const auto &pod : snapshot.pods()) {
    const auto placement = std::find_if(model.current_placements().begin(),
                                        model.current_placements().end(), [&](const auto &item) {
                                          return item.workload_id() == pod.workload_id() &&
                                                 item.replica_id() == pod.replica_id() &&
                                                 item.node_id() == pod.node_id();
                                        });
    if (placement == model.current_placements().end() || pod.pod_name().empty() ||
        pod.pod_uid().empty() || pod.controller_uid().empty() || !pod.ready() ||
        pod.terminating() || !pod_uids.insert(pod.pod_uid()).second ||
        !pod_names.insert(pod.pod_name()).second ||
        !pod_replica_ids.insert(ReplicaKey(pod.workload_id(), pod.replica_id())).second)
      return false;
  }
  std::set<std::string> controllers;
  std::set<std::string> deployment_uids;
  std::set<std::string> replicaset_uids;
  for (const auto &controller : snapshot.controllers())
    if (controller.workload_id().empty() || !workloads.contains(controller.workload_id()) ||
        controller.namespace_() != snapshot.namespace_() || controller.deployment_name().empty() ||
        controller.deployment_uid().empty() || controller.deployment_resource_version().empty() ||
        controller.deployment_generation() == 0 || controller.replicaset_name().empty() ||
        controller.replicaset_uid().empty() || controller.replicaset_resource_version().empty() ||
        controller.pod_template_sha256().empty() || controller.desired_replicas() == 0 ||
        controller.active_replicaset_count() != 1 || controller.pdb_resource_version().empty() ||
        controller.observed_at_ms() > model.evaluation_time_ms() ||
        model.evaluation_time_ms() - controller.observed_at_ms() >
            model.policy().maximum_snapshot_age_ms() ||
        !controllers.insert(controller.workload_id()).second ||
        !deployment_uids.insert(controller.deployment_uid()).second ||
        !replicaset_uids.insert(controller.replicaset_uid()).second)
      return false;
  if (controllers.size() != workloads.size())
    return false;
  for (const auto &workload : model.workloads()) {
    const auto controller = std::find_if(
        snapshot.controllers().begin(), snapshot.controllers().end(),
        [&](const auto &item) { return item.workload_id() == workload.workload_id(); });
    const auto count =
        std::count_if(model.current_placements().begin(), model.current_placements().end(),
                      [&](const auto &p) { return p.workload_id() == workload.workload_id(); });
    if (controller == snapshot.controllers().end() ||
        controller->desired_replicas() != static_cast<std::uint32_t>(count))
      return false;
    for (const auto &pod : snapshot.pods())
      if (pod.workload_id() == workload.workload_id() &&
          pod.controller_uid() != controller->replicaset_uid())
        return false;
  }
  return true;
}

std::vector<v2::RuntimeCapability> RequiredCapabilities() {
  return {v2::RUNTIME_CAPABILITY_ENFORCED_NODE_SET,
          v2::RUNTIME_CAPABILITY_ENFORCED_RESOURCE_REQUESTS,
          v2::RUNTIME_CAPABILITY_NETWORK_BANDWIDTH_ADMISSION,
          v2::RUNTIME_CAPABILITY_POD_UID_EVICTION,
          v2::RUNTIME_CAPABILITY_PDB_ADMISSION,
          v2::RUNTIME_CAPABILITY_ATOMIC_MUTATION_FENCING,
          v2::RUNTIME_CAPABILITY_OWNER_IDENTITY_RECHECK,
          v2::RUNTIME_CAPABILITY_READINESS_OBSERVATION,
          v2::RUNTIME_CAPABILITY_HARD_POD_ANTI_AFFINITY};
}

bool CapabilityListValid(const EvacuationPlan &plan) {
  const auto expected = RequiredCapabilities();
  if (plan.required_runtime_capabilities_size() != static_cast<int>(expected.size()))
    return false;
  for (std::size_t i = 0; i < expected.size(); ++i)
    if (plan.required_runtime_capabilities(static_cast<int>(i)) != expected[i])
      return false;
  return true;
}

} // namespace

std::string EvacuationVerifierBuildId() { return VerifierBuildId() + "-evacuation-v2"; }

v2::VerificationResult VerifyEvacuationV2(const EvacuationSnapshot &snapshot,
                                          const EvacuationPlan &plan) {
  v2::VerificationResult result;
  result.set_schema_version(2);
  result.set_plan_id(plan.plan_id());
  result.set_verifier_build_id(EvacuationVerifierBuildId());
  result.set_execution_admitted(false);
  result.set_accepted(true);
  for (const auto capability : RequiredCapabilities())
    result.add_required_runtime_capabilities(capability);

  const bool snapshot_valid = SnapshotIdentityValid(snapshot);
  AddInvariant(&result, "v2_snapshot_identity_inventory_and_hash", snapshot_valid,
               snapshot_valid
                   ? "model snapshot, identities, owner records, freshness and hashes match"
                   : "unknown version/field, incomplete inventory, or mismatched snapshot binding");
  const auto &model = snapshot.model_snapshot();
  bool plan_hash_valid =
      plan.schema_version() == 2 && !plan.plan_id().empty() &&
      EvacuationPlanSha256(plan) == plan.plan_id() &&
      plan.snapshot_sha256() == snapshot.content_sha256() &&
      plan.snapshot_id() == model.snapshot_id() && plan.base_generation() == model.generation() &&
      plan.created_at_ms() == model.evaluation_time_ms() &&
      plan.expires_at_ms() == model.expires_at_ms() &&
      plan.policy_sha256() == MessageSha256(model.policy()) &&
      plan.machine_catalog_sha256() == model.machine_catalog().catalog_sha256() &&
      plan.optimizer_build_id() == OptimizerBuildId() &&
      plan.verifier_build_id() == EvacuationVerifierBuildId() && !HasUnknownFieldsDeep(plan);
  AddInvariant(&result, "v2_plan_canonical_binding_and_replay", plan_hash_valid,
               plan_hash_valid
                   ? "schema, snapshot, policy, catalog, build IDs and canonical plan hash match"
                   : "tampered or unsupported v2 plan binding");
  AddInvariant(&result, "v2_planner_candidate_flag", plan.feasible(),
               plan.feasible() ? "planner supplied a candidate" : plan.infeasible_reason());
  const bool capabilities_valid = CapabilityListValid(plan);
  AddInvariant(&result, "v2_runtime_requirements_explicit", capabilities_valid,
               capabilities_valid
                   ? "required runtime capabilities are explicit; execution remains disabled"
                   : "runtime capability contract is missing, reordered, or unsupported");

  bool accepted_operation = plan.operations_size() == 1 && plan.profile_bindings_size() == 1 &&
                            snapshot_valid && plan_hash_valid && capabilities_valid;
  std::string operation_detail = "exactly one supported source UID operation is required";
  std::size_t outcomes = 0;
  if (accepted_operation) {
    const auto &operation = plan.operations(0);
    const auto *workload = WorkloadById(model, operation.workload_id());
    const auto *source = PodByKey(snapshot, operation.workload_id(), operation.replica_id());
    const auto *controller = ControllerByWorkload(snapshot, operation.workload_id());
    const auto *source_node = NodeById(model, operation.source_node_id());
    const auto *source_node_identity = NodeIdentityById(snapshot, operation.source_node_id());
    const auto &binding = plan.profile_bindings(0);
    const bool source_bound =
        workload != nullptr && source != nullptr && controller != nullptr &&
        source_node != nullptr && source_node_identity != nullptr &&
        source->node_id() == operation.source_node_id() &&
        source->pod_name() == operation.source_pod_name() &&
        source->pod_uid() == operation.source_pod_uid() &&
        source->controller_uid() == operation.source_controller_uid() &&
        controller->replicaset_uid() == operation.source_controller_uid() &&
        source_node_identity->node_uid() == operation.source_node_uid() && source_node->active() &&
        source_node->health() == v1::NODE_HEALTH_HEALTHY && source->ready() &&
        !source->terminating() && controller->pdb_allowed_disruptions() >= 1 &&
        controller->desired_replicas() == operation.baseline_replicas() &&
        controller->active_replicaset_count() == 1;
    AddInvariant(&result, "v2_source_pod_owner_and_node_uid", source_bound,
                 source_bound
                     ? "source Pod UID, owner UID, logical replica and node UID are bound"
                     : "source identity is absent, stale, or owned by another controller/node");
    std::size_t source_occupants = 0;
    for (const auto &pod : model.current_placements())
      if (pod.node_id() == operation.source_node_id())
        ++source_occupants;
    const bool source_emptyable =
        source_occupants == 1 && operation.deactivate_source_node_after_empty();
    AddInvariant(&result, "v2_source_node_empty_after_move", source_emptyable,
                 source_emptyable
                     ? "source node contains only the named source Pod"
                     : "source-node deactivation would leave another placement behind");

    std::uint32_t baseline_count = 0;
    for (const auto &pod : model.current_placements())
      if (pod.workload_id() == operation.workload_id())
        ++baseline_count;
    bool profile_valid = false;
    std::optional<ProfileCapacity> profile;
    if (workload != nullptr && binding.workload_id() == operation.workload_id()) {
      profile = RecomputeProfileCapacity(model, *workload, binding.profile_id());
      profile_valid = profile.has_value() &&
                      profile->profile->profile_sha256() == binding.profile_sha256() &&
                      profile->point_index == binding.safe_point_index();
    }
    AddInvariant(&result, "v2_profile_derived_slo", profile_valid,
                 profile_valid
                     ? "selected profile digest and joint QPS/concurrency SLO point recomputed"
                     : "no valid profile-derived SLO bound for this workload");

    bool operation_well_formed =
        source_bound && source_emptyable && profile_valid && operation.sequence() == 0 &&
        baseline_count > 0 && operation.baseline_replicas() == baseline_count &&
        operation.replica_requests().SerializeAsString() ==
            workload->current_resources().SerializeAsString() &&
        operation.destinations().mode() == v2::DESTINATION_MODE_CONSTRAINT_NODE_SET &&
        operation.destinations().exact_destination_node_id().empty() &&
        operation.destinations().exclude_source_node() &&
        operation.destinations().require_hard_pod_anti_affinity() &&
        operation.destinations().allowed_node_ids_size() >= 2 &&
        operation.destinations().allowed_node_ids_size() <= 8 &&
        operation.failure_tolerance() == workload->failure_tolerance() &&
        operation.n_minus_1_failure_domain_key() ==
            (workload->failure_tolerance() == v1::FAILURE_TOLERANCE_N_MINUS_1
                 ? (workload->topology_key().empty() ? "zone" : workload->topology_key())
                 : "") &&
        operation.topology_key() == workload->topology_key() &&
        operation.minimum_topology_domains() == workload->min_topology_domains() &&
        operation.maximum_topology_skew() == workload->max_topology_skew() &&
        operation.availability_floor_replicas() == workload->availability_floor() &&
        operation.readiness_stabilization_ms() == workload->readiness_stabilization_ms() &&
        workload->startup_ms() <=
            std::numeric_limits<std::uint64_t>::max() - workload->readiness_stabilization_ms() &&
        operation.readiness_timeout_ms() >=
            std::max<std::uint64_t>(1, workload->startup_ms() +
                                           workload->readiness_stabilization_ms()) &&
        workload->max_moves() >= 1 &&
        model.policy().maximum_plan_actions() >=
            static_cast<std::uint32_t>(operation.actions_size()) &&
        operation.deactivate_source_node_after_empty() &&
        ExactStages(operation, baseline_count, *profile);
    std::set<std::string> destination_ids;
    std::string previous;
    for (const auto &destination : operation.destinations().allowed_node_ids()) {
      const auto *node = NodeById(model, destination);
      const bool taints_allowed =
          workload != nullptr && node != nullptr && TaintsAllowed(*workload, *node);
      bool workload_already_present = false;
      for (const auto &pod : model.current_placements())
        if (pod.workload_id() == operation.workload_id() && pod.node_id() == destination)
          workload_already_present = true;
      if (destination.empty() || destination == operation.source_node_id() || node == nullptr ||
          !node->active() || node->health() != v1::NODE_HEALTH_HEALTHY ||
          workload_already_present || !taints_allowed ||
          !destination_ids.insert(destination).second ||
          (!previous.empty() && previous >= destination))
        operation_well_formed = false;
      previous = destination;
    }
    std::set<std::string> recovery_ids(operation.recovery_destination_node_ids().begin(),
                                       operation.recovery_destination_node_ids().end());
    if (recovery_ids != destination_ids)
      operation_well_formed = false;
    if (workload == nullptr || baseline_count + 1 > workload->max_replicas())
      operation_well_formed = false;
    AddInvariant(
        &result, "v2_constraint_contract_and_action_order", operation_well_formed,
        operation_well_formed
            ? "destination set, explicit intermediate states and fail-closed dependencies are valid"
            : "exact destination, malformed limits, unsupported field, or unsafe action sequence");

    bool baseline_feasible = operation_well_formed && profile_valid;
    std::vector<ReplicaPlacement> baseline(model.current_placements().begin(),
                                           model.current_placements().end());
    if (baseline_feasible)
      baseline_feasible = ResourceStateValid(snapshot, baseline, {}) &&
                          TopologyStateValid(snapshot, *workload, baseline, {});
    bool stages_feasible = baseline_feasible;
    if (stages_feasible)
      stages_feasible =
          CheckPlacementOutcomes(snapshot, operation, *workload, *profile, baseline, &outcomes);
    AddInvariant(
        &result, "v2_all_intermediate_and_final_outcomes", stages_feasible,
        stages_feasible
            ? "every permitted scheduler outcome fits capacity, topology, availability and profile "
              "SLO"
            : "at least one enumerated pending, ready, N+2 overlap, or final outcome is unsafe");
    const bool n1_feasible =
        stages_feasible &&
        NMinusOneRecoverySafe(snapshot, operation, *workload, *profile, baseline, &outcomes);
    AddInvariant(
        &result, "v2_n_minus_1_recovery_reserve", n1_feasible,
        n1_feasible
            ? "each modeled failure domain preserves serving SLO and every permitted recovery fits"
            : "failure-domain serving or recovery reserve is insufficient/incomplete");
    AddInvariant(&result, "v2_bounded_outcome_enumeration", outcomes <= kMaxProofOutcomes,
                 outcomes <= kMaxProofOutcomes
                     ? "all outcomes were exhaustively enumerated"
                     : "verification outcome bound exceeded; no sampling used");
    accepted_operation = source_bound && source_emptyable && profile_valid &&
                         operation_well_formed && baseline_feasible && stages_feasible &&
                         n1_feasible && outcomes <= kMaxProofOutcomes;
    operation_detail = accepted_operation ? "single operation has a complete conditional proof"
                                          : "operation failed an independent v2 safety invariant";
  } else {
    AddInvariant(&result, "v2_operation_count", false,
                 "v2 supports exactly one fully bound source Pod UID operation per plan");
    AddInvariant(&result, "v2_source_pod_owner_and_node_uid", false,
                 "operation and snapshot inventory were not bound");
    AddInvariant(&result, "v2_source_node_empty_after_move", false,
                 "source node deactivation is not justified");
    AddInvariant(&result, "v2_profile_derived_slo", false, "profile binding is absent");
    AddInvariant(&result, "v2_constraint_contract_and_action_order", false,
                 "operation is missing or malformed");
    AddInvariant(&result, "v2_all_intermediate_and_final_outcomes", false, "operation is missing");
    AddInvariant(&result, "v2_n_minus_1_recovery_reserve", false, "operation is missing");
    AddInvariant(&result, "v2_bounded_outcome_enumeration", false, "operation is missing");
  }
  AddInvariant(&result, "v2_execution_admission_separate", !result.execution_admitted(),
               "mathematical verification never enables the live evacuation adapter");
  if (accepted_operation && result.accepted())
    result.set_accepted(true);
  else
    result.set_accepted(false);
  result.set_canonical_result_sha256(EvacuationResultSha256(result));
  (void)operation_detail;
  return result;
}

} // namespace fleetright
