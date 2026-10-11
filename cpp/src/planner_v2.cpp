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
using v2::EvacuationSnapshot;
using v2::PodIdentity;
using ProtoResource = v1::ResourceVector;
constexpr std::uint64_t kPpm = 1'000'000;
constexpr std::uint32_t kMinimumTailPpm = 990'000;
constexpr std::uint64_t kMaxProfileTrials = 1'000'000'000'000ULL;
constexpr std::size_t kMaxV2Nodes = 64;
constexpr std::size_t kMaxV2Workloads = 100;
constexpr std::size_t kMaxV2Profiles = 10'000;
constexpr std::size_t kMaxV2Placements = 10'000;

struct ProfileChoice {
  const v1::CapacityProfile *profile = nullptr;
  std::uint32_t point_index = 0;
  std::uint64_t qps = 0;
  std::uint64_t concurrency = 0;
  std::uint32_t required_replicas = 0;
};

std::string ReplicaKey(const std::string &workload, const std::string &replica) {
  return workload + "\x1f" + replica;
}

bool SameResource(const ProtoResource &left, const ProtoResource &right) {
  return left.cpu_millicores() == right.cpu_millicores() &&
         left.memory_mib() == right.memory_mib() && left.network_mbps() == right.network_mbps();
}

bool ValidResource(const ProtoResource &value) {
  return value.cpu_millicores() >= 0 && value.memory_mib() >= 0 && value.network_mbps() >= 0;
}

bool AddResource(const ProtoResource &left, const ProtoResource &right, ProtoResource *out) {
  const auto sum = CheckedAdd({CpuMillicores(left.cpu_millicores()), MemoryMiB(left.memory_mib()),
                               NetworkMbps(left.network_mbps())},
                              {CpuMillicores(right.cpu_millicores()), MemoryMiB(right.memory_mib()),
                               NetworkMbps(right.network_mbps())});
  if (!sum)
    return false;
  out->set_cpu_millicores(sum->cpu.value);
  out->set_memory_mib(sum->memory.value);
  out->set_network_mbps(sum->network.value);
  return true;
}

std::optional<ProtoResource> NodeCapacity(const v1::NodeState &node, std::uint32_t headroom_ppm) {
  if (!ValidResource(node.allocatable()) || !ValidResource(node.reserved()) || headroom_ppm > kPpm)
    return std::nullopt;
  ProtoResource result;
  const auto available = [headroom_ppm](std::int64_t allocatable, std::int64_t reserved,
                                        std::int64_t *out) {
    if (allocatable < 0 || reserved < 0)
      return false;
    __extension__ using UInt128 = unsigned __int128;
    const UInt128 held = static_cast<UInt128>(allocatable) * (kPpm - headroom_ppm) / kPpm;
    if (held > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) ||
        static_cast<std::int64_t>(held) < reserved)
      return false;
    *out = static_cast<std::int64_t>(held) - reserved;
    return true;
  };
  std::int64_t cpu = 0;
  std::int64_t memory = 0;
  std::int64_t network = 0;
  if (!available(node.allocatable().cpu_millicores(), node.reserved().cpu_millicores(), &cpu) ||
      !available(node.allocatable().memory_mib(), node.reserved().memory_mib(), &memory) ||
      !available(node.allocatable().network_mbps(), node.reserved().network_mbps(), &network))
    return std::nullopt;
  result.set_cpu_millicores(cpu);
  result.set_memory_mib(memory);
  result.set_network_mbps(network);
  return result;
}

std::string LabelValue(const v1::NodeState &node, const std::string &key) {
  for (const auto &label : node.labels())
    if (label.key() == key)
      return label.value();
  return {};
}

bool TaintsAllowed(const v1::NodeState &node, const WorkloadSpec &workload) {
  for (const auto &taint : node.taints()) {
    if (std::find(workload.tolerated_taint_keys().begin(), workload.tolerated_taint_keys().end(),
                  taint.key()) == workload.tolerated_taint_keys().end())
      return false;
  }
  return true;
}

bool Fits(const ProtoResource &used, const ProtoResource &limit) {
  return ValidResource(used) && ValidResource(limit) &&
         used.cpu_millicores() <= limit.cpu_millicores() &&
         used.memory_mib() <= limit.memory_mib() && used.network_mbps() <= limit.network_mbps();
}

bool ProfileHashValid(const v1::CapacityProfile &profile) {
  auto copy = profile;
  const auto expected = copy.profile_sha256();
  copy.clear_profile_sha256();
  return !expected.empty() && MessageSha256(copy) == expected;
}

std::optional<std::uint32_t> WilsonLowerPpm(std::uint64_t successes, std::uint64_t trials) {
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

std::optional<std::uint32_t> WilsonUpperPpm(std::uint64_t failures, std::uint64_t trials) {
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

bool SafePoint(const v1::CapacityPoint &point, const WorkloadSpec &workload) {
  if (point.request_count() == 0 || point.request_count() > kMaxProfileTrials ||
      point.threshold_trials() != point.request_count() ||
      point.threshold_successes() > point.threshold_trials() ||
      point.error_count() > point.request_count() || point.latency_success_lower_ppm() > kPpm ||
      point.error_rate_upper_ppm() > kPpm || point.error_rate_wilson_upper_ppm() > kPpm ||
      point.memory_pressure_ppm() > kPpm || point.cpu_throttling_ppm() > kPpm ||
      point.run_dispersion_ppm() > kPpm || point.repeated_run_count() > 10'000)
    return false;
  const auto latency = WilsonLowerPpm(point.threshold_successes(), point.threshold_trials());
  const auto errors = WilsonUpperPpm(point.error_count(), point.request_count());
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

std::optional<ProfileChoice> SelectProfile(const OptimizationSnapshot &snapshot,
                                           const WorkloadSpec &workload) {
  std::optional<ProfileChoice> best;
  for (const auto &profile : snapshot.capacity_profiles()) {
    if (profile.schema_version() != 1 || profile.workload_id() != workload.workload_id() ||
        !SameResource(profile.allocation(), workload.current_resources()) ||
        profile.image_digest() != workload.image_digest() ||
        profile.request_mix_id() != workload.request_mix_id() ||
        profile.runtime_config_class() != workload.runtime_config_class() ||
        profile.platform_class() != workload.platform_class() || !ProfileHashValid(profile) ||
        profile.generated_at_ms() > snapshot.evaluation_time_ms() ||
        profile.valid_until_ms() <= snapshot.evaluation_time_ms() ||
        snapshot.evaluation_time_ms() - profile.generated_at_ms() >
            snapshot.policy().maximum_profile_age_ms() ||
        profile.confidence_ppm() < snapshot.policy().minimum_profile_confidence_ppm() ||
        profile.confidence_ppm() > kPpm || profile.conservative_haircut_ppm() > kPpm ||
        !(snapshot.policy().allow_synthetic_profiles() &&
          profile.measurement_kind().starts_with("synthetic_")) ||
        !ValidResource(profile.allocation()))
      continue;
    for (int index = 0; index < profile.safe_points_size(); ++index) {
      const auto &point = profile.safe_points(index);
      if (!SafePoint(point, workload) ||
          profile.confidence_ppm() > point.latency_success_lower_ppm())
        continue;
      const auto qps = MultiplyPpmFloor(
          point.safe_qps_milli(), workload.profile_confidence_factor_ppm(),
          workload.model_accuracy_factor_ppm(), workload.degraded_cluster_factor_ppm());
      const auto concurrency = MultiplyPpmFloor(
          point.safe_concurrency(), workload.profile_confidence_factor_ppm(),
          workload.model_accuracy_factor_ppm(), workload.degraded_cluster_factor_ppm());
      if (!qps || !concurrency || *qps == 0 || *concurrency == 0)
        continue;
      const auto required = std::max({CeilDivide(workload.demand_qps_milli(), *qps),
                                      CeilDivide(workload.concurrency_bound(), *concurrency),
                                      static_cast<std::uint64_t>(workload.min_replicas()),
                                      static_cast<std::uint64_t>(workload.availability_floor())});
      if (required > std::numeric_limits<std::uint32_t>::max())
        continue;
      ProfileChoice choice{&profile, static_cast<std::uint32_t>(index), *qps, *concurrency,
                           static_cast<std::uint32_t>(required)};
      if (!best ||
          std::tie(choice.required_replicas, choice.profile->profile_id(), choice.point_index) <
              std::tie(best->required_replicas, best->profile->profile_id(), best->point_index))
        best = choice;
    }
  }
  return best;
}

const v1::NodeState *NodeById(const OptimizationSnapshot &snapshot, const std::string &id) {
  for (const auto &node : snapshot.nodes())
    if (node.node_id() == id)
      return &node;
  return nullptr;
}

std::map<std::string, const WorkloadSpec *> WorkloadIndex(const OptimizationSnapshot &snapshot) {
  std::map<std::string, const WorkloadSpec *> result;
  for (const auto &workload : snapshot.workloads())
    result.emplace(workload.workload_id(), &workload);
  return result;
}

bool ResourceStateSafe(const EvacuationSnapshot &snapshot,
                       const std::vector<ReplicaPlacement> &pods,
                       const std::set<std::string> &inactive_nodes) {
  const auto &model = snapshot.model_snapshot();
  const auto workloads = WorkloadIndex(model);
  std::map<std::string, const v1::NodeState *> nodes;
  std::map<std::string, ProtoResource> limits;
  std::map<std::string, ProtoResource> used;
  for (const auto &node : model.nodes()) {
    if (!node.active() || node.health() != v1::NODE_HEALTH_HEALTHY ||
        inactive_nodes.contains(node.node_id()))
      continue;
    const auto limit = NodeCapacity(node, model.policy().headroom_ppm());
    if (!limit)
      return false;
    nodes.emplace(node.node_id(), &node);
    limits.emplace(node.node_id(), *limit);
  }
  for (const auto &pod : pods) {
    const auto workload = workloads.find(pod.workload_id());
    const auto node = nodes.find(pod.node_id());
    if (workload == workloads.end() || node == nodes.end() ||
        !TaintsAllowed(*node->second, *workload->second))
      return false;
    ProtoResource after;
    if (!AddResource(used[pod.node_id()], workload->second->current_resources(), &after) ||
        !Fits(after, limits.at(pod.node_id())))
      return false;
    used[pod.node_id()] = std::move(after);
  }
  return true;
}

std::vector<ReplicaPlacement> AppendReplica(const std::vector<ReplicaPlacement> &source,
                                            const std::string &workload_id,
                                            const std::string &replica_id,
                                            const std::string &node_id) {
  auto result = source;
  ReplicaPlacement pod;
  pod.set_workload_id(workload_id);
  pod.set_replica_id(replica_id);
  pod.set_node_id(node_id);
  result.push_back(std::move(pod));
  return result;
}

bool TopologyStateSafe(const EvacuationSnapshot &snapshot, const WorkloadSpec &workload,
                       const std::vector<ReplicaPlacement> &pods,
                       const std::set<std::string> &inactive_nodes) {
  if (workload.min_topology_domains() == 0)
    return true;
  std::map<std::string, std::uint32_t> domain_counts;
  const auto &model = snapshot.model_snapshot();
  for (const auto &node : model.nodes()) {
    if (!node.active() || node.health() != v1::NODE_HEALTH_HEALTHY ||
        inactive_nodes.contains(node.node_id()) || !TaintsAllowed(node, workload))
      continue;
    const auto domain = LabelValue(node, workload.topology_key());
    if (domain.empty())
      return false;
    domain_counts.try_emplace(domain, 0U);
  }
  if (domain_counts.size() < workload.min_topology_domains())
    return false;
  for (const auto &pod : pods) {
    if (pod.workload_id() != workload.workload_id() || inactive_nodes.contains(pod.node_id()))
      continue;
    const auto *node = NodeById(model, pod.node_id());
    if (node == nullptr)
      return false;
    const auto domain = LabelValue(*node, workload.topology_key());
    const auto entry = domain_counts.find(domain);
    if (domain.empty() || entry == domain_counts.end() ||
        entry->second == std::numeric_limits<std::uint32_t>::max())
      return false;
    ++entry->second;
  }
  const auto occupied = std::count_if(domain_counts.begin(), domain_counts.end(),
                                      [](const auto &item) { return item.second != 0; });
  if (occupied < workload.min_topology_domains())
    return false;
  const auto [min_it, max_it] = std::minmax_element(
      domain_counts.begin(), domain_counts.end(),
      [](const auto &left, const auto &right) { return left.second < right.second; });
  return max_it->second - min_it->second <= workload.max_topology_skew();
}

std::uint32_t CountWorkload(const std::vector<ReplicaPlacement> &pods,
                            const std::string &workload_id) {
  return static_cast<std::uint32_t>(std::count_if(
      pods.begin(), pods.end(), [&](const auto &pod) { return pod.workload_id() == workload_id; }));
}

std::vector<ReplicaPlacement> WithoutSource(const std::vector<ReplicaPlacement> &pods,
                                            const std::string &workload,
                                            const std::string &replica) {
  std::vector<ReplicaPlacement> result;
  for (const auto &pod : pods)
    if (!(pod.workload_id() == workload && pod.replica_id() == replica))
      result.push_back(pod);
  return result;
}

bool CandidateStateSafe(const EvacuationSnapshot &snapshot, const WorkloadSpec &workload,
                        const ProfileChoice &profile, const std::vector<ReplicaPlacement> &baseline,
                        const std::string &source_replica, const std::string &source_node,
                        const std::vector<std::string> &destinations,
                        std::string *rejection_reason) {
  if (destinations.size() < 2) {
    *rejection_reason = "fewer than two enforceable scheduler destinations";
    return false;
  }
  std::set<std::string> active_nodes;
  for (const auto &node : snapshot.model_snapshot().nodes())
    if (node.active() && node.health() == v1::NODE_HEALTH_HEALTHY)
      active_nodes.insert(node.node_id());
  if (!active_nodes.contains(source_node)) {
    *rejection_reason = "source node is not active and healthy";
    return false;
  }
  const std::uint32_t baseline_count = CountWorkload(baseline, workload.workload_id());
  const std::uint64_t required_ready =
      std::max({static_cast<std::uint64_t>(profile.required_replicas),
                static_cast<std::uint64_t>(workload.availability_floor()),
                static_cast<std::uint64_t>(workload.min_replicas())});
  if (baseline_count < required_ready || baseline_count < 1 ||
      baseline_count == std::numeric_limits<std::uint32_t>::max()) {
    *rejection_reason = "baseline ready replicas are below the profile-derived availability floor";
    return false;
  }

  for (const auto &first_destination : destinations) {
    auto first = AppendReplica(baseline, workload.workload_id(), "v2-surge-0", first_destination);
    if (!ResourceStateSafe(snapshot, first, {})) {
      *rejection_reason = "insufficient resource capacity for a permitted surge placement";
      return false;
    }
    if (!TopologyStateSafe(snapshot, workload, first, {})) {
      *rejection_reason = "topology requirements fail for a permitted first placement";
      return false;
    }
    for (const auto &second_destination : destinations) {
      // Required Pod anti-affinity is part of the proposed scheduler constraints.
      if (second_destination == first_destination)
        continue;
      auto overlap = AppendReplica(first, workload.workload_id(), "v2-surge-1", second_destination);
      if (!ResourceStateSafe(snapshot, overlap, {})) {
        *rejection_reason = "insufficient resource capacity for N+2 termination overlap";
        return false;
      }
      if (!TopologyStateSafe(snapshot, workload, overlap, {})) {
        *rejection_reason = "topology requirements fail for a permitted overlap placement";
        return false;
      }
      auto final = WithoutSource(overlap, workload.workload_id(), source_replica);
      if (!ResourceStateSafe(snapshot, final, {source_node})) {
        *rejection_reason = "insufficient resource capacity for a permitted final placement";
        return false;
      }
      if (!TopologyStateSafe(snapshot, workload, final, {source_node})) {
        *rejection_reason = "topology requirements fail for a permitted final placement";
        return false;
      }
      if (CountWorkload(final, workload.workload_id()) != baseline_count + 1) {
        *rejection_reason = "final replica count differs from the N+1 contract";
        return false;
      }
    }
  }
  return true;
}

bool PlannerNMinusOneStageSafe(const EvacuationSnapshot &snapshot, const WorkloadSpec &workload,
                               std::uint32_t required, const std::vector<ReplicaPlacement> &state,
                               const std::set<std::string> &pending,
                               const std::string &source_replica, bool source_available,
                               const std::string &failure_key, const std::string &failure_domain,
                               std::size_t *proof_outcomes) {
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
  if (++*proof_outcomes > 250'000 || ready < required ||
      !ResourceStateSafe(snapshot, survivors, failed_nodes) ||
      !TopologyStateSafe(snapshot, workload, survivors, failed_nodes))
    return false;
  return true;
}

bool PlannerNMinusOneRecoverySafe(const EvacuationSnapshot &snapshot, const WorkloadSpec &workload,
                                  const ProfileChoice &profile,
                                  const std::vector<ReplicaPlacement> &baseline,
                                  const std::string &source_replica, const std::string &source_node,
                                  const std::vector<std::string> &destinations,
                                  const std::vector<std::string> &recovery_nodes) {
  std::size_t n1_workloads = 0;
  for (const auto &item : snapshot.model_snapshot().workloads())
    if (item.failure_tolerance() == v1::FAILURE_TOLERANCE_N_MINUS_1)
      ++n1_workloads;
  if (n1_workloads == 0) {
    return workload.failure_tolerance() != v1::FAILURE_TOLERANCE_N_MINUS_1;
  }
  if (n1_workloads != 1 || workload.failure_tolerance() != v1::FAILURE_TOLERANCE_N_MINUS_1)
    return false;

  const std::string failure_key =
      workload.topology_key().empty() ? "zone" : workload.topology_key();
  std::set<std::string> failure_domains;
  for (const auto &node : snapshot.model_snapshot().nodes()) {
    if (!node.active() || node.health() != v1::NODE_HEALTH_HEALTHY || node.node_id() == source_node)
      continue;
    const auto domain = LabelValue(node, failure_key);
    if (domain.empty())
      return false;
    failure_domains.insert(domain);
  }
  if (failure_domains.size() < 2)
    return false;

  std::set<std::string> stage_failure_domains;
  for (const auto &node : snapshot.model_snapshot().nodes()) {
    if (!node.active() || node.health() != v1::NODE_HEALTH_HEALTHY)
      continue;
    const auto domain = LabelValue(node, failure_key);
    if (domain.empty())
      return false;
    stage_failure_domains.insert(domain);
  }
  if (stage_failure_domains.size() < 2)
    return false;

  const std::uint32_t baseline_count = CountWorkload(baseline, workload.workload_id());
  if (baseline_count == 0 || baseline_count >= std::numeric_limits<std::uint32_t>::max() - 1)
    return false;
  const std::uint32_t target = baseline_count + 1;
  const std::uint32_t required = std::max(workload.availability_floor(), profile.required_replicas);
  std::size_t proof_outcomes = 0;
  constexpr std::size_t kMaximumProofOutcomes = 250'000;
  for (const auto &first : destinations) {
    const auto pending = AppendReplica(baseline, workload.workload_id(), "v2-stage-pending", first);
    const auto ready_state =
        AppendReplica(baseline, workload.workload_id(), "v2-stage-ready", first);
    for (const auto &domain : stage_failure_domains)
      if (!PlannerNMinusOneStageSafe(snapshot, workload, required, pending, {"v2-stage-pending"},
                                     source_replica, true, failure_key, domain, &proof_outcomes) ||
          !PlannerNMinusOneStageSafe(snapshot, workload, required, ready_state, {}, source_replica,
                                     true, failure_key, domain, &proof_outcomes))
        return false;
    for (const auto &second : destinations) {
      if (first == second)
        continue;
      auto overlap =
          AppendReplica(ready_state, workload.workload_id(), "v2-stage-second-pending", second);
      auto final = WithoutSource(baseline, workload.workload_id(), source_replica);
      final = AppendReplica(final, workload.workload_id(), "v2-stage-final-ready-a", first);
      final = AppendReplica(final, workload.workload_id(), "v2-stage-final-ready-b", second);
      for (const auto &domain : stage_failure_domains)
        if (!PlannerNMinusOneStageSafe(snapshot, workload, required, overlap,
                                       {"v2-stage-second-pending"}, source_replica, false,
                                       failure_key, domain, &proof_outcomes) ||
            !PlannerNMinusOneStageSafe(snapshot, workload, required, final, {}, source_replica,
                                       false, failure_key, domain, &proof_outcomes))
          return false;
    }
  }
  for (const auto &first : destinations) {
    for (const auto &second : destinations) {
      if (first == second)
        continue;
      auto final = WithoutSource(baseline, workload.workload_id(), source_replica);
      final = AppendReplica(final, workload.workload_id(), "v2-final-a", first);
      final = AppendReplica(final, workload.workload_id(), "v2-final-b", second);
      for (const auto &domain : failure_domains) {
        std::set<std::string> failed_nodes{source_node};
        for (const auto &node : snapshot.model_snapshot().nodes())
          if (node.active() && node.health() == v1::NODE_HEALTH_HEALTHY &&
              LabelValue(node, failure_key) == domain)
            failed_nodes.insert(node.node_id());

        std::vector<ReplicaPlacement> survivors;
        for (const auto &pod : final)
          if (!failed_nodes.contains(pod.node_id()))
            survivors.push_back(pod);
        const auto serving = CountWorkload(survivors, workload.workload_id());
        if (serving < required || serving > target)
          return false;
        const std::uint32_t missing = target - serving;
        if (missing > 2)
          return false;
        if (missing == 0)
          continue;

        std::vector<std::string> candidates;
        for (const auto &node_id : recovery_nodes) {
          if (failed_nodes.contains(node_id))
            continue;
          const bool occupied =
              std::any_of(survivors.begin(), survivors.end(), [&](const auto &pod) {
                return pod.workload_id() == workload.workload_id() && pod.node_id() == node_id;
              });
          if (!occupied)
            candidates.push_back(node_id);
        }
        if (candidates.size() < missing)
          return false;

        bool safe = true;
        std::vector<std::string> chosen;
        const auto enumerate = [&](const auto &self) -> void {
          if (!safe)
            return;
          if (chosen.size() == missing) {
            if (++proof_outcomes > kMaximumProofOutcomes) {
              safe = false;
              return;
            }
            auto recovered = survivors;
            for (std::size_t i = 0; i < chosen.size(); ++i) {
              recovered = AppendReplica(recovered, workload.workload_id(),
                                        "v2-recovery-" + std::to_string(i), chosen[i]);
              if (CountWorkload(recovered, workload.workload_id()) < required ||
                  !ResourceStateSafe(snapshot, recovered, failed_nodes) ||
                  !TopologyStateSafe(snapshot, workload, recovered, failed_nodes)) {
                safe = false;
                return;
              }
            }
            if (CountWorkload(recovered, workload.workload_id()) < target ||
                !ResourceStateSafe(snapshot, recovered, failed_nodes) ||
                !TopologyStateSafe(snapshot, workload, recovered, failed_nodes))
              safe = false;
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
        if (!safe)
          return false;
      }
    }
  }
  return true;
}

bool SnapshotValid(const EvacuationSnapshot &snapshot) {
  const auto &model = snapshot.model_snapshot();
  if (snapshot.schema_version() != 2 || snapshot.owner_contract_version() != 1 ||
      snapshot.cluster_uid().empty() || snapshot.namespace_().empty() ||
      model.schema_version() != 1 || model.snapshot_id().empty() ||
      model.policy().schema_version() != 1 || model.machine_catalog().schema_version() != 1 ||
      model.policy().headroom_ppm() > kPpm || model.evaluation_time_ms() < model.observed_at_ms() ||
      model.expires_at_ms() <= model.evaluation_time_ms() ||
      model.evaluation_time_ms() - model.observed_at_ms() >
          model.policy().maximum_snapshot_age_ms() ||
      snapshot.inventory_observed_at_ms() > model.evaluation_time_ms() ||
      model.evaluation_time_ms() - snapshot.inventory_observed_at_ms() >
          model.policy().maximum_snapshot_age_ms() ||
      model.nodes_size() == 0 || model.nodes_size() > static_cast<int>(kMaxV2Nodes) ||
      model.workloads_size() == 0 || model.workloads_size() > static_cast<int>(kMaxV2Workloads) ||
      model.capacity_profiles_size() > static_cast<int>(kMaxV2Profiles) ||
      model.current_placements_size() > static_cast<int>(kMaxV2Placements) ||
      HasUnknownFieldsDeep(snapshot))
    return false;
  auto model_copy = model;
  const auto model_hash = model_copy.content_sha256();
  model_copy.clear_content_sha256();
  if (model_hash.empty() || MessageSha256(model_copy) != model_hash)
    return false;
  auto catalog_copy = model.machine_catalog();
  const auto catalog_hash = catalog_copy.catalog_sha256();
  catalog_copy.clear_catalog_sha256();
  if (catalog_hash.empty() || MessageSha256(catalog_copy) != catalog_hash ||
      snapshot.content_sha256().empty() ||
      EvacuationSnapshotSha256(snapshot) != snapshot.content_sha256())
    return false;

  std::set<std::string> node_ids;
  std::map<std::string, const v1::MachineType *> machines;
  for (const auto &machine : model.machine_catalog().machines())
    if (machine.machine_type().empty() ||
        !machines.emplace(machine.machine_type(), &machine).second)
      return false;
  for (const auto &node : model.nodes()) {
    const auto found = machines.find(node.machine_type());
    if (node.node_id().empty() || !node_ids.insert(node.node_id()).second ||
        found == machines.end() ||
        !SameResource(node.allocatable(), found->second->allocatable()) ||
        node.hourly_cost_micros() != found->second->hourly_cost_micros() ||
        !v1::NodeHealth_IsValid(node.health()) || node.health() == v1::NODE_HEALTH_UNSPECIFIED ||
        !NodeCapacity(node, model.policy().headroom_ppm()))
      return false;
    std::set<std::string> labels;
    for (const auto &label : node.labels())
      if (label.key().empty() || !labels.insert(label.key()).second)
        return false;
  }
  std::set<std::string> workload_ids;
  for (const auto &workload : model.workloads()) {
    if (workload.workload_id().empty() || !workload_ids.insert(workload.workload_id()).second ||
        workload.min_replicas() > workload.max_replicas() ||
        workload.availability_floor() > workload.max_replicas() ||
        workload.error_slo_ppm() > kPpm || workload.latency_slo_ms() == 0 ||
        !ValidResource(workload.current_resources()) ||
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
  }
  std::set<std::string> pod_keys;
  for (const auto &placement : model.current_placements())
    if (placement.workload_id().empty() || placement.replica_id().empty() ||
        !workload_ids.contains(placement.workload_id()) ||
        !node_ids.contains(placement.node_id()) ||
        !pod_keys.insert(ReplicaKey(placement.workload_id(), placement.replica_id())).second)
      return false;
  if (snapshot.nodes_size() != model.nodes_size() ||
      snapshot.pods_size() != model.current_placements_size())
    return false;

  std::map<std::string, std::string> node_uids;
  std::set<std::string> seen_node_ids;
  for (const auto &node : snapshot.nodes())
    if (node.node_id().empty() || node.node_uid().empty() || node.resource_version().empty() ||
        !node_ids.contains(node.node_id()) || !seen_node_ids.insert(node.node_id()).second ||
        !node_uids.emplace(node.node_id(), node.node_uid()).second)
      return false;
  std::map<std::string, const PodIdentity *> pod_by_key;
  std::set<std::string> pod_uids;
  std::set<std::string> pod_names;
  for (const auto &pod : snapshot.pods()) {
    const auto key = ReplicaKey(pod.workload_id(), pod.replica_id());
    const auto *node = NodeById(model, pod.node_id());
    const bool matches = std::any_of(
        model.current_placements().begin(), model.current_placements().end(), [&](const auto &p) {
          return p.workload_id() == pod.workload_id() && p.replica_id() == pod.replica_id() &&
                 p.node_id() == pod.node_id();
        });
    if (pod.workload_id().empty() || pod.replica_id().empty() || pod.pod_name().empty() ||
        pod.pod_uid().empty() || pod.owner_kind().empty() || pod.owner_name().empty() ||
        pod.owner_uid().empty() || pod.logical_slot_id() != pod.replica_id() ||
        !pod.controller_uid().empty() || node == nullptr || !matches || !pod.ready() ||
        pod.terminating() || !pod_uids.insert(pod.pod_uid()).second ||
        !pod_names.insert(pod.pod_name()).second || !pod_by_key.emplace(key, &pod).second)
      return false;
  }
  std::set<std::string> controller_ids;
  std::set<std::string> owner_uids;
  for (const auto &controller : snapshot.controllers()) {
    if (controller.workload_id().empty() || !workload_ids.contains(controller.workload_id()) ||
        controller.namespace_() != snapshot.namespace_() ||
        controller.owner_kind() != "FleetRightWorkload" || controller.owner_name().empty() ||
        controller.owner_uid().empty() || controller.owner_resource_version().empty() ||
        controller.owner_generation() == 0 ||
        controller.controller_type() != "fleetright-workload-controller/v1" ||
        controller.execution_protocol() != "monotonic-scale-up-v1" ||
        !controller.deployment_name().empty() || !controller.deployment_uid().empty() ||
        !controller.deployment_resource_version().empty() ||
        controller.deployment_generation() != 0 || !controller.replicaset_name().empty() ||
        !controller.replicaset_uid().empty() || !controller.replicaset_resource_version().empty() ||
        controller.active_replicaset_count() != 0 || controller.pod_template_sha256().empty() ||
        controller.desired_replicas() == 0 || controller.pdb_resource_version().empty() ||
        controller.observed_at_ms() > model.evaluation_time_ms() ||
        model.evaluation_time_ms() - controller.observed_at_ms() >
            model.policy().maximum_snapshot_age_ms() ||
        !controller_ids.insert(controller.workload_id()).second ||
        !owner_uids.insert(controller.owner_uid()).second)
      return false;
    if (controller.owner_name() != controller.workload_id())
      return false;
  }
  if (controller_ids.size() != workload_ids.size())
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
          (pod.owner_kind() != controller->owner_kind() ||
           pod.owner_name() != controller->owner_name() ||
           pod.owner_uid() != controller->owner_uid()))
        return false;
  }
  return true;
}

std::string NoPlanReason(const std::string &reason) { return "NO FEASIBLE PLAN: " + reason; }

void AddAction(v2::EvacuationOperation *operation, std::uint32_t sequence,
               v2::EvacuationStage stage, const std::string &postcondition,
               std::optional<std::uint32_t> dependency) {
  auto *action = operation->add_actions();
  action->set_sequence(sequence);
  action->set_action_id("evacuation-" + std::to_string(sequence));
  action->set_stage(stage);
  if (dependency)
    action->add_depends_on_sequences(*dependency);
  action->set_expected_postcondition(postcondition);
  action->set_on_failure(v2::FAILURE_BEHAVIOR_FAIL_CLOSED_KEEP_SURGE);
}

bool CompleteCandidate(const EvacuationSnapshot &snapshot, const WorkloadSpec &workload,
                       const ProfileChoice &profile, const PodIdentity &source,
                       const v2::ControllerIdentity &controller,
                       const std::vector<std::string> &destinations,
                       const std::vector<std::string> &recovery_nodes,
                       std::string *rejection_reason, v2::EvacuationPlan *plan) {
  const auto &model = snapshot.model_snapshot();
  std::vector<ReplicaPlacement> baseline(model.current_placements().begin(),
                                         model.current_placements().end());
  const auto count = CountWorkload(baseline, workload.workload_id());
  // ponytail: node sets are capped at eight to bound universal outcome enumeration.
  if (count == 0 || count >= workload.max_replicas() || controller.desired_replicas() != count ||
      controller.pdb_allowed_disruptions() < 1 || workload.max_moves() < 1) {
    *rejection_reason = "replica, move, or observed PDB disruption budget is insufficient";
    return false;
  }
  if (destinations.size() < 2) {
    *rejection_reason = "fewer than two enforceable scheduler destinations";
    return false;
  }
  if (destinations.size() > 8) {
    *rejection_reason = "scheduler outcome set exceeds the v2 bound of eight destinations";
    return false;
  }
  if (model.policy().maximum_plan_actions() < 6) {
    *rejection_reason = "six evacuation stages exceed the configured action budget";
    return false;
  }
  if (!CandidateStateSafe(snapshot, workload, profile, baseline, source.replica_id(),
                          source.node_id(), destinations, rejection_reason))
    return false;
  if (!PlannerNMinusOneRecoverySafe(snapshot, workload, profile, baseline, source.replica_id(),
                                    source.node_id(), destinations, recovery_nodes)) {
    *rejection_reason = "N-1 availability or recovery reserve is insufficient";
    return false;
  }
  const auto *source_node = NodeById(model, source.node_id());
  if (source_node == nullptr) {
    *rejection_reason = "source node identity is missing";
    return false;
  }
  std::size_t source_occupants = 0;
  for (const auto &pod : baseline)
    if (pod.node_id() == source.node_id())
      ++source_occupants;
  if (source_occupants != 1) {
    *rejection_reason = "source node cannot be emptied by this single-Pod operation";
    return false;
  }

  auto *operation = plan->add_operations();
  operation->set_sequence(0);
  operation->set_workload_id(workload.workload_id());
  operation->set_replica_id(source.replica_id());
  operation->set_source_pod_name(source.pod_name());
  operation->set_source_pod_uid(source.pod_uid());
  operation->set_source_owner_kind(controller.owner_kind());
  operation->set_source_owner_name(controller.owner_name());
  operation->set_source_owner_uid(controller.owner_uid());
  operation->set_source_logical_slot_id(source.logical_slot_id());
  operation->set_source_node_id(source.node_id());
  for (const auto &node : snapshot.nodes())
    if (node.node_id() == source.node_id())
      operation->set_source_node_uid(node.node_uid());
  auto *constraint = operation->mutable_destinations();
  constraint->set_mode(v2::DESTINATION_MODE_CONSTRAINT_NODE_SET);
  for (const auto &node : destinations)
    constraint->add_allowed_node_ids(node);
  constraint->set_require_hard_pod_anti_affinity(true);
  constraint->set_exclude_source_node(true);
  *operation->mutable_replica_requests() = workload.current_resources();
  operation->set_baseline_replicas(count);
  operation->set_surge_desired_replicas(count + 1);
  operation->set_final_desired_replicas(count + 1);
  operation->set_maximum_live_replicas(count + 2);
  operation->set_maximum_temporary_surge_replicas(2);
  operation->set_required_pdb_disruptions(1);
  operation->set_maximum_concurrent_disruptions(1);
  operation->set_availability_floor_replicas(workload.availability_floor());
  operation->set_profile_required_replicas(profile.required_replicas);
  operation->set_readiness_stabilization_ms(workload.readiness_stabilization_ms());
  const auto timeout = workload.startup_ms() > std::numeric_limits<std::uint64_t>::max() -
                                                   workload.readiness_stabilization_ms()
                           ? std::numeric_limits<std::uint64_t>::max()
                           : workload.startup_ms() + workload.readiness_stabilization_ms();
  if (timeout == std::numeric_limits<std::uint64_t>::max()) {
    plan->mutable_operations()->RemoveLast();
    return false;
  }
  operation->set_readiness_timeout_ms(std::max<std::uint64_t>(timeout, 1));
  operation->set_topology_key(workload.topology_key());
  operation->set_minimum_topology_domains(workload.min_topology_domains());
  operation->set_maximum_topology_skew(workload.max_topology_skew());
  operation->set_failure_tolerance(workload.failure_tolerance());
  operation->set_n_minus_1_failure_domain_key(
      workload.failure_tolerance() == v1::FAILURE_TOLERANCE_N_MINUS_1
          ? (workload.topology_key().empty() ? "zone" : workload.topology_key())
          : "");
  operation->set_n_minus_1_recovery_target_replicas(count + 1);
  for (const auto &node : recovery_nodes)
    operation->add_recovery_destination_node_ids(node);
  operation->set_deactivate_source_node_after_empty(true);
  operation->set_on_failure(v2::FAILURE_BEHAVIOR_FAIL_CLOSED_KEEP_SURGE);

  auto add_state = [&](v2::IntermediateStateKind kind, std::uint32_t ready, std::uint32_t live,
                       bool source_consumes, bool source_available) {
    auto *state = operation->add_permitted_intermediate_states();
    state->set_kind(kind);
    state->set_minimum_ready_replicas(ready);
    state->set_maximum_live_replicas(live);
    state->set_source_pod_consumes_resources(source_consumes);
    state->set_source_pod_counts_as_available(source_available);
  };
  add_state(v2::INTERMEDIATE_STATE_BASELINE, count, count, true, true);
  add_state(v2::INTERMEDIATE_STATE_FIRST_REPLACEMENT_PENDING, count, count + 1, true, true);
  add_state(v2::INTERMEDIATE_STATE_FIRST_REPLACEMENT_READY,
            std::max(count + 1, profile.required_replicas), count + 1, true, true);
  add_state(v2::INTERMEDIATE_STATE_SOURCE_TERMINATING_OVERLAP, count, count + 2, true, false);
  add_state(v2::INTERMEDIATE_STATE_FINAL, std::max(count + 1, profile.required_replicas), count + 1,
            false, false);

  AddAction(operation, 0, v2::EVACUATION_STAGE_APPLY_DESTINATION_CONSTRAINT,
            "approved node set and hard anti-affinity are active", std::nullopt);
  AddAction(operation, 1, v2::EVACUATION_STAGE_SET_SURGE_REPLICAS,
            "desired replicas equal baseline plus one", 0);
  AddAction(operation, 2, v2::EVACUATION_STAGE_WAIT_REPLACEMENT_READY,
            "one replacement is stably ready for the configured duration", 1);
  AddAction(operation, 3, v2::EVACUATION_STAGE_EVICT_SOURCE_UID,
            "the named source Pod UID is evicted through the observed PDB", 2);
  AddAction(operation, 4, v2::EVACUATION_STAGE_WAIT_SOURCE_ABSENT,
            "source UID is absent and desired replicas are stably ready at baseline plus one", 3);
  AddAction(operation, 5, v2::EVACUATION_STAGE_DEACTIVATE_SOURCE_NODE,
            "source node has no remaining workload Pods", 4);
  return true;
}

} // namespace

v2::EvacuationPlan PlanEvacuationV2(const EvacuationSnapshot &snapshot) {
  v2::EvacuationPlan plan;
  plan.set_schema_version(2);
  plan.set_optimizer_build_id(OptimizerBuildId());
  plan.set_verifier_build_id(EvacuationVerifierBuildId());
  plan.set_feasible(false);
  plan.set_infeasible_reason(NoPlanReason("invalid, stale, unknown-field, or unbound v2 snapshot"));
  if (!snapshot.model_snapshot().snapshot_id().empty())
    plan.set_snapshot_id(snapshot.model_snapshot().snapshot_id());
  plan.set_base_generation(snapshot.model_snapshot().generation());
  plan.set_created_at_ms(snapshot.model_snapshot().evaluation_time_ms());
  plan.set_expires_at_ms(snapshot.model_snapshot().expires_at_ms());
  plan.set_snapshot_sha256(snapshot.content_sha256());
  plan.set_policy_sha256(MessageSha256(snapshot.model_snapshot().policy()));
  plan.set_machine_catalog_sha256(snapshot.model_snapshot().machine_catalog().catalog_sha256());
  if (!SnapshotValid(snapshot)) {
    plan.set_plan_id(EvacuationPlanSha256(plan));
    return plan;
  }
  std::string rejection_reason = "no supported constraint-based evacuation candidate";
  const auto &model = snapshot.model_snapshot();
  std::map<std::string, const v2::ControllerIdentity *> controllers;
  for (const auto &controller : snapshot.controllers())
    controllers.emplace(controller.workload_id(), &controller);
  std::map<std::string, const PodIdentity *> pods;
  for (const auto &pod : snapshot.pods())
    pods.emplace(ReplicaKey(pod.workload_id(), pod.replica_id()), &pod);
  for (const auto &workload : model.workloads()) {
    if (workload.failure_tolerance() == v1::FAILURE_TOLERANCE_N_MINUS_1) {
      const auto n1_count =
          std::count_if(model.workloads().begin(), model.workloads().end(), [](const auto &item) {
            return item.failure_tolerance() == v1::FAILURE_TOLERANCE_N_MINUS_1;
          });
      if (n1_count > 1) {
        plan.set_infeasible_reason(NoPlanReason(
            "v2 supports recovery proof for one N-1 workload per evacuation operation"));
        break;
      }
    }
    const auto controller_it = controllers.find(workload.workload_id());
    if (controller_it == controllers.end())
      continue;
    const auto profile = SelectProfile(model, workload);
    if (!profile) {
      rejection_reason = "no fresh capacity profile satisfies the workload SLO";
      continue;
    }
    std::vector<std::string> eligible;
    for (const auto &node : model.nodes()) {
      if (node.active() && node.health() == v1::NODE_HEALTH_HEALTHY &&
          TaintsAllowed(node, workload)) {
        const bool already_present =
            std::any_of(model.current_placements().begin(), model.current_placements().end(),
                        [&](const auto &placement) {
                          return placement.workload_id() == workload.workload_id() &&
                                 placement.node_id() == node.node_id();
                        });
        if (!already_present)
          eligible.push_back(node.node_id());
      }
    }
    std::sort(eligible.begin(), eligible.end());
    if (eligible.size() < 2)
      rejection_reason = "fewer than two active healthy destination nodes";
    std::vector<const PodIdentity *> sources;
    for (const auto &pod : snapshot.pods())
      if (pod.workload_id() == workload.workload_id())
        sources.push_back(&pod);
    std::sort(sources.begin(), sources.end(), [](const auto *left, const auto *right) {
      return std::tie(left->node_id(), left->replica_id(), left->pod_uid()) <
             std::tie(right->node_id(), right->replica_id(), right->pod_uid());
    });
    for (const auto *source : sources) {
      std::vector<std::string> destinations;
      for (const auto &node_id : eligible) {
        if (node_id == source->node_id())
          continue;
        const bool workload_already_on_node =
            std::any_of(model.current_placements().begin(), model.current_placements().end(),
                        [&](const auto &placement) {
                          return placement.workload_id() == workload.workload_id() &&
                                 placement.node_id() == node_id;
                        });
        if (!workload_already_on_node)
          destinations.push_back(node_id);
      }
      if (destinations.size() < 2)
        continue;
      // ponytail: one source UID per plan; expand to multiple sources only after outcome products
      // for concurrent evictions are independently enumerated and bounded.
      const auto *controller = controller_it->second;
      if (controller->owner_kind() != source->owner_kind() ||
          controller->owner_name() != source->owner_name() ||
          controller->owner_uid() != source->owner_uid() ||
          source->logical_slot_id() != source->replica_id() ||
          controller->pdb_allowed_disruptions() < 1) {
        rejection_reason = "source Pod owner or observed PDB disruption is unsupported";
        continue;
      }
      const std::vector<ReplicaPlacement> baseline(model.current_placements().begin(),
                                                   model.current_placements().end());
      if (!CompleteCandidate(snapshot, workload, *profile, *source, *controller, destinations,
                             destinations, &rejection_reason, &plan))
        continue;
      auto *binding = plan.add_profile_bindings();
      binding->set_workload_id(workload.workload_id());
      binding->set_profile_id(profile->profile->profile_id());
      binding->set_profile_sha256(profile->profile->profile_sha256());
      binding->set_safe_point_index(profile->point_index);
      for (const auto capability :
           {v2::RUNTIME_CAPABILITY_ENFORCED_NODE_SET,
            v2::RUNTIME_CAPABILITY_ENFORCED_RESOURCE_REQUESTS,
            v2::RUNTIME_CAPABILITY_NETWORK_BANDWIDTH_ADMISSION,
            v2::RUNTIME_CAPABILITY_POD_UID_EVICTION, v2::RUNTIME_CAPABILITY_PDB_ADMISSION,
            v2::RUNTIME_CAPABILITY_ATOMIC_MUTATION_FENCING,
            v2::RUNTIME_CAPABILITY_OWNER_IDENTITY_RECHECK,
            v2::RUNTIME_CAPABILITY_READINESS_OBSERVATION,
            v2::RUNTIME_CAPABILITY_HARD_POD_ANTI_AFFINITY})
        plan.add_required_runtime_capabilities(capability);
      plan.set_feasible(true);
      plan.clear_infeasible_reason();
      plan.set_plan_id(EvacuationPlanSha256(plan));
      return plan;
    }
  }
  if (plan.infeasible_reason().starts_with("NO FEASIBLE PLAN: invalid"))
    plan.set_infeasible_reason(NoPlanReason(rejection_reason));
  plan.set_plan_id(EvacuationPlanSha256(plan));
  return plan;
}

} // namespace fleetright
