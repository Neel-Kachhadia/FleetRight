#include "fleetright/planner.hpp"

#include "fleetright/hash.hpp"
#include "fleetright/units.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

namespace fleetright {
namespace {
using v1::CapacityProfile;
using v1::OptimizationSnapshot;
using v1::ReplicaPlacement;
using ProtoResourceVector = v1::ResourceVector;
using v1::WorkloadSpec;

constexpr std::uint64_t kPpm = 1'000'000;
constexpr std::uint32_t kTailSuccessPpm = 990'000;
constexpr std::size_t kMaxWorkloads = 1'000;
constexpr std::size_t kMaxNodes = 500;
constexpr std::size_t kMaxProfiles = 10'000;
constexpr std::size_t kMaxPlacements = 100'000;
constexpr std::uint64_t kMaxProfileTrials = 1'000'000'000'000ULL;
__extension__ using UInt128 = unsigned __int128;

struct Candidate {
  const CapacityProfile *profile = nullptr;
  const v1::CapacityPoint *point = nullptr;
  std::uint32_t point_index = 0;
  std::uint64_t effective_qps = 0;
  std::uint64_t effective_concurrency = 0;
  std::uint32_t replicas = 0;
  std::uint64_t shape_score = 0;
};

struct Replica {
  std::string workload_id;
  std::string replica_id;
  std::uint32_t priority = 0;
  ProtoResourceVector resources;
  std::string current_node;
  std::uint64_t dominant_score = 0;
};

struct NodeUse {
  const v1::NodeState *node = nullptr;
  ProtoResourceVector capacity;
  ProtoResourceVector used;
  std::set<std::string> workloads;
  std::map<std::string, std::uint32_t> topology_counts;
};

std::string ReplicaKey(const std::string &workload_id, const std::string &replica_id) {
  return workload_id + "\x1f" + replica_id;
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
  const auto ppm = static_cast<std::uint32_t>(std::floor(lower * kPpm));
  return ppm;
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

bool PointEvidenceConsistent(const v1::CapacityPoint &point) {
  if (point.request_count() == 0 || point.request_count() > kMaxProfileTrials ||
      point.threshold_trials() != point.request_count() ||
      point.threshold_successes() > point.threshold_trials() ||
      point.error_count() > point.request_count() || point.latency_success_lower_ppm() > kPpm ||
      point.error_rate_upper_ppm() > kPpm || point.error_rate_wilson_upper_ppm() > kPpm ||
      point.memory_pressure_ppm() > kPpm || point.cpu_throttling_ppm() > kPpm ||
      point.run_dispersion_ppm() > kPpm || point.repeated_run_count() > 10'000) {
    return false;
  }
  const auto latency_lower = WilsonLowerPpm(point.threshold_successes(), point.threshold_trials());
  const auto error_upper = WilsonUpperPpm(point.error_count(), point.request_count());
  if (!latency_lower || !error_upper || point.latency_success_lower_ppm() > *latency_lower ||
      point.error_rate_wilson_upper_ppm() < *error_upper) {
    return false;
  }
  __extension__ using UInt128 = unsigned __int128;
  const UInt128 raw_rate =
      (static_cast<UInt128>(point.error_count()) * kPpm + point.request_count() - 1) /
      point.request_count();
  return raw_rate <= point.error_rate_upper_ppm();
}

bool SameResource(const ProtoResourceVector &left, const ProtoResourceVector &right) {
  return left.cpu_millicores() == right.cpu_millicores() &&
         left.memory_mib() == right.memory_mib() && left.network_mbps() == right.network_mbps();
}

bool CatalogMatchesNodes(const OptimizationSnapshot &snapshot) {
  std::map<std::string, const v1::MachineType *> machines;
  for (const auto &machine : snapshot.machine_catalog().machines()) {
    if (machine.machine_type().empty() || machine.allocatable().cpu_millicores() < 0 ||
        machine.allocatable().memory_mib() < 0 || machine.allocatable().network_mbps() < 0 ||
        machine.hourly_cost_micros() == 0 ||
        !machines.emplace(machine.machine_type(), &machine).second)
      return false;
  }
  if (machines.empty())
    return false;
  std::set<std::string> node_ids;
  for (const auto &node : snapshot.nodes()) {
    const auto machine = machines.find(node.machine_type());
    if (node.node_id().empty() || !node_ids.insert(node.node_id()).second ||
        machine == machines.end() ||
        !SameResource(node.allocatable(), machine->second->allocatable()) ||
        node.hourly_cost_micros() != machine->second->hourly_cost_micros() ||
        !v1::NodeHealth_IsValid(node.health()) || node.health() == v1::NODE_HEALTH_UNSPECIFIED) {
      return false;
    }
    std::set<std::string> label_keys;
    for (const auto &label : node.labels()) {
      if (label.key().empty() || !label_keys.insert(label.key()).second)
        return false;
    }
    std::set<std::string> taint_keys;
    for (const auto &taint : node.taints()) {
      if (taint.key().empty() || !taint_keys.insert(taint.key()).second)
        return false;
    }
  }
  return true;
}

std::uint64_t ScaledRatio(std::uint64_t value, std::uint64_t denominator) {
  if (denominator == 0)
    return std::numeric_limits<std::uint64_t>::max();
  __extension__ using UInt128 = unsigned __int128;
  const UInt128 scaled = static_cast<UInt128>(value) * kPpm / denominator;
  return scaled > std::numeric_limits<std::uint64_t>::max()
             ? std::numeric_limits<std::uint64_t>::max()
             : static_cast<std::uint64_t>(scaled);
}

std::uint64_t SaturatingAdd(std::uint64_t left, std::uint64_t right) {
  return left > std::numeric_limits<std::uint64_t>::max() - right
             ? std::numeric_limits<std::uint64_t>::max()
             : left + right;
}

struct PpmRatio {
  std::uint64_t numerator;
  UInt128 denominator;
};

std::uint32_t RatioPpm(PpmRatio input) {
  if (input.numerator == 0)
    return 0;
  if (input.denominator == 0)
    return static_cast<std::uint32_t>(kPpm);
  const UInt128 scaled = static_cast<UInt128>(input.numerator) * kPpm;
  const UInt128 ratio = (scaled + input.denominator - 1) / input.denominator;
  return static_cast<std::uint32_t>(std::min<UInt128>(ratio, kPpm));
}

struct BoundaryRiskInput {
  std::uint64_t qps_demand_milli;
  std::uint64_t concurrency_demand;
  std::uint64_t effective_qps_per_replica_milli;
  std::uint64_t effective_concurrency_per_replica;
  std::uint32_t replicas;
};

std::uint32_t BoundaryRiskPpm(const BoundaryRiskInput &input) {
  const auto qps_risk =
      RatioPpm({input.qps_demand_milli,
                static_cast<UInt128>(input.effective_qps_per_replica_milli) * input.replicas});
  const auto concurrency_risk =
      RatioPpm({input.concurrency_demand,
                static_cast<UInt128>(input.effective_concurrency_per_replica) * input.replicas});
  return std::max(qps_risk, concurrency_risk);
}

bool ValidResource(const ProtoResourceVector &resource) {
  return resource.cpu_millicores() >= 0 && resource.memory_mib() >= 0 &&
         resource.network_mbps() >= 0;
}

bool AddResource(const ProtoResourceVector &left, const ProtoResourceVector &right,
                 ProtoResourceVector *out) {
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

bool FitsProto(const ProtoResourceVector &used, const ProtoResourceVector &capacity) {
  return ValidResource(used) && ValidResource(capacity) &&
         used.cpu_millicores() <= capacity.cpu_millicores() &&
         used.memory_mib() <= capacity.memory_mib() &&
         used.network_mbps() <= capacity.network_mbps();
}

std::optional<ProtoResourceVector> NodeCapacity(const v1::NodeState &node,
                                                std::uint32_t headroom_ppm) {
  if (!ValidResource(node.allocatable()) || !ValidResource(node.reserved()) ||
      headroom_ppm > kPpm) {
    return std::nullopt;
  }
  ProtoResourceVector result;
  const auto dimension = [headroom_ppm](std::int64_t alloc, std::int64_t reserved,
                                        std::int64_t *available) {
    if (alloc < 0 || reserved < 0)
      return false;
    __extension__ using UInt128 = unsigned __int128;
    const auto kept = static_cast<UInt128>(alloc) * (kPpm - headroom_ppm) / kPpm;
    if (kept > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) ||
        static_cast<std::int64_t>(kept) < reserved) {
      return false;
    }
    *available = static_cast<std::int64_t>(kept) - reserved;
    return true;
  };
  std::int64_t cpu = 0;
  std::int64_t memory = 0;
  std::int64_t network = 0;
  if (!dimension(node.allocatable().cpu_millicores(), node.reserved().cpu_millicores(), &cpu) ||
      !dimension(node.allocatable().memory_mib(), node.reserved().memory_mib(), &memory) ||
      !dimension(node.allocatable().network_mbps(), node.reserved().network_mbps(), &network)) {
    return std::nullopt;
  }
  result.set_cpu_millicores(cpu);
  result.set_memory_mib(memory);
  result.set_network_mbps(network);
  return result;
}

std::optional<std::uint64_t> RequiredReplicas(std::uint64_t qps, std::uint64_t concurrency,
                                              std::uint64_t effective_qps,
                                              std::uint64_t effective_concurrency) {
  if (effective_qps == 0 || effective_concurrency == 0)
    return std::nullopt;
  return std::max(CeilDivide(qps, effective_qps), CeilDivide(concurrency, effective_concurrency));
}

bool ProfileHashValid(const CapacityProfile &profile) {
  auto copy = profile;
  const std::string expected = copy.profile_sha256();
  copy.clear_profile_sha256();
  return !expected.empty() && MessageSha256(copy) == expected;
}

bool ProfileCompatible(const CapacityProfile &profile, const WorkloadSpec &workload,
                       const OptimizationSnapshot &snapshot) {
  const auto &policy = snapshot.policy();
  const bool empirical_container_profile =
      profile.measurement_kind() == "docker_runtime" && profile.enforceable_resource_limits();
  const bool test_profile =
      policy.allow_synthetic_profiles() && profile.measurement_kind().starts_with("synthetic_");
  if (profile.schema_version() != 1 || profile.profile_id().empty() ||
      profile.workload_id() != workload.workload_id() ||
      profile.image_digest() != workload.image_digest() ||
      profile.request_mix_id() != workload.request_mix_id() ||
      profile.runtime_config_class() != workload.runtime_config_class() ||
      profile.platform_class() != workload.platform_class() || !ProfileHashValid(profile) ||
      profile.generated_at_ms() > snapshot.evaluation_time_ms() ||
      profile.valid_until_ms() <= snapshot.evaluation_time_ms() ||
      snapshot.evaluation_time_ms() - profile.generated_at_ms() > policy.maximum_profile_age_ms() ||
      profile.confidence_ppm() < policy.minimum_profile_confidence_ppm() ||
      profile.confidence_ppm() > kPpm || profile.conservative_haircut_ppm() > kPpm ||
      (!empirical_container_profile && !test_profile) || !ValidResource(profile.allocation())) {
    return false;
  }
  return true;
}

std::optional<Candidate> CandidateFor(const CapacityProfile &profile, const WorkloadSpec &workload,
                                      const OptimizationSnapshot &snapshot) {
  if (!ProfileCompatible(profile, workload, snapshot))
    return std::nullopt;
  std::optional<Candidate> best;
  for (int point_number = 0; point_number < profile.safe_points_size(); ++point_number) {
    const auto &point = profile.safe_points(point_number);
    if (!PointEvidenceConsistent(point) || point.repeated_run_count() < 2 ||
        point.latency_success_lower_ppm() < kTailSuccessPpm ||
        point.error_rate_wilson_upper_ppm() > workload.error_slo_ppm() ||
        point.error_rate_upper_ppm() > workload.error_slo_ppm() ||
        point.p99_upper_ms() > workload.latency_slo_ms() || point.queue_growth_upper_milli() > 0 ||
        point.memory_pressure_ppm() > workload.max_memory_pressure_ppm() ||
        point.cpu_throttling_ppm() > workload.max_throttle_ppm() || !point.stable_across_runs() ||
        point.run_dispersion_ppm() > 250'000U) {
      continue;
    }
    if (profile.confidence_ppm() > point.latency_success_lower_ppm())
      continue;
    const auto qps = MultiplyPpmFloor(
        point.safe_qps_milli(), workload.profile_confidence_factor_ppm(),
        workload.model_accuracy_factor_ppm(), workload.degraded_cluster_factor_ppm());
    const auto concurrency = MultiplyPpmFloor(
        point.safe_concurrency(), workload.profile_confidence_factor_ppm(),
        workload.model_accuracy_factor_ppm(), workload.degraded_cluster_factor_ppm());
    if (!qps || !concurrency || *qps == 0 || *concurrency == 0)
      continue;
    const auto required = RequiredReplicas(workload.demand_qps_milli(),
                                           workload.concurrency_bound(), *qps, *concurrency);
    if (!required)
      continue;
    std::uint64_t replicas = std::max<std::uint64_t>(
        *required, std::max(workload.min_replicas(), workload.availability_floor()));
    if (workload.failure_tolerance() == v1::FAILURE_TOLERANCE_N_MINUS_1) {
      if (replicas == std::numeric_limits<std::uint64_t>::max())
        continue;
      ++replicas;
    }
    if (replicas > workload.max_replicas() ||
        replicas > std::numeric_limits<std::uint32_t>::max()) {
      continue;
    }
    const auto &shape = profile.allocation();
    if (shape.cpu_millicores() <= 0 || shape.memory_mib() <= 0 || shape.network_mbps() < 0)
      continue;
    if (workload.recent_oom_or_reclaim() &&
        shape.memory_mib() < workload.current_resources().memory_mib())
      continue;
    Candidate candidate{&profile, &point,       static_cast<std::uint32_t>(point_number),
                        *qps,     *concurrency, static_cast<std::uint32_t>(replicas),
                        0};
    // Fixed normalization keeps shape ordering independent of host floating-point behavior.
    candidate.shape_score = SaturatingAdd(
        SaturatingAdd(ScaledRatio(static_cast<std::uint64_t>(shape.cpu_millicores()), 16'000),
                      ScaledRatio(static_cast<std::uint64_t>(shape.memory_mib()), 32'768)),
        ScaledRatio(static_cast<std::uint64_t>(shape.network_mbps()), 10'000));
    if (!best ||
        std::tie(candidate.shape_score, candidate.replicas, candidate.profile->profile_id()) <
            std::tie(best->shape_score, best->replicas, best->profile->profile_id())) {
      best = candidate;
    }
  }
  return best;
}

struct WorkloadRisk {
  std::optional<Candidate> evidence;
  std::uint32_t uncertainty_ppm = static_cast<std::uint32_t>(kPpm);
  std::uint32_t boundary_ppm = static_cast<std::uint32_t>(kPpm);
};

WorkloadRisk CurrentRiskFor(const OptimizationSnapshot &snapshot, const WorkloadSpec &workload,
                            std::uint32_t current_replicas) {
  WorkloadRisk result;
  for (const auto &profile : snapshot.capacity_profiles()) {
    if (profile.workload_id() != workload.workload_id() ||
        !SameResource(profile.allocation(), workload.current_resources()))
      continue;
    const auto candidate = CandidateFor(profile, workload, snapshot);
    if (candidate && candidate->replicas <= current_replicas &&
        (!result.evidence ||
         std::tie(candidate->shape_score, candidate->replicas, candidate->profile->profile_id()) <
             std::tie(result.evidence->shape_score, result.evidence->replicas,
                      result.evidence->profile->profile_id()))) {
      result.evidence = candidate;
    }
  }
  if (result.evidence) {
    const auto &candidate = *result.evidence;
    const auto floor =
        std::min({candidate.profile->confidence_ppm(), workload.profile_confidence_factor_ppm(),
                  workload.model_accuracy_factor_ppm(), workload.degraded_cluster_factor_ppm()});
    result.uncertainty_ppm = static_cast<std::uint32_t>(kPpm - floor);
    result.boundary_ppm = BoundaryRiskPpm({workload.demand_qps_milli(),
                                           workload.concurrency_bound(), candidate.effective_qps,
                                           candidate.effective_concurrency, current_replicas});
  }
  return result;
}

std::string LabelValue(const v1::NodeState &node, const std::string &key) {
  for (const auto &label : node.labels()) {
    if (label.key() == key)
      return label.value();
  }
  return {};
}

bool TaintsAllowed(const v1::NodeState &node, const WorkloadSpec &workload) {
  for (const auto &taint : node.taints()) {
    if (std::find(workload.tolerated_taint_keys().begin(), workload.tolerated_taint_keys().end(),
                  taint.key()) == workload.tolerated_taint_keys().end()) {
      return false;
    }
  }
  return true;
}

std::uint64_t FragmentationScore(const ProtoResourceVector &used,
                                 const ProtoResourceVector &capacity) {
  if (!FitsProto(used, capacity))
    return std::numeric_limits<std::uint64_t>::max();
  const auto ratio = [](std::int64_t free, std::int64_t total) -> std::uint64_t {
    if (total <= 0)
      return free == 0 ? 0 : kPpm;
    return ScaledRatio(static_cast<std::uint64_t>(free), static_cast<std::uint64_t>(total));
  };
  const auto cpu =
      ratio(capacity.cpu_millicores() - used.cpu_millicores(), capacity.cpu_millicores());
  const auto memory = ratio(capacity.memory_mib() - used.memory_mib(), capacity.memory_mib());
  const auto network =
      ratio(capacity.network_mbps() - used.network_mbps(), capacity.network_mbps());
  return (cpu * cpu + memory * memory + network * network) / kPpm;
}

bool TopologyAllows(const WorkloadSpec &workload,
                    const google::protobuf::RepeatedPtrField<ReplicaPlacement> &placements,
                    const std::map<std::string, const v1::NodeState *> &nodes,
                    const std::string &candidate_node) {
  if (workload.min_topology_domains() == 0)
    return true;
  std::map<std::string, std::uint32_t> counts;
  const auto add = [&](const std::string &node_id, std::uint32_t *count) {
    const auto node = nodes.find(node_id);
    if (node == nodes.end())
      return false;
    const auto domain = LabelValue(*node->second, workload.topology_key());
    if (domain.empty())
      return false;
    *count = ++counts[domain];
    return true;
  };
  for (const auto &placement : placements) {
    if (placement.workload_id() == workload.workload_id()) {
      std::uint32_t ignored = 0;
      if (!add(placement.node_id(), &ignored))
        return false;
    }
  }
  std::uint32_t ignored = 0;
  if (!add(candidate_node, &ignored))
    return false;
  if (counts.size() < workload.min_topology_domains())
    return true;
  const auto [minimum, maximum] =
      std::minmax_element(counts.begin(), counts.end(), [](const auto &left, const auto &right) {
        return left.second < right.second;
      });
  return maximum->second - minimum->second <= workload.max_topology_skew();
}

bool BasicPlacementFeasible(const OptimizationSnapshot &snapshot,
                            const std::vector<ReplicaPlacement> &placements,
                            const std::map<std::string, ProtoResourceVector> &shapes,
                            const std::set<std::string> &deactivated) {
  std::map<std::string, const WorkloadSpec *> workloads;
  for (const auto &workload : snapshot.workloads())
    workloads[workload.workload_id()] = &workload;
  std::map<std::string, const v1::NodeState *> nodes;
  std::map<std::string, NodeUse> uses;
  for (const auto &node : snapshot.nodes()) {
    nodes[node.node_id()] = &node;
    if (!node.active() || node.health() != v1::NODE_HEALTH_HEALTHY ||
        deactivated.contains(node.node_id()))
      continue;
    const auto capacity = NodeCapacity(node, snapshot.policy().headroom_ppm());
    if (!capacity)
      return false;
    uses.emplace(node.node_id(), NodeUse{&node, *capacity, ProtoResourceVector(), {}, {}});
  }
  std::set<std::string> replica_ids;
  std::map<std::string, std::uint32_t> counts;
  std::map<std::string, std::map<std::string, std::uint32_t>> topology_counts;
  for (const auto &placement : placements) {
    const auto workload = workloads.find(placement.workload_id());
    const auto node = uses.find(placement.node_id());
    const auto shape = shapes.find(placement.workload_id());
    if (workload == workloads.end() || node == uses.end() || shape == shapes.end() ||
        placement.replica_id().empty() || !TaintsAllowed(*node->second.node, *workload->second) ||
        !replica_ids.insert(ReplicaKey(placement.workload_id(), placement.replica_id())).second ||
        node->second.workloads.contains(placement.workload_id())) {
      return false;
    }
    ProtoResourceVector sum;
    if (!AddResource(node->second.used, shape->second, &sum) ||
        !FitsProto(sum, node->second.capacity))
      return false;
    node->second.used = std::move(sum);
    node->second.workloads.insert(placement.workload_id());
    ++counts[placement.workload_id()];
    if (workload->second->min_topology_domains() > 0) {
      const auto domain = LabelValue(*node->second.node, workload->second->topology_key());
      if (domain.empty())
        return false;
      ++topology_counts[placement.workload_id()][domain];
    }
  }
  for (const auto &workload : snapshot.workloads()) {
    const auto count = counts[workload.workload_id()];
    if (count < workload.min_replicas() || count > workload.max_replicas() ||
        count < workload.availability_floor())
      return false;
    if (workload.min_topology_domains() > 0) {
      const auto &domains = topology_counts[workload.workload_id()];
      if (domains.size() < workload.min_topology_domains())
        return false;
      const auto [minimum, maximum] = std::minmax_element(
          domains.begin(), domains.end(),
          [](const auto &left, const auto &right) { return left.second < right.second; });
      if (maximum->second - minimum->second > workload.max_topology_skew())
        return false;
    }
  }
  return true;
}

bool NMinusOnePlacementFeasible(const OptimizationSnapshot &snapshot,
                                const v1::OptimizationPlan &plan,
                                const std::map<std::string, ProtoResourceVector> &shapes,
                                std::set<std::string> *deactivated, std::string *detail) {
  std::set<std::string> domain_keys;
  for (const auto &workload : snapshot.workloads()) {
    if (workload.failure_tolerance() == v1::FAILURE_TOLERANCE_N_MINUS_1) {
      domain_keys.insert(workload.topology_key().empty() ? "zone" : workload.topology_key());
    }
  }
  if (domain_keys.empty()) {
    *detail = "no workload requires N-1";
    return true;
  }

  // Keep the cheapest unused healthy nodes that make every required pool span
  // at least two failure domains before testing recovery placements.
  auto healthy_nodes = [&]() {
    std::vector<const v1::NodeState *> result;
    for (const auto &node : snapshot.nodes()) {
      if (node.active() && node.health() == v1::NODE_HEALTH_HEALTHY)
        result.push_back(&node);
    }
    std::sort(result.begin(), result.end(), [](const auto *left, const auto *right) {
      return std::tuple(left->hourly_cost_micros(), left->node_id()) <
             std::tuple(right->hourly_cost_micros(), right->node_id());
    });
    return result;
  };
  const auto nodes = healthy_nodes();
  for (const auto &key : domain_keys) {
    std::set<std::string> values;
    for (const auto *node : nodes) {
      if (deactivated->contains(node->node_id()))
        continue;
      const auto value = LabelValue(*node, key);
      if (value.empty()) {
        *detail = "N-1 workload has an unlabeled healthy failure domain";
        return false;
      }
      values.insert(value);
    }
    for (const auto *node : nodes) {
      if (values.size() >= 2)
        break;
      if (!deactivated->contains(node->node_id()))
        continue;
      const auto value = LabelValue(*node, key);
      if (value.empty())
        continue;
      values.insert(value);
      deactivated->erase(node->node_id());
    }
    if (values.size() < 2) {
      *detail = "fewer than two healthy failure domains can remain active";
      return false;
    }
  }

  std::set<std::pair<std::string, std::string>> failure_domains;
  for (const auto &key : domain_keys) {
    for (const auto *node : nodes) {
      if (deactivated->contains(node->node_id()))
        continue;
      const auto value = LabelValue(*node, key);
      if (!value.empty())
        failure_domains.emplace(key, value);
    }
  }
  std::map<std::string, const WorkloadSpec *> workloads;
  std::map<std::string, const CapacityProfile *> profiles;
  std::map<std::string, std::uint32_t> targets;
  for (const auto &workload : snapshot.workloads())
    workloads[workload.workload_id()] = &workload;
  for (const auto &profile : snapshot.capacity_profiles())
    profiles[profile.profile_id()] = &profile;
  for (const auto &decision : plan.workload_decisions()) {
    targets[decision.workload_id()] = decision.proposed_replicas();
  }

  const auto topology_fits = [&](const WorkloadSpec &workload,
                                 const std::vector<ReplicaPlacement> &placements,
                                 const v1::NodeState &candidate) {
    if (workload.min_topology_domains() == 0)
      return true;
    std::map<std::string, std::uint32_t> counts;
    for (const auto &placement : placements) {
      if (placement.workload_id() != workload.workload_id())
        continue;
      const auto node_it = std::find_if(nodes.begin(), nodes.end(), [&](const auto *node) {
        return node->node_id() == placement.node_id();
      });
      if (node_it == nodes.end())
        return false;
      const auto value = LabelValue(**node_it, workload.topology_key());
      if (value.empty())
        return false;
      ++counts[value];
    }
    const auto candidate_domain = LabelValue(candidate, workload.topology_key());
    if (candidate_domain.empty())
      return false;
    ++counts[candidate_domain];
    if (counts.size() < workload.min_topology_domains())
      return false;
    const auto [minimum, maximum] =
        std::minmax_element(counts.begin(), counts.end(), [](const auto &left, const auto &right) {
          return left.second < right.second;
        });
    return maximum->second - minimum->second <= workload.max_topology_skew();
  };

  for (const auto &[failed_key, failed_value] : failure_domains) {
    std::set<std::string> failed_nodes;
    for (const auto *node : nodes) {
      if (LabelValue(*node, failed_key) == failed_value)
        failed_nodes.insert(node->node_id());
    }
    std::vector<ReplicaPlacement> survivors;
    for (const auto &placement : plan.placements()) {
      if (!failed_nodes.contains(placement.node_id()))
        survivors.push_back(placement);
    }
    std::map<std::string, ProtoResourceVector> used;
    for (const auto &placement : survivors) {
      const auto shape = shapes.find(placement.workload_id());
      if (shape == shapes.end() ||
          !AddResource(used[placement.node_id()], shape->second, &used[placement.node_id()])) {
        *detail = "N-1 surviving placement resource arithmetic failed";
        return false;
      }
    }
    for (const auto &workload : snapshot.workloads()) {
      if (workload.failure_tolerance() != v1::FAILURE_TOLERANCE_N_MINUS_1)
        continue;
      const auto target = targets.find(workload.workload_id());
      if (target == targets.end()) {
        *detail = "N-1 workload has no plan decision";
        return false;
      }
      std::uint32_t survivor_count = static_cast<std::uint32_t>(
          std::count_if(survivors.begin(), survivors.end(), [&](const auto &placement) {
            return placement.workload_id() == workload.workload_id();
          }));
      const auto decision = std::find_if(
          plan.workload_decisions().begin(), plan.workload_decisions().end(),
          [&](const auto &value) { return value.workload_id() == workload.workload_id(); });
      if (decision == plan.workload_decisions().end()) {
        *detail = "N-1 workload has no profile-bearing decision";
        return false;
      }
      const auto profile = profiles.find(decision->capacity_profile_id());
      if (profile == profiles.end() || !ProfileCompatible(*profile->second, workload, snapshot)) {
        *detail = "N-1 workload has no compatible capacity profile";
        return false;
      }
      const auto capacity = CandidateFor(*profile->second, workload, snapshot);
      if (!capacity ||
          survivor_count < std::max(workload.min_replicas(), workload.availability_floor()) ||
          survivor_count + 1 < capacity->replicas) {
        *detail = "surviving replicas cannot preserve demand after failure of ";
        detail->append(failed_key).append("=").append(failed_value);
        return false;
      }
      const auto shape = shapes.find(workload.workload_id());
      if (shape == shapes.end()) {
        *detail = "N-1 workload has no resource shape";
        return false;
      }
      if (survivor_count > target->second) {
        *detail = "N-1 survivor count exceeds planned replicas";
        return false;
      }
      const auto recovery_count = target->second - survivor_count;
      for (std::uint32_t replica = 0; replica < recovery_count; ++replica) {
        bool placed = false;
        for (const auto *node : nodes) {
          if (failed_nodes.contains(node->node_id()) || !TaintsAllowed(*node, workload) ||
              std::any_of(survivors.begin(), survivors.end(),
                          [&](const auto &placement) {
                            return placement.workload_id() == workload.workload_id() &&
                                   placement.node_id() == node->node_id();
                          }) ||
              !topology_fits(workload, survivors, *node))
            continue;
          const auto capacity_limit = NodeCapacity(*node, snapshot.policy().headroom_ppm());
          if (!capacity_limit)
            continue;
          ProtoResourceVector next;
          if (!AddResource(used[node->node_id()], shape->second, &next) ||
              !FitsProto(next, *capacity_limit))
            continue;
          if (deactivated->erase(node->node_id()) > 0) {
            // This inactive-in-plan node is retained as recovery reserve.
          }
          used[node->node_id()] = std::move(next);
          ReplicaPlacement recovery;
          recovery.set_workload_id(workload.workload_id());
          recovery.set_replica_id(workload.workload_id() + "-n1-recovery-" +
                                  std::to_string(replica));
          recovery.set_node_id(node->node_id());
          survivors.push_back(std::move(recovery));
          ++survivor_count;
          placed = true;
          break;
        }
        if (!placed) {
          *detail = "no surviving active-node recovery placement after failure of ";
          detail->append(failed_key).append("=").append(failed_value);
          return false;
        }
      }
    }
  }
  *detail = "all required failure domains retain SLO capacity and recovery placement";
  return true;
}

bool SnapshotHashesValid(const OptimizationSnapshot &snapshot) {
  auto copy = snapshot;
  const std::string expected = copy.content_sha256();
  copy.clear_content_sha256();
  auto catalog = snapshot.machine_catalog();
  const std::string catalog_hash = catalog.catalog_sha256();
  catalog.clear_catalog_sha256();
  return !expected.empty() && MessageSha256(copy) == expected && !catalog_hash.empty() &&
         MessageSha256(catalog) == catalog_hash;
}

bool CurrentStateHardFeasible(const OptimizationSnapshot &snapshot,
                              const v1::OptimizationPlan &candidate_plan) {
  std::vector<ReplicaPlacement> current_placements(snapshot.current_placements().begin(),
                                                   snapshot.current_placements().end());
  std::map<std::string, ProtoResourceVector> current_shapes;
  std::map<std::string, std::uint32_t> current_counts;
  v1::OptimizationPlan current_plan = candidate_plan;
  current_plan.clear_placements();
  for (const auto &placement : current_placements) {
    *current_plan.add_placements() = placement;
    ++current_counts[placement.workload_id()];
  }
  for (const auto &workload : snapshot.workloads())
    current_shapes.emplace(workload.workload_id(), workload.current_resources());
  if (!BasicPlacementFeasible(snapshot, current_placements, current_shapes, {}))
    return false;

  for (auto &decision : *current_plan.mutable_workload_decisions()) {
    const auto workload = std::find_if(
        snapshot.workloads().begin(), snapshot.workloads().end(),
        [&](const auto &item) { return item.workload_id() == decision.workload_id(); });
    if (workload == snapshot.workloads().end())
      return false;
    const auto count = current_counts[decision.workload_id()];
    decision.set_current_replicas(count);
    decision.set_proposed_replicas(count);
    *decision.mutable_proposed_resources() = workload->current_resources();
    const auto risk = CurrentRiskFor(snapshot, *workload, count);
    if (!risk.evidence)
      return false;
    decision.set_capacity_profile_id(risk.evidence->profile->profile_id());
    decision.set_capacity_profile_sha256(risk.evidence->profile->profile_sha256());
  }

  std::set<std::string> deactivated;
  std::string detail;
  return NMinusOnePlacementFeasible(snapshot, current_plan, current_shapes, &deactivated, &detail);
}

std::uint64_t CurrentCount(const OptimizationSnapshot &snapshot, const std::string &workload_id) {
  return static_cast<std::uint64_t>(
      std::count_if(snapshot.current_placements().begin(), snapshot.current_placements().end(),
                    [&](const auto &placement) { return placement.workload_id() == workload_id; }));
}

std::optional<std::uint64_t> CurrentCost(const OptimizationSnapshot &snapshot) {
  std::uint64_t total = 0;
  for (const auto &node : snapshot.nodes()) {
    if (!node.active())
      continue;
    if (total > std::numeric_limits<std::uint64_t>::max() - node.hourly_cost_micros())
      return std::nullopt;
    total += node.hourly_cost_micros();
  }
  return total;
}

struct ObjectiveBreakdown {
  std::uint64_t infrastructure = 0;
  std::uint64_t movement = 0;
  std::uint64_t churn = 0;
  std::uint64_t uncertainty = 0;
  std::uint64_t boundary = 0;
  std::uint64_t current_uncertainty = 0;
  std::uint64_t current_boundary = 0;
  std::uint64_t total = 0;
  std::uint64_t current_total = 0;
  std::uint64_t hourly_infrastructure = 0;
  std::uint64_t actions = 0;
  std::uint64_t moves = 0;
};

std::optional<std::uint64_t> CostOverHorizon(std::uint64_t hourly_cost,
                                             std::uint64_t horizon_seconds) {
  const UInt128 total = static_cast<UInt128>(hourly_cost) * horizon_seconds;
  const UInt128 cost = (total + 3'599) / 3'600;
  if (cost > std::numeric_limits<std::uint64_t>::max())
    return std::nullopt;
  return static_cast<std::uint64_t>(cost);
}

struct CostMultiplier {
  std::uint64_t coefficient_micros;
  UInt128 exposure;
  std::uint64_t divisor = kPpm;
};

std::optional<std::uint64_t> CostScaled(const CostMultiplier &input) {
  const UInt128 scaled = static_cast<UInt128>(input.coefficient_micros) * input.exposure;
  const UInt128 cost = (scaled + input.divisor - 1) / input.divisor;
  if (cost > std::numeric_limits<std::uint64_t>::max())
    return std::nullopt;
  return static_cast<std::uint64_t>(cost);
}

std::optional<std::uint64_t> AddCost(std::uint64_t left, std::uint64_t right) {
  if (left > std::numeric_limits<std::uint64_t>::max() - right)
    return std::nullopt;
  return left + right;
}

std::optional<ObjectiveBreakdown> CurrentObjective(const OptimizationSnapshot &snapshot) {
  ObjectiveBreakdown result;
  const auto current_cost = CurrentCost(snapshot);
  if (!current_cost)
    return std::nullopt;
  result.hourly_infrastructure = *current_cost;
  const auto infrastructure =
      CostOverHorizon(result.hourly_infrastructure, snapshot.policy().planning_horizon_seconds());
  if (!infrastructure)
    return std::nullopt;
  result.infrastructure = *infrastructure;
  for (const auto &workload : snapshot.workloads()) {
    const auto count = CurrentCount(snapshot, workload.workload_id());
    if (count > std::numeric_limits<std::uint32_t>::max())
      return std::nullopt;
    const auto risk = CurrentRiskFor(snapshot, workload, static_cast<std::uint32_t>(count));
    const auto uncertainty = CostScaled({snapshot.policy().uncertainty_cost_micros_per_replica(),
                                         static_cast<UInt128>(risk.uncertainty_ppm) * count});
    const auto boundary =
        CostScaled({snapshot.policy().boundary_cost_micros_per_workload(), risk.boundary_ppm});
    if (!uncertainty || !boundary)
      return std::nullopt;
    const auto next_uncertainty = AddCost(result.current_uncertainty, *uncertainty);
    const auto next_boundary = AddCost(result.current_boundary, *boundary);
    if (!next_uncertainty || !next_boundary)
      return std::nullopt;
    result.current_uncertainty = *next_uncertainty;
    result.current_boundary = *next_boundary;
  }
  auto total = AddCost(result.infrastructure, result.current_uncertainty);
  if (!total)
    return std::nullopt;
  total = AddCost(*total, result.current_boundary);
  if (!total)
    return std::nullopt;
  result.current_total = *total;
  result.total = *total;
  return result;
}

std::optional<std::uint64_t>
CountMoved(const OptimizationSnapshot &snapshot,
           const google::protobuf::RepeatedPtrField<ReplicaPlacement> &placements) {
  std::map<std::string, std::string> targets;
  for (const auto &placement : placements)
    targets[ReplicaKey(placement.workload_id(), placement.replica_id())] = placement.node_id();
  std::uint64_t moves = 0;
  for (const auto &placement : snapshot.current_placements()) {
    const auto found = targets.find(ReplicaKey(placement.workload_id(), placement.replica_id()));
    if (found != targets.end() && found->second != placement.node_id()) {
      if (moves == std::numeric_limits<std::uint64_t>::max())
        return std::nullopt;
      ++moves;
    }
  }
  return moves;
}

std::optional<ObjectiveBreakdown>
EvaluateObjective(const OptimizationSnapshot &snapshot, const v1::OptimizationPlan &plan,
                  const google::protobuf::RepeatedPtrField<ReplicaPlacement> &placements,
                  const std::set<std::string> &deactivated) {
  ObjectiveBreakdown result;
  for (const auto &node : snapshot.nodes()) {
    if (!node.active() || deactivated.contains(node.node_id()))
      continue;
    const auto sum = AddCost(result.hourly_infrastructure, node.hourly_cost_micros());
    if (!sum)
      return std::nullopt;
    result.hourly_infrastructure = *sum;
  }
  const auto infrastructure =
      CostOverHorizon(result.hourly_infrastructure, snapshot.policy().planning_horizon_seconds());
  const auto moves = CountMoved(snapshot, placements);
  if (!infrastructure || !moves)
    return std::nullopt;
  result.infrastructure = *infrastructure;
  result.moves = *moves;
  result.actions = *moves;
  const auto add_action = [&](std::uint64_t amount) {
    if (result.actions > std::numeric_limits<std::uint64_t>::max() - amount)
      return false;
    result.actions += amount;
    return true;
  };
  if (!add_action(static_cast<std::uint64_t>(deactivated.size())))
    return std::nullopt;
  for (const auto &decision : plan.workload_decisions()) {
    const auto *workload = [&]() -> const WorkloadSpec * {
      const auto found = std::find_if(
          snapshot.workloads().begin(), snapshot.workloads().end(),
          [&](const auto &item) { return item.workload_id() == decision.workload_id(); });
      return found == snapshot.workloads().end() ? nullptr : &*found;
    }();
    if (workload == nullptr)
      return std::nullopt;
    const std::uint64_t replica_delta =
        decision.current_replicas() > decision.proposed_replicas()
            ? decision.current_replicas() - decision.proposed_replicas()
            : decision.proposed_replicas() - decision.current_replicas();
    if (!add_action(replica_delta) || (decision.proposed_resources().SerializeAsString() !=
                                           workload->current_resources().SerializeAsString() &&
                                       !add_action(1))) {
      return std::nullopt;
    }
  }
  const auto movement = CostScaled({snapshot.policy().movement_cost_micros(), *moves, 1});
  const auto churn = CostScaled(
      {snapshot.policy().action_churn_cost_micros(), static_cast<UInt128>(result.actions), 1});
  if (!movement || !churn)
    return std::nullopt;
  result.movement = *movement;
  result.churn = *churn;
  for (const auto &decision : plan.workload_decisions()) {
    if (decision.uncertainty_exposure_ppm() > kPpm || decision.boundary_risk_ppm() > kPpm)
      return std::nullopt;
    const auto uncertainty = CostScaled(
        {snapshot.policy().uncertainty_cost_micros_per_replica(),
         static_cast<UInt128>(decision.uncertainty_exposure_ppm()) * decision.proposed_replicas()});
    const auto boundary = CostScaled(
        {snapshot.policy().boundary_cost_micros_per_workload(), decision.boundary_risk_ppm()});
    if (!uncertainty || !boundary)
      return std::nullopt;
    const auto next_uncertainty = AddCost(result.uncertainty, *uncertainty);
    const auto next_boundary = AddCost(result.boundary, *boundary);
    if (!next_uncertainty || !next_boundary)
      return std::nullopt;
    result.uncertainty = *next_uncertainty;
    result.boundary = *next_boundary;
  }
  auto total = AddCost(result.infrastructure, result.movement);
  if (!total)
    return std::nullopt;
  total = AddCost(*total, result.churn);
  if (!total)
    return std::nullopt;
  total = AddCost(*total, result.uncertainty);
  if (!total)
    return std::nullopt;
  total = AddCost(*total, result.boundary);
  if (!total)
    return std::nullopt;
  result.total = *total;
  return result;
}

struct ObjectivePair {
  const ObjectiveBreakdown &current;
  const ObjectiveBreakdown &proposed;
};

void StoreObjective(v1::OptimizationPlan *plan, const ObjectivePair &objectives) {
  plan->set_current_objective_cost_micros(objectives.current.current_total);
  plan->set_proposed_objective_cost_micros(objectives.proposed.total);
  plan->set_proposed_movement_cost_micros(objectives.proposed.movement);
  plan->set_proposed_churn_cost_micros(objectives.proposed.churn);
  plan->set_proposed_uncertainty_cost_micros(objectives.proposed.uncertainty);
  plan->set_proposed_boundary_cost_micros(objectives.proposed.boundary);
  plan->set_current_uncertainty_cost_micros(objectives.current.current_uncertainty);
  plan->set_current_boundary_cost_micros(objectives.current.current_boundary);
}

struct ImprovementInput {
  std::uint64_t before_micros;
  std::uint64_t after_micros;
  std::uint32_t minimum_improvement_ppm;
};

bool ImprovementMeetsPolicy(const ImprovementInput &input) {
  if (input.after_micros >= input.before_micros)
    return false;
  const UInt128 saving = static_cast<UInt128>(input.before_micros - input.after_micros) * kPpm;
  const UInt128 required =
      static_cast<UInt128>(input.before_micros) * input.minimum_improvement_ppm;
  return saving >= required;
}

void HoldCurrentState(const OptimizationSnapshot &snapshot, v1::OptimizationPlan *plan,
                      const std::string &reason, bool insufficient_improvement) {
  bool n1_profiles_available = true;
  plan->clear_placements();
  for (const auto &placement : snapshot.current_placements())
    *plan->add_placements() = placement;
  plan->clear_evacuation_steps();
  plan->clear_deactivated_node_ids();
  for (auto &decision : *plan->mutable_workload_decisions()) {
    decision.set_proposed_replicas(decision.current_replicas());
    const auto workload = std::find_if(
        snapshot.workloads().begin(), snapshot.workloads().end(),
        [&](const auto &item) { return item.workload_id() == decision.workload_id(); });
    if (workload == snapshot.workloads().end()) {
      plan->set_feasible(false);
      plan->set_infeasible_reason("no-op decision references an unknown workload");
      continue;
    }
    *decision.mutable_proposed_resources() = workload->current_resources();
    const auto risk = CurrentRiskFor(snapshot, *workload, decision.current_replicas());
    decision.set_uncertainty_exposure_ppm(risk.uncertainty_ppm);
    decision.set_boundary_risk_ppm(risk.boundary_ppm);
    const bool evidence_safe = risk.evidence.has_value();
    if (evidence_safe) {
      const auto &candidate = *risk.evidence;
      decision.set_capacity_profile_id(candidate.profile->profile_id());
      decision.set_capacity_profile_sha256(candidate.profile->profile_sha256());
      decision.set_profile_point_index(candidate.point_index);
      decision.set_effective_qps_per_replica_milli(candidate.effective_qps);
      decision.set_effective_concurrency_per_replica(candidate.effective_concurrency);
      decision.set_profile_confidence_ppm(candidate.profile->confidence_ppm());
    } else {
      decision.clear_capacity_profile_id();
      decision.clear_capacity_profile_sha256();
      decision.set_profile_point_index(0);
      decision.set_effective_qps_per_replica_milli(0);
      decision.set_effective_concurrency_per_replica(0);
      decision.set_profile_confidence_ppm(0);
    }
    if (workload != snapshot.workloads().end() &&
        workload->failure_tolerance() == v1::FAILURE_TOLERANCE_N_MINUS_1 && !evidence_safe)
      n1_profiles_available = false;
    decision.set_destructive(false);
    decision.clear_binding_constraints();
    decision.add_rejected_alternatives(reason);
  }
  plan->set_proposed_cost_micros_per_hour(plan->current_cost_micros_per_hour());
  plan->set_action_count(0);
  const auto objective = EvaluateObjective(snapshot, *plan, plan->placements(), {});
  const auto current_objective = CurrentObjective(snapshot);
  if (objective && current_objective && objective->total == current_objective->current_total) {
    StoreObjective(plan, {*current_objective, *objective});
  } else {
    plan->set_feasible(false);
    plan->set_infeasible_reason("no-op objective does not match the current state");
  }
  if (insufficient_improvement)
    plan->add_rejection_codes(v1::REJECTION_CODE_INSUFFICIENT_IMPROVEMENT);
  if (n1_profiles_available && plan->infeasible_reason().empty()) {
    plan->set_feasible(true);
  } else if (!n1_profiles_available) {
    plan->set_feasible(false);
    plan->set_infeasible_reason(
        "current N-1 state lacks a fresh capacity profile for its resource shape");
    plan->add_rejection_codes(v1::REJECTION_CODE_NO_SAFE_ENVELOPE);
  }
}

} // namespace

std::string OptimizerBuildId() {
#ifdef FLEETRIGHT_BUILD_ID
  return FLEETRIGHT_BUILD_ID;
#else
  return "fleetright-planner-unknown-build";
#endif
}

static v1::OptimizationPlan PlanWithProfileOverride(const OptimizationSnapshot &snapshot,
                                                    const std::string &override_workload_id,
                                                    const std::string &override_profile_id,
                                                    std::uint32_t profile_trial_budget) {
  v1::OptimizationPlan plan;
  plan.set_schema_version(1);
  plan.set_snapshot_id(snapshot.snapshot_id());
  plan.set_base_generation(snapshot.generation());
  plan.set_created_at_ms(snapshot.evaluation_time_ms());
  plan.set_expires_at_ms(snapshot.expires_at_ms());
  plan.set_optimizer_build_id(OptimizerBuildId());
  const auto current_cost = CurrentCost(snapshot);
  if (!current_cost) {
    plan.set_feasible(false);
    plan.set_infeasible_reason("node cost accumulation overflow");
    plan.set_plan_id(MessageSha256(plan));
    return plan;
  }
  plan.set_current_cost_micros_per_hour(*current_cost);

  if (snapshot.schema_version() != 1 || snapshot.snapshot_id().empty() ||
      snapshot.workloads_size() > static_cast<int>(kMaxWorkloads) ||
      snapshot.nodes_size() > static_cast<int>(kMaxNodes) ||
      snapshot.capacity_profiles_size() > static_cast<int>(kMaxProfiles) ||
      snapshot.current_placements_size() > static_cast<int>(kMaxPlacements) ||
      snapshot.policy().schema_version() != 1 || snapshot.machine_catalog().schema_version() != 1 ||
      snapshot.policy().headroom_ppm() > kPpm ||
      snapshot.policy().minimum_profile_confidence_ppm() > kPpm ||
      snapshot.policy().minimum_improvement_ppm() > kPpm ||
      snapshot.policy().planning_horizon_seconds() == 0 ||
      snapshot.policy().planning_horizon_seconds() > 31'536'000 ||
      snapshot.policy().maximum_profile_age_ms() == 0 ||
      snapshot.policy().maximum_snapshot_age_ms() == 0 ||
      snapshot.evaluation_time_ms() < snapshot.observed_at_ms() ||
      snapshot.expires_at_ms() <= snapshot.evaluation_time_ms() ||
      snapshot.evaluation_time_ms() - snapshot.observed_at_ms() >
          snapshot.policy().maximum_snapshot_age_ms() ||
      !SnapshotHashesValid(snapshot) || !CatalogMatchesNodes(snapshot)) {
    plan.set_feasible(false);
    plan.set_infeasible_reason("invalid, stale, oversized, or unhashed immutable snapshot");
    plan.add_rejection_codes(v1::REJECTION_CODE_NO_SAFE_ENVELOPE);
    plan.set_plan_id(MessageSha256(plan));
    return plan;
  }

  std::map<std::string, const WorkloadSpec *> workload_by_id;
  std::map<std::string, std::string> current_node_by_key;
  std::map<std::string, ProtoResourceVector> shape_by_workload;
  std::vector<const WorkloadSpec *> ordered_workloads;
  std::set<std::string> workload_ids;
  for (const auto &workload : snapshot.workloads()) {
    if (workload.workload_id().empty() || !workload_ids.insert(workload.workload_id()).second ||
        workload.min_replicas() > workload.max_replicas() || workload.max_replicas() > 100'000U ||
        workload.availability_floor() > workload.max_replicas() ||
        workload.error_slo_ppm() > kPpm || workload.latency_slo_ms() == 0 ||
        workload.failure_tolerance() == v1::FAILURE_TOLERANCE_UNSPECIFIED ||
        !v1::FailureTolerance_IsValid(static_cast<int>(workload.failure_tolerance())) ||
        workload.min_topology_domains() > workload.max_replicas() ||
        (workload.min_topology_domains() > 0 && workload.topology_key().empty()) ||
        !ValidResource(workload.current_resources()) ||
        workload.profile_confidence_factor_ppm() == 0 ||
        workload.model_accuracy_factor_ppm() == 0 || workload.degraded_cluster_factor_ppm() == 0 ||
        workload.profile_confidence_factor_ppm() > kPpm ||
        workload.model_accuracy_factor_ppm() > kPpm ||
        workload.degraded_cluster_factor_ppm() > kPpm || workload.max_throttle_ppm() > kPpm ||
        workload.max_memory_pressure_ppm() > kPpm ||
        workload.max_scale_down_fraction_ppm() > kPpm) {
      plan.set_feasible(false);
      plan.set_infeasible_reason("invalid workload units, bounds, or safety factors");
      plan.set_plan_id(MessageSha256(plan));
      return plan;
    }
    workload_by_id[workload.workload_id()] = &workload;
    ordered_workloads.push_back(&workload);
  }
  std::set<std::string> profile_ids;
  for (const auto &profile : snapshot.capacity_profiles()) {
    if (profile.profile_id().empty() || !profile_ids.insert(profile.profile_id()).second) {
      plan.set_feasible(false);
      plan.set_infeasible_reason("capacity profile IDs must be nonempty and unique");
      plan.set_plan_id(MessageSha256(plan));
      return plan;
    }
  }
  std::set<std::string> current_keys;
  for (const auto &placement : snapshot.current_placements()) {
    const std::string key = ReplicaKey(placement.workload_id(), placement.replica_id());
    if (placement.workload_id().empty() || placement.replica_id().empty() ||
        !workload_ids.contains(placement.workload_id()) || !current_keys.insert(key).second) {
      plan.set_feasible(false);
      plan.set_infeasible_reason(
          "current placements contain unknown or duplicate workload/replica IDs");
      plan.set_plan_id(MessageSha256(plan));
      return plan;
    }
    current_node_by_key[key] = placement.node_id();
  }

  std::sort(ordered_workloads.begin(), ordered_workloads.end(),
            [](const auto *left, const auto *right) {
              if (left->priority() != right->priority())
                return left->priority() > right->priority();
              return left->workload_id() < right->workload_id();
            });
  std::vector<Replica> replicas;
  for (const auto *workload : ordered_workloads) {
    std::optional<Candidate> candidate;
    for (const auto &profile : snapshot.capacity_profiles()) {
      if (profile.workload_id() != workload->workload_id())
        continue;
      if (workload->workload_id() == override_workload_id &&
          profile.profile_id() != override_profile_id)
        continue;
      const auto next = CandidateFor(profile, *workload, snapshot);
      if (next &&
          (!candidate || std::tie(next->shape_score, next->replicas, next->profile->profile_id()) <
                             std::tie(candidate->shape_score, candidate->replicas,
                                      candidate->profile->profile_id()))) {
        candidate = next;
      }
    }

    auto *decision = plan.add_workload_decisions();
    decision->set_workload_id(workload->workload_id());
    const auto current = CurrentCount(snapshot, workload->workload_id());
    if (current > std::numeric_limits<std::uint32_t>::max()) {
      plan.set_feasible(false);
      plan.set_infeasible_reason("current replica count exceeds supported bound");
      plan.set_plan_id(MessageSha256(plan));
      return plan;
    }
    decision->set_current_replicas(static_cast<std::uint32_t>(current));
    decision->set_demand_qps_milli(workload->demand_qps_milli());
    decision->set_demand_concurrency(workload->concurrency_bound());
    decision->set_destructive(false);
    if (!candidate) {
      decision->set_proposed_replicas(static_cast<std::uint32_t>(current));
      *decision->mutable_proposed_resources() = workload->current_resources();
      decision->add_rejected_alternatives(
          "rightsizing blocked: no fresh, compatible, high-confidence joint capacity profile");
      plan.add_rejection_codes(v1::REJECTION_CODE_NO_SAFE_ENVELOPE);
      shape_by_workload[workload->workload_id()] = workload->current_resources();
      for (const auto &placement : snapshot.current_placements()) {
        if (placement.workload_id() == workload->workload_id())
          *plan.add_placements() = placement;
      }
      continue;
    }

    std::uint32_t target = candidate->replicas;
    if (current > target) {
      const auto max_remove = CeilDivide(
          current * static_cast<std::uint64_t>(workload->max_scale_down_fraction_ppm()), kPpm);
      const auto safe_step = current > max_remove ? current - max_remove : 0;
      target = std::max(target, static_cast<std::uint32_t>(safe_step));
    }
    if (target < workload->min_replicas() || target > workload->max_replicas()) {
      plan.set_feasible(false);
      plan.set_infeasible_reason("required replica count violates configured range");
      plan.set_plan_id(MessageSha256(plan));
      return plan;
    }
    decision->set_proposed_replicas(target);
    *decision->mutable_proposed_resources() = candidate->profile->allocation();
    decision->set_capacity_profile_id(candidate->profile->profile_id());
    decision->set_capacity_profile_sha256(candidate->profile->profile_sha256());
    decision->set_profile_point_index(candidate->point_index);
    decision->set_effective_qps_per_replica_milli(candidate->effective_qps);
    decision->set_effective_concurrency_per_replica(candidate->effective_concurrency);
    decision->set_profile_confidence_ppm(candidate->profile->confidence_ppm());
    const auto uncertainty_floor =
        std::min({candidate->profile->confidence_ppm(), workload->profile_confidence_factor_ppm(),
                  workload->model_accuracy_factor_ppm(), workload->degraded_cluster_factor_ppm()});
    decision->set_uncertainty_exposure_ppm(static_cast<std::uint32_t>(kPpm - uncertainty_floor));
    decision->set_boundary_risk_ppm(
        BoundaryRiskPpm({workload->demand_qps_milli(), workload->concurrency_bound(),
                         candidate->effective_qps, candidate->effective_concurrency, target}));
    decision->add_binding_constraints("empirical joint QPS/concurrency envelope");
    decision->add_binding_constraints("Wilson latency-success lower bound and error upper bound");
    decision->set_destructive(target < current ||
                              candidate->profile->allocation().cpu_millicores() <
                                  workload->current_resources().cpu_millicores() ||
                              candidate->profile->allocation().memory_mib() <
                                  workload->current_resources().memory_mib());
    shape_by_workload[workload->workload_id()] = candidate->profile->allocation();
    for (std::uint32_t i = 0; i < target; ++i) {
      const std::string replica_id = workload->workload_id() + "-" + std::to_string(i);
      const auto source = current_node_by_key.find(ReplicaKey(workload->workload_id(), replica_id));
      replicas.push_back(
          Replica{workload->workload_id(), replica_id, workload->priority(),
                  candidate->profile->allocation(),
                  source == current_node_by_key.end() ? std::string() : source->second});
    }
  }

  const bool every_workload_profiled =
      std::all_of(plan.workload_decisions().begin(), plan.workload_decisions().end(),
                  [](const auto &decision) { return !decision.capacity_profile_id().empty(); });
  if (!every_workload_profiled) {
    plan.clear_placements();
    for (const auto &placement : snapshot.current_placements())
      *plan.add_placements() = placement;
    plan.clear_evacuation_steps();
    plan.clear_deactivated_node_ids();
    for (auto &decision : *plan.mutable_workload_decisions()) {
      decision.set_proposed_replicas(decision.current_replicas());
      *decision.mutable_proposed_resources() =
          workload_by_id.at(decision.workload_id())->current_resources();
      decision.clear_capacity_profile_id();
      decision.clear_capacity_profile_sha256();
      decision.set_effective_qps_per_replica_milli(0);
      decision.set_effective_concurrency_per_replica(0);
      decision.set_destructive(false);
      decision.add_rejected_alternatives(
          "cluster optimization held: a workload lacks profile evidence");
    }
    HoldCurrentState(snapshot, &plan,
                     "cluster optimization held: a workload lacks profile evidence", false);
    plan.set_plan_id(MessageSha256(plan));
    return plan;
  }

  std::map<std::string, const v1::NodeState *> node_by_id;
  std::map<std::string, NodeUse> uses;
  for (const auto &node : snapshot.nodes()) {
    if (!node.node_id().empty())
      node_by_id[node.node_id()] = &node;
    if (!node.active() || node.health() != v1::NODE_HEALTH_HEALTHY)
      continue;
    const auto capacity = NodeCapacity(node, snapshot.policy().headroom_ppm());
    if (!capacity) {
      plan.set_feasible(false);
      plan.set_infeasible_reason("node reserve/headroom arithmetic is invalid");
      plan.set_plan_id(MessageSha256(plan));
      return plan;
    }
    uses.emplace(node.node_id(), NodeUse{&node, *capacity, ProtoResourceVector(), {}, {}});
  }
  std::uint64_t maximum_cpu = 1;
  std::uint64_t maximum_memory = 1;
  std::uint64_t maximum_network = 1;
  for (const auto &node : snapshot.nodes()) {
    maximum_cpu =
        std::max(maximum_cpu, static_cast<std::uint64_t>(node.allocatable().cpu_millicores()));
    maximum_memory =
        std::max(maximum_memory, static_cast<std::uint64_t>(node.allocatable().memory_mib()));
    maximum_network =
        std::max(maximum_network, static_cast<std::uint64_t>(node.allocatable().network_mbps()));
  }
  for (auto &replica : replicas) {
    const auto &resource = replica.resources;
    replica.dominant_score = std::max(
        {ScaledRatio(static_cast<std::uint64_t>(resource.cpu_millicores()), maximum_cpu),
         ScaledRatio(static_cast<std::uint64_t>(resource.memory_mib()), maximum_memory),
         ScaledRatio(static_cast<std::uint64_t>(resource.network_mbps()), maximum_network)});
  }
  std::sort(replicas.begin(), replicas.end(), [&](const Replica &left, const Replica &right) {
    if (left.dominant_score != right.dominant_score)
      return left.dominant_score > right.dominant_score;
    if (left.priority != right.priority)
      return left.priority > right.priority;
    return std::tie(left.workload_id, left.replica_id) <
           std::tie(right.workload_id, right.replica_id);
  });

  for (const auto &replica : replicas) {
    const auto workload = workload_by_id.find(replica.workload_id);
    if (workload == workload_by_id.end())
      continue;
    // Only the minimum is used; pointers avoid copying node ids and a full sort per replica.
    std::vector<std::tuple<bool, std::uint64_t, const std::string *>> choices;
    for (auto &[node_id, use] : uses) {
      if (!TaintsAllowed(*use.node, *workload->second) ||
          use.workloads.contains(replica.workload_id))
        continue;
      ProtoResourceVector sum;
      if (!AddResource(use.used, replica.resources, &sum) || !FitsProto(sum, use.capacity))
        continue;
      bool topology_ok = true;
      if (!TopologyAllows(*workload->second, *plan.mutable_placements(), node_by_id, node_id)) {
        topology_ok = false;
      }
      if (!topology_ok)
        continue;
      const bool movement = !replica.current_node.empty() && replica.current_node != node_id;
      choices.emplace_back(movement, FragmentationScore(sum, use.capacity), &node_id);
    }
    if (choices.empty()) {
      plan.set_feasible(false);
      plan.set_infeasible_reason("no deterministic feasible node for replica " +
                                 replica.replica_id);
      plan.add_rejection_codes(v1::REJECTION_CODE_NO_PLACEMENT);
      plan.set_plan_id(MessageSha256(plan));
      return plan;
    }
    // Same (movement, score, node id) order the previous full sort used; node ids are unique.
    const auto &destination = *std::get<2>(*std::min_element(
        choices.begin(), choices.end(), [](const auto &left, const auto &right) {
          return std::tie(std::get<0>(left), std::get<1>(left), *std::get<2>(left)) <
                 std::tie(std::get<0>(right), std::get<1>(right), *std::get<2>(right));
        }));
    auto &use = uses.at(destination);
    ProtoResourceVector sum;
    if (!AddResource(use.used, replica.resources, &sum)) {
      plan.set_feasible(false);
      plan.set_infeasible_reason("resource addition overflow");
      plan.set_plan_id(MessageSha256(plan));
      return plan;
    }
    use.used = std::move(sum);
    use.workloads.insert(replica.workload_id);
    auto *placement = plan.add_placements();
    placement->set_workload_id(replica.workload_id);
    placement->set_replica_id(replica.replica_id);
    placement->set_node_id(destination);
  }

  std::set<std::string> initially_used_nodes;
  for (const auto &placement : plan.placements())
    initially_used_nodes.insert(placement.node_id());
  std::set<std::string> incumbent_deactivated;
  for (const auto &node : snapshot.nodes()) {
    if (node.active() && !initially_used_nodes.contains(node.node_id()))
      incumbent_deactivated.insert(node.node_id());
  }
  std::string initial_n1_detail;
  if (!NMinusOnePlacementFeasible(snapshot, plan, shape_by_workload, &incumbent_deactivated,
                                  &initial_n1_detail)) {
    plan.set_feasible(false);
    plan.set_infeasible_reason(initial_n1_detail);
    plan.add_rejection_codes(v1::REJECTION_CODE_N_MINUS_1);
    plan.set_plan_id(MessageSha256(plan));
    return plan;
  }
  std::uint64_t initial_cost = 0;
  for (const auto &node : snapshot.nodes()) {
    if (!node.active() || incumbent_deactivated.contains(node.node_id()))
      continue;
    if (initial_cost > std::numeric_limits<std::uint64_t>::max() - node.hourly_cost_micros()) {
      plan.set_feasible(false);
      plan.set_infeasible_reason("node cost accumulation overflow");
      plan.set_plan_id(MessageSha256(plan));
      return plan;
    }
    initial_cost += node.hourly_cost_micros();
  }
  plan.set_proposed_cost_micros_per_hour(initial_cost);

  // ponytail: cap full feasibility rescans near 64k scenario units; use incremental move state
  // if larger clusters need more local-search depth without increasing planner latency.
  const std::uint64_t scenario_scale =
      std::max<std::uint64_t>({1, static_cast<std::uint64_t>(snapshot.workloads_size()),
                               static_cast<std::uint64_t>(plan.placements_size()) / 2,
                               static_cast<std::uint64_t>(snapshot.nodes_size()) * 2});
  const std::uint32_t scenario_limit =
      static_cast<std::uint32_t>(std::max<std::uint64_t>(16, 64'000 / scenario_scale));
  const std::uint32_t limit = std::min({snapshot.policy().maximum_local_search_iterations(),
                                        profile_trial_budget, 256U, scenario_limit});
  const bool has_n1_workload = std::any_of(
      snapshot.workloads().begin(), snapshot.workloads().end(), [](const auto &workload) {
        return workload.failure_tolerance() == v1::FAILURE_TOLERANCE_N_MINUS_1;
      });
  std::map<std::string, std::uint32_t> placements_per_node;
  std::map<std::string, const v1::NodeState *> active_node_by_id;
  for (const auto &node : snapshot.nodes()) {
    if (node.active() && node.health() == v1::NODE_HEALTH_HEALTHY) {
      active_node_by_id[node.node_id()] = &node;
    }
  }
  for (const auto &placement : plan.placements())
    ++placements_per_node[placement.node_id()];
  std::uint32_t iterations = 0;
  bool improved = true;
  while (improved && iterations < limit) {
    improved = false;
    for (int index = 0; index < plan.placements_size() && iterations < limit; ++index) {
      const auto original = plan.placements(index);
      for (const auto &node : snapshot.nodes()) {
        if (iterations++ >= limit)
          break;
        if (!node.active() || node.health() != v1::NODE_HEALTH_HEALTHY ||
            node.node_id() == original.node_id())
          continue;
        if (!has_n1_workload && placements_per_node[original.node_id()] != 1)
          continue;
        if (!has_n1_workload && placements_per_node[node.node_id()] == 0) {
          const auto source = active_node_by_id.find(original.node_id());
          if (source == active_node_by_id.end() ||
              node.hourly_cost_micros() >= source->second->hourly_cost_micros())
            continue;
        }
        auto candidate_plan = plan;
        candidate_plan.mutable_placements(index)->set_node_id(node.node_id());
        std::set<std::string> candidate_used_nodes;
        for (const auto &item : candidate_plan.placements()) {
          candidate_used_nodes.insert(item.node_id());
        }
        std::set<std::string> deactivated;
        for (const auto &candidate_node : snapshot.nodes()) {
          if (candidate_node.active() && !candidate_used_nodes.contains(candidate_node.node_id())) {
            deactivated.insert(candidate_node.node_id());
          }
        }
        const std::vector<ReplicaPlacement> candidate_placements(
            candidate_plan.placements().begin(), candidate_plan.placements().end());
        if (!BasicPlacementFeasible(snapshot, candidate_placements, shape_by_workload, deactivated))
          continue;
        std::string n1_detail;
        if (!NMinusOnePlacementFeasible(snapshot, candidate_plan, shape_by_workload, &deactivated,
                                        &n1_detail))
          continue;
        const auto current_objective =
            EvaluateObjective(snapshot, plan, plan.placements(), incumbent_deactivated);
        const auto candidate_objective =
            EvaluateObjective(snapshot, candidate_plan, candidate_plan.placements(), deactivated);
        if (current_objective && candidate_objective &&
            ImprovementMeetsPolicy({current_objective->total, candidate_objective->total,
                                    snapshot.policy().minimum_improvement_ppm()})) {
          plan = std::move(candidate_plan);
          incumbent_deactivated = std::move(deactivated);
          improved = true;
          break;
        }
      }
      if (improved)
        break;
    }
  }

  std::set<std::string> used_nodes;
  std::map<std::string, std::string> target_node_by_key;
  for (const auto &placement : plan.placements()) {
    used_nodes.insert(placement.node_id());
    target_node_by_key[ReplicaKey(placement.workload_id(), placement.replica_id())] =
        placement.node_id();
  }
  plan.clear_evacuation_steps();
  for (const auto &current : snapshot.current_placements()) {
    const auto target =
        target_node_by_key.find(ReplicaKey(current.workload_id(), current.replica_id()));
    if (target != target_node_by_key.end() && target->second != current.node_id()) {
      auto *step = plan.add_evacuation_steps();
      step->set_sequence(static_cast<std::uint32_t>(plan.evacuation_steps_size() - 1));
      step->set_workload_id(current.workload_id());
      step->set_replica_id(current.replica_id());
      step->set_source_node_id(current.node_id());
      step->set_destination_node_id(target->second);
    }
  }
  std::set<std::string> deactivated;
  for (const auto &node : snapshot.nodes()) {
    if (node.active() && !used_nodes.contains(node.node_id())) {
      deactivated.insert(node.node_id());
    }
  }
  std::string n1_detail;
  if (!NMinusOnePlacementFeasible(snapshot, plan, shape_by_workload, &deactivated, &n1_detail)) {
    plan.set_feasible(false);
    plan.set_infeasible_reason(n1_detail);
    plan.add_rejection_codes(v1::REJECTION_CODE_N_MINUS_1);
    plan.set_plan_id(MessageSha256(plan));
    return plan;
  }
  plan.clear_deactivated_node_ids();
  for (const auto &node_id : deactivated)
    plan.add_deactivated_node_ids(node_id);
  std::uint64_t proposed_cost = 0;
  for (const auto &node : snapshot.nodes()) {
    if (!node.active() || deactivated.contains(node.node_id()))
      continue;
    if (proposed_cost > std::numeric_limits<std::uint64_t>::max() - node.hourly_cost_micros()) {
      plan.set_feasible(false);
      plan.set_infeasible_reason("node cost accumulation overflow");
      plan.set_plan_id(MessageSha256(plan));
      return plan;
    }
    proposed_cost += node.hourly_cost_micros();
  }
  plan.set_proposed_cost_micros_per_hour(proposed_cost);
  const std::vector<ReplicaPlacement> final_target(plan.placements().begin(),
                                                   plan.placements().end());
  if (!BasicPlacementFeasible(snapshot, final_target, shape_by_workload, deactivated)) {
    plan.set_feasible(false);
    plan.set_infeasible_reason("final placement fails planner-side resource or topology checks");
    plan.add_rejection_codes(v1::REJECTION_CODE_NO_PLACEMENT);
    plan.set_plan_id(MessageSha256(plan));
    return plan;
  }
  std::uint64_t actions = static_cast<std::uint64_t>(plan.evacuation_steps_size());
  for (const auto &decision : plan.workload_decisions()) {
    actions += decision.current_replicas() > decision.proposed_replicas()
                   ? decision.current_replicas() - decision.proposed_replicas()
                   : decision.proposed_replicas() - decision.current_replicas();
    const auto *workload = workload_by_id.at(decision.workload_id());
    if (decision.proposed_resources().SerializeAsString() !=
        workload->current_resources().SerializeAsString())
      ++actions;
  }
  actions += static_cast<std::uint64_t>(plan.deactivated_node_ids_size());
  if (actions > snapshot.policy().maximum_plan_actions() ||
      actions > std::numeric_limits<std::uint32_t>::max()) {
    plan.set_feasible(false);
    plan.set_infeasible_reason("plan exceeds configured action budget");
    plan.set_plan_id(MessageSha256(plan));
    return plan;
  }
  plan.set_action_count(static_cast<std::uint32_t>(actions));
  plan.set_feasible(true);
  const auto baseline_objective = CurrentObjective(snapshot);
  const auto proposed_objective = EvaluateObjective(snapshot, plan, plan.placements(), deactivated);
  if (!baseline_objective || !proposed_objective || proposed_objective->actions != actions) {
    plan.set_feasible(false);
    plan.set_infeasible_reason("objective arithmetic overflow or action accounting mismatch");
    plan.set_plan_id(MessageSha256(plan));
    return plan;
  }
  const bool current_state_hard_feasible = CurrentStateHardFeasible(snapshot, plan);
  if (current_state_hard_feasible &&
      !ImprovementMeetsPolicy({baseline_objective->current_total, proposed_objective->total,
                               snapshot.policy().minimum_improvement_ppm()})) {
    HoldCurrentState(snapshot, &plan,
                     "candidate rejected: weighted objective does not meet minimum improvement",
                     true);
  } else {
    StoreObjective(&plan, {*baseline_objective, *proposed_objective});
  }
  plan.set_plan_id(MessageSha256(plan));
  return plan;
}

v1::OptimizationPlan Plan(const OptimizationSnapshot &snapshot) {
  auto best = PlanWithProfileOverride(snapshot, {}, {},
                                      snapshot.policy().maximum_local_search_iterations());
  if (snapshot.workloads_size() == 0 || snapshot.capacity_profiles_size() == 0 ||
      snapshot.policy().maximum_local_search_iterations() == 0 ||
      snapshot.workloads_size() > static_cast<int>(kMaxWorkloads) ||
      snapshot.nodes_size() > static_cast<int>(kMaxNodes) ||
      snapshot.capacity_profiles_size() > static_cast<int>(kMaxProfiles) ||
      snapshot.current_placements_size() > static_cast<int>(kMaxPlacements))
    return best;

  struct ProfileTrial {
    std::string workload_id;
    std::string profile_id;
    std::uint64_t local_cost;
    std::uint32_t replicas;
    std::uint64_t shape_score;
  };
  std::vector<ProfileTrial> trials;
  for (const auto &workload : snapshot.workloads()) {
    const auto current = CurrentCount(snapshot, workload.workload_id());
    if (current > std::numeric_limits<std::uint32_t>::max())
      continue;
    std::string selected_profile;
    for (const auto &decision : best.workload_decisions()) {
      if (decision.workload_id() == workload.workload_id()) {
        selected_profile = decision.capacity_profile_id();
        break;
      }
    }
    for (const auto &profile : snapshot.capacity_profiles()) {
      if (profile.workload_id() != workload.workload_id() ||
          profile.profile_id() == selected_profile)
        continue;
      const auto candidate = CandidateFor(profile, workload, snapshot);
      if (!candidate)
        continue;
      std::uint32_t target = candidate->replicas;
      if (current > target) {
        const auto max_remove = CeilDivide(
            current * static_cast<std::uint64_t>(workload.max_scale_down_fraction_ppm()), kPpm);
        const auto safe_step = current > max_remove ? current - max_remove : 0;
        target = std::max(target, static_cast<std::uint32_t>(safe_step));
      }
      const auto uncertainty_floor =
          std::min({candidate->profile->confidence_ppm(), workload.profile_confidence_factor_ppm(),
                    workload.model_accuracy_factor_ppm(), workload.degraded_cluster_factor_ppm()});
      const auto uncertainty =
          CostScaled({snapshot.policy().uncertainty_cost_micros_per_replica(),
                      static_cast<UInt128>(kPpm - uncertainty_floor) * target});
      const auto boundary = CostScaled(
          {snapshot.policy().boundary_cost_micros_per_workload(),
           BoundaryRiskPpm({workload.demand_qps_milli(), workload.concurrency_bound(),
                            candidate->effective_qps, candidate->effective_concurrency, target})});
      const auto delta = current > target ? current - target : target - current;
      const auto action_churn = CostScaled(
          {snapshot.policy().action_churn_cost_micros(),
           delta + (SameResource(candidate->profile->allocation(), workload.current_resources())
                        ? 0U
                        : 1U)});
      if (!uncertainty || !boundary || !action_churn)
        continue;
      auto local_cost = AddCost(*uncertainty, *boundary);
      if (!local_cost)
        continue;
      local_cost = AddCost(*local_cost, *action_churn);
      if (!local_cost)
        continue;
      trials.push_back({workload.workload_id(), candidate->profile->profile_id(), *local_cost,
                        target, candidate->shape_score});
    }
  }
  std::sort(trials.begin(), trials.end(), [](const auto &left, const auto &right) {
    return std::tie(left.local_cost, left.replicas, left.shape_score, left.workload_id,
                    left.profile_id) < std::tie(right.local_cost, right.replicas, right.shape_score,
                                                right.workload_id, right.profile_id);
  });
  // ponytail: try single-profile alternatives only; evaluate combinations if evidence shows
  // independent profile changes miss material joint savings.
  const std::size_t max_trials = snapshot.workloads_size() <= 4    ? 8
                                 : snapshot.workloads_size() <= 64 ? 4
                                                                   : 1;
  const auto trial_count = std::min<std::size_t>(
      {trials.size(), max_trials, snapshot.policy().maximum_local_search_iterations()});
  // ponytail: large profile trials score the deterministic feasible placement only; keep the
  // configured local-search budget for the incumbent plan unless measurements justify more.
  const auto trial_local_search_budget =
      snapshot.workloads_size() <= 64 ? snapshot.policy().maximum_local_search_iterations() : 0U;
  for (std::size_t index = 0; index < trial_count; ++index) {
    auto candidate = PlanWithProfileOverride(snapshot, trials[index].workload_id,
                                             trials[index].profile_id, trial_local_search_budget);
    if (candidate.feasible() && (!best.feasible() || candidate.proposed_objective_cost_micros() <
                                                         best.proposed_objective_cost_micros()))
      best = std::move(candidate);
  }
  return best;
}

} // namespace fleetright
