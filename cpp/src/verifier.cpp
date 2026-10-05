#include "fleetright/verifier.hpp"

#include "fleetright/hash.hpp"
#include "fleetright/units.hpp"

#include <google/protobuf/unknown_field_set.h>

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
#include <unordered_set>
#include <vector>

namespace fleetright {
namespace {
constexpr std::uint64_t kPpm = 1'000'000;
constexpr std::uint32_t kTailSuccessPpm = 990'000;
constexpr std::uint64_t kMaxProfileTrials = 1'000'000'000'000ULL;
__extension__ using UInt128 = unsigned __int128;
using v1::CapacityProfile;
using v1::OptimizationPlan;
using v1::OptimizationSnapshot;
using v1::ReplicaPlacement;
using ProtoResourceVector = v1::ResourceVector;
using v1::WorkloadSpec;

struct Usage {
  ProtoResourceVector used;
  ProtoResourceVector limit;
  const v1::NodeState *node = nullptr;
};

struct SnapshotIndex {
  std::unordered_map<std::string, const WorkloadSpec *> workloads;
  std::unordered_map<std::string, const v1::NodeState *> nodes;
  std::unordered_map<std::string, const CapacityProfile *> profiles;
  mutable std::unordered_map<const CapacityProfile *, bool> profile_hashes_valid;
};

SnapshotIndex IndexSnapshot(const OptimizationSnapshot &snapshot) {
  SnapshotIndex index;
  index.workloads.reserve(static_cast<std::size_t>(snapshot.workloads_size()));
  index.nodes.reserve(static_cast<std::size_t>(snapshot.nodes_size()));
  index.profiles.reserve(static_cast<std::size_t>(snapshot.capacity_profiles_size()));
  for (const auto &workload : snapshot.workloads())
    index.workloads.emplace(workload.workload_id(), &workload);
  for (const auto &node : snapshot.nodes())
    index.nodes.emplace(node.node_id(), &node);
  for (const auto &profile : snapshot.capacity_profiles())
    index.profiles.emplace(profile.profile_id(), &profile);
  return index;
}

void AddResult(v1::VerificationResult *result, const std::string &name, bool passed,
               const std::string &detail, std::int64_t margin = 0) {
  auto *check = result->add_invariants();
  check->set_invariant(name);
  check->set_passed(passed);
  check->set_detail(detail);
  check->set_margin(margin);
  if (!passed)
    result->set_accepted(false);
}

bool ResourceNonNegative(const ProtoResourceVector &value) {
  return value.cpu_millicores() >= 0 && value.memory_mib() >= 0 && value.network_mbps() >= 0;
}

bool AddChecked(const ProtoResourceVector &a, const ProtoResourceVector &b,
                ProtoResourceVector *output) {
  const auto sum = CheckedAdd(
      {CpuMillicores(a.cpu_millicores()), MemoryMiB(a.memory_mib()), NetworkMbps(a.network_mbps())},
      {CpuMillicores(b.cpu_millicores()), MemoryMiB(b.memory_mib()),
       NetworkMbps(b.network_mbps())});
  if (!sum)
    return false;
  output->set_cpu_millicores(sum->cpu.value);
  output->set_memory_mib(sum->memory.value);
  output->set_network_mbps(sum->network.value);
  return true;
}

bool FitsResources(const ProtoResourceVector &usage, const ProtoResourceVector &limit) {
  return ResourceNonNegative(usage) && ResourceNonNegative(limit) &&
         usage.cpu_millicores() <= limit.cpu_millicores() &&
         usage.memory_mib() <= limit.memory_mib() && usage.network_mbps() <= limit.network_mbps();
}

bool LimitForNode(const v1::NodeState &node, std::uint32_t headroom_ppm,
                  ProtoResourceVector *limit) {
  if (headroom_ppm > kPpm || !ResourceNonNegative(node.allocatable()) ||
      !ResourceNonNegative(node.reserved()))
    return false;
  const auto safe_dimension = [headroom_ppm](std::int64_t total, std::int64_t reserved,
                                             std::int64_t *result) {
    if (total < 0 || reserved < 0)
      return false;
    __extension__ using UInt128 = unsigned __int128;
    const UInt128 usable = static_cast<UInt128>(total) * (kPpm - headroom_ppm) / kPpm;
    if (usable > static_cast<UInt128>(std::numeric_limits<std::int64_t>::max()) ||
        static_cast<std::int64_t>(usable) < reserved)
      return false;
    *result = static_cast<std::int64_t>(usable) - reserved;
    return true;
  };
  std::int64_t cpu = 0;
  std::int64_t memory = 0;
  std::int64_t network = 0;
  if (!safe_dimension(node.allocatable().cpu_millicores(), node.reserved().cpu_millicores(),
                      &cpu) ||
      !safe_dimension(node.allocatable().memory_mib(), node.reserved().memory_mib(), &memory) ||
      !safe_dimension(node.allocatable().network_mbps(), node.reserved().network_mbps(), &network))
    return false;
  limit->set_cpu_millicores(cpu);
  limit->set_memory_mib(memory);
  limit->set_network_mbps(network);
  return true;
}

std::uint64_t DivideUp(std::uint64_t numerator, std::uint64_t denominator) {
  if (denominator == 0)
    return std::numeric_limits<std::uint64_t>::max();
  return numerator / denominator + (numerator % denominator != 0 ? 1U : 0U);
}

bool IsPpm(std::uint32_t value) { return value <= kPpm; }
bool PointSafe(const v1::CapacityPoint &point, const WorkloadSpec &workload);

struct PpmRatio {
  std::uint64_t numerator;
  UInt128 denominator;
};

std::uint32_t RatioPpm(const PpmRatio &input) {
  if (input.numerator == 0)
    return 0;
  if (input.denominator == 0)
    return static_cast<std::uint32_t>(kPpm);
  const UInt128 ratio =
      (static_cast<UInt128>(input.numerator) * kPpm + input.denominator - 1) / input.denominator;
  return static_cast<std::uint32_t>(std::min<UInt128>(ratio, kPpm));
}

bool DecisionEvidenceMatches(const v1::WorkloadDecision &decision, const CapacityProfile &profile,
                             const WorkloadSpec &workload) {
  if (decision.profile_point_index() >= static_cast<std::uint32_t>(profile.safe_points_size()) ||
      decision.profile_confidence_ppm() != profile.confidence_ppm())
    return false;
  const auto &point = profile.safe_points(static_cast<int>(decision.profile_point_index()));
  if (!PointSafe(point, workload) || profile.confidence_ppm() > point.latency_success_lower_ppm())
    return false;
  const auto qps = MultiplyPpmFloor(
      point.safe_qps_milli(), workload.profile_confidence_factor_ppm(),
      workload.model_accuracy_factor_ppm(), workload.degraded_cluster_factor_ppm());
  const auto concurrency = MultiplyPpmFloor(
      point.safe_concurrency(), workload.profile_confidence_factor_ppm(),
      workload.model_accuracy_factor_ppm(), workload.degraded_cluster_factor_ppm());
  if (!qps || !concurrency || *qps == 0 || *concurrency == 0 ||
      decision.effective_qps_per_replica_milli() != *qps ||
      decision.effective_concurrency_per_replica() != *concurrency)
    return false;
  const auto uncertainty_floor =
      std::min({profile.confidence_ppm(), workload.profile_confidence_factor_ppm(),
                workload.model_accuracy_factor_ppm(), workload.degraded_cluster_factor_ppm()});
  const auto expected_uncertainty = static_cast<std::uint32_t>(kPpm - uncertainty_floor);
  const auto qps_risk = RatioPpm(
      {workload.demand_qps_milli(), static_cast<UInt128>(*qps) * decision.proposed_replicas()});
  const auto concurrency_risk =
      RatioPpm({workload.concurrency_bound(),
                static_cast<UInt128>(*concurrency) * decision.proposed_replicas()});
  return decision.uncertainty_exposure_ppm() == expected_uncertainty &&
         decision.boundary_risk_ppm() == std::max(qps_risk, concurrency_risk);
}

std::optional<std::uint32_t> RecomputedWilsonLower(std::uint64_t successes, std::uint64_t trials) {
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

std::optional<std::uint32_t> RecomputedWilsonUpper(std::uint64_t failures, std::uint64_t trials) {
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
      point.error_count() > point.request_count() || !IsPpm(point.latency_success_lower_ppm()) ||
      !IsPpm(point.error_rate_upper_ppm()) || !IsPpm(point.error_rate_wilson_upper_ppm()) ||
      !IsPpm(point.memory_pressure_ppm()) || !IsPpm(point.cpu_throttling_ppm()) ||
      !IsPpm(point.run_dispersion_ppm()) || point.repeated_run_count() > 10'000)
    return false;
  const auto latency = RecomputedWilsonLower(point.threshold_successes(), point.threshold_trials());
  const auto errors = RecomputedWilsonUpper(point.error_count(), point.request_count());
  if (!latency || !errors || point.latency_success_lower_ppm() > *latency ||
      point.error_rate_wilson_upper_ppm() < *errors)
    return false;
  __extension__ using UInt128 = unsigned __int128;
  const UInt128 observed_error =
      (static_cast<UInt128>(point.error_count()) * kPpm + point.request_count() - 1) /
      point.request_count();
  return observed_error <= point.error_rate_upper_ppm();
}

const CapacityProfile *ProfileById(const SnapshotIndex &index, const std::string &profile_id) {
  const auto found = index.profiles.find(profile_id);
  return found == index.profiles.end() ? nullptr : found->second;
}

const WorkloadSpec *WorkloadById(const SnapshotIndex &index, const std::string &id) {
  const auto found = index.workloads.find(id);
  return found == index.workloads.end() ? nullptr : found->second;
}

const v1::NodeState *NodeById(const SnapshotIndex &index, const std::string &id) {
  const auto found = index.nodes.find(id);
  return found == index.nodes.end() ? nullptr : found->second;
}

bool ProfileDigestValid(const CapacityProfile &profile);

bool ProfileHashValid(const SnapshotIndex &index, const CapacityProfile &profile) {
  const auto found = index.profile_hashes_valid.find(&profile);
  if (found != index.profile_hashes_valid.end())
    return found->second;
  return index.profile_hashes_valid.emplace(&profile, ProfileDigestValid(profile)).first->second;
}

std::string GetLabel(const v1::NodeState &node, const std::string &key) {
  for (const auto &label : node.labels()) {
    if (label.key() == key)
      return label.value();
  }
  return {};
}

bool AllowedTaints(const WorkloadSpec &workload, const v1::NodeState &node) {
  for (const auto &taint : node.taints()) {
    bool tolerated = false;
    for (const auto &key : workload.tolerated_taint_keys()) {
      if (key == taint.key())
        tolerated = true;
    }
    if (!tolerated)
      return false;
  }
  return true;
}

bool ProfileDigestValid(const CapacityProfile &profile) {
  auto content = profile;
  const std::string recorded = content.profile_sha256();
  content.clear_profile_sha256();
  return !recorded.empty() && MessageSha256(content) == recorded;
}

bool PointSafe(const v1::CapacityPoint &point, const WorkloadSpec &workload) {
  return PointEvidenceConsistent(point) && point.repeated_run_count() >= 2 &&
         point.latency_success_lower_ppm() >= kTailSuccessPpm &&
         point.error_rate_wilson_upper_ppm() <= workload.error_slo_ppm() &&
         point.error_rate_upper_ppm() <= workload.error_slo_ppm() &&
         point.p99_upper_ms() <= workload.latency_slo_ms() &&
         point.queue_growth_upper_milli() <= 0 &&
         point.memory_pressure_ppm() <= workload.max_memory_pressure_ppm() &&
         point.cpu_throttling_ppm() <= workload.max_throttle_ppm() && point.stable_across_runs() &&
         point.run_dispersion_ppm() <= 250'000U;
}

bool CompatibleProfile(const SnapshotIndex &index, const CapacityProfile &p, const WorkloadSpec &w,
                       const OptimizationSnapshot &s) {
  const bool empirical_container_profile =
      p.measurement_kind() == "docker_runtime" && p.enforceable_resource_limits();
  const bool test_profile =
      s.policy().allow_synthetic_profiles() && p.measurement_kind().starts_with("synthetic_");
  return p.schema_version() == 1 && p.profile_id().size() > 0 &&
         p.workload_id() == w.workload_id() && p.image_digest() == w.image_digest() &&
         p.request_mix_id() == w.request_mix_id() &&
         p.runtime_config_class() == w.runtime_config_class() &&
         p.platform_class() == w.platform_class() && ProfileHashValid(index, p) &&
         p.generated_at_ms() <= s.evaluation_time_ms() &&
         p.valid_until_ms() > s.evaluation_time_ms() &&
         s.evaluation_time_ms() - p.generated_at_ms() <= s.policy().maximum_profile_age_ms() &&
         p.confidence_ppm() >= s.policy().minimum_profile_confidence_ppm() &&
         IsPpm(p.confidence_ppm()) && IsPpm(p.conservative_haircut_ppm()) &&
         (empirical_container_profile || test_profile) && ResourceNonNegative(p.allocation());
}

bool ProfileSupports(const SnapshotIndex &index, const CapacityProfile &p, const WorkloadSpec &w,
                     const OptimizationSnapshot &s, std::uint64_t qps_out,
                     std::uint64_t concurrency_out, std::uint32_t target_replicas) {
  if (!CompatibleProfile(index, p, w, s) || w.profile_confidence_factor_ppm() == 0 ||
      w.model_accuracy_factor_ppm() == 0 || w.degraded_cluster_factor_ppm() == 0 ||
      !IsPpm(w.profile_confidence_factor_ppm()) || !IsPpm(w.model_accuracy_factor_ppm()) ||
      !IsPpm(w.degraded_cluster_factor_ppm()))
    return false;
  for (const auto &point : p.safe_points()) {
    if (!PointSafe(point, w) || p.confidence_ppm() > point.latency_success_lower_ppm())
      continue;
    const auto qps =
        MultiplyPpmFloor(point.safe_qps_milli(), w.profile_confidence_factor_ppm(),
                         w.model_accuracy_factor_ppm(), w.degraded_cluster_factor_ppm());
    const auto concurrency =
        MultiplyPpmFloor(point.safe_concurrency(), w.profile_confidence_factor_ppm(),
                         w.model_accuracy_factor_ppm(), w.degraded_cluster_factor_ppm());
    if (qps && concurrency && *qps > 0 && *concurrency > 0 &&
        DivideUp(qps_out, *qps) <= target_replicas &&
        DivideUp(concurrency_out, *concurrency) <= target_replicas)
      return true;
  }
  return false;
}

std::uint64_t RequiredCount(const WorkloadSpec &workload, const CapacityProfile &profile,
                            const OptimizationSnapshot &snapshot) {
  std::uint64_t best = std::numeric_limits<std::uint64_t>::max();
  for (const auto &point : profile.safe_points()) {
    if (!PointSafe(point, workload) || profile.confidence_ppm() > point.latency_success_lower_ppm())
      continue;
    const auto qps = MultiplyPpmFloor(
        point.safe_qps_milli(), workload.profile_confidence_factor_ppm(),
        workload.model_accuracy_factor_ppm(), workload.degraded_cluster_factor_ppm());
    const auto concurrency = MultiplyPpmFloor(
        point.safe_concurrency(), workload.profile_confidence_factor_ppm(),
        workload.model_accuracy_factor_ppm(), workload.degraded_cluster_factor_ppm());
    if (!qps || !concurrency || *qps == 0 || *concurrency == 0)
      continue;
    const std::uint64_t needed = std::max(DivideUp(workload.demand_qps_milli(), *qps),
                                          DivideUp(workload.concurrency_bound(), *concurrency));
    const std::uint64_t floor = std::max(workload.min_replicas(), workload.availability_floor());
    const std::uint64_t target = std::max(needed, floor);
    if (workload.failure_tolerance() == v1::FAILURE_TOLERANCE_N_MINUS_1 &&
        target == std::numeric_limits<std::uint64_t>::max())
      continue;
    best = std::min(
        best, target + (workload.failure_tolerance() == v1::FAILURE_TOLERANCE_N_MINUS_1 ? 1U : 0U));
  }
  (void)snapshot;
  return best;
}

struct VerifiedWorkloadRisk {
  const CapacityProfile *profile = nullptr;
  std::uint32_t point_index = 0;
  std::uint64_t effective_qps = 0;
  std::uint64_t effective_concurrency = 0;
  std::uint32_t uncertainty_ppm = static_cast<std::uint32_t>(kPpm);
  std::uint32_t boundary_ppm = static_cast<std::uint32_t>(kPpm);
};

VerifiedWorkloadRisk CurrentRiskFor(const OptimizationSnapshot &snapshot,
                                    const SnapshotIndex &index, const WorkloadSpec &workload,
                                    std::uint32_t current_replicas) {
  struct SafePointChoice {
    const CapacityProfile *profile;
    std::uint32_t point_index;
    std::uint64_t required_replicas;
    std::uint64_t qps;
    std::uint64_t concurrency;
  };
  std::optional<SafePointChoice> best;
  for (const auto &profile : snapshot.capacity_profiles()) {
    if (profile.workload_id() != workload.workload_id() ||
        profile.allocation().SerializeAsString() !=
            workload.current_resources().SerializeAsString() ||
        !CompatibleProfile(index, profile, workload, snapshot))
      continue;
    for (int point_number = 0; point_number < profile.safe_points_size(); ++point_number) {
      const auto &point = profile.safe_points(point_number);
      if (!PointSafe(point, workload) ||
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
      std::uint64_t required =
          std::max({DivideUp(workload.demand_qps_milli(), *qps),
                    DivideUp(workload.concurrency_bound(), *concurrency),
                    static_cast<std::uint64_t>(workload.min_replicas()),
                    static_cast<std::uint64_t>(workload.availability_floor())});
      if (workload.failure_tolerance() == v1::FAILURE_TOLERANCE_N_MINUS_1) {
        if (required == std::numeric_limits<std::uint64_t>::max())
          continue;
        ++required;
      }
      if (required > current_replicas || required > workload.max_replicas())
        continue;
      SafePointChoice choice{&profile, static_cast<std::uint32_t>(point_number), required, *qps,
                             *concurrency};
      if (!best ||
          std::tie(choice.required_replicas, choice.profile->profile_id(), choice.point_index) <
              std::tie(best->required_replicas, best->profile->profile_id(), best->point_index)) {
        best = choice;
      }
    }
  }
  VerifiedWorkloadRisk risk;
  if (!best)
    return risk;
  risk.profile = best->profile;
  risk.point_index = best->point_index;
  risk.effective_qps = best->qps;
  risk.effective_concurrency = best->concurrency;
  const auto floor =
      std::min({best->profile->confidence_ppm(), workload.profile_confidence_factor_ppm(),
                workload.model_accuracy_factor_ppm(), workload.degraded_cluster_factor_ppm()});
  risk.uncertainty_ppm = static_cast<std::uint32_t>(kPpm - floor);
  const auto qps_risk =
      RatioPpm({workload.demand_qps_milli(), static_cast<UInt128>(best->qps) * current_replicas});
  const auto concurrency_risk = RatioPpm(
      {workload.concurrency_bound(), static_cast<UInt128>(best->concurrency) * current_replicas});
  risk.boundary_ppm = std::max(qps_risk, concurrency_risk);
  return risk;
}

bool SnapshotHashMatches(const OptimizationSnapshot &snapshot) {
  const auto &digest = snapshot.content_sha256();
  if (digest.size() != 64 ||
      snapshot.GetReflection()->GetUnknownFields(snapshot).field_count() != 0) {
    return false;
  }
  auto bytes = DeterministicBytes(snapshot);
  constexpr std::size_t kHashFieldSuffixBytes = 2 + 64;
  if (bytes.size() < kHashFieldSuffixBytes)
    return false;
  const std::size_t suffix = bytes.size() - kHashFieldSuffixBytes;
  // Field 13 is the final v1 OptimizationSnapshot field and a SHA-256 hex digest is 64 bytes.
  if (static_cast<unsigned char>(bytes[suffix]) != 0x6aU ||
      static_cast<unsigned char>(bytes[suffix + 1]) != 64U ||
      bytes.compare(suffix + 2, digest.size(), digest) != 0)
    return false;
  bytes.resize(suffix);
  return Sha256(bytes) == digest;
}

bool CatalogHashMatches(const v1::MachineCatalog &catalog) {
  auto copy = catalog;
  const auto digest = copy.catalog_sha256();
  copy.clear_catalog_sha256();
  return !digest.empty() && MessageSha256(copy) == digest;
}

bool CatalogMatchesNodes(const OptimizationSnapshot &snapshot) {
  std::map<std::string, const v1::MachineType *> machines;
  for (const auto &machine : snapshot.machine_catalog().machines()) {
    if (machine.machine_type().empty() || !ResourceNonNegative(machine.allocatable()) ||
        machine.hourly_cost_micros() == 0 ||
        !machines.emplace(machine.machine_type(), &machine).second)
      return false;
  }
  if (machines.empty())
    return false;
  for (const auto &node : snapshot.nodes()) {
    const auto machine = machines.find(node.machine_type());
    if (machine == machines.end() ||
        machine->second->allocatable().cpu_millicores() != node.allocatable().cpu_millicores() ||
        machine->second->allocatable().memory_mib() != node.allocatable().memory_mib() ||
        machine->second->allocatable().network_mbps() != node.allocatable().network_mbps() ||
        machine->second->hourly_cost_micros() != node.hourly_cost_micros())
      return false;
  }
  return true;
}

bool CheckState(const OptimizationSnapshot &snapshot, const SnapshotIndex &index,
                const std::vector<ReplicaPlacement> &placements,
                const std::map<std::string, ProtoResourceVector> &shapes,
                const std::set<std::string> &removed_nodes,
                bool allow_duplicate_replica_id = false) {
  // This pass returns only a boolean, so hash iteration order cannot affect canonical output.
  std::unordered_map<std::string, Usage> usage;
  usage.reserve(static_cast<std::size_t>(snapshot.nodes_size()));
  for (const auto &node : snapshot.nodes()) {
    if (!node.active() || removed_nodes.contains(node.node_id()) ||
        node.health() != v1::NODE_HEALTH_HEALTHY)
      continue;
    Usage value;
    if (!LimitForNode(node, snapshot.policy().headroom_ppm(), &value.limit))
      return false;
    value.node = &node;
    usage.emplace(node.node_id(), value);
  }
  std::unordered_map<std::string, std::unordered_set<std::string>> workload_nodes;
  std::unordered_map<std::string, std::unordered_set<std::string>> replica_keys;
  std::unordered_map<std::string, std::unordered_map<std::string, std::uint32_t>> domains;
  std::unordered_map<std::string, std::uint32_t> counts;
  for (const auto &placement : placements) {
    const auto *workload = WorkloadById(index, placement.workload_id());
    const auto node = usage.find(placement.node_id());
    const auto shape = shapes.find(placement.workload_id());
    if (workload == nullptr || node == usage.end() || shape == shapes.end() ||
        !AllowedTaints(*workload, *node->second.node) || !ResourceNonNegative(shape->second))
      return false;
    if (!allow_duplicate_replica_id &&
        !replica_keys[placement.workload_id()].insert(placement.replica_id()).second)
      return false;
    if (!workload_nodes[placement.workload_id()].insert(placement.node_id()).second)
      return false;
    ProtoResourceVector sum;
    if (!AddChecked(node->second.used, shape->second, &sum) ||
        !FitsResources(sum, node->second.limit))
      return false;
    node->second.used = std::move(sum);
    ++counts[placement.workload_id()];
    if (!workload->topology_key().empty()) {
      const auto domain = GetLabel(*node->second.node, workload->topology_key());
      if (domain.empty())
        return false;
      ++domains[placement.workload_id()][domain];
    }
  }
  for (const auto &[workload_id, count] : counts) {
    const auto *workload = WorkloadById(index, workload_id);
    if (workload == nullptr || count < workload->min_replicas() ||
        count > workload->max_replicas() || count < workload->availability_floor())
      return false;
    const auto &groups = domains[workload_id];
    if (workload->min_topology_domains() > 0) {
      if (groups.size() < workload->min_topology_domains())
        return false;
      std::uint32_t min_count = std::numeric_limits<std::uint32_t>::max();
      std::uint32_t max_count = 0;
      for (const auto &[domain, amount] : groups) {
        (void)domain;
        min_count = std::min(min_count, amount);
        max_count = std::max(max_count, amount);
      }
      if (max_count - min_count > workload->max_topology_skew())
        return false;
    }
  }
  for (const auto &workload : snapshot.workloads()) {
    if (counts[workload.workload_id()] == 0 && workload.min_replicas() > 0)
      return false;
  }
  return true;
}

bool CheckNMinusOne(const OptimizationSnapshot &snapshot, const SnapshotIndex &index,
                    const OptimizationPlan &plan,
                    const std::map<std::string, ProtoResourceVector> &shapes, std::string *detail) {
  std::map<std::string, std::set<std::string>> domains_by_key;
  std::set<std::string> domain_keys;
  const std::set<std::string> deactivated(plan.deactivated_node_ids().begin(),
                                          plan.deactivated_node_ids().end());
  for (const auto &workload : snapshot.workloads()) {
    if (workload.failure_tolerance() != v1::FAILURE_TOLERANCE_N_MINUS_1)
      continue;
    if (!v1::FailureTolerance_IsValid(static_cast<int>(workload.failure_tolerance()))) {
      *detail = "unknown failure tolerance enum";
      return false;
    }
    const std::string key = workload.topology_key().empty() ? "zone" : workload.topology_key();
    domain_keys.insert(key);
    for (const auto &node : snapshot.nodes()) {
      if (node.active() && node.health() == v1::NODE_HEALTH_HEALTHY &&
          !deactivated.contains(node.node_id())) {
        const auto value = GetLabel(node, key);
        if (value.empty()) {
          *detail = "N-1 workload lacks a labeled failure domain";
          return false;
        }
        domains_by_key[key].insert(value);
      }
    }
  }
  if (domain_keys.empty()) {
    *detail = "no workload requires N-1";
    return true;
  }
  std::set<std::pair<std::string, std::string>> failure_domains;
  for (const auto &key : domain_keys) {
    if (domains_by_key[key].size() < 2) {
      *detail = "fewer than two healthy, active failure domains remain for " + key;
      return false;
    }
    for (const auto &label : domains_by_key[key])
      failure_domains.emplace(key, label);
  }
  const auto targets_for = [&](const std::string &id) {
    std::uint32_t count = 0;
    for (const auto &placement : plan.placements()) {
      if (placement.workload_id() == id)
        ++count;
    }
    return count;
  };
  for (const auto &[failed_key, failed_label] : failure_domains) {
    std::set<std::string> failed_node_ids;
    for (const auto &node : snapshot.nodes()) {
      if (node.active() && node.health() == v1::NODE_HEALTH_HEALTHY &&
          !deactivated.contains(node.node_id()) && GetLabel(node, failed_key) == failed_label) {
        failed_node_ids.insert(node.node_id());
      }
    }
    std::vector<ReplicaPlacement> survivors;
    for (const auto &placement : plan.placements()) {
      if (!failed_node_ids.contains(placement.node_id()))
        survivors.push_back(placement);
    }
    for (const auto &workload : snapshot.workloads()) {
      if (workload.failure_tolerance() != v1::FAILURE_TOLERANCE_N_MINUS_1)
        continue;
      const auto survivor_count = static_cast<std::uint32_t>(
          std::count_if(survivors.begin(), survivors.end(),
                        [&](const auto &p) { return p.workload_id() == workload.workload_id(); }));
      const auto *decision = [&]() -> const v1::WorkloadDecision * {
        for (const auto &item : plan.workload_decisions()) {
          if (item.workload_id() == workload.workload_id())
            return &item;
        }
        return nullptr;
      }();
      const auto *profile =
          decision == nullptr ? nullptr : ProfileById(index, decision->capacity_profile_id());
      if (decision == nullptr || profile == nullptr ||
          survivor_count > targets_for(workload.workload_id()) ||
          !ProfileSupports(index, *profile, workload, snapshot, workload.demand_qps_milli(),
                           workload.concurrency_bound(), survivor_count) ||
          survivor_count < workload.availability_floor() ||
          survivor_count < workload.min_replicas()) {
        *detail = "serving replicas after failure of ";
        detail->append(failed_key)
            .append("=")
            .append(failed_label)
            .append(" cannot preserve bounded demand");
        return false;
      }
      const auto count_to_restore = targets_for(workload.workload_id()) - survivor_count;
      for (std::uint32_t i = 0; i < count_to_restore; ++i) {
        // Recovery is a separate deterministic placement simulation over residual capacity.
        bool placed = false;
        for (const auto &node : snapshot.nodes()) {
          if (!node.active() || node.health() != v1::NODE_HEALTH_HEALTHY)
            continue;
          if (deactivated.contains(node.node_id()) || failed_node_ids.contains(node.node_id()) ||
              !AllowedTaints(workload, node))
            continue;
          const bool occupied = std::any_of(survivors.begin(), survivors.end(), [&](const auto &p) {
            return p.workload_id() == workload.workload_id() && p.node_id() == node.node_id();
          });
          if (occupied)
            continue;
          ProtoResourceVector limit;
          if (!LimitForNode(node, snapshot.policy().headroom_ppm(), &limit))
            continue;
          ProtoResourceVector used;
          for (const auto &current : survivors) {
            if (current.node_id() != node.node_id())
              continue;
            const auto shape = shapes.find(current.workload_id());
            if (shape == shapes.end() || !AddChecked(used, shape->second, &used)) {
              *detail = "recovery resource arithmetic failed";
              return false;
            }
          }
          const auto shape = shapes.find(workload.workload_id());
          ProtoResourceVector after;
          if (shape != shapes.end() && AddChecked(used, shape->second, &after) &&
              FitsResources(after, limit)) {
            bool topology_ok = true;
            if (!workload.topology_key().empty()) {
              std::map<std::string, std::uint32_t> topology_counts;
              for (const auto &current : survivors) {
                if (current.workload_id() != workload.workload_id())
                  continue;
                const auto *current_node = NodeById(index, current.node_id());
                if (current_node == nullptr) {
                  topology_ok = false;
                  break;
                }
                ++topology_counts[GetLabel(*current_node, workload.topology_key())];
              }
              ++topology_counts[GetLabel(node, workload.topology_key())];
              if (topology_counts.contains("") ||
                  topology_counts.size() < workload.min_topology_domains())
                topology_ok = false;
              if (topology_ok && workload.min_topology_domains() > 0) {
                std::uint32_t minimum = std::numeric_limits<std::uint32_t>::max();
                std::uint32_t maximum = 0;
                for (const auto &[domain, amount] : topology_counts) {
                  (void)domain;
                  minimum = std::min(minimum, amount);
                  maximum = std::max(maximum, amount);
                }
                if (maximum - minimum > workload.max_topology_skew())
                  topology_ok = false;
              }
            }
            if (!topology_ok)
              continue;
            ReplicaPlacement recovery;
            recovery.set_workload_id(workload.workload_id());
            recovery.set_replica_id(workload.workload_id() + "-recovery-" + std::to_string(i));
            recovery.set_node_id(node.node_id());
            survivors.push_back(std::move(recovery));
            placed = true;
            break;
          }
        }
        if (!placed) {
          *detail = "no active surviving-node recovery placement after failure of ";
          detail->append(failed_key).append("=").append(failed_label);
          return false;
        }
      }
    }
  }
  *detail = "each modeled failure domain retains service capacity and recovery placement";
  return true;
}

bool CurrentStateHardFeasible(const OptimizationSnapshot &snapshot, const SnapshotIndex &index,
                              const OptimizationPlan &candidate_plan) {
  OptimizationPlan current = candidate_plan;
  current.clear_placements();
  current.clear_deactivated_node_ids();
  current.clear_evacuation_steps();
  std::map<std::string, ProtoResourceVector> shapes;
  std::map<std::string, std::uint32_t> counts;
  for (const auto &workload : snapshot.workloads())
    shapes[workload.workload_id()] = workload.current_resources();
  for (const auto &placement : snapshot.current_placements()) {
    *current.add_placements() = placement;
    ++counts[placement.workload_id()];
  }
  for (auto &decision : *current.mutable_workload_decisions()) {
    const auto *workload = WorkloadById(index, decision.workload_id());
    if (workload == nullptr)
      return false;
    const auto count = counts[decision.workload_id()];
    decision.set_current_replicas(count);
    decision.set_proposed_replicas(count);
    *decision.mutable_proposed_resources() = workload->current_resources();
    const auto risk = CurrentRiskFor(snapshot, index, *workload, count);
    if (risk.profile == nullptr)
      return false;
    decision.set_capacity_profile_id(risk.profile->profile_id());
    decision.set_capacity_profile_sha256(risk.profile->profile_sha256());
  }
  if (!CheckState(
          snapshot, index,
          std::vector<ReplicaPlacement>(current.placements().begin(), current.placements().end()),
          shapes, {}))
    return false;
  std::string detail;
  return CheckNMinusOne(snapshot, index, current, shapes, &detail);
}

std::optional<std::uint64_t> CostOfNodes(const OptimizationSnapshot &snapshot,
                                         const std::set<std::string> &deactivated) {
  std::uint64_t sum = 0;
  for (const auto &node : snapshot.nodes()) {
    if (!node.active() || deactivated.contains(node.node_id()))
      continue;
    if (sum > std::numeric_limits<std::uint64_t>::max() - node.hourly_cost_micros())
      return std::nullopt;
    sum += node.hourly_cost_micros();
  }
  return sum;
}

std::optional<std::uint64_t> VerifiedHorizonCost(std::uint64_t hourly, std::uint64_t seconds) {
  const UInt128 amount = static_cast<UInt128>(hourly) * seconds;
  const UInt128 total = (amount + 3'599) / 3'600;
  if (total > std::numeric_limits<std::uint64_t>::max())
    return std::nullopt;
  return static_cast<std::uint64_t>(total);
}

struct PenaltyInput {
  std::uint64_t coefficient;
  UInt128 exposure;
  std::uint64_t divisor;
};

std::optional<std::uint64_t> VerifiedPenalty(const PenaltyInput &input) {
  const UInt128 amount = static_cast<UInt128>(input.coefficient) * input.exposure;
  const UInt128 total = (amount + input.divisor - 1) / input.divisor;
  if (total > std::numeric_limits<std::uint64_t>::max())
    return std::nullopt;
  return static_cast<std::uint64_t>(total);
}

struct VerifiedObjective {
  std::uint64_t current = 0;
  std::uint64_t proposed = 0;
  std::uint64_t current_uncertainty = 0;
  std::uint64_t current_boundary = 0;
  std::uint64_t movement = 0;
  std::uint64_t churn = 0;
  std::uint64_t uncertainty = 0;
  std::uint64_t boundary = 0;
};

struct VerifiedScoreInput {
  const OptimizationSnapshot &snapshot;
  const SnapshotIndex &index;
  const OptimizationPlan &plan;
  std::uint64_t actions;
  std::uint64_t current_hourly;
  std::uint64_t proposed_hourly;
};

std::optional<VerifiedObjective> IndependentlyScore(const VerifiedScoreInput &input) {
  const auto &snapshot = input.snapshot;
  const auto &index = input.index;
  const auto &plan = input.plan;
  VerifiedObjective result;
  const auto current =
      VerifiedHorizonCost(input.current_hourly, snapshot.policy().planning_horizon_seconds());
  const auto infrastructure =
      VerifiedHorizonCost(input.proposed_hourly, snapshot.policy().planning_horizon_seconds());
  if (!current || !infrastructure)
    return std::nullopt;
  result.current = *current;
  std::map<std::string, std::uint32_t> current_counts;
  for (const auto &placement : snapshot.current_placements())
    ++current_counts[placement.workload_id()];
  for (const auto &workload : snapshot.workloads()) {
    const auto risk =
        CurrentRiskFor(snapshot, index, workload, current_counts[workload.workload_id()]);
    const auto uncertainty = VerifiedPenalty(
        {snapshot.policy().uncertainty_cost_micros_per_replica(),
         static_cast<UInt128>(risk.uncertainty_ppm) * current_counts[workload.workload_id()],
         kPpm});
    const auto boundary = VerifiedPenalty(
        {snapshot.policy().boundary_cost_micros_per_workload(), risk.boundary_ppm, kPpm});
    if (!uncertainty || !boundary ||
        result.current_uncertainty > std::numeric_limits<std::uint64_t>::max() - *uncertainty ||
        result.current_boundary > std::numeric_limits<std::uint64_t>::max() - *boundary)
      return std::nullopt;
    result.current_uncertainty += *uncertainty;
    result.current_boundary += *boundary;
  }
  if (result.current > std::numeric_limits<std::uint64_t>::max() - result.current_uncertainty)
    return std::nullopt;
  result.current += result.current_uncertainty;
  if (result.current > std::numeric_limits<std::uint64_t>::max() - result.current_boundary)
    return std::nullopt;
  result.current += result.current_boundary;
  const auto movement = VerifiedPenalty({snapshot.policy().movement_cost_micros(),
                                         static_cast<UInt128>(plan.evacuation_steps_size()), 1});
  const auto churn = VerifiedPenalty(
      {snapshot.policy().action_churn_cost_micros(), static_cast<UInt128>(input.actions), 1});
  if (!movement || !churn)
    return std::nullopt;
  result.movement = *movement;
  result.churn = *churn;
  result.proposed = *infrastructure;
  for (const auto &decision : plan.workload_decisions()) {
    if (!IsPpm(decision.uncertainty_exposure_ppm()) || !IsPpm(decision.boundary_risk_ppm()))
      return std::nullopt;
    const auto uncertainty = VerifiedPenalty(
        {snapshot.policy().uncertainty_cost_micros_per_replica(),
         static_cast<UInt128>(decision.uncertainty_exposure_ppm()) * decision.proposed_replicas(),
         kPpm});
    const auto boundary = VerifiedPenalty({snapshot.policy().boundary_cost_micros_per_workload(),
                                           decision.boundary_risk_ppm(), kPpm});
    if (!uncertainty || !boundary ||
        result.uncertainty > std::numeric_limits<std::uint64_t>::max() - *uncertainty ||
        result.boundary > std::numeric_limits<std::uint64_t>::max() - *boundary)
      return std::nullopt;
    result.uncertainty += *uncertainty;
    result.boundary += *boundary;
  }
  for (const auto penalty : {result.movement, result.churn, result.uncertainty, result.boundary}) {
    if (result.proposed > std::numeric_limits<std::uint64_t>::max() - penalty)
      return std::nullopt;
    result.proposed += penalty;
  }
  return result;
}

struct ImprovementInput {
  std::uint64_t before;
  std::uint64_t after;
  std::uint32_t minimum_ppm;
};

bool VerifiedImprovement(const ImprovementInput &input) {
  if (input.after >= input.before)
    return false;
  const UInt128 saving = static_cast<UInt128>(input.before - input.after) * kPpm;
  const UInt128 required = static_cast<UInt128>(input.before) * input.minimum_ppm;
  return saving >= required;
}

} // namespace

std::string VerifierBuildId() {
#ifdef FLEETRIGHT_BUILD_ID
  return FLEETRIGHT_BUILD_ID;
#else
  return "fleetright-verifier-unknown-build";
#endif
}

v1::VerificationResult Verify(const OptimizationSnapshot &snapshot, const OptimizationPlan &plan) {
  v1::VerificationResult result;
  const auto index = IndexSnapshot(snapshot);
  const bool catalog_hash_matches = CatalogHashMatches(snapshot.machine_catalog());
  result.set_plan_id(plan.plan_id());
  result.set_accepted(true);
  result.set_verifier_build_id(VerifierBuildId());

  auto plan_copy = plan;
  const std::string plan_id = plan_copy.plan_id();
  plan_copy.clear_plan_id();
  const bool snapshot_fresh =
      snapshot.schema_version() == 1 && snapshot.policy().schema_version() == 1 &&
      snapshot.generation() == plan.base_generation() &&
      snapshot.snapshot_id() == plan.snapshot_id() &&
      snapshot.evaluation_time_ms() >= snapshot.observed_at_ms() &&
      snapshot.expires_at_ms() > snapshot.evaluation_time_ms() &&
      snapshot.policy().headroom_ppm() <= kPpm &&
      snapshot.policy().minimum_improvement_ppm() <= kPpm &&
      snapshot.policy().planning_horizon_seconds() > 0 &&
      snapshot.policy().planning_horizon_seconds() <= 31'536'000 &&
      snapshot.evaluation_time_ms() - snapshot.observed_at_ms() <=
          snapshot.policy().maximum_snapshot_age_ms() &&
      plan.expires_at_ms() == snapshot.expires_at_ms() &&
      plan.created_at_ms() == snapshot.evaluation_time_ms() && SnapshotHashMatches(snapshot);
  AddResult(&result, "snapshot_identity_freshness_and_hash", snapshot_fresh,
            snapshot_fresh ? "snapshot generation, expiry, and content hash match"
                           : "stale, mismatched, or invalid snapshot identity");
  AddResult(&result, "plan_canonical_hash", !plan_id.empty() && MessageSha256(plan_copy) == plan_id,
            "plan identifier is SHA-256 over deterministic protobuf without its ID");
  AddResult(&result, "planner_feasibility_flag", plan.feasible(),
            plan.feasible() ? "planner supplied a feasible candidate" : plan.infeasible_reason());
  AddResult(&result, "plan_binding",
            plan.schema_version() == 1 && !plan.plan_id().empty() &&
                plan.optimizer_build_id().size() > 0,
            "plan schema, ID, and optimizer build are present");

  bool ids_unique = true;
  std::set<std::string> workload_ids;
  std::set<std::string> node_ids;
  for (const auto &workload : snapshot.workloads()) {
    if (workload.workload_id().empty() || !workload_ids.insert(workload.workload_id()).second ||
        workload.min_replicas() > workload.max_replicas() ||
        workload.availability_floor() > workload.max_replicas() ||
        workload.min_topology_domains() > workload.max_replicas() ||
        (workload.min_topology_domains() > 0 && workload.topology_key().empty()) ||
        workload.error_slo_ppm() > kPpm || workload.latency_slo_ms() == 0 ||
        !ResourceNonNegative(workload.current_resources()) ||
        workload.failure_tolerance() == v1::FAILURE_TOLERANCE_UNSPECIFIED ||
        !v1::FailureTolerance_IsValid(static_cast<int>(workload.failure_tolerance())) ||
        !IsPpm(workload.profile_confidence_factor_ppm()) ||
        !IsPpm(workload.model_accuracy_factor_ppm()) ||
        !IsPpm(workload.degraded_cluster_factor_ppm()) || !IsPpm(workload.max_throttle_ppm()) ||
        !IsPpm(workload.max_memory_pressure_ppm()) ||
        !IsPpm(workload.max_scale_down_fraction_ppm()))
      ids_unique = false;
  }
  for (const auto &node : snapshot.nodes()) {
    if (node.node_id().empty() || !node_ids.insert(node.node_id()).second ||
        !ResourceNonNegative(node.allocatable()) || !ResourceNonNegative(node.reserved()) ||
        !v1::NodeHealth_IsValid(node.health()) || node.health() == v1::NODE_HEALTH_UNSPECIFIED) {
      ids_unique = false;
    }
  }
  std::set<std::string> current_replica_ids;
  std::map<std::string, std::uint32_t> snapshot_current_counts;
  for (const auto &placement : snapshot.current_placements()) {
    const std::string key = placement.workload_id() + "\x1f" + placement.replica_id();
    if (placement.workload_id().empty() || placement.replica_id().empty() ||
        !workload_ids.contains(placement.workload_id()) ||
        !node_ids.contains(placement.node_id()) || !current_replica_ids.insert(key).second ||
        snapshot_current_counts[placement.workload_id()] ==
            std::numeric_limits<std::uint32_t>::max()) {
      ids_unique = false;
    } else {
      ++snapshot_current_counts[placement.workload_id()];
    }
  }
  AddResult(&result, "input_ids_units_and_enums", ids_unique,
            ids_unique ? "IDs are unique and canonical numeric/enums are in range"
                       : "duplicate ID, negative unit, or unknown safety enum");

  const bool catalog_valid =
      snapshot.machine_catalog().schema_version() == 1 && catalog_hash_matches &&
      snapshot.machine_catalog().machines_size() > 0 && CatalogMatchesNodes(snapshot);
  AddResult(&result, "machine_catalog_hash", catalog_valid,
            catalog_valid ? "catalog hash and schema match" : "catalog is missing or hash differs");

  std::map<std::string, const v1::WorkloadDecision *> decisions;
  bool decisions_valid = plan.workload_decisions_size() == snapshot.workloads_size();
  for (const auto &decision : plan.workload_decisions()) {
    if (!decisions.emplace(decision.workload_id(), &decision).second ||
        WorkloadById(index, decision.workload_id()) == nullptr)
      decisions_valid = false;
  }
  bool current_counts_bound = decisions_valid;
  for (const auto &workload : snapshot.workloads()) {
    const auto found = decisions.find(workload.workload_id());
    if (found == decisions.end() ||
        found->second->current_replicas() != snapshot_current_counts[workload.workload_id()]) {
      current_counts_bound = false;
    }
  }
  AddResult(&result, "current_replica_snapshot_binding", current_counts_bound,
            current_counts_bound ? "reported current counts match immutable placements"
                                 : "current replica counts do not match snapshot placements");
  std::map<std::string, ProtoResourceVector> shapes;
  bool profiles_valid = current_counts_bound;
  for (const auto &workload : snapshot.workloads()) {
    const auto decision_it = decisions.find(workload.workload_id());
    if (decision_it == decisions.end()) {
      profiles_valid = false;
      continue;
    }
    const auto &decision = *decision_it->second;
    shapes[workload.workload_id()] = decision.proposed_resources();
    const bool has_profile = !decision.capacity_profile_id().empty();
    const bool changed =
        decision.proposed_replicas() != snapshot_current_counts[workload.workload_id()] ||
        decision.proposed_resources().SerializeAsString() !=
            workload.current_resources().SerializeAsString();
    if (!changed && !has_profile) {
      if (!decision.capacity_profile_sha256().empty() || decision.profile_point_index() != 0 ||
          decision.effective_qps_per_replica_milli() != 0 ||
          decision.effective_concurrency_per_replica() != 0 ||
          decision.profile_confidence_ppm() != 0 || decision.uncertainty_exposure_ppm() != kPpm ||
          decision.boundary_risk_ppm() != kPpm)
        profiles_valid = false;
      continue;
    }
    const auto *profile = ProfileById(index, decision.capacity_profile_id());
    if (profile == nullptr || profile->profile_sha256() != decision.capacity_profile_sha256() ||
        !CompatibleProfile(index, *profile, workload, snapshot) ||
        !DecisionEvidenceMatches(decision, *profile, workload) ||
        !ProfileSupports(index, *profile, workload, snapshot, workload.demand_qps_milli(),
                         workload.concurrency_bound(), decision.proposed_replicas()) ||
        decision.proposed_replicas() < RequiredCount(workload, *profile, snapshot) ||
        decision.proposed_resources().SerializeAsString() !=
            profile->allocation().SerializeAsString() ||
        (workload.recent_oom_or_reclaim() &&
         decision.proposed_resources().memory_mib() < workload.current_resources().memory_mib())) {
      profiles_valid = false;
    }
  }
  AddResult(&result, "profile_slo_evidence_and_shape", profiles_valid,
            profiles_valid ? "every changed workload has fresh empirical joint capacity evidence"
                           : "changed workload lacks compatible confidence/SLO/profile evidence");

  std::map<std::string, std::uint32_t> planned_counts;
  std::set<std::string> placement_keys;
  bool placement_ids = true;
  for (const auto &placement : plan.placements()) {
    ++planned_counts[placement.workload_id()];
    if (placement.replica_id().empty() || WorkloadById(index, placement.workload_id()) == nullptr ||
        !placement_keys.insert(placement.workload_id() + "\x1f" + placement.replica_id()).second) {
      placement_ids = false;
    }
  }
  bool counts_ok = current_counts_bound && placement_ids;
  for (const auto &workload : snapshot.workloads()) {
    const auto found = decisions.find(workload.workload_id());
    if (found == decisions.end()) {
      counts_ok = false;
      continue;
    }
    const auto target = found->second->proposed_replicas();
    if (planned_counts[workload.workload_id()] != target || target < workload.min_replicas() ||
        target > workload.max_replicas() || target < workload.availability_floor())
      counts_ok = false;
  }
  AddResult(&result, "replica_ranges_and_single_placements", counts_ok,
            counts_ok ? "each target replica appears once within configured bounds"
                      : "replica count, min/max, or unique target placement failed");

  std::set<std::string> deactivated(plan.deactivated_node_ids().begin(),
                                    plan.deactivated_node_ids().end());
  bool deactivation_ok =
      deactivated.size() == static_cast<std::size_t>(plan.deactivated_node_ids_size());
  for (const auto &id : deactivated) {
    if (NodeById(index, id) == nullptr ||
        std::any_of(plan.placements().begin(), plan.placements().end(),
                    [&](const auto &p) { return p.node_id() == id; }))
      deactivation_ok = false;
  }
  const std::vector<ReplicaPlacement> final_placements(plan.placements().begin(),
                                                       plan.placements().end());
  const bool final_fit = CheckState(snapshot, index, final_placements, shapes, deactivated);
  AddResult(&result, "cpu_memory_network_headroom_topology_taints", final_fit && deactivation_ok,
            final_fit && deactivation_ok
                ? "final placement respects all modeled node dimensions"
                : "capacity, taint, topology, or deactivation check failed");

  std::vector<ReplicaPlacement> intermediate(snapshot.current_placements().begin(),
                                             snapshot.current_placements().end());
  bool sequence_ok = true;
  std::map<std::string, std::string> prior_node;
  for (const auto &p : intermediate)
    prior_node[p.workload_id() + "\x1f" + p.replica_id()] = p.node_id();
  std::uint32_t expected_sequence = 0;
  for (const auto &step : plan.evacuation_steps()) {
    const auto *workload = WorkloadById(index, step.workload_id());
    const auto *destination = NodeById(index, step.destination_node_id());
    const std::string key = step.workload_id() + "\x1f" + step.replica_id();
    const auto source = prior_node.find(key);
    if (step.sequence() != expected_sequence++ || workload == nullptr || destination == nullptr ||
        source == prior_node.end() || source->second != step.source_node_id() ||
        destination->health() != v1::NODE_HEALTH_HEALTHY || !destination->active() ||
        !AllowedTaints(*workload, *destination)) {
      sequence_ok = false;
      break;
    }
    ReplicaPlacement staged;
    staged.set_workload_id(step.workload_id());
    staged.set_replica_id(step.replica_id() + "-surge");
    staged.set_node_id(step.destination_node_id());
    intermediate.push_back(staged);
    auto interim_shapes = shapes;
    interim_shapes[step.workload_id()] =
        step.source_node_id().empty() ? workload->current_resources() : shapes[step.workload_id()];
    if (!CheckState(snapshot, index, intermediate, interim_shapes, {}, true)) {
      sequence_ok = false;
      break;
    }
    intermediate.erase(std::remove_if(intermediate.begin(), intermediate.end(),
                                      [&](const auto &p) {
                                        return p.workload_id() == step.workload_id() &&
                                               p.replica_id() == step.replica_id() &&
                                               p.node_id() == step.source_node_id();
                                      }),
                       intermediate.end());
    for (auto &p : intermediate) {
      if (p.workload_id() == step.workload_id() && p.replica_id() == step.replica_id() + "-surge") {
        p.set_replica_id(step.replica_id());
      }
    }
    prior_node[key] = step.destination_node_id();
    if (!CheckState(snapshot, index, intermediate, shapes, {})) {
      sequence_ok = false;
      break;
    }
  }
  if (sequence_ok) {
    // Validate the moved-to baseline once. Scale-in only removes resource use;
    // per-step count and topology checks below preserve the other invariants.
    sequence_ok = CheckState(snapshot, index, intermediate, shapes, {});
  }
  if (sequence_ok) {
    std::set<std::string> target_keys;
    std::map<std::string, std::uint32_t> wanted_counts;
    std::map<std::string, std::uint32_t> live_counts;
    std::map<std::string, std::map<std::string, std::uint32_t>> domains;
    std::set<std::string> present_keys;
    for (const auto &p : final_placements) {
      target_keys.insert(p.workload_id() + "\x1f" + p.replica_id());
      ++wanted_counts[p.workload_id()];
    }
    for (const auto &p : intermediate) {
      present_keys.insert(p.workload_id() + "\x1f" + p.replica_id());
      ++live_counts[p.workload_id()];
      const auto *workload = WorkloadById(index, p.workload_id());
      const auto *node = NodeById(index, p.node_id());
      if (workload == nullptr || node == nullptr) {
        sequence_ok = false;
        break;
      }
      if (!workload->topology_key().empty()) {
        ++domains[p.workload_id()][GetLabel(*node, workload->topology_key())];
      }
    }
    std::map<std::string, std::vector<std::size_t>> removable_indices;
    for (std::size_t i = 0; i < intermediate.size(); ++i) {
      const auto &p = intermediate[i];
      const std::string key = p.workload_id() + "\x1f" + p.replica_id();
      if (!target_keys.contains(key))
        removable_indices[p.workload_id()].push_back(i);
    }
    const auto topology_valid = [](const WorkloadSpec &workload,
                                   const std::map<std::string, std::uint32_t> &counts) {
      if (workload.topology_key().empty())
        return true;
      if (counts.size() < workload.min_topology_domains())
        return false;
      if (workload.min_topology_domains() == 0)
        return true;
      std::uint32_t minimum = std::numeric_limits<std::uint32_t>::max();
      std::uint32_t maximum = 0;
      for (const auto &[domain, amount] : counts) {
        (void)domain;
        minimum = std::min(minimum, amount);
        maximum = std::max(maximum, amount);
      }
      return maximum - minimum <= workload.max_topology_skew();
    };
    std::vector<bool> removed(intermediate.size(), false);
    for (const auto &[workload_id, wanted] : wanted_counts) {
      auto remaining = live_counts[workload_id] > wanted ? live_counts[workload_id] - wanted : 0U;
      while (sequence_ok && remaining > 0) {
        bool progress = false;
        for (const auto i : removable_indices[workload_id]) {
          const auto &current = intermediate[i];
          const std::string key = current.workload_id() + "\x1f" + current.replica_id();
          if (removed[i] || current.workload_id() != workload_id || target_keys.contains(key)) {
            continue;
          }
          const auto *workload = WorkloadById(index, workload_id);
          const auto *node = NodeById(index, current.node_id());
          if (workload == nullptr || node == nullptr ||
              live_counts[workload_id] <=
                  std::max(workload->min_replicas(), workload->availability_floor())) {
            sequence_ok = false;
            break;
          }
          auto &workload_domains = domains[workload_id];
          const std::string domain = workload->topology_key().empty()
                                         ? std::string()
                                         : GetLabel(*node, workload->topology_key());
          if (!domain.empty()) {
            auto domain_it = workload_domains.find(domain);
            if (domain_it == workload_domains.end()) {
              sequence_ok = false;
              break;
            }
            if (--domain_it->second == 0)
              workload_domains.erase(domain_it);
          }
          if (!topology_valid(*workload, workload_domains)) {
            if (!domain.empty())
              ++workload_domains[domain];
            continue;
          }
          removed[i] = true;
          present_keys.erase(key);
          --live_counts[workload_id];
          --remaining;
          progress = true;
          break;
        }
        if (!progress)
          sequence_ok = false;
      }
    }
    if (sequence_ok) {
      std::vector<ReplicaPlacement> compacted;
      compacted.reserve(intermediate.size());
      for (std::size_t i = 0; i < intermediate.size(); ++i) {
        if (!removed[i])
          compacted.push_back(std::move(intermediate[i]));
      }
      intermediate = std::move(compacted);
    }
    while (sequence_ok && present_keys.size() < target_keys.size()) {
      bool added = false;
      for (const auto &target : final_placements) {
        const std::string key = target.workload_id() + "\x1f" + target.replica_id();
        if (present_keys.contains(key))
          continue;
        const auto *workload = WorkloadById(index, target.workload_id());
        const auto *node = NodeById(index, target.node_id());
        if (workload == nullptr || node == nullptr ||
            live_counts[target.workload_id()] >= workload->max_replicas() ||
            live_counts[target.workload_id()] + 1 <
                std::max(workload->min_replicas(), workload->availability_floor()))
          continue;
        auto next_domains = domains[target.workload_id()];
        if (!workload->topology_key().empty()) {
          ++next_domains[GetLabel(*node, workload->topology_key())];
        }
        if (!topology_valid(*workload, next_domains))
          continue;
        intermediate.push_back(target);
        present_keys.insert(key);
        ++live_counts[target.workload_id()];
        domains[target.workload_id()] = std::move(next_domains);
        added = true;
        break;
      }
      if (!added)
        sequence_ok = false;
    }
  }
  if (sequence_ok && intermediate.size() != final_placements.size())
    sequence_ok = false;
  if (sequence_ok) {
    std::set<std::string> intermediate_set;
    std::set<std::string> final_set;
    for (const auto &p : intermediate)
      intermediate_set.insert(p.workload_id() + "\x1f" + p.replica_id() + "\x1f" + p.node_id());
    for (const auto &p : final_placements)
      final_set.insert(p.workload_id() + "\x1f" + p.replica_id() + "\x1f" + p.node_id());
    sequence_ok = intermediate_set == final_set;
  }
  AddResult(&result, "eviction_recreation_intermediate_states", sequence_ok,
            sequence_ok ? "each move stages a replacement before source eviction"
                        : "evacuation sequence or intermediate state is unsafe");

  std::string n1_detail;
  const bool n1_ok = CheckNMinusOne(snapshot, index, plan, shapes, &n1_detail);
  AddResult(&result, "n_minus_1_serving_and_recovery", n1_ok, n1_detail);

  std::uint64_t actual_actions = 0;
  bool action_arithmetic_ok = true;
  const auto add_action_count = [&](std::uint64_t amount) {
    if (actual_actions > std::numeric_limits<std::uint64_t>::max() - amount) {
      action_arithmetic_ok = false;
      return;
    }
    actual_actions += amount;
  };
  add_action_count(static_cast<std::uint64_t>(plan.evacuation_steps_size()));
  add_action_count(static_cast<std::uint64_t>(plan.deactivated_node_ids_size()));
  std::map<std::string, std::uint64_t> moves_by_workload;
  for (const auto &step : plan.evacuation_steps())
    ++moves_by_workload[step.workload_id()];
  for (const auto &decision : plan.workload_decisions()) {
    const auto current = snapshot_current_counts[decision.workload_id()];
    const auto replica_delta = current > decision.proposed_replicas()
                                   ? current - decision.proposed_replicas()
                                   : decision.proposed_replicas() - current;
    add_action_count(replica_delta);
    const auto *workload = WorkloadById(index, decision.workload_id());
    if (workload == nullptr) {
      action_arithmetic_ok = false;
      continue;
    }
    if (decision.proposed_resources().SerializeAsString() !=
        workload->current_resources().SerializeAsString()) {
      add_action_count(1);
    }
    if (current > decision.proposed_replicas()) {
      const auto removed = current - decision.proposed_replicas();
      const auto limit = DivideUp(
          static_cast<std::uint64_t>(current) * workload->max_scale_down_fraction_ppm(), kPpm);
      if (removed > limit)
        action_arithmetic_ok = false;
    }
    if (moves_by_workload[decision.workload_id()] > workload->max_moves())
      action_arithmetic_ok = false;
  }
  const bool actions_ok = action_arithmetic_ok && actual_actions == plan.action_count() &&
                          actual_actions <= snapshot.policy().maximum_plan_actions();
  AddResult(&result, "action_and_disruption_budgets", actions_ok,
            actions_ok ? "replica, resize, move, and node actions fit policy budgets"
                       : "action count or per-workload disruption budget differs");

  const auto expected_current_cost = CostOfNodes(snapshot, {});
  const auto expected_proposed_cost = CostOfNodes(snapshot, deactivated);
  const bool cost_ok = expected_current_cost && expected_proposed_cost &&
                       plan.current_cost_micros_per_hour() == *expected_current_cost &&
                       plan.proposed_cost_micros_per_hour() == *expected_proposed_cost;
  AddResult(&result, "cost_catalog_consistency", catalog_valid && cost_ok,
            cost_ok ? "hourly cost matches active machine catalog"
                    : "reported cost overflows or does not match node inventory");

  std::optional<VerifiedObjective> objective;
  if (expected_current_cost && expected_proposed_cost)
    objective = IndependentlyScore(
        {snapshot, index, plan, actual_actions, *expected_current_cost, *expected_proposed_cost});
  const bool current_state_safe = CurrentStateHardFeasible(snapshot, index, plan);
  bool improvement_ok = objective.has_value();
  if (objective) {
    improvement_ok = actual_actions == 0
                         ? objective->proposed == objective->current
                         : (!current_state_safe ||
                            VerifiedImprovement({objective->current, objective->proposed,
                                                 snapshot.policy().minimum_improvement_ppm()}));
  }
  const bool objective_ok =
      objective && plan.current_objective_cost_micros() == objective->current &&
      plan.proposed_objective_cost_micros() == objective->proposed &&
      plan.proposed_movement_cost_micros() == objective->movement &&
      plan.proposed_churn_cost_micros() == objective->churn &&
      plan.proposed_uncertainty_cost_micros() == objective->uncertainty &&
      plan.proposed_boundary_cost_micros() == objective->boundary &&
      plan.current_uncertainty_cost_micros() == objective->current_uncertainty &&
      plan.current_boundary_cost_micros() == objective->current_boundary && improvement_ok;
  AddResult(&result, "weighted_objective_and_minimum_improvement", objective_ok,
            objective_ok
                ? "hourly infrastructure cost, movement, churn, uncertainty, and boundary "
                  "risk match the fixed-horizon score"
                : "weighted score, component breakdown, overflow, or required savings differ");

  auto hash_copy = result;
  hash_copy.clear_canonical_result_sha256();
  result.set_canonical_result_sha256(MessageSha256(hash_copy));
  return result;
}

} // namespace fleetright
