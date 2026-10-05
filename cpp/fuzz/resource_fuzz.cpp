#include "fleetright/planner.hpp"
#include "fleetright/units.hpp"
#include "fleetright/v1/model.pb.h"
#include "fleetright/v1/optimizer.pb.h"
#include "fleetright/verifier.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data, std::size_t size) {
  constexpr std::size_t kMaxInputBytes = 64 * 1024;
  if (size > kMaxInputBytes)
    return 0;
  constexpr std::size_t kArithmeticBytes = sizeof(std::int64_t) * 6 + sizeof(std::uint32_t) * 3;
  if (size >= kArithmeticBytes) {
    std::int64_t values[6]{};
    std::uint32_t factors[3]{};
    std::memcpy(values, data, sizeof(values));
    std::memcpy(factors, data + sizeof(values), sizeof(factors));
    const fleetright::ResourceVector left{fleetright::CpuMillicores(values[0]),
                                          fleetright::MemoryMiB(values[1]),
                                          fleetright::NetworkMbps(values[2])};
    const fleetright::ResourceVector right{fleetright::CpuMillicores(values[3]),
                                           fleetright::MemoryMiB(values[4]),
                                           fleetright::NetworkMbps(values[5])};
    const auto sum = fleetright::CheckedAdd(left, right);
    if (sum && fleetright::NonNegative(*sum))
      (void)fleetright::Fits(*sum, *sum);
    (void)fleetright::MultiplyPpmFloor(static_cast<std::uint64_t>(values[0]), factors[0],
                                       factors[1], factors[2]);
    (void)fleetright::CeilDivide(static_cast<std::uint64_t>(values[1]),
                                 static_cast<std::uint64_t>(values[2]));
  }

  fleetright::v1::CapacityProfile profile;
  (void)profile.ParseFromArray(data, static_cast<int>(size));
  fleetright::v1::OptimizationSnapshot snapshot;
  if (snapshot.ParseFromArray(data, static_cast<int>(size))) {
    const auto plan = fleetright::Plan(snapshot);
    (void)fleetright::Verify(snapshot, plan);
  }
  fleetright::v1::OptimizationPlan plan;
  if (plan.ParseFromArray(data, static_cast<int>(size))) {
    fleetright::v1::OptimizationSnapshot candidate;
    if (candidate.ParseFromArray(data, static_cast<int>(size))) {
      (void)fleetright::Verify(candidate, plan);
    }
  }
  return 0;
}
