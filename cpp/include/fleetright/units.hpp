#pragma once

#include <cstdint>
#include <limits>
#include <optional>

namespace fleetright {

template <typename Tag, typename Rep> struct Quantity {
  Rep value = 0;
  explicit constexpr Quantity(Rep raw = 0) : value(raw) {}
};

struct CpuTag;
struct MemoryTag;
struct NetworkTag;
struct RequestRateTag;
struct ConcurrencyTag;
using CpuMillicores = Quantity<CpuTag, std::int64_t>;
using MemoryMiB = Quantity<MemoryTag, std::int64_t>;
using NetworkMbps = Quantity<NetworkTag, std::int64_t>;
using MilliRequestsPerSecond = Quantity<RequestRateTag, std::uint64_t>;
using Concurrency = Quantity<ConcurrencyTag, std::uint64_t>;

struct ResourceVector {
  CpuMillicores cpu;
  MemoryMiB memory;
  NetworkMbps network;
};

inline bool NonNegative(const ResourceVector &value) {
  return value.cpu.value >= 0 && value.memory.value >= 0 && value.network.value >= 0;
}

inline std::optional<ResourceVector> CheckedAdd(const ResourceVector &left,
                                                const ResourceVector &right) {
  if (!NonNegative(left) || !NonNegative(right) ||
      left.cpu.value > std::numeric_limits<std::int64_t>::max() - right.cpu.value ||
      left.memory.value > std::numeric_limits<std::int64_t>::max() - right.memory.value ||
      left.network.value > std::numeric_limits<std::int64_t>::max() - right.network.value) {
    return std::nullopt;
  }
  return ResourceVector{CpuMillicores(left.cpu.value + right.cpu.value),
                        MemoryMiB(left.memory.value + right.memory.value),
                        NetworkMbps(left.network.value + right.network.value)};
}

inline bool Fits(const ResourceVector &used, const ResourceVector &capacity) {
  return NonNegative(used) && NonNegative(capacity) && used.cpu.value <= capacity.cpu.value &&
         used.memory.value <= capacity.memory.value && used.network.value <= capacity.network.value;
}

inline std::uint64_t CeilDivide(std::uint64_t numerator, std::uint64_t denominator) {
  return denominator == 0 ? std::numeric_limits<std::uint64_t>::max()
                          : numerator / denominator + (numerator % denominator != 0 ? 1U : 0U);
}

// PPM factors are multiplied before one downward rounding, so no intermediate
// round-up can manufacture capacity.
inline std::optional<std::uint64_t> MultiplyPpmFloor(std::uint64_t value, std::uint32_t first,
                                                     std::uint32_t second, std::uint32_t third) {
  if (first == 0 || second == 0 || third == 0 || first > 1'000'000U || second > 1'000'000U ||
      third > 1'000'000U) {
    return std::nullopt;
  }
  __extension__ using UInt128 = unsigned __int128;
  const UInt128 product = static_cast<UInt128>(value) * first * second * third;
  const UInt128 result = product / (static_cast<UInt128>(1'000'000U) * 1'000'000U * 1'000'000U);
  if (result > std::numeric_limits<std::uint64_t>::max())
    return std::nullopt;
  return static_cast<std::uint64_t>(result);
}

} // namespace fleetright
