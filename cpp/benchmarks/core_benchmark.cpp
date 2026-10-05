#include "fleetright/planner.hpp"
#include "fleetright/verifier.hpp"

#include <charconv>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string>
#include <system_error>
#include <vector>

namespace {
constexpr std::streamoff kMaximumArtifactBytes = 64LL * 1024 * 1024;

template <typename Message> bool ReadMessage(const std::string &path, Message *message) {
  std::ifstream input(path, std::ios::binary | std::ios::ate);
  if (!input)
    return false;
  const auto size = input.tellg();
  if (size < 0 || size > kMaximumArtifactBytes)
    return false;
  std::string bytes(static_cast<std::size_t>(size), '\0');
  input.seekg(0);
  if (!bytes.empty())
    input.read(bytes.data(), size);
  return (input.good() || input.eof()) && message->ParseFromString(bytes);
}

double Seconds(std::chrono::steady_clock::duration duration) {
  return std::chrono::duration<double>(duration).count();
}

void PrintSamples(const std::vector<double> &samples) {
  std::cout << '[';
  for (std::size_t i = 0; i < samples.size(); ++i) {
    if (i != 0)
      std::cout << ',';
    std::cout << samples[i];
  }
  std::cout << ']';
}
} // namespace

int main(int argc, char **argv) {
  if (argc != 4) {
    std::cerr << "usage: fleetright-core-benchmark SNAPSHOT.pb PLAN.pb ITERATIONS\n";
    return 64;
  }
  std::size_t iterations = 0;
  const std::string iteration_text(argv[3]);
  const auto [end, error] = std::from_chars(
      iteration_text.data(), iteration_text.data() + iteration_text.size(), iterations);
  if (error != std::errc{} || end != iteration_text.data() + iteration_text.size() ||
      iterations < 20 || iterations > 200) {
    std::cerr << "iterations must be between 20 and 200\n";
    return 64;
  }

  fleetright::v1::OptimizationSnapshot snapshot;
  fleetright::v1::OptimizationPlan plan;
  if (!ReadMessage(argv[1], &snapshot) || !ReadMessage(argv[2], &plan)) {
    std::cerr << "invalid or oversized benchmark protobuf input\n";
    return 65;
  }
  const auto expected_plan = fleetright::Plan(snapshot);
  if (!expected_plan.feasible() || expected_plan.plan_id() != plan.plan_id()) {
    std::cerr << "benchmark plan does not match the deterministic planner result\n";
    return 65;
  }
  const auto expected_verification = fleetright::Verify(snapshot, plan);
  if (!expected_verification.accepted()) {
    std::cerr << "benchmark plan is rejected by the independent verifier\n";
    return 65;
  }

  std::vector<double> planner_seconds;
  std::vector<double> verifier_seconds;
  planner_seconds.reserve(iterations);
  verifier_seconds.reserve(iterations);
  for (std::size_t i = 0; i < iterations; ++i) {
    const auto planner_start = std::chrono::steady_clock::now();
    const auto candidate = fleetright::Plan(snapshot);
    planner_seconds.push_back(Seconds(std::chrono::steady_clock::now() - planner_start));
    if (!candidate.feasible() || candidate.plan_id() != plan.plan_id()) {
      std::cerr << "planner output changed during identical-input benchmark replay\n";
      return 1;
    }

    const auto verifier_start = std::chrono::steady_clock::now();
    const auto verification = fleetright::Verify(snapshot, plan);
    verifier_seconds.push_back(Seconds(std::chrono::steady_clock::now() - verifier_start));
    if (!verification.accepted() ||
        verification.canonical_result_sha256() != expected_verification.canonical_result_sha256()) {
      std::cerr << "verifier output changed during identical-input benchmark replay\n";
      return 1;
    }
  }

  std::cout << std::setprecision(std::numeric_limits<double>::max_digits10)
            << "{\"schema_version\":1,\"iterations\":" << iterations << ",\"planner_build_id\":\""
            << expected_plan.optimizer_build_id() << "\",\"verifier_build_id\":\""
            << expected_verification.verifier_build_id()
            << "\",\"planner_deterministic_replay\":true"
               ",\"verifier_deterministic_replay\":true,\"accepted\":true"
               ",\"planner_seconds\":";
  PrintSamples(planner_seconds);
  std::cout << ",\"verifier_seconds\":";
  PrintSamples(verifier_seconds);
  std::cout << "}\n";
  return 0;
}
