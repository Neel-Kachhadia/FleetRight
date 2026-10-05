#include "fleetright/planner.hpp"

#include <fstream>
#include <iostream>
#include <limits>
#include <string>

namespace {
constexpr std::streamoff kMaximumArtifactBytes = 64 * 1024 * 1024;

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
  return input.good() || input.eof() ? message->ParseFromString(bytes) : false;
}

template <typename Message> bool WriteMessage(const std::string &path, const Message &message) {
  std::string bytes;
  if (!message.SerializeToString(&bytes) ||
      bytes.size() > static_cast<std::size_t>(kMaximumArtifactBytes)) {
    return false;
  }
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  return output.good();
}
} // namespace

int main(int argc, char **argv) {
  if (argc != 3) {
    std::cerr << "usage: fleetright-plan SNAPSHOT.pb PLAN.pb\n";
    return 64;
  }
  fleetright::v1::OptimizationSnapshot snapshot;
  if (!ReadMessage(argv[1], &snapshot)) {
    std::cerr << "invalid or oversized OptimizationSnapshot protobuf\n";
    return 65;
  }
  const auto plan = fleetright::Plan(snapshot);
  if (!WriteMessage(argv[2], plan)) {
    std::cerr << "could not write OptimizationPlan protobuf\n";
    return 74;
  }
  std::cout << "plan_id=" << plan.plan_id() << " feasible=" << (plan.feasible() ? "true" : "false")
            << " placements=" << plan.placements_size() << " actions=" << plan.action_count()
            << '\n';
  return 0;
}
