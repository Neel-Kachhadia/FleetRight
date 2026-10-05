#include "fleetright/verifier.hpp"

#include <fstream>
#include <iostream>
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
  if (argc != 4) {
    std::cerr << "usage: fleetright-verify SNAPSHOT.pb PLAN.pb RESULT.pb\n";
    return 64;
  }
  fleetright::v1::OptimizationSnapshot snapshot;
  fleetright::v1::OptimizationPlan plan;
  if (!ReadMessage(argv[1], &snapshot) || !ReadMessage(argv[2], &plan)) {
    std::cerr << "invalid or oversized verifier protobuf input\n";
    return 65;
  }
  const auto result = fleetright::Verify(snapshot, plan);
  if (!WriteMessage(argv[3], result)) {
    std::cerr << "could not write VerificationResult protobuf\n";
    return 74;
  }
  for (const auto &invariant : result.invariants()) {
    if (!invariant.passed())
      std::cerr << invariant.invariant() << ": " << invariant.detail() << '\n';
  }
  std::cout << "plan_id=" << result.plan_id()
            << " accepted=" << (result.accepted() ? "true" : "false")
            << " result_sha256=" << result.canonical_result_sha256() << '\n';
  return 0;
}
