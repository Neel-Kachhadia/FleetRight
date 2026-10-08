#pragma once

#include "fleetright/hash.hpp"
#include "fleetright/v2/evacuation.pb.h"

#include <google/protobuf/message.h>

namespace fleetright {

inline std::string EvacuationSnapshotSha256(v2::EvacuationSnapshot snapshot) {
  snapshot.clear_content_sha256();
  return Sha256("fleetright.v2.EvacuationSnapshot:2:" + DeterministicBytes(snapshot));
}

inline std::string EvacuationPlanSha256(v2::EvacuationPlan plan) {
  plan.clear_plan_id();
  return Sha256("fleetright.v2.EvacuationPlan:2:" + DeterministicBytes(plan));
}

inline std::string EvacuationResultSha256(v2::VerificationResult result) {
  result.clear_canonical_result_sha256();
  return Sha256("fleetright.v2.VerificationResult:2:" + DeterministicBytes(result));
}

inline bool HasUnknownFieldsDeep(const google::protobuf::Message &message) {
  const auto *reflection = message.GetReflection();
  if (!reflection->GetUnknownFields(message).empty())
    return true;
  const auto *descriptor = message.GetDescriptor();
  for (int i = 0; i < descriptor->field_count(); ++i) {
    const auto *field = descriptor->field(i);
    if (field->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE)
      continue;
    if (field->is_repeated()) {
      const int count = reflection->FieldSize(message, field);
      for (int j = 0; j < count; ++j) {
        if (HasUnknownFieldsDeep(reflection->GetRepeatedMessage(message, field, j)))
          return true;
      }
    } else if (reflection->HasField(message, field) &&
               HasUnknownFieldsDeep(reflection->GetMessage(message, field))) {
      return true;
    }
  }
  return false;
}

} // namespace fleetright
