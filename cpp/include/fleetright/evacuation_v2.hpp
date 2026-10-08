#pragma once

#include <string>

#include "fleetright/v2/evacuation.pb.h"

namespace fleetright {

v2::EvacuationPlan PlanEvacuationV2(const v2::EvacuationSnapshot &snapshot);
v2::VerificationResult VerifyEvacuationV2(const v2::EvacuationSnapshot &snapshot,
                                          const v2::EvacuationPlan &plan);
std::string EvacuationVerifierBuildId();

} // namespace fleetright
