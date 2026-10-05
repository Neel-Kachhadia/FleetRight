#pragma once

#include <string>

#include "fleetright/v1/model.pb.h"
#include "fleetright/v1/optimizer.pb.h"

namespace fleetright {

v1::VerificationResult Verify(const v1::OptimizationSnapshot &snapshot,
                              const v1::OptimizationPlan &plan);
std::string VerifierBuildId();

} // namespace fleetright
