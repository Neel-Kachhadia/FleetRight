#pragma once

#include <string>

#include "fleetright/v1/model.pb.h"
#include "fleetright/v1/optimizer.pb.h"

namespace fleetright {

v1::OptimizationPlan Plan(const v1::OptimizationSnapshot &snapshot);
std::string OptimizerBuildId();

} // namespace fleetright
