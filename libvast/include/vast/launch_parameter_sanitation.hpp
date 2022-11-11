//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/config_option_set.hpp>
#include "vast/command.hpp"

namespace vast::launch_parameter_sanitation {

void sanitize_missing_arguments(std::vector<std::string>& arguments,
                                const vast::command& cmd);

} // namespace vast::launch_parameter_sanitation
