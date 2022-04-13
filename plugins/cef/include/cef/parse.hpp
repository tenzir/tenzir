//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include <caf/expected.hpp>

#include <string>

namespace vast::plugins::cef {

/// Tokenizes a line of ASCII as CEF event.
caf::expected<std::vector<std::string_view>> tokenize(std::string_view line);

} // namespace vast::plugins::cef
