//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include <string>

namespace vast::system {

/// Attempts to parse `query` as (::expression, ::pipeline).
caf::expected<std::pair<expression, std::optional<pipeline>>>
parse_query(const std::string& query);

/// Attempts to parse `[begin, end)` as (::expression, ::pipeline).
caf::expected<std::pair<expression, std::optional<pipeline>>>
parse_query(std::vector<std::string>::const_iterator begin,
            std::vector<std::string>::const_iterator end);

caf::expected<std::pair<expression, std::optional<pipeline>>>
parse_query(const std::vector<std::string>& args);

caf::expected<std::pair<expression, std::optional<pipeline>>>
parse_query(const spawn_arguments& args);

} // namespace vast::system
