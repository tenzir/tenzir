//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/expression.hpp"

#include <caf/expected.hpp>

/// Utilities to work with [Sigma](https://github.com/Neo23x0/sigma).
namespace vast::plugins::sigma {

/// Parses a *rule* as VAST expression.
/// @param yaml The rule contents.
/// @returns The VAST expression corresponding to *yaml*.
caf::expected<expression> parse_rule(const data& yaml);

/// Parses a *search identifier* as VAST expression.
/// @param yaml The contents converted from YAML.
/// @returns The VAST expression corresponding to *yaml*.
caf::expected<expression> parse_search_id(const data& yaml);

} // namespace vast::plugins::sigma
