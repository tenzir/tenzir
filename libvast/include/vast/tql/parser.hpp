//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/diagnostics.hpp"

namespace vast::tql {

/// Update the operator aliases. This should be called once on startup.
void set_operator_aliases(std::unordered_map<std::string, std::string> map);

/// Parse and set source locations.
auto parse(std::string source, diagnostic_handler& diag)
  -> std::optional<std::vector<located<operator_ptr>>>;

/// No diagnostics will be emitted, no locations will be set.
auto parse_internal(std::string source) -> caf::expected<pipeline>;

/// No locations will be set.
auto make_parser_interface(std::string source, diagnostic_handler& diag)
  -> std::unique_ptr<parser_interface>;

/// Strips the locations to convert the operators to a pipeline.
auto to_pipeline(std::vector<located<operator_ptr>> ops) -> pipeline;

} // namespace vast::tql
