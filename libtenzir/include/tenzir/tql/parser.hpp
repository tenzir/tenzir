//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/diagnostics.hpp"
#include "tenzir/pipeline.hpp"

namespace tenzir::tql {

/// Update the operator aliases. This should be called once on startup.
void set_operator_aliases(std::unordered_map<std::string, std::string> map);

/// Parse and set source locations.
auto parse(std::string source, diagnostic_handler& diag)
  -> std::optional<std::vector<located<operator_ptr>>>;

/// No diagnostics will be emitted, no locations will be set.
auto parse_internal(std::string source) -> caf::expected<pipeline>;

/// Parse a pipeline (without locations) and also return the diagnostics.
auto parse_internal_with_diags(std::string source)
  -> std::pair<std::optional<pipeline>, std::vector<diagnostic>>;

/// No locations will be set.
auto make_parser_interface(std::string source, diagnostic_handler& diag)
  -> std::unique_ptr<parser_interface>;

/// Strips the locations to convert the operators to a pipeline.
auto to_pipeline(std::vector<located<operator_ptr>> ops) -> pipeline;

} // namespace tenzir::tql
