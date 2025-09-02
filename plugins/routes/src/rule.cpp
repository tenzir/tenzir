//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors

#include "routes/rule.hpp"
#include "tenzir/tql2/parser.hpp"

#include <tenzir/diagnostics.hpp>
#include <tenzir/view.hpp>
#include <utility>

namespace tenzir::plugins::routes {

auto rule::make(const view<record>& data, session ctx) -> failure_or<rule> {
  auto result = rule{};
  auto has_errors = false;
  for (const auto& [key, value] : data) {
    if (key == "where") {
      const auto* where_str = try_as<std::string_view>(&value);
      if (not where_str) {
        diagnostic::error("where must be a string")
          .note("invalid rule definition")
          .emit(ctx);
        has_errors = true;
        continue;
      }
      auto where_expr = parse_expression_with_bad_diagnostics(*where_str, ctx);
      if (not where_expr) {
        has_errors = true;
        continue;
      }
      result.where = std::move(*where_expr);
      result.where_str = std::string{*where_str};
      continue;
    }
    if (key == "output") {
      const auto* output_str = try_as<std::string_view>(&value);
      if (not output_str) {
        diagnostic::error("output must be a string")
          .note("invalid rule definition")
          .emit(ctx);
        has_errors = true;
        continue;
      }
      result.output = std::string{*output_str};
      continue;
    }
    if (key == "final") {
      const auto* final_bool = try_as<view<bool>>(&value);
      if (not final_bool) {
        diagnostic::error("final must be a boolean")
          .note("invalid rule definition")
          .emit(ctx);
        has_errors = true;
        continue;
      }
      result.final = *final_bool;
      continue;
    }
    diagnostic::error("unknown field '{}'", key)
      .note("valid fields are: 'where', 'output', 'final'")
      .note("invalid rule definition")
      .emit(ctx);
    has_errors = true;
  }
  if (result.output.empty()) {
    diagnostic::error("missing required field 'output'")
      .note("invalid rule definition")
      .emit(ctx);
    has_errors = true;
  }
  if (has_errors) {
    return failure::promise();
  }
  return result;
}

auto rule::to_record() const -> record {
  auto result = record{};
  result["where"] = where_str;
  result["output"] = output;
  result["final"] = final;
  return result;
}

auto rule::evaluate(const table_slice& slice) const -> std::pair<table_slice, table_slice> {
  // TODO: Implement actual rule evaluation logic.
  // This should:
  // 1. Evaluate the 'where' expression against the input slice
  // 2. Return a pair where:
  //    - first: rows that match the rule (should be sent to 'output')
  //    - second: rows that don't match (should be evaluated by next rule)
  // 3. Respect the `final` attribute to stop further evaluation if needed.
  return std::make_pair(table_slice{}, table_slice{});
}

} // namespace tenzir::plugins::routes
