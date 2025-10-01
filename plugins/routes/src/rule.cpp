//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors

#include "routes/rule.hpp"

#include "tenzir/tql2/eval.hpp"
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
      result.output = output_name{std::string{*output_str}};
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
  if (result.output.name.empty()) {
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
  result["output"] = output.name;
  result["final"] = final;
  return result;
}

auto rule::evaluate(std::vector<table_slice> slices) const
  -> evaluation_result {
  // TODO: Implement actual rule evaluation logic.
  // This should:
  // 1. Evaluate the 'where' expression against the input slice
  // 2. Return a pair where:
  //    - first: rows that match the rule (should be sent to 'output')
  //    - second: rows that don't match (should be evaluated by next rule)
  // 3. Respect the `final` attribute to stop further evaluation if needed.
  /// TODO: diagnostics?
  auto dh = null_diagnostic_handler{};
  auto res = evaluation_result{};
  res.matched.reserve(slices.size() / 2);
  res.unmatched.reserve(slices.size() / 2);
  for (auto& slice : slices) {
    const auto ms = eval(where, slice, dh);
    auto slice_start = size_t{0};
    auto consume_n
      = [&slice_start, slice](std::vector<table_slice>& target, size_t count) {
          TENZIR_ASSERT(count > 0);
          const auto slice_end = slice_start + count;
          target.emplace_back(subslice(slice, slice_start, slice_end));
          slice_start = slice_end;
        };
    for (auto&& part : ms) {
      auto matches = part.as<bool_type>();
      /// If it is not a boolean expression
      if (not matches) {
        consume_n(res.unmatched, part.length());
        continue;
      }
      /// The truthiness of the current span
      auto current = value_at(bool_type{}, *matches->array, 0);
      /// The length of the current span
      auto current_length = 0;
      for (auto i = int64_t{0}; i < matches->length(); ++i) {
        const auto v = value_at(bool_type{}, *matches->array, i);
        /// If the value has not changed, we can continue
        if (v == current) {
          ++current_length;
          continue;
        }
        /// We consume the current length into the result and reset current
        consume_n(current == true ? res.matched : res.unmatched,
                  current_length);
        current_length = 0;
        current = v;
      }
    }
  }
  return res;
}

} // namespace tenzir::plugins::routes
