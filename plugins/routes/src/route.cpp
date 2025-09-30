//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors

#include "routes/route.hpp"

#include <tenzir/diagnostics.hpp>
#include <tenzir/view.hpp>

namespace tenzir::plugins::routes {

auto route::make(const view<record>& data, session ctx) -> failure_or<route> {
  auto result = route{};
  auto has_errors = false;
  for (const auto& [key, value] : data) {
    if (key == "input") {
      const auto* input_str = try_as<std::string_view>(&value);
      if (not input_str) {
        diagnostic::error("input must be a string")
          .note("invalid route definition")
          .emit(ctx);
        has_errors = true;
        continue;
      }
      result.input = output{std::string{*input_str}};
      continue;
    }
    if (key == "rules") {
      const auto* rules_list = try_as<view<list>>(&value);
      if (not rules_list) {
        diagnostic::error("rules must be a list")
          .note("invalid route definition")
          .emit(ctx);
        has_errors = true;
        continue;
      }
      for (const auto& rule_value : *rules_list) {
        const auto* rule_record = try_as<view<record>>(&rule_value);
        if (not rule_record) {
          diagnostic::error("rule entries must be records")
            .note("invalid route definition")
            .emit(ctx);
          has_errors = true;
          continue;
        }
        auto parsed_rule = rule::make(*rule_record, ctx);
        if (not parsed_rule) {
          has_errors = true;
        } else {
          result.rules.push_back(*parsed_rule);
        }
      }
      continue;
    }
    diagnostic::error("unknown field '{}'", key)
      .note("valid fields are: 'input', 'rules'")
      .note("invalid route definition")
      .emit(ctx);
    has_errors = true;
  }
  if (result.input.name.empty()) {
    diagnostic::error("missing required field 'input'")
      .note("invalid route definition")
      .emit(ctx);
    has_errors = true;
  }
  if (result.rules.empty()) {
    diagnostic::error("missing required field 'rules' or rules list is empty")
      .note("invalid route definition")
      .emit(ctx);
    has_errors = true;
  }
  if (has_errors) {
    return failure::promise();
  }
  return result;
}

auto route::to_record() const -> record {
  auto result = record{};
  result["input"] = input.name;
  auto rules_list = list{};
  rules_list.reserve(rules.size());
  for (const auto& rule : rules) {
    rules_list.emplace_back(rule.to_record());
  }
  result["rules"] = std::move(rules_list);
  return result;
}

} // namespace tenzir::plugins::routes
