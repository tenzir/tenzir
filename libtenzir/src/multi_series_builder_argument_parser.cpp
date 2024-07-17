
//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/multi_series_builder_argument_parser.hpp"

#include "tenzir/location.hpp"

namespace tenzir {
namespace {
struct selector {
  std::optional<std::string> prefix;
  std::string field_name;
};

auto parse_selector(std::string_view x, location source) -> selector {
  if (x.empty()) {
    diagnostic::error("selector must not be empty").primary(source).throw_();
  }
  auto split = detail::split(x, ":");
  TENZIR_ASSERT(not x.empty());
  if (split.size() > 2 or split[0].empty()) {
    diagnostic::error("invalid selector `{}`: must contain at most "
                      "one `:` and field name must not be empty",
                      x)
      .primary(source)
      .throw_();
  }
  return selector{
    split.size() == 2 ? std::optional{std::string(std::move(split[1]))}
                      : std::nullopt,
    std::string(split[0]),
  };
}
} // namespace

void add_schema_only_option(argument_parser& parser,
                            std::optional<location>& schema_only) {
  parser.add("--no-infer", schema_only);
}
void add_schema_only_option(argument_parser2& parser,
                            std::optional<location>& schema_only) {
  parser.add("no_extra_fields", schema_only);
}

auto multi_series_builder_argument_parser::add_to_parser(
  argument_parser& parser) -> void {
  add_schema_only_option(parser, schema_only_);
  parser.add("--merge", merge_);
  parser.add("--schema", schema_, "<schema>");
  parser.add("--selector", selector_, "<selector>");
}

auto multi_series_builder_argument_parser::add_to_parser(
  argument_parser2& parser) -> void {
  add_schema_only_option(parser, schema_only_);
  parser.add("merge", merge_);
  parser.add("schema", schema_);
  parser.add("selector", selector_);
}

auto multi_series_builder_argument_parser::get_settings()
  -> multi_series_builder::settings_type& {
  if (schema_only_ and not(selector_ or schema_)) {
    diagnostic::error("`--no-infer` requires either `--schema` or `--selector`")
      .primary(*schema_only_)
      .throw_();
  }
  settings_.schema_only = schema_only_.has_value();
  return settings_;
}

auto multi_series_builder_argument_parser::get_policy()
  -> multi_series_builder::policy_type& {
  bool has_merge = false;
  bool has_schema = false;
  bool has_selector = false;
  // policy detection
  if (merge_) {
    has_merge = true;
  }
  if (schema_) {
    has_schema = true;
  }
  if (selector_) {
    has_selector = true;
  }

  if (has_schema and has_selector) {
    diagnostic::error("`--schema` and `--selector` cannot be combined")
      .primary(schema_->source)
      .primary(selector_->source)
      .throw_();
  }
  if (has_merge and has_selector) {
    diagnostic::error("`--merge` and `--selector` cannot be combined")
      .primary(*merge_)
      .secondary(selector_->source)
      .throw_();
  }
  if (has_merge) {
    if (has_schema) {
      settings_.default_name = schema_->inner;
      policy_
        = multi_series_builder::policy_merge{.seed_schema = schema_->inner};
    } else {
      policy_ = multi_series_builder::policy_merge{};
    }
  } else if (has_selector) {
    policy_ = multi_series_builder::policy_selector{};
  } else if (has_schema) {
    policy_
      = multi_series_builder::policy_precise{.seed_schema = schema_->inner};
  } else {
    policy_ = multi_series_builder::policy_precise{};
  }
  if (auto pol = std::get_if<multi_series_builder::policy_selector>(&policy_)) {
    auto [prefix, field_name]
      = parse_selector(selector_->inner, selector_->source);
    pol->field_name = std::move(field_name);
    pol->naming_prefix = std::move(*prefix);
  }
  return policy_;
}

auto common_parser_options_parser::add_to_parser(argument_parser& parser)
  -> void {
  parser.add("--raw", raw_);
  parser.add("--unnest-separator", unnest_, "<nested-key-separator>");
}

auto common_parser_options_parser::add_to_parser(argument_parser2& parser)
  -> void {
  parser.add("raw", raw_);
  parser.add("unflatten", unnest_);
}
} // namespace tenzir