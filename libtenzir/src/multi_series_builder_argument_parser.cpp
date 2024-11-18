
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
auto parse_selector(std::string_view x, location source, diagnostic_handler& dh)
  -> std::optional<multi_series_builder::policy_selector> {
  if (x.empty()) {
    diagnostic::error("selector must not be empty").primary(source).emit(dh);
    return std::nullopt;
  }
  auto split = detail::split(x, ":");
  TENZIR_ASSERT(not x.empty());
  if (split.size() > 2 or split[0].empty()) {
    diagnostic::error("invalid selector `{}`: must contain at most "
                      "one `:` and field name must not be empty",
                      x)
      .primary(source)
      .emit(dh);
    return std::nullopt;
  }
  if (split.size() == 2) {
    return multi_series_builder::policy_selector{
      std::string{split[0]},
      std::string{split[1]},
    };
  } else {
    return multi_series_builder::policy_selector{
      std::string{split[0]},
    };
  }
}
} // namespace

auto multi_series_builder_argument_parser::add_settings_to_parser(
  argument_parser& parser, bool add_unflatten_option,
  bool add_merge_option) -> void {
  is_tql1_ = true;
  parser.add("--schema-only", schema_only_);
  if (add_merge_option) {
    parser.add("--merge", merge_);
  }
  parser.add("--raw", raw_);
  if (add_unflatten_option) {
    parser.add("--unnest-separator", unnest_, "<nested-key-separator>");
  }
}

auto multi_series_builder_argument_parser::add_policy_to_parser(
  argument_parser& parser) -> void {
  is_tql1_ = true;
  parser.add("--schema", schema_, "<schema>");
  parser.add("--selector", selector_, "<selector>");
}

auto multi_series_builder_argument_parser::add_all_to_parser(
  argument_parser& parser) -> void {
  add_policy_to_parser(parser);
  add_settings_to_parser(parser);
}

auto multi_series_builder_argument_parser::add_settings_to_parser(
  argument_parser2& parser, bool add_unflatten_option,
  bool add_merge_option) -> void {
  parser.add("schema_only", schema_only_);
  if (add_merge_option) {
    parser.add("merge", merge_);
  }
  parser.add("raw", raw_);
  if (add_unflatten_option) {
    parser.add("unflatten", unnest_);
  }
  parser.add("_timeout", timeout_);
  parser.add("_batch_size", batch_size_);
}

auto multi_series_builder_argument_parser::add_policy_to_parser(
  argument_parser2& parser) -> void {
  parser.add("schema", schema_);
  parser.add("selector", selector_);
}

auto multi_series_builder_argument_parser::add_all_to_parser(
  argument_parser2& parser) -> void {
  add_policy_to_parser(parser);
  add_settings_to_parser(parser);
}

auto multi_series_builder_argument_parser::get_settings(diagnostic_handler& dh)
  -> bool {
  (void)get_policy(dh); // force update policy.
  settings_.schema_only |= schema_only_.has_value();
  if (settings_.schema_only
      and std::holds_alternative<multi_series_builder::policy_default>(
        policy_)) {
    // This error message is worded to support cases where the `merge` policy
    // was defaulted by the parser
    diagnostic::error("`--schema-only` requires a `--schema` or `--selector`")
      .primary(*schema_only_)
      .emit(dh);
    return false;
  }
  // This is intentionally a `|=`, because the parser may have a default that
  // enables merging and we dont wont to accidentally disable it
  settings_.merge |= merge_.has_value();
  if (auto* p = std::get_if<multi_series_builder::policy_schema>(&policy_)) {
    if (p->seed_schema.empty() and settings_.schema_only) {
      diagnostic::error(
        "`--schema-only` requires a valid `--schema` or `--selector`")
        .primary(*schema_only_)
        .emit(dh);
      return false;
    }
    if (not p->seed_schema.empty()) {
      const auto schemas = modules::schemas();

      auto it
        = std::find_if(schemas.begin(), schemas.end(), [p](const auto& t) {
            return t.name() == p->seed_schema;
          });
      if (it == schemas.end()) {
        if (settings_.schema_only) {
          diagnostic::error("schema `{}` does not exist, but `--schema-only` "
                            "was specified",
                            schema_->inner)
            .primary(*schema_only_)
            .primary(*schema_)
            .emit(dh);
          return false;
        } else {
          diagnostic::warning("schema `{}` does not exist", schema_->inner)
            .primary(*schema_)
            .hint("if you know the input's shape, define the schema")
            .emit(dh);
        }
      }
    }
  }
  if (unnest_) {
    if (unnest_->inner.empty()) {
      diagnostic::error("unflatten-separator must not be empty")
        .primary(unnest_->source)
        .emit(dh);
      return false;
    }
    settings_.unnest_separator = unnest_->inner;
  }
  settings_.raw = raw_.has_value();
  if (timeout_.has_value()) {
    settings_.timeout = *timeout_;
  }
  if (batch_size_.has_value()) {
    settings_.desired_batch_size = *batch_size_;
  }
  return true;
}

auto multi_series_builder_argument_parser::get_policy(diagnostic_handler& dh)
  -> bool {
  bool has_schema = false;
  bool has_selector = false;
  // policy detection
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
      .emit(dh);
    return false;
  }
  std::string seed_type;
  if (has_schema) {
    if (schema_->inner.empty()) {
      diagnostic::error("`--schema` must not be empty")
        .primary(schema_->source)
        .emit(dh);
      return false;
    }
    seed_type = schema_->inner;
  }
  if (has_selector) {
    auto p = parse_selector(selector_->inner, selector_->source, dh);
    if (not p) {
      return false;
    }
    policy_ = std::move(*p);
  } else if (has_schema) {
    // this needs an extra guard for "has_schema", because it could otherwise be
    // resetting a non-empty default seed.
    // the same issue for merge is already handled above
    policy_ = multi_series_builder::policy_schema{
      .seed_schema = seed_type,
    };
  }
  return true;
}
} // namespace tenzir
