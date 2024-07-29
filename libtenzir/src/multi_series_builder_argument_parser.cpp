
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
auto multi_series_builder_argument_parser::add_settings_to_parser(
  argument_parser& parser, bool no_unflatten_option) -> void {
  add_schema_only_option(parser, schema_only_);
  parser.add("--raw", raw_);
  if (not no_unflatten_option) {
    parser.add("--unnest-separator", unnest_, "<nested-key-separator>");
  }
}

auto multi_series_builder_argument_parser::add_policy_to_parser(
  argument_parser& parser) -> void {
  parser.add("--merge", merge_);
  parser.add("--schema", schema_, "<schema>");
  parser.add("--selector", selector_, "<selector>");
}
auto multi_series_builder_argument_parser::add_all_to_parser(
  argument_parser& parser) -> void {
  add_policy_to_parser(parser);
  add_settings_to_parser(parser);
}

auto multi_series_builder_argument_parser::add_settings_to_parser(
  argument_parser2& parser, bool no_unflatten_option) -> void {
  add_schema_only_option(parser, schema_only_);
  parser.add("raw", raw_);
  if (not no_unflatten_option) {
    parser.add("unflatten", unnest_);
  }
}

auto multi_series_builder_argument_parser::add_policy_to_parser(
  argument_parser2& parser) -> void {
  parser.add("merge", merge_);
  parser.add("schema", schema_);
  parser.add("selector", selector_);
}
auto multi_series_builder_argument_parser::add_all_to_parser(
  argument_parser2& parser) -> void {
  add_policy_to_parser(parser);
  add_settings_to_parser(parser);
}

auto multi_series_builder_argument_parser::get_settings()
  -> multi_series_builder::settings_type& {
  policy_ = get_policy();
  if ( schema_only_ ) {
    if ( not multi_series_builder::specifies_schema(policy_) ) {
      diagnostic::error("`--raw` requires a schema to be set via `--schema` or `--selector`")
      .primary(*schema_only_)
      .throw_();
    }
  }
  if (unnest_) {
    if (unnest_->inner.empty()) {
      diagnostic::error("unflatten-separator must not be empty")
        .primary(unnest_->source)
        .throw_();
    }
    settings_.unnest_separator = unnest_->inner;
  }
  if (raw_ and schema_ and merge_) {
    // In merging mode, we directly write into a series builder
    // this means that data needs to be parsed to the correct type before
    // writing to the builder however, when calling
    // `field_generator::data_unparsed`, we dont know the schema
    // TODO
    // [ ] technically its only an issue with *known* schemas. For unknown
    // schemas there is no type issue [ ] This could also be resolved by having
    // the merging mode keep track of the type at non-trivial cost
    diagnostic::error("`--merge --schema` and `--raw` are incompatible")
      .primary(*raw_)
      .primary(*schema_)
      .primary(*merge_)
      .throw_();
  }
  settings_.raw = raw_.has_value();
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
    auto [prefix, field_name]
      = parse_selector(selector_->inner, selector_->source);
    policy_ = multi_series_builder::policy_selector{
      std::move(field_name),
      std::move(*prefix),
    };
  } else if (has_schema) {
    policy_
      = multi_series_builder::policy_precise{.seed_schema = schema_->inner};
  } else if (not has_manual_defaults_) {
    policy_ = multi_series_builder::policy_precise{};
  }

  return policy_;
}

auto multi_series_builder_options::get_schemas() const
  -> std::vector<tenzir::type> {
  auto res = detail::multi_series_builder::get_schemas_unnested(
    multi_series_builder::specifies_schema(policy),
    not settings.unnest_separator.empty());

  if (settings.schema_only) {
    if (auto p = std::get_if<multi_series_builder::policy_precise>(&policy);
        p and p->seed_schema) {
      for (const auto& s : res) {
        if (s.name() == p->seed_schema) {
          return res;
        }
      }
      diagnostic::error("no known schema for `--no-infer`")
        .note("schema `{}` was specified, but no schema by that name could be "
              "found",
              p->seed_schema)
        .throw_();
    }
  }

  return res;
}
} // namespace tenzir