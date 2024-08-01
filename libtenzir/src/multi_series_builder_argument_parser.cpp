
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

auto multi_series_builder_argument_parser::add_settings_to_parser(
  argument_parser& parser, bool no_unflatten_option) -> void {
  is_tql1_ = true;
  parser.add("--expand-schema", expand_schema_);
  parser.add("--raw", raw_);
  if (not no_unflatten_option) {
    parser.add("--unnest-separator", unnest_, "<nested-key-separator>");
  }
}

auto multi_series_builder_argument_parser::add_policy_to_parser(
  argument_parser& parser) -> void {
  is_tql1_ = true;
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
  parser.add("expand_schema", expand_schema_);
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
  (void)get_policy(); // force update policy.
  settings_.expand_schema |= expand_schema_.has_value();
  // if a schema is set and expand_schema
  if (auto* p = std::get_if<multi_series_builder::policy_precise>(&policy_); p and not p->seed_schema.empty()) {
    const auto schemas = modules::schemas();

    auto it = std::find_if(schemas.begin(), schemas.end(), [p](const auto& t) {
      return t.name() == p->seed_schema;
    });
    if (it == schemas.end()) {
      if (settings_.expand_schema) {
        diagnostic::error(
          "`--expand-schema` specified, but given `--schema` does not exist")
          .primary(*expand_schema_)
          .primary(*schema_)
          .note("schema `{}` could not be found", schema_->inner)
          .throw_();
      } else {
        diagnostic::warning("Given `--schema` does not exist" )
          .primary(*schema_)
          .note("schema `{}` could not be found", schema_->inner)
          .hint("Consider defining the schema if you know the input's shape")
          .throw_();
      }
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
  std::string seed_type;
  if (has_schema) {
    if (schema_->inner.empty()) {
      diagnostic::error("`--schema` must not be empty")
        .primary(schema_->source)
        .throw_();
    }
    seed_type = schema_->inner;
  }
  if (has_merge) {
    policy_ = multi_series_builder::policy_merge{
      .seed_schema = seed_type,
    };
  } else if (has_selector) {
    auto [prefix, field_name]
      = parse_selector(selector_->inner, selector_->source);
    policy_ = multi_series_builder::policy_selector{
      std::move(field_name),
      std::move(*prefix),
    };
  } else if (has_schema) {
    // this needs an extra guard for "has_schema", because it could otherwise be
    // resetting a non-empty default seed merge is already handled above
    policy_ = multi_series_builder::policy_precise{
      .seed_schema = seed_type,
    };
  }

  return policy_;
}

auto multi_series_builder_options::get_schemas() const
  -> std::vector<tenzir::type> {
  if (std::holds_alternative<multi_series_builder::policy_selector>(policy)) {
    return modules::schemas();
  }
  return {};
}
} // namespace tenzir