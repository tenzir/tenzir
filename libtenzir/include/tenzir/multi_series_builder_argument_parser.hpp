//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/argument_parser.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/multi_series_builder.hpp"

#include <tenzir/error.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>

#include "argument_parser2.hpp"

namespace tenzir {

/// Parses a selector string like "field" or "field:prefix" into a
/// policy_selector. Returns nullopt if the format is invalid.
auto parse_selector_value(std::string_view x)
  -> std::optional<multi_series_builder::policy_selector>;

/// simple utility to parse the command line arguments for a
/// multi_series_builder's settings and policy
struct multi_series_builder_argument_parser {
public:
  enum class merge_option {
    no,
    hidden,
    yes,
  };

  multi_series_builder_argument_parser() = default;
  multi_series_builder_argument_parser(
    multi_series_builder::settings_type settings,
    multi_series_builder::policy_type policy)
    : has_manual_defaults_{true},
      settings_{std::move(settings)},
      policy_{std::move(policy)} {
  }

  auto add_settings_to_parser(argument_parser& parser,
                              bool add_unflatten_option = true,
                              bool add_merge_option = true) -> void;
  auto add_policy_to_parser(argument_parser& parser) -> void;
  auto add_all_to_parser(argument_parser& parser) -> void;
  auto add_settings_to_parser(argument_parser2& parser,
                              bool add_unflatten_option = true,
                              merge_option add_merge_option = merge_option::yes)
    -> void;
  auto add_policy_to_parser(argument_parser2& parser) -> void;
  auto add_all_to_parser(argument_parser2& parser) -> void;

  auto get_options(diagnostic_handler& dh)
    -> failure_or<multi_series_builder::options> {
    auto good = get_policy(dh);
    good &= get_settings(dh);
    if (good) {
      return multi_series_builder::options{
        .policy = policy_,
        .settings = settings_,
      };
    } else {
      return failure::promise();
    }
  }

private:
  auto get_settings(diagnostic_handler& dh) -> bool;
  auto get_policy(diagnostic_handler& dh) -> bool;

  // If we leave these public, the json parser can keep supporting its old
  // options by checking/setting values here
  // this is only relevant to tql1
public:
  bool has_manual_defaults_ = false;
  bool is_tql1_ = false;
  multi_series_builder::settings_type settings_ = {};
  multi_series_builder::policy_type policy_
    = multi_series_builder::policy_default{};

  // Policy schema
  std::optional<located<std::string>> schema_;

  // Policy selector
  std::optional<located<std::string>> selector_;

  // settings
  std::optional<location> merge_;
  std::optional<location> schema_only_;
  std::optional<located<std::string>> unnest_;
  std::optional<location> raw_;
  std::optional<duration> timeout_;
  std::optional<uint64_t> batch_size_;
};

// -- Describer integration ----------------------------------------------------

using merge_option = multi_series_builder_argument_parser::merge_option;

/// Options for controlling which MSB arguments are added to a Describer.
struct msb_describer_options {
  merge_option merge = merge_option::yes;
  bool add_schema = true;
  bool add_selector = true;
  bool add_unflatten = true;
  bool schema_only_requires_schema_or_selector = true;
};

/// Returned by add_msb_to_describer. Stores Argument handles internally
/// and is callable with a DescribeCtx to perform validation.
template <class Args>
struct msb_validator {
  Argument<Args, std::string> schema;
  Argument<Args, std::string> selector;
  Argument<Args, bool> schema_only;
  Argument<Args, bool> merge;
  Argument<Args, bool> raw;
  Argument<Args, std::string> unflatten_separator;
  Argument<Args, duration> batch_timeout;
  Argument<Args, uint64_t> batch_size;
  bool schema_only_requires_schema_or_selector = true;

  auto operator()(DescribeCtx& ctx) const -> Empty {
    auto schema_val = ctx.get(schema);
    auto schema_loc = ctx.get_location(schema);
    auto selector_val = ctx.get(selector);
    auto selector_loc = ctx.get_location(selector);
    auto schema_only_val = ctx.get(schema_only).value_or(false);
    auto schema_only_loc = ctx.get_location(schema_only);
    auto unflatten_val = ctx.get(unflatten_separator);
    auto unflatten_loc = ctx.get_location(unflatten_separator);
    if (schema_val and selector_val) {
      diagnostic::error("`schema` and `selector` cannot be combined")
        .primary(schema_loc.value_or(location::unknown))
        .primary(selector_loc.value_or(location::unknown))
        .emit(ctx);
    }
    if (schema_val and schema_val->empty()) {
      diagnostic::error("`schema` must not be empty")
        .primary(schema_loc.value_or(location::unknown))
        .emit(ctx);
    }
    if (selector_val) {
      if (selector_val->empty()) {
        diagnostic::error("selector must not be empty")
          .primary(selector_loc.value_or(location::unknown))
          .emit(ctx);
      } else if (not parse_selector_value(*selector_val)) {
        diagnostic::error("invalid selector `{}`: must contain at most one `:` "
                          "and field name must not be empty",
                          *selector_val)
          .primary(selector_loc.value_or(location::unknown))
          .emit(ctx);
      }
    }
    if (unflatten_val and unflatten_val->empty()) {
      diagnostic::error("`unflatten_separator` must not be empty")
        .primary(unflatten_loc.value_or(location::unknown))
        .emit(ctx);
    }
    if (schema_only_val and schema_only_requires_schema_or_selector
        and not schema_val and not selector_val) {
      diagnostic::error("`schema_only` requires a `schema` or `selector`")
        .primary(schema_only_loc.value_or(location::unknown))
        .emit(ctx);
    }
    return {};
  }
};

/// Registers MSB named arguments on a Describer and returns a validator.
/// The setters write directly into the multi_series_builder::options member
/// pointed to by `opts_ptr`.
template <class Args, class... Impls>
auto add_msb_to_describer(Describer<Args, Impls...>& d,
                          multi_series_builder::options Args::* opts_ptr,
                          msb_describer_options msb_opts = {})
  -> msb_validator<Args> {
  using namespace _::operator_plugin;
  auto result = msb_validator<Args>{};
  result.schema_only_requires_schema_or_selector
    = msb_opts.schema_only_requires_schema_or_selector;
  // Policy args.
  if (msb_opts.add_schema) {
    result.schema = d.template named_with_setter<std::string>(
      "schema", Setter<located<std::string>>{
                  [opts_ptr](Any& args, located<std::string> v) {
                    auto& opts = (&args.template as<Args>())->*opts_ptr;
                    opts.policy = multi_series_builder::policy_schema{
                      .seed_schema = std::move(v.inner),
                    };
                  }});
  }
  if (msb_opts.add_selector) {
    result.selector = d.template named_with_setter<std::string>(
      "selector", Setter<located<std::string>>{
                    [opts_ptr](Any& args, located<std::string> v) {
                      auto& opts = (&args.template as<Args>())->*opts_ptr;
                      if (auto parsed = parse_selector_value(v.inner)) {
                        opts.policy = std::move(*parsed);
                      }
                    }});
  }
  // Settings args.
  result.schema_only = d.named_flag_with_setter(
    "schema_only",
    Setter<located<bool>>{[opts_ptr](Any& args, located<bool> v) {
      if (v.inner) {
        ((&args.template as<Args>())->*opts_ptr).settings.schema_only = true;
      }
    }});
  if (msb_opts.merge == merge_option::yes) {
    result.merge = d.named_flag_with_setter(
      "merge", Setter<located<bool>>{[opts_ptr](Any& args, located<bool> v) {
        if (v.inner) {
          ((&args.template as<Args>())->*opts_ptr).settings.merge = true;
        }
      }});
  } else if (msb_opts.merge == merge_option::hidden) {
    result.merge = d.named_flag_with_setter(
      "_merge", Setter<located<bool>>{[opts_ptr](Any& args, located<bool> v) {
        if (v.inner) {
          ((&args.template as<Args>())->*opts_ptr).settings.merge = true;
        }
      }});
  }
  result.raw = d.named_flag_with_setter(
    "raw", Setter<located<bool>>{[opts_ptr](Any& args, located<bool> v) {
      if (v.inner) {
        ((&args.template as<Args>())->*opts_ptr).settings.raw = true;
      }
    }});
  if (msb_opts.add_unflatten) {
    result.unflatten_separator = d.template named_with_setter<std::string>(
      "unflatten_separator",
      Setter<located<std::string>>{
        [opts_ptr](Any& args, located<std::string> v) {
          ((&args.template as<Args>())->*opts_ptr).settings.unnest_separator
            = std::move(v.inner);
        }});
  }
  result.batch_timeout = d.template named_with_setter<duration>(
    "_batch_timeout",
    Setter<located<duration>>{[opts_ptr](Any& args, located<duration> v) {
      ((&args.template as<Args>())->*opts_ptr).settings.timeout = v.inner;
    }});
  result.batch_size = d.template named_with_setter<uint64_t>(
    "_batch_size",
    Setter<located<uint64_t>>{[opts_ptr](Any& args, located<uint64_t> v) {
      ((&args.template as<Args>())->*opts_ptr).settings.desired_batch_size
        = v.inner;
    }});
  return result;
}

} // namespace tenzir
