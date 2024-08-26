//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/argument_parser.hpp"
#include "tenzir/multi_series_builder.hpp"

#include <tenzir/error.hpp>
#include <tenzir/module.hpp>
#include <tenzir/plugin.hpp>

#include "argument_parser2.hpp"

namespace tenzir {

// simple utility combining multi_series_builder::settings_type and
// multi_series_builder::policy_type
struct multi_series_builder_options {
  multi_series_builder::policy_type policy
    = multi_series_builder::policy_precise{};
  multi_series_builder::settings_type settings = {};

  friend auto inspect(auto& f, multi_series_builder_options& x) -> bool {
    return f.object(x).fields(f.field("policy", x.policy),
                              f.field("settings", x.settings));
  }

  auto get_schemas() const -> std::vector<tenzir::type>;
};

// adds the schema_only/no-infer option to a parser for use in parser-parsers
// this is outside of the `multi_series_builder_argument_parser`, since its
// needed for parsers that dont support any of the other options
void add_schema_only_option(argument_parser& parser,
                            std::optional<location>& schema_only);
void add_schema_only_option(argument_parser2& parser,
                            std::optional<location>& schema_only);

/// simple utility to parse the command line arguments for a
/// multi_series_builder's settings and policy
struct multi_series_builder_argument_parser {
public:
  multi_series_builder_argument_parser() = default;
  multi_series_builder_argument_parser(
    multi_series_builder::settings_type settings,
    multi_series_builder::policy_type policy)
    : has_manual_defaults_{true},
      settings_{std::move(settings)},
      policy_{std::move(policy)} {
  }

  auto add_settings_to_parser(argument_parser& parser, bool add_unflatten_option = true, bool add_unique_selector_option = true ) -> void;
  auto add_policy_to_parser(argument_parser& parser) -> void;
  auto add_all_to_parser(argument_parser& parser) -> void;
  auto add_settings_to_parser(argument_parser2& parser, bool add_unflatten_option = true, bool add_unique_selector_option = true) -> void;
  auto add_policy_to_parser(argument_parser2& parser) -> void;
  auto add_all_to_parser(argument_parser2& parser) -> void;

  auto get_options(diagnostic_handler& dh) -> failure_or<multi_series_builder_options> {
    auto good = get_policy(dh);
    good |= get_settings(dh);
    if ( good ) {
      return multi_series_builder_options{
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
  std::vector<tenzir::type> schemas_;
  bool has_manual_defaults_ = false;
  bool is_tql1_ = false;
  multi_series_builder::settings_type settings_ = {};
  multi_series_builder::policy_type policy_
    = multi_series_builder::policy_precise{};

  std::optional<location> merge_;

  // Policy merge & Policy default(precise)
  std::optional<located<std::string>> schema_;

  // Policy selector
  std::optional<located<std::string>> selector_;
  std::optional<location> unique_selector_;

  // settings
  std::optional<location> schema_only_;
  std::optional<located<std::string>> unnest_;
  std::optional<location> raw_;
};
} // namespace tenzir