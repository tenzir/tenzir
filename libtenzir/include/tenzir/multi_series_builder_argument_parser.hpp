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
  multi_series_builder_argument_parser(
    multi_series_builder::settings_type settings = {},
    multi_series_builder::policy_type policy
    = multi_series_builder::policy_precise{})
    : settings_{std::move(settings)}, policy_{std::move(policy)} {
  }

public:
  auto add_to_parser(argument_parser& parser) -> void;
  auto add_to_parser(argument_parser2& parser) -> void;

  auto get_settings() -> multi_series_builder::settings_type&;
  auto get_policy() -> multi_series_builder::policy_type&;
  // If we leave these public, the json parser can keep supporting its old
  // options by checking/setting values here
  // TODO do we even want that?
  // private:
  multi_series_builder::settings_type settings_ = {};
  multi_series_builder::policy_type policy_
    = multi_series_builder::policy_precise{};

  std::optional<location> merge_;

  // Policy merge & Policy default(precise)
  std::optional<located<std::string>> schema_;

  // Policy selector
  std::optional<located<std::string>> selector_;
  std::optional<location> schema_only_;
};

struct common_parser_options_parser {
  auto add_to_parser(argument_parser& parser) -> void;
  auto add_to_parser(argument_parser2& parser) -> void;

  auto get_unnest() const -> std::string {
    if (unnest_) {
      if (unnest_->inner.empty()) {
        diagnostic::error("got empty unflatten-separator")
          .note("get {}")
          .throw_();
      }
      return unnest_->inner;
    }
    return {};
  }
  bool get_raw() const {
    return raw_;
  }

private:
  std::optional<located<std::string>> unnest_;
  bool raw_ = false;
};

} // namespace tenzir