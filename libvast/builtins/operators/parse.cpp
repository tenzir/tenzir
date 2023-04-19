//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/concept/parseable/string.hpp>
#include <vast/concept/parseable/vast/identifier.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/error.hpp>
#include <vast/legacy_pipeline.hpp>
#include <vast/logger.hpp>
#include <vast/plugin.hpp>

#include <arrow/type.h>
#include <caf/error.hpp>

namespace vast::plugins::parse {

namespace {

class parse_operator final : public crtp_operator<parse_operator> {
public:
  explicit parse_operator(const parser_plugin& parser,
                          std::vector<std::string> args)
    : parser_plugin_{parser}, args_{std::move(args)} {
  }

  auto
  operator()(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> caf::expected<generator<table_slice>> {
    auto parser = parser_plugin_.make_parser(args_, std::move(input), ctrl);
    if (not parser) {
      return std::move(parser.error());
    }
    return std::move(*parser);
  }

  auto to_string() const -> std::string override {
    return fmt::format("parse {}", parser_plugin_.name());
  }

private:
  const parser_plugin& parser_plugin_;
  std::vector<std::string> args_;
};

class plugin final : public virtual operator_plugin {
public:
  auto initialize(const record&, const record&) -> caf::error override {
    return {};
  }

  auto name() const -> std::string override {
    return "parse";
  };

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    auto parsed = parsers::name_args.apply(f, l);
    if (!parsed) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error,
                        fmt::format("failed to parse 'parse' operator: '{}'",
                                    pipeline)),
      };
    }
    auto& [name, args] = *parsed;
    const auto* parser = plugins::find<parser_plugin>(name);
    if (!parser) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::lookup_error,
                        fmt::format("no parser found for '{}'", name)),
      };
    }
    return {
      std::string_view{f, l},
      std::make_unique<parse_operator>(*parser, std::move(args)),
    };
  }
};

} // namespace

} // namespace vast::plugins::parse

VAST_REGISTER_PLUGIN(vast::plugins::parse::plugin)
