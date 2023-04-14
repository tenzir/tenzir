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

namespace vast::plugins::from_read {

namespace {

class from_plugin final : public virtual operator_plugin {
public:
  auto initialize(const record&, const record&) -> caf::error override {
    return {};
  }

  auto name() const -> std::string override {
    return "from";
  };

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::optional_ws_or_comment, parsers::end_of_pipeline_operator,
      parsers::plugin_name, parsers::required_ws_or_comment;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    // TODO: handle options for loader and parser
    auto loader_options = record{};
    auto parser_options = record{};
    const auto p = optional_ws_or_comment >> plugin_name
                   >> -(required_ws_or_comment >> "read"
                        >> required_ws_or_comment >> plugin_name)
                   >> optional_ws_or_comment >> end_of_pipeline_operator;
    auto parsed = std::tuple{std::string{}, std::optional<std::string>{}};
    if (!p(f, l, parsed)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error,
                        fmt::format("failed to parse from operator: '{}'",
                                    pipeline)),
      };
    }
    auto& [loader_name, parser_name] = parsed;
    const auto* loader = plugins::find<loader_plugin>(loader_name);
    if (not loader) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to find loader "
                                                      "'{}' in pipeline '{}'",
                                                      loader_name, pipeline)),
      };
    }
    if (not parser_name) {
      std::tie(parser_name, parser_options)
        = loader->default_parser(loader_options);
    }
    const auto* parser = plugins::find<parser_plugin>(*parser_name);
    if (not parser) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to find parser "
                                                      "'{}' in pipeline '{}'",
                                                      parser_name, pipeline)),
      };
    }
    return {
      std::string_view{f, l},
      // TODO: This ignores options
      pipeline::parse_as_operator(
        fmt::format("load {} | parse {}", loader_name, *parser_name), {}),
    };
  }
};

class read_plugin final : public virtual operator_plugin {
public:
  auto initialize(const record&, const record&) -> caf::error override {
    return {};
  }

  auto name() const -> std::string override {
    return "read";
  };

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::optional_ws_or_comment, parsers::end_of_pipeline_operator,
      parsers::plugin_name, parsers::required_ws_or_comment;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    // TODO: handle options for loader and parser
    auto loader_options = record{};
    auto parser_options = record{};
    const auto p = optional_ws_or_comment >> plugin_name
                   >> -(required_ws_or_comment >> "from"
                        >> required_ws_or_comment >> plugin_name)
                   >> optional_ws_or_comment >> end_of_pipeline_operator;
    auto parsed = std::tuple{std::string{}, std::optional<std::string>{}};
    if (!p(f, l, parsed)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error,
                        fmt::format("failed to parse read operator: '{}'",
                                    pipeline)),
      };
    }
    auto& [parser_name, loader_name] = parsed;
    const auto* parser = plugins::find<parser_plugin>(parser_name);
    if (not parser) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to find parser "
                                                      "'{}' in pipeline '{}'",
                                                      parser_name, pipeline)),
      };
    }
    if (not loader_name) {
      std::tie(loader_name, loader_options)
        = parser->default_loader(parser_options);
    }
    const auto* loader = plugins::find<loader_plugin>(*loader_name);
    if (not loader) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to find loader "
                                                      "'{}' in pipeline '{}'",
                                                      loader_name, pipeline)),
      };
    }
    return {
      std::string_view{f, l},
      // TODO: This ignores options
      pipeline::parse_as_operator(
        fmt::format("load {} | parse {}", *loader_name, parser_name), {}),
    };
  }
};

} // namespace
} // namespace vast::plugins::from_read

VAST_REGISTER_PLUGIN(vast::plugins::from_read::from_plugin)
VAST_REGISTER_PLUGIN(vast::plugins::from_read::read_plugin)
