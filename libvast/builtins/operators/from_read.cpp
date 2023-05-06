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
#include <vast/logger.hpp>
#include <vast/plugin.hpp>

#include <arrow/type.h>
#include <caf/error.hpp>

namespace vast::plugins::from_read {

namespace {

auto load_parse(auto&& loader, auto&& loader_args, auto&& parser,
                auto&& parser_args) -> caf::expected<operator_ptr> {
  auto expanded = fmt::format("load {} {} | local parse {} {}", loader,
                              escape_operator_args(loader_args), parser,
                              escape_operator_args(parser_args));
  VAST_DEBUG("from/read expanded to '{}'", expanded);
  return pipeline::parse_as_operator(expanded);
}

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
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    auto parsed = parsers::name_args_opt_keyword_name_args("read").apply(f, l);
    if (not parsed) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error,
                        fmt::format("failed to parse from operator: '{}'",
                                    pipeline)),
      };
    }
    auto& [loader_name, loader_args, opt_parser] = *parsed;
    const auto* loader = plugins::find<loader_plugin>(loader_name);
    if (not loader) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to find loader "
                                                      "'{}' in pipeline '{}' ",
                                                      loader_name, pipeline)),
      };
    }
    if (not opt_parser) {
      opt_parser.emplace(loader->default_parser(loader_args));
    }
    auto& [parser_name, parser_args] = *opt_parser;
    if (not plugins::find<parser_plugin>(parser_name)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to find parser "
                                                      "'{}' in pipeline '{}'",
                                                      parser_name, pipeline)),
      };
    }
    return {std::string_view{f, l},
            load_parse(loader_name, loader_args, parser_name, parser_args)};
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
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    auto parsed = parsers::name_args_opt_keyword_name_args("from").apply(f, l);
    if (not parsed) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error,
                        fmt::format("failed to parse read operator: '{}'",
                                    pipeline)),
      };
    }
    auto& [parser_name, parser_args, opt_loader] = *parsed;
    const auto* parser = plugins::find<parser_plugin>(parser_name);
    if (not parser) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to find parser "
                                                      "'{}' in pipeline '{}'",
                                                      parser_name, pipeline)),
      };
    }
    if (not opt_loader) {
      opt_loader.emplace(parser->default_loader(parser_args));
    }
    auto& [loader_name, loader_args] = *opt_loader;
    if (not plugins::find<loader_plugin>(loader_name)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to find loader "
                                                      "'{}' in pipeline '{}'",
                                                      loader_name, pipeline)),
      };
    }
    return {std::string_view{f, l},
            load_parse(loader_name, loader_args, parser_name, parser_args)};
  }
};

} // namespace
} // namespace vast::plugins::from_read

VAST_REGISTER_PLUGIN(vast::plugins::from_read::from_plugin)
VAST_REGISTER_PLUGIN(vast::plugins::from_read::read_plugin)
