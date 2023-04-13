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

class load_operator final : public crtp_operator<load_operator> {
public:
  explicit load_operator(const loader_plugin& loader, record config)
    : loader_plugin_{loader}, config_{std::move(config)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> caf::expected<generator<chunk_ptr>> {
    return loader_plugin_.make_loader(config_, ctrl);
  }

  auto to_string() const -> std::string override {
    return fmt::format("load {} <{}>", loader_plugin_.name(), config_);
  }

private:
  const loader_plugin& loader_plugin_;
  record config_;
};

class parse_operator final : public crtp_operator<parse_operator> {
public:
  explicit parse_operator(const parser_plugin& parser, record config)
    : parser_plugin_{parser}, config_{std::move(config)} {
  }

  auto
  operator()(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> caf::expected<generator<table_slice>> {
    auto parser = parser_plugin_.make_parser(config_, ctrl);
    if (not parser) {
      return std::move(parser.error());
    }
    return (*parser)(std::move(input));
  }

  auto to_string() const -> std::string override {
    return fmt::format("parse {} <{}>", parser_plugin_.name(), config_);
  }

private:
  const parser_plugin& parser_plugin_;
  record config_;
};

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
      parsers::identifier, parsers::required_ws_or_comment;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto p = optional_ws_or_comment >> identifier
                   >> -(required_ws_or_comment >> string_parser{"read"}
                        >> required_ws_or_comment >> identifier)
                   >> optional_ws_or_comment >> end_of_pipeline_operator;
    auto parsed = std::tuple{
      std::string{}, std::optional<std::tuple<std::string, std::string>>{}};
    if (!p(f, l, parsed)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error,
                        fmt::format("failed to parse from operator: '{}'",
                                    pipeline)),
      };
    }
    auto loader_name = std::move(std::get<0>(parsed));
    auto loader = plugins::find<loader_plugin>(loader_name);
    if (!loader) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::lookup_error,
                        fmt::format("no loader found for '{}'", loader_name)),
      };
    }

    auto parser_name = std::string{};
    auto parser_config = record{};
    if (auto read_opt = std::get<1>(parsed)) {
      parser_name = std::move(std::get<1>(*read_opt));
    } else if (auto default_parser = loader->get_default_parser({})) {
      std::tie(parser_name, parser_config) = std::move(*default_parser);
    } else {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("the {} loader must be "
                                                      "followed by 'read ...'",
                                                      loader_name)),
      };
    }
    auto parser = plugins::find<parser_plugin>(parser_name);
    if (!parser) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::lookup_error,
                        fmt::format("no parser found for '{}'", parser_name)),
      };
    }
    auto ops = std::vector<operator_ptr>{};
    ops.push_back(std::make_unique<load_operator>(*loader, record{}));
    ops.push_back(
      std::make_unique<parse_operator>(*parser, std::move(parser_config)));
    return {
      std::string_view{f, l},
      std::make_unique<class pipeline>(std::move(ops)),
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
      parsers::identifier, parsers::required_ws_or_comment;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto p = optional_ws_or_comment >> identifier
                   >> -(required_ws_or_comment >> string_parser{"from"}
                        >> required_ws_or_comment >> identifier)
                   >> optional_ws_or_comment >> end_of_pipeline_operator;
    auto parsed = std::tuple{
      std::string{}, std::optional<std::tuple<std::string, std::string>>{}};
    if (!p(f, l, parsed)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error,
                        fmt::format("failed to parse read operator: '{}'",
                                    pipeline)),
      };
    }
    auto parser_name = std::move(std::get<0>(parsed));
    auto parser = plugins::find<parser_plugin>(parser_name);
    if (not parser) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::lookup_error,
                        fmt::format("no parser found for '{}'", parser_name)),
      };
    }
    auto loader_name = std::string{};
    auto loader_cfg = record{};
    if (auto read_opt = std::get<1>(parsed)) {
      loader_name = std::move(std::get<1>(*read_opt));
    } else if (auto default_loader = parser->make_default_loader()) {
      std::tie(loader_name, loader_cfg) = std::move(*default_loader);
    } else {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("the {} parser must be "
                                                      "followed by 'from ...'",
                                                      parser_name)),
      };
    }
    auto loader = plugins::find<loader_plugin>(loader_name);
    if (!loader) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::lookup_error,
                        fmt::format("no loader plugin found for '{}'",
                                    loader_name)),
      };
    }
    auto ops = std::vector<operator_ptr>(2u);
    ops[0] = std::make_unique<load_operator>(*loader, std::move(loader_cfg));
    ops[1] = std::make_unique<parse_operator>(*parser, record{});
    return {
      std::string_view{f, l},
      std::make_unique<class pipeline>(std::move(ops)),
    };
  }
};

} // namespace
} // namespace vast::plugins::from_read

VAST_REGISTER_PLUGIN(vast::plugins::from_read::from_plugin)
VAST_REGISTER_PLUGIN(vast::plugins::from_read::read_plugin)
