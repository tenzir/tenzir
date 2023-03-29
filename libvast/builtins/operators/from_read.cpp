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

class from_operator final : public crtp_operator<from_operator> {
public:
  explicit from_operator(const loader_plugin& loader, record config)
    : loader_plugin_{loader}, config_{std::move(config)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> caf::expected<generator<chunk_ptr>> {
    return loader_plugin_.make_loader(config_, ctrl);
  }

  auto to_string() const -> std::string override {
    return fmt::format("from {} <{}>", loader_plugin_.name(), config_);
  }

private:
  const loader_plugin& loader_plugin_;
  record config_;
};

class read_operator final : public crtp_operator<read_operator> {
public:
  // TODO: Replace `plugin` with `parser_plugin` (once we have it).
  explicit read_operator(const plugin& parser, record config)
    : parser_plugin_{parser}, config_{std::move(config)} {
  }

  auto operator()(generator<chunk_ptr>, operator_control_plane&) const
    -> caf::expected<generator<table_slice>> {
    // TODO: Implement this.
    return caf::make_error(ec::unimplemented,
                           "parser_plugin does not exist yet");
  }

  auto to_string() const -> std::string override {
    return fmt::format("read {} <{}>", parser_plugin_.name(), config_);
  }

private:
  const plugin& parser_plugin_;
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
    auto parser = plugins::find<plugin>(parser_name);
    if (!parser) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::lookup_error,
                        fmt::format("no parser found for '{}'", parser_name)),
      };
    }
    auto ops = std::vector<operator_ptr>{};
    ops.push_back(std::make_unique<from_operator>(*loader, record{}));
    ops.push_back(
      std::make_unique<read_operator>(*parser, std::move(parser_config)));
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

    // TODO: Implement this (following the same logic as `from_plugin`) as soon
    // as we have a `parser_plugin`.
    return {
      std::string_view{f, l},
      caf::make_error(ec::unimplemented,
                      "this operator is not implemented yet"),
    };
  }
};

} // namespace
} // namespace vast::plugins::from_read

VAST_REGISTER_PLUGIN(vast::plugins::from_read::from_plugin)
// VAST_REGISTER_PLUGIN(vast::plugins::from_read::read_plugin)
