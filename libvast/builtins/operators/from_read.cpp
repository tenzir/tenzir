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
  explicit from_operator(const loader_plugin& loader)
    : loader_plugin_{&loader} {
  }

  explicit from_operator(const plugin& parser) : parser_plugin_{&parser} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> caf::expected<generator<chunk_ptr>> {
    if (loader_plugin_) {
      VAST_ASSERT(!parser_plugin_);
      auto loader = loader_plugin_->make_loader({}, ctrl);
      if (!loader) {
        return loader.error();
      }
      // TODO: Reconsider this.
      return std::invoke(
        [](loader_plugin::loader loader) -> generator<chunk_ptr> {
          for (auto&& x : loader()) {
            co_yield x;
          }
        },
        std::move(*loader));
    } else {
      VAST_ASSERT(parser_plugin_);
      // TODO: Implement.
      return caf::make_error(ec::unimplemented,
                             "parser_plugin does not exist yet");
    }
  }

  auto to_string() const -> std::string override {
    if (loader_plugin_) {
      return fmt::format("from {}", loader_plugin_->name());
    } else {
      VAST_ASSERT(parser_plugin_);
      // This can not be round-tripped.
      return fmt::format("<implicit from for {}>", parser_plugin_->name());
    }
  }

private:
  const loader_plugin* loader_plugin_{nullptr};
  const plugin* parser_plugin_{nullptr};
};

class read_operator final : public crtp_operator<read_operator> {
public:
  explicit read_operator(const loader_plugin& loader)
    : loader_plugin_{&loader} {
  }

  explicit read_operator(const plugin& parser) : parser_plugin_{&parser} {
  }

  auto
  operator()(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> caf::expected<generator<table_slice>> {
    if (parser_plugin_) {
      VAST_ASSERT(!loader_plugin_);
      // TODO: Implement.
      return caf::make_error(ec::unimplemented,
                             "parser_plugin does not exist yet");
    } else {
      VAST_ASSERT(loader_plugin_);
      auto parser = loader_plugin_->make_default_parser({}, ctrl);
      if (!parser) {
        return parser.error();
      }
      // TODO: Reconsider this.
      return std::invoke(
        [](loader_plugin::parser parser,
           generator<chunk_ptr> input) -> generator<table_slice> {
          for (auto&& x : parser(std::move(input))) {
            co_yield x;
          }
        },
        std::move(*parser), std::move(input));
    }
  }

  auto to_string() const -> std::string override {
    if (parser_plugin_) {
      return fmt::format("read {}", parser_plugin_->name());
    } else {
      VAST_ASSERT(loader_plugin_);
      // This can not be round-tripped.
      return fmt::format("<implicit read for {}>", loader_plugin_->name());
    }
  }

private:
  const plugin* parser_plugin_{nullptr};
  const loader_plugin* loader_plugin_{nullptr};
};

class from_plugin final : public virtual operator_plugin {
public:
  caf::error initialize(const record&, const record&) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
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
    auto from = std::move(std::get<0>(parsed));
    auto loader = plugins::find<loader_plugin>(from);
    if (!loader) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::lookup_error,
                        fmt::format("no loader found for '{}'", from)),
      };
    }
    auto parser = static_cast<const plugin*>(nullptr);
    if (auto read_opt = std::get<1>(parsed)) {
      auto read = std::move(std::get<1>(*read_opt));
      parser = plugins::find<plugin>(read);
      if (!parser) {
        return {
          std::string_view{f, l},
          caf::make_error(ec::lookup_error,
                          fmt::format("no parser found for '{}'", read)),
        };
      }
    }
    auto ops = std::vector<operator_ptr>{};
    ops.push_back(std::make_unique<from_operator>(*loader));
    if (parser) {
      ops.push_back(std::make_unique<read_operator>(*parser));
    } else {
      ops.push_back(std::make_unique<read_operator>(*loader));
    }
    return {
      std::string_view{f, l},
      std::make_unique<class pipeline>(std::move(ops)),
    };
  }
};

class read_plugin final : public virtual operator_plugin {
public:
  caf::error initialize(const record&, const record&) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
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
    auto& read = std::get<0>(parsed);
    auto parser = plugins::find<plugin>(read);
    if (!parser) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::lookup_error,
                        fmt::format("no parser found for '{}'", read)),
      };
    }
    auto loader = static_cast<const loader_plugin*>(nullptr);
    if (auto& from_opt = std::get<1>(parsed)) {
      auto& from = std::get<1>(*from_opt);
      loader = plugins::find<loader_plugin>(from);
      if (!loader) {
        return {
          std::string_view{f, l},
          caf::make_error(ec::lookup_error,
                          fmt::format("no loader found for '{}'", from)),
        };
      }
    }
    auto ops = std::vector<operator_ptr>{};
    if (loader) {
      ops.push_back(std::make_unique<from_operator>(*loader));
    } else {
      ops.push_back(std::make_unique<from_operator>(*parser));
    }
    ops.push_back(std::make_unique<read_operator>(*parser));
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
