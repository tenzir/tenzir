//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/concept/parseable/vast/expression.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/concept/printable/to_string.hpp>
#include <vast/error.hpp>
#include <vast/legacy_pipeline.hpp>
#include <vast/plugin.hpp>

namespace vast::plugins::vast {

class plugin final : public virtual legacy_language_plugin {
  caf::error initialize([[maybe_unused]] const record& plugin_config,
                        [[maybe_unused]] const record& global_config) override {
    return caf::none;
  }

  [[nodiscard]] std::string name() const override {
    return "VAST";
  }

  [[nodiscard]] caf::expected<
    std::pair<expression, std::optional<legacy_pipeline>>>
  make_query(std::string_view query) const override {
    static const auto match_everything = expression{predicate{
      meta_extractor{meta_extractor::kind::type},
      relational_operator::not_equal,
      data{"this expression matches everything"},
    }};
    if (query.empty()) {
      return std::pair{
        match_everything,
        std::optional<legacy_pipeline>{},
      };
    }
    using parsers::space, parsers::expr, parsers::eoi,
      parsers::optional_ws_or_comment;
    // Try to parse pipline first, because e.g. `head` is both a valid
    // expression and a valid pipeline.
    auto parsed_pipeline = legacy_pipeline::parse("export", query);
    if (parsed_pipeline) {
      return std::pair{match_everything, std::move(*parsed_pipeline)};
    }
    auto f = query.begin();
    const auto l = query.end();
    auto parsed_expr = expression{};
    bool has_expr = true;
    const auto expr_parser = optional_ws_or_comment >> expr;
    if (!expr_parser(f, l, parsed_expr)) {
      VAST_DEBUG("failed to parse expr from '{}'", query);
      parsed_expr = match_everything;
      has_expr = false;
    }
    VAST_DEBUG("parsed expr = {}", parsed_expr);
    // <expr> | <pipeline>
    //       ^ we start here
    const auto has_no_pipeline_parser = optional_ws_or_comment >> eoi;
    if (has_no_pipeline_parser(f, l, unused)) {
      return std::pair{
        std::move(parsed_expr),
        std::optional<legacy_pipeline>{},
      };
    }
    if (has_expr) {
      const auto has_pipeline_parser = optional_ws_or_comment >> '|';
      if (!has_pipeline_parser(f, l, unused)) {
        return caf::make_error(ec::syntax_error,
                               fmt::format("failed to parse "
                                           "pipeline in query "
                                           "'{}': missing pipe",
                                           query));
      }
    }
    const auto pipeline_query = std::string_view{f, l};
    parsed_pipeline = legacy_pipeline::parse("export", pipeline_query);
    if (!parsed_pipeline) {
      return caf::make_error(ec::syntax_error,
                             fmt::format("failed to parse pipeline in query "
                                         "'{}': {}",
                                         query, parsed_pipeline.error()));
    }
    VAST_DEBUG("parsed pipeline = {}", pipeline_query);
    return std::pair{
      std::move(parsed_expr),
      std::move(*parsed_pipeline),
    };
  }
};

} // namespace vast::plugins::vast

VAST_REGISTER_PLUGIN(vast::plugins::vast::plugin)
