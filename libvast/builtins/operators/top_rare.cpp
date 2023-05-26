//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/detail/string_literal.hpp>
#include <vast/error.hpp>
#include <vast/plugin.hpp>

namespace vast::plugins::top_rare {

namespace {

template <detail::string_literal Name, detail::string_literal SortOrder>
class top_rare_plugin final : public virtual operator_plugin {
public:
  auto initialize(const record&, const record&) -> caf::error override {
    return {};
  }

  auto name() const -> std::string override {
    return std::string{Name.str()};
  };

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::optional_ws_or_comment, parsers::required_ws_or_comment,
      parsers::end_of_pipeline_operator, parsers::operator_arg, parsers::count;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto p = required_ws_or_comment >> operator_arg
                   >> optional_ws_or_comment >> end_of_pipeline_operator;
    auto field = std::string{};
    if (!p(f, l, field)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse "
                                                      "{} operator: '{}'",
                                                      Name.str(), pipeline)),
      };
    }
    return {
      std::string_view{f, l},
      pipeline::parse_as_operator(fmt::format("summarize count=count({0}) by "
                                              "{0} | sort count {1}",
                                              field, SortOrder.str())),
    };
  }
};

using top_plugin = top_rare_plugin<"top", "desc">;
using rare_plugin = top_rare_plugin<"rare", "asc">;

} // namespace

} // namespace vast::plugins::top_rare

VAST_REGISTER_PLUGIN(vast::plugins::top_rare::top_plugin)
VAST_REGISTER_PLUGIN(vast::plugins::top_rare::rare_plugin)
