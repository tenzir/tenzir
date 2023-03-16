//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/logical_operator.hpp"

#include <vast/arrow_table_slice.hpp>
#include <vast/concept/convertible/data.hpp>
#include <vast/concept/convertible/to.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/dump_operator.hpp>
#include <vast/element_type.hpp>
#include <vast/error.hpp>
#include <vast/logical_pipeline.hpp>
#include <vast/plugin.hpp>
#include <vast/print_operator.hpp>
#include <vast/type.hpp>

#include <arrow/type.h>
#include <caf/expected.hpp>

#include <algorithm>
#include <utility>

namespace vast::plugins::to {

namespace {

class plugin final : public virtual logical_operator_plugin {
public:
  // plugin API
  caf::error initialize([[maybe_unused]] const record& plugin_config,
                        [[maybe_unused]] const record& global_config) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "to";
  };

  [[nodiscard]] std::pair<std::string_view, caf::expected<logical_operator_ptr>>
  make_logical_operator(std::string_view pipeline) const override {
    using parsers::end_of_pipeline_operator, parsers::required_ws_or_comment,
      parsers::optional_ws_or_comment, parsers::extractor, parsers::identifier,
      parsers::extractor_char, parsers::extractor_list;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto p = optional_ws_or_comment >> identifier
                   >> -(required_ws_or_comment >> string_parser{"write"}
                        >> required_ws_or_comment >> identifier)
                   >> optional_ws_or_comment >> end_of_pipeline_operator;
    auto result = std::tuple{
      std::string{}, std::optional<std::tuple<std::string, std::string>>{}};
    if (!p(f, l, result)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse "
                                                      "to operator: '{}'",
                                                      pipeline)),
      };
    }
    auto dumper_name = std::get<0>(result);
    auto dumper = plugins::find<dumper_plugin>(dumper_name);
    if (!dumper) {
      return {std::string_view{f, l},
              caf::make_error(ec::syntax_error,
                              fmt::format("failed to parse "
                                          "write operator: no '{}' dumper "
                                          "found",
                                          dumper_name))};
    }
    std::optional<const printer_plugin*> printer;
    auto printer_argument = std::get<1>(result);
    if (printer_argument) {
      auto printer_name = std::get<1>(*printer_argument);
      printer.emplace(plugins::find<printer_plugin>(printer_name));
      if (!printer) {
        return {std::string_view{f, l},
                caf::make_error(ec::syntax_error,
                                fmt::format("failed to parse "
                                            "to operator: no '{}' printer "
                                            "found",
                                            printer_name))};
      }
    } else {
      printer.emplace(dumper->make_default_printer());
    }
    auto write_op = std::make_unique<print_operator>(std::move(**printer));
    auto dump_op = std::make_unique<dump_operator>(std::move(*dumper));
    auto ops = std::vector<logical_operator_ptr>{};
    ops.push_back(std::move(write_op));
    ops.push_back(std::move(dump_op));
    auto sub_pipeline = logical_pipeline::make(std::move(ops));
    return {
      std::string_view{f, l},
      std::make_unique<logical_pipeline>(std::move(*sub_pipeline)),
    };
  }
};

} // namespace

} // namespace vast::plugins::to

VAST_REGISTER_PLUGIN(vast::plugins::to::plugin)
