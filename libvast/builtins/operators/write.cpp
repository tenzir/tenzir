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
#include <vast/error.hpp>
#include <vast/legacy_pipeline.hpp>
#include <vast/plugin.hpp>
#include <vast/type.hpp>

#include <arrow/type.h>
#include <caf/expected.hpp>

#include <algorithm>
#include <utility>

namespace vast::plugins::write {

namespace {

class write_operator : public logical_operator<events, void> {
public:
  explicit write_operator(const printer_plugin& printer,
                          std::optional<const dumper_plugin*> dumper) noexcept
    : printer_plugin_{printer}, dumper_plugin_{dumper} {
  }

  [[nodiscard]] auto
  make_physical_operator(const type& input_schema,
                         operator_control_plane* ctrl) noexcept
    -> caf::expected<physical_operator<events, void>> override {
    if (dumper_plugin_) {
      if (not printer_plugin_.printer_allows_joining()
          and (*dumper_plugin_)->dumper_requires_joining()) {
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("Output joining clash between {} "
                                           "dumper and {} printer",
                                           (*dumper_plugin_)->name(),
                                           printer_plugin_.name()));
      }
    }
    auto new_printer = printer_plugin_.make_printer({}, input_schema, *ctrl);
    if (!new_printer) {
      return new_printer.error();
    }
    printer_ = std::move(*new_printer);
    auto new_dumper
      = (dumper_plugin_)
          ? (*dumper_plugin_)->make_dumper({}, input_schema, *ctrl)
          : printer_plugin_.make_default_dumper({}, input_schema, *ctrl);
    if (!new_dumper) {
      return new_dumper.error();
    }
    dumper_ = std::move(*new_dumper);
    return [this](generator<table_slice> input) -> generator<std::monostate> {
      return dumper_(printer_(std::move(input)));
    };
  }

  [[nodiscard]] auto to_string() const noexcept -> std::string override {
    auto str = fmt::format("write {}", printer_plugin_.name());
    if (dumper_plugin_) {
      str.append(fmt::format(" to {}", (*dumper_plugin_)->name()));
    }
    return str;
  }

private:
  const printer_plugin& printer_plugin_;
  const std::optional<const dumper_plugin*> dumper_plugin_;
  printer_plugin::printer printer_;
  dumper_plugin::dumper dumper_;
};

class plugin final : public virtual logical_operator_plugin {
public:
  // plugin API
  caf::error initialize([[maybe_unused]] const record& plugin_config,
                        [[maybe_unused]] const record& global_config) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "write";
  };

  [[nodiscard]] std::pair<std::string_view, caf::expected<logical_operator_ptr>>
  make_logical_operator(std::string_view pipeline) const override {
    using parsers::end_of_pipeline_operator, parsers::required_ws_or_comment,
      parsers::optional_ws_or_comment, parsers::extractor, parsers::identifier,
      parsers::extractor_char, parsers::extractor_list;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto p = optional_ws_or_comment >> identifier
                   >> -(required_ws_or_comment >> string_parser{"to"}
                        >> required_ws_or_comment >> identifier)
                   >> optional_ws_or_comment >> end_of_pipeline_operator;
    auto result = std::tuple{
      std::string{}, std::optional<std::tuple<std::string, std::string>>{}};
    if (!p(f, l, result)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse "
                                                      "write operator: '{}'",
                                                      pipeline)),
      };
    }
    auto printer_name = std::get<0>(result);
    auto printer = plugins::find<printer_plugin>(printer_name);
    if (!printer) {
      return {std::string_view{f, l},
              caf::make_error(ec::syntax_error,
                              fmt::format("failed to parse "
                                          "write operator: no '{}' printer "
                                          "found",
                                          printer_name))};
    }
    std::optional<const dumper_plugin*> dumper;
    auto dumper_argument = std::get<1>(result);
    if (dumper_argument) {
      auto dumper_name = std::get<1>(*dumper_argument);
      dumper.emplace(plugins::find<dumper_plugin>(dumper_name));
      if (!dumper) {
        return {std::string_view{f, l},
                caf::make_error(ec::syntax_error,
                                fmt::format("failed to parse "
                                            "write operator: no '{}' dumper "
                                            "found",
                                            dumper_name))};
      }
    }
    return {
      std::string_view{f, l},
      std::make_unique<write_operator>(*printer, std::move(dumper)),
    };
  }
};

} // namespace

} // namespace vast::plugins::write

VAST_REGISTER_PLUGIN(vast::plugins::write::plugin)
