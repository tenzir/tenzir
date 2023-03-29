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
#include <vast/element_type.hpp>
#include <vast/error.hpp>
#include <vast/logical_pipeline.hpp>
#include <vast/plugin.hpp>
#include <vast/type.hpp>

#include <arrow/type.h>
#include <caf/expected.hpp>

#include <algorithm>
#include <utility>

namespace vast::plugins::write_to {

namespace {

/// The logical operator for printing data that will have to be joined later
/// during pipeline execution.
class print_operator : public logical_operator<table_slice, chunk_ptr> {
public:
  explicit print_operator(const printer_plugin& printer) noexcept
    : printer_plugin_{printer} {
  }

  [[nodiscard]] auto
  make_physical_operator(const type& input_schema,
                         operator_control_plane& ctrl) noexcept
    -> caf::expected<physical_operator<table_slice, chunk_ptr>> override {
    auto new_printer = printer_plugin_.make_printer({}, input_schema, ctrl);
    if (!new_printer) {
      return new_printer.error();
    }
    printer_ = std::move(*new_printer);
    return [this](generator<table_slice> input) -> generator<chunk_ptr> {
      return printer_(std::move(input));
    };
  }

  [[nodiscard]] auto to_string() const noexcept -> std::string override {
    return fmt::format("write {}", printer_plugin_.name());
  }

private:
  const printer_plugin& printer_plugin_;
  printer_plugin::printer printer_;
};

/// The logical operator for dumping data that will have to be joined later
/// during pipeline execution.
class dump_operator : public logical_operator<chunk_ptr, void> {
public:
  explicit dump_operator(const dumper_plugin& dumper) noexcept
    : dumper_plugin_{dumper} {
  }

  [[nodiscard]] auto
  make_physical_operator(const type& input_schema,
                         operator_control_plane& ctrl) noexcept
    -> caf::expected<physical_operator<chunk_ptr, void>> override {
    auto new_dumper = dumper_plugin_.make_dumper({}, input_schema, ctrl);
    if (!new_dumper) {
      return new_dumper.error();
    }
    dumper_ = std::move(*new_dumper);
    return [this](generator<chunk_ptr> input) -> generator<std::monostate> {
      return dumper_(std::move(input));
    };
  }

  [[nodiscard]] auto to_string() const noexcept -> std::string override {
    return fmt::format("to {}", dumper_plugin_.name());
  }

private:
  const dumper_plugin& dumper_plugin_;
  dumper_plugin::dumper dumper_;
};

/// The logical operator for printing and dumping data without joining.
class print_dump_operator : public logical_operator<table_slice, void> {
public:
  explicit print_dump_operator(const printer_plugin& printer,
                               const dumper_plugin& dumper) noexcept
    : printer_plugin_{printer}, dumper_plugin_{dumper} {
  }

  [[nodiscard]] auto
  make_physical_operator(const type& input_schema,
                         operator_control_plane& ctrl) noexcept
    -> caf::expected<physical_operator<table_slice, void>> override {
    auto new_printer = printer_plugin_.make_printer({}, input_schema, ctrl);
    if (!new_printer) {
      return new_printer.error();
    }
    printer_ = std::move(*new_printer);
    auto new_dumper = dumper_plugin_.make_dumper({}, input_schema, ctrl);
    if (!new_dumper) {
      return new_dumper.error();
    }
    dumper_ = std::move(*new_dumper);
    return [this](generator<table_slice> input) -> generator<std::monostate> {
      return dumper_(printer_(std::move(input)));
    };
  }

  [[nodiscard]] auto to_string() const noexcept -> std::string override {
    return fmt::format("write {} to {}", printer_plugin_.name(),
                       dumper_plugin_.name());
  }

private:
  const printer_plugin& printer_plugin_;
  printer_plugin::printer printer_;
  const dumper_plugin& dumper_plugin_;
  dumper_plugin::dumper dumper_;
};

class write_plugin final : public virtual logical_operator_plugin {
public:
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
    const dumper_plugin* dumper;
    auto dumper_argument = std::get<1>(result);
    if (dumper_argument) {
      auto dumper_name = std::get<1>(*dumper_argument);
      dumper = plugins::find<dumper_plugin>(dumper_name);
      if (!dumper) {
        return {std::string_view{f, l},
                caf::make_error(ec::syntax_error,
                                fmt::format("failed to parse "
                                            "write operator: no '{}' dumper "
                                            "found",
                                            dumper_name))};
      }
    } else {
      dumper = printer->make_default_dumper();
      if (!dumper) {
        return {std::string_view{f, l},
                caf::make_error(ec::invalid_configuration,
                                fmt::format("failed to parse write operator: "
                                            "no available default sink for "
                                            "printing '{}' output "
                                            "found",
                                            printer->name()))};
      }
    }
    if (dumper->dumper_requires_joining()
        and not printer->printer_allows_joining()) {
      return {std::string_view{f, l},
              caf::make_error(ec::invalid_argument,
                              fmt::format("writing '{0}' to '{1}' is not "
                                          "allowed; the sink '{1}' requires a "
                                          "single input, and the format '{0}' "
                                          "has potentially multiple outputs",
                                          printer->name(), dumper->name()))};
    } else if (not dumper->dumper_requires_joining()) {
      auto op = std::make_unique<print_dump_operator>(std::move(*printer),
                                                      std::move(*dumper));
      return {std::string_view{f, l}, std::move(op)};
    }
    auto print_op = std::make_unique<print_operator>(std::move(*printer));
    auto dump_op = std::make_unique<dump_operator>(std::move(*dumper));
    auto ops = std::vector<logical_operator_ptr>{};
    ops.emplace_back(std::move(print_op));
    ops.emplace_back(std::move(dump_op));
    auto sub_pipeline = logical_pipeline::make(std::move(ops));
    return {
            std::string_view{f, l},
            std::make_unique<logical_pipeline>(std::move(*sub_pipeline)),
            };
  }
};

class to_plugin final : public virtual logical_operator_plugin {
public:
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
    const printer_plugin* printer;
    auto printer_argument = std::get<1>(result);
    if (printer_argument) {
      auto printer_name = std::get<1>(*printer_argument);
      printer = plugins::find<printer_plugin>(printer_name);
      if (!printer) {
        return {std::string_view{f, l},
                caf::make_error(ec::syntax_error,
                                fmt::format("failed to parse "
                                            "to operator: no '{}' printer "
                                            "found",
                                            printer_name))};
      }
    } else {
      printer = dumper->make_default_printer();
      if (!printer) {
        return {std::string_view{f, l},
                caf::make_error(ec::invalid_configuration,
                                fmt::format("failed to parse write operator: "
                                            "no available default printer for "
                                            "sink '{}' "
                                            "found",
                                            dumper->name()))};
      }
    }
    if (dumper->dumper_requires_joining()
        and not printer->printer_allows_joining()) {
      return {std::string_view{f, l},
              caf::make_error(ec::invalid_argument,
                              fmt::format("writing '{0}' to '{1}' is not "
                                          "allowed; the sink '{1}' requires a "
                                          "single input, and the format '{0}' "
                                          "has potentially multiple outputs",
                                          printer->name(), dumper->name()))};
    } else if (not dumper->dumper_requires_joining()) {
      auto op = std::make_unique<print_dump_operator>(std::move(*printer),
                                                      std::move(*dumper));
      return {std::string_view{f, l}, std::move(op)};
    }
    auto print_op = std::make_unique<print_operator>(std::move(*printer));
    auto dump_op = std::make_unique<dump_operator>(std::move(*dumper));
    auto ops = std::vector<logical_operator_ptr>{};
    ops.emplace_back(std::move(print_op));
    ops.emplace_back(std::move(dump_op));
    auto sub_pipeline = logical_pipeline::make(std::move(ops));
    return {
            std::string_view{f, l},
            std::make_unique<logical_pipeline>(std::move(*sub_pipeline)),
            };
  }
};

} // namespace

} // namespace vast::plugins::write_to

VAST_REGISTER_PLUGIN(vast::plugins::write_to::write_plugin)
VAST_REGISTER_PLUGIN(vast::plugins::write_to::to_plugin)
