//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/arrow_table_slice.hpp>
#include <vast/concept/convertible/data.hpp>
#include <vast/concept/convertible/to.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/element_type.hpp>
#include <vast/error.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>
#include <vast/type.hpp>

#include <arrow/type.h>
#include <caf/expected.hpp>

#include <algorithm>
#include <string>
#include <utility>

namespace vast::plugins::write_to {

namespace {

// TODO: Perhaps unify this with `from_read.cpp::load_parse`.
auto print_save(auto&& printer, auto&& printer_args, auto&& saver,
                auto&& saver_args) -> caf::expected<operator_ptr> {
  auto expanded = fmt::format("local print {} {} | save {} {}", printer,
                              escape_operator_args(printer_args), saver,
                              escape_operator_args(saver_args));
  return pipeline::parse_as_operator(expanded);
}

struct print_and_save_state {
  printer_plugin::printer printer;
  saver_plugin::saver saver;
};

/// The operator for printing and saving data without joining.
class print_and_save_operator final
  : public schematic_operator<print_and_save_operator, print_and_save_state,
                              std::monostate> {
public:
  explicit print_and_save_operator(const printer_plugin& printer,
                                   std::vector<std::string> print_args,
                                   const saver_plugin& saver,
                                   std::vector<std::string> save_args) noexcept
    : printer_plugin_{printer},
      print_args{std::move(print_args)},
      saver_plugin_{saver},
      save_args{std::move(save_args)} {
  }

  auto initialize(const type& schema, operator_control_plane& ctrl) const
    -> caf::expected<state_type> override {
    auto printer = printer_plugin_.make_printer(print_args, schema, ctrl);
    if (not printer) {
      return std::move(printer.error());
    }
    auto saver = saver_plugin_.make_saver(
      save_args, {.input_schema = schema, .format = printer_plugin_.name()},
      ctrl);
    if (not saver) {
      return std::move(saver.error());
    }
    return print_and_save_state{
      .printer = std::move(*printer),
      .saver = std::move(*saver),
    };
  }

  auto process(table_slice slice, state_type& state) const
    -> output_type override {
    for (auto&& x : state.printer->process(std::move(slice))) {
      state.saver(std::move(x));
    }
    return {};
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto to_string() const noexcept -> std::string override {
    return fmt::format("write {}{}{} to {}{}{}", printer_plugin_.name(),
                       print_args.empty() ? "" : " ",
                       escape_operator_args(print_args), saver_plugin_.name(),
                       save_args.empty() ? "" : " ",
                       escape_operator_args(save_args));
  }

private:
  const printer_plugin& printer_plugin_;
  std::vector<std::string> print_args;
  const saver_plugin& saver_plugin_;
  std::vector<std::string> save_args;
};

class write_plugin final : public virtual operator_plugin {
public:
  auto initialize(const record&, const record&) -> caf::error override {
    return {};
  }

  auto name() const -> std::string override {
    return "write";
  };

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    auto parsed = parsers::name_args_opt_keyword_name_args("to").apply(f, l);
    if (!parsed) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error,
                        fmt::format("failed to parse write operator: '{}'",
                                    pipeline)),
      };
    }
    auto& [printer_name, printer_args, opt_saver] = *parsed;
    const auto* printer = plugins::find<printer_plugin>(printer_name);
    if (not printer) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to find printer "
                                                      "'{}' in pipeline '{}'",
                                                      printer_name, pipeline)),
      };
    }
    if (not opt_saver) {
      opt_saver.emplace(printer->default_saver(printer_args));
    }
    auto& [saver_name, saver_args] = *opt_saver;
    const auto* saver = plugins::find<saver_plugin>(saver_name);
    if (not saver) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to find saver "
                                                      "'{}' in pipeline '{}'",
                                                      saver_name, pipeline)),
      };
    }
    // It could be that `printer->printer_allows_joining()` is false, but
    // `saver->saver_does_joining()` is true. This can cause conflicts later,
    // but `print` contains the neccesary checks and would abort the execution.
    if (not saver->saver_does_joining()) {
      return {
        std::string_view{f, l},
        std::make_unique<print_and_save_operator>(
          *printer, std::move(printer_args), *saver, std::move(saver_args)),
      };
    }
    return {std::string_view{f, l},
            print_save(printer_name, printer_args, saver_name, saver_args)};
  }
};

class to_plugin final : public virtual operator_plugin {
public:
  auto initialize(const record&, const record&) -> caf::error override {
    return {};
  }

  auto name() const -> std::string override {
    return "to";
  };

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::optional_ws_or_comment, parsers::end_of_pipeline_operator,
      parsers::plugin_name, parsers::required_ws_or_comment;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    auto parsed = parsers::name_args_opt_keyword_name_args("write").apply(f, l);
    if (not parsed) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error,
                        fmt::format("failed to parse to operator: '{}'",
                                    pipeline)),
      };
    }
    auto& [saver_name, saver_args, opt_printer] = *parsed;
    const auto* saver = plugins::find<saver_plugin>(saver_name);
    if (not saver) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to find saver "
                                                      "'{}' in pipeline '{}'",
                                                      saver_name, pipeline)),
      };
    }
    if (not opt_printer) {
      opt_printer.emplace(saver->default_printer(saver_args));
    }
    auto& [printer_name, printer_args] = *opt_printer;
    const auto* printer = plugins::find<printer_plugin>(printer_name);
    if (not printer) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to find printer "
                                                      "'{}' in pipeline '{}'",
                                                      printer_name, pipeline)),
      };
    }
    if (saver->saver_does_joining() && not printer->printer_allows_joining()) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::invalid_argument,
                        fmt::format("writing '{0}' to '{1}' is not allowed; "
                                    "the connector '{1}' requires a single "
                                    "input, and the format '{0}' has "
                                    "potentially multiple outputs",
                                    printer_name, saver_name)),
      };
    }
    if (not saver->saver_does_joining()) {
      return {
        std::string_view{f, l},
        std::make_unique<print_and_save_operator>(
          *printer, std::move(printer_args), *saver, std::move(saver_args)),
      };
    }
    return {std::string_view{f, l},
            print_save(printer_name, printer_args, saver_name, saver_args)};
  }
};

} // namespace

} // namespace vast::plugins::write_to

VAST_REGISTER_PLUGIN(vast::plugins::write_to::write_plugin)
VAST_REGISTER_PLUGIN(vast::plugins::write_to::to_plugin)
