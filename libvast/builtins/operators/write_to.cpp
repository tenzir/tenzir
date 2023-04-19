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
    auto saver = saver_plugin_.make_saver(save_args, schema, ctrl);
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
    for (auto&& x : state.printer(std::move(slice))) {
      state.saver(std::move(x));
    }
    return {};
  }

  auto to_string() const noexcept -> std::string override {
    return fmt::format("write {} to {}", printer_plugin_.name(),
                       saver_plugin_.name());
  }

private:
  const printer_plugin& printer_plugin_;
  std::vector<std::string> print_args;
  const saver_plugin& saver_plugin_;
  std::vector<std::string> save_args;
};

class write_plugin final : public virtual operator_plugin {
public:
  auto initialize([[maybe_unused]] const record& plugin_config,
                  [[maybe_unused]] const record& global_config)
    -> caf::error override {
    return {};
  }

  [[nodiscard]] auto name() const -> std::string override {
    return "write";
  };

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::optional_ws_or_comment, parsers::end_of_pipeline_operator,
      parsers::plugin_name, parsers::required_ws_or_comment;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    // TODO: handle options for saver and printer
    auto saver_options = std::vector<std::string>{};
    auto printer_options = std::vector<std::string>{};
    const auto p = optional_ws_or_comment >> plugin_name
                   >> -(required_ws_or_comment >> "to" >> required_ws_or_comment
                        >> plugin_name)
                   >> optional_ws_or_comment >> end_of_pipeline_operator;
    auto parsed = std::tuple{std::string{}, std::optional<std::string>{}};
    if (!p(f, l, parsed)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error,
                        fmt::format("failed to parse write operator: '{}'",
                                    pipeline)),
      };
    }
    auto& [printer_name, saver_name] = parsed;
    const auto* printer = plugins::find<printer_plugin>(printer_name);
    if (not printer) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to find printer "
                                                      "'{}' in pipeline '{}'",
                                                      printer_name, pipeline)),
      };
    }
    if (not saver_name) {
      std::tie(saver_name, saver_options)
        = printer->default_saver(printer_options);
    }
    const auto* saver = plugins::find<saver_plugin>(*saver_name);
    if (not saver) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to find saver "
                                                      "'{}' in pipeline '{}'",
                                                      saver_name, pipeline)),
      };
    }
    if (saver->saver_requires_joining()
        && not printer->printer_allows_joining()) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::invalid_argument,
                        fmt::format("writing '{0}' to '{1}' is not allowed; "
                                    "the connector '{1}' requires a single "
                                    "input, and the format '{0}' has "
                                    "potentially multiple outputs",
                                    printer_name, *saver_name)),
      };
    }
    if (not saver->saver_requires_joining()) {
      return {
        std::string_view{f, l},
        std::make_unique<print_and_save_operator>(*printer, printer_options,
                                                  *saver, saver_options),
      };
    }
    return {
      std::string_view{f, l},
      // TODO: This ignores options
      pipeline::parse_as_operator(
        fmt::format("print {} | save {}", printer_name, *saver_name)),
    };
  }
};

class to_plugin final : public virtual operator_plugin {
public:
  auto initialize([[maybe_unused]] const record& plugin_config,
                  [[maybe_unused]] const record& global_config)
    -> caf::error override {
    return {};
  }

  [[nodiscard]] auto name() const -> std::string override {
    return "to";
  };

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::optional_ws_or_comment, parsers::end_of_pipeline_operator,
      parsers::plugin_name, parsers::required_ws_or_comment;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    // TODO: handle options for saver and printer
    auto saver_options = std::vector<std::string>{};
    auto printer_options = std::vector<std::string>{};
    const auto p = optional_ws_or_comment >> plugin_name
                   >> -(required_ws_or_comment >> "write"
                        >> required_ws_or_comment >> plugin_name)
                   >> optional_ws_or_comment >> end_of_pipeline_operator;
    auto parsed = std::tuple{std::string{}, std::optional<std::string>{}};
    if (!p(f, l, parsed)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error,
                        fmt::format("failed to parse to operator: '{}'",
                                    pipeline)),
      };
    }
    auto& [saver_name, printer_name] = parsed;
    const auto* saver = plugins::find<saver_plugin>(saver_name);
    if (not saver) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to find saver "
                                                      "'{}' in pipeline '{}'",
                                                      saver_name, pipeline)),
      };
    }
    if (not printer_name) {
      std::tie(printer_name, printer_options)
        = saver->default_printer(saver_options);
    }
    const auto* printer = plugins::find<printer_plugin>(*printer_name);
    if (not printer) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to find printer "
                                                      "'{}' in pipeline '{}'",
                                                      printer_name, pipeline)),
      };
    }
    if (saver->saver_requires_joining()
        && not printer->printer_allows_joining()) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::invalid_argument,
                        fmt::format("writing '{0}' to '{1}' is not allowed; "
                                    "the connector '{1}' requires a single "
                                    "input, and the format '{0}' has "
                                    "potentially multiple outputs",
                                    *printer_name, saver_name)),
      };
    }
    if (not saver->saver_requires_joining()) {
      return {
        std::string_view{f, l},
        std::make_unique<print_and_save_operator>(*printer, printer_options,
                                                  *saver, saver_options),
      };
    }
    return {
      std::string_view{f, l},
      // TODO: This ignores options
      pipeline::parse_as_operator(
        fmt::format("print {} | save {}", *printer_name, saver_name)),
    };
  }
};

} // namespace

} // namespace vast::plugins::write_to

VAST_REGISTER_PLUGIN(vast::plugins::write_to::write_plugin)
VAST_REGISTER_PLUGIN(vast::plugins::write_to::to_plugin)
