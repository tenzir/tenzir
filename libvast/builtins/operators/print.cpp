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
#include <utility>

namespace vast::plugins::print {

namespace {

class print_operator final
  : public schematic_operator<print_operator, printer_plugin::printer,
                              generator<chunk_ptr>> {
public:
  explicit print_operator(const printer_plugin& printer,
                          std::vector<std::string> args,
                          bool allows_joining) noexcept
    : printer_plugin_{printer},
      args_{std::move(args)},
      allows_joining_{allows_joining} {
  }

  auto initialize(const type& schema, operator_control_plane& ctrl) const
    -> caf::expected<state_type> override {
    if (not allows_joining_ && last_schema_) {
      // TODO
      return caf::make_error(
        ec::logic_error,
        fmt::format("'{}' does not support heterogeneous outputs; cannot "
                    "initialize for '{}' after '{}'",
                    to_string(), printer_plugin_.name(), schema, last_schema_));
    }
    last_schema_ = schema;
    return printer_plugin_.make_printer(args_, schema, ctrl);
  }

  auto process(table_slice slice, state_type& state) const
    -> output_type override {
    return state(std::move(slice));
  }

  auto to_string() const noexcept -> std::string override {
    return fmt::format("print {}", printer_plugin_.name());
  }

private:
  const printer_plugin& printer_plugin_;
  std::vector<std::string> args_;
  bool allows_joining_;
  mutable type last_schema_ = {};
};

class plugin final : public virtual operator_plugin {
public:
  auto initialize(const record&, const record&) -> caf::error override {
    return {};
  }

  auto name() const -> std::string override {
    return "print";
  };

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    auto parsed = parsers::name_args.apply(f, l);
    if (not parsed) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error,
                        fmt::format("failed to parse print operator: '{}'",
                                    pipeline)),
      };
    }
    auto& [name, args] = *parsed;
    const auto* printer = plugins::find<printer_plugin>(name);
    if (!printer) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::lookup_error,
                        fmt::format("no printer found for '{}'", name)),
      };
    }
    return {
      std::string_view{f, l},
      std::make_unique<print_operator>(*printer, std::move(args),
                                       printer->printer_allows_joining()),
    };
  }
};

} // namespace

} // namespace vast::plugins::print

VAST_REGISTER_PLUGIN(vast::plugins::print::plugin)
