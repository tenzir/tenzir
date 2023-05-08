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

class print_operator final : public crtp_operator<print_operator> {
public:
  explicit print_operator(const printer_plugin& printer,
                          std::vector<std::string> args,
                          bool allows_joining) noexcept
    : printer_plugin_{printer},
      args_{std::move(args)},
      allows_joining_{allows_joining} {
  }

  auto operator()(generator<table_slice> input,
                  operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    if (allows_joining_) {
      auto p = printer_plugin_.make_printer(args_, {}, ctrl);
      if (!p) {
        ctrl.abort(caf::make_error(
          ec::print_error,
          fmt::format("failed to initialize printer: {}", p.error())));
        co_return;
      }
      for (auto&& slice : input) {
        for (auto&& chunk : (*p)->process(std::move(slice))) {
          co_yield std::move(chunk);
        }
      }
      for (auto&& chunk : (*p)->finish()) {
        co_yield std::move(chunk);
      }
    } else {
      auto state = std::optional<std::pair<printer_plugin::printer, type>>{};
      for (auto&& slice : input) {
        if (slice.rows() == 0) {
          co_yield {};
          continue;
        }
        if (!state) {
          auto p = printer_plugin_.make_printer(args_, slice.schema(), ctrl);
          if (!p) {
            ctrl.abort(caf::make_error(
              ec::print_error,
              fmt::format("failed to initialize printer: {}", p.error())));
            co_return;
          }
          state = std::pair{std::move(*p), slice.schema()};
        } else if (state->second != slice.schema()) {
          ctrl.abort(caf::make_error(
            ec::logic_error,
            fmt::format("'{}' does not support heterogeneous outputs; cannot "
                        "initialize for '{}' after '{}'",
                        to_string(), printer_plugin_.name(), slice.schema(),
                        state->second)));
          co_return;
        }
        for (auto&& chunk : state->first->process(std::move(slice))) {
          co_yield std::move(chunk);
        }
      }
      if (state)
        for (auto&& chunk : state->first->finish()) {
          co_yield std::move(chunk);
        }
    }
  }

  auto to_string() const noexcept -> std::string override {
    return fmt::format("print {}{}{}", printer_plugin_.name(),
                       args_.empty() ? "" : " ", escape_operator_args(args_));
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
