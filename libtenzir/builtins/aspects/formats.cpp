//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/collect.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice_builder.hpp>

#include <algorithm>

namespace tenzir::plugins::formats {

namespace {

/// A type that represents a format.
auto format_type() -> type {
  return type{
    "tenzir.format",
    record_type{
      {"name", string_type{}},
      {"printer", bool_type{}},
      {"parser", bool_type{}},
    },
  };
}

class plugin final : public virtual aspect_plugin {
public:
  auto name() const -> std::string override {
    return "formats";
  }

  auto show(exec_ctx ctx) const -> generator<table_slice> override {
    auto parsers = collect(plugins::get<parser_parser_plugin>());
    auto printers = collect(plugins::get<printer_parser_plugin>());
    auto formats = std::set<std::string>{};
    for (const auto* plugin : parsers)
      formats.insert(plugin->name());
    for (const auto* plugin : printers)
      formats.insert(plugin->name());
    auto builder = table_slice_builder{format_type()};
    for (const auto& format : formats) {
      if (not(builder.add(format)
              && builder.add(plugins::find<parser_parser_plugin>(format)
                             != nullptr)
              && builder.add(plugins::find<printer_parser_plugin>(format)
                             != nullptr))) {
        diagnostic::error("failed to add format").emit(ctrl.diagnostics());
        co_return;
      }
    }
    co_yield builder.finish();
  }
};

} // namespace

} // namespace tenzir::plugins::formats

TENZIR_REGISTER_PLUGIN(tenzir::plugins::formats::plugin)
