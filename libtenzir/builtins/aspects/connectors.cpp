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

namespace tenzir::plugins::connectors {

namespace {

/// A type that represents a connector.
auto connector_type() -> type {
  return type{
    "tenzir.connector",
    record_type{
      {"name", string_type{}},
      {"loader", bool_type{}},
      {"saver", bool_type{}},
    },
  };
}

class plugin final : public virtual aspect_plugin {
public:
  auto name() const -> std::string override {
    return "connectors";
  }

  auto show(exec_ctx ctx) const -> generator<table_slice> override {
    auto loaders = collect(plugins::get<loader_parser_plugin>());
    auto savers = collect(plugins::get<saver_parser_plugin>());
    auto connectors = std::set<std::string>{};
    for (const auto* plugin : loaders)
      connectors.insert(plugin->name());
    for (const auto* plugin : savers)
      connectors.insert(plugin->name());
    auto builder = table_slice_builder{connector_type()};
    for (const auto& connector : connectors) {
      if (not(builder.add(connector)
              && builder.add(plugins::find<loader_parser_plugin>(connector)
                             != nullptr)
              && builder.add(plugins::find<saver_parser_plugin>(connector)
                             != nullptr))) {
        diagnostic::error("failed to add connector").emit(ctrl.diagnostics());
        co_return;
      }
    }
    co_yield builder.finish();
  }
};

} // namespace

} // namespace tenzir::plugins::connectors

TENZIR_REGISTER_PLUGIN(tenzir::plugins::connectors::plugin)
