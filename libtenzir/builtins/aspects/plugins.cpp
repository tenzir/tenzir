//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::plugins {

namespace {

class plugin final : public virtual aspect_plugin {
public:
  auto name() const -> std::string override {
    return "plugins";
  }

  auto show(operator_control_plane&) const -> generator<table_slice> override {
    auto builder = series_builder{};
    for (const auto& plugin : tenzir::plugins::get()) {
      auto row = builder.record();
      row.field("name").data(plugin->name());
      auto version
        = std::string{plugin.version() ? plugin.version() : "bundled"};
      row.field("version").data(version);
      row.field("kind").data(fmt::to_string(plugin.type()));
      auto types = row.field("types").list();
#define TENZIR_ADD_PLUGIN_TYPE(type, name)                                     \
  do {                                                                         \
    if (plugin.as<type##_plugin>()) {                                          \
      types.data(name);                                                        \
    }                                                                          \
  } while (false)
      TENZIR_ADD_PLUGIN_TYPE(aggregation_function, "aggregation_function");
      TENZIR_ADD_PLUGIN_TYPE(aspect, "aspect");
      TENZIR_ADD_PLUGIN_TYPE(command, "command");
      TENZIR_ADD_PLUGIN_TYPE(component, "component");
      TENZIR_ADD_PLUGIN_TYPE(context, "context");
      TENZIR_ADD_PLUGIN_TYPE(loader_parser, "loader");
      TENZIR_ADD_PLUGIN_TYPE(metrics, "metrics");
      TENZIR_ADD_PLUGIN_TYPE(operator_parser, "operator");
      TENZIR_ADD_PLUGIN_TYPE(parser_parser, "parser");
      TENZIR_ADD_PLUGIN_TYPE(printer_parser, "printer");
      TENZIR_ADD_PLUGIN_TYPE(rest_endpoint, "rest_endpoint");
      TENZIR_ADD_PLUGIN_TYPE(saver_parser, "saver");
      TENZIR_ADD_PLUGIN_TYPE(store, "store");
      TENZIR_ADD_PLUGIN_TYPE(operator_factory, "tql2.operator");
      TENZIR_ADD_PLUGIN_TYPE(aggregation, "tql2.aggregation_function");
      TENZIR_ADD_PLUGIN_TYPE(function, "tql2.function");
#undef TENZIR_ADD_PLUGIN_TYPE
      auto dependencies = row.field("dependencies").list();
      for (const auto& dependency : plugin.dependencies()) {
        dependencies.data(dependency);
      }
    }
    for (auto&& slice : builder.finish_as_table_slice("tenzir.plugin")) {
      co_yield std::move(slice);
    }
  }
};

} // namespace

} // namespace tenzir::plugins::plugins

TENZIR_REGISTER_PLUGIN(tenzir::plugins::plugins::plugin)
