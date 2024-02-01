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
#define TENZIR_ADD_PLUGIN_TYPE(category)                                       \
  do {                                                                         \
    if (plugin.as<category##_plugin>()) {                                      \
      types.data(#category);                                                   \
    }                                                                          \
  } while (false)
      TENZIR_ADD_PLUGIN_TYPE(analyzer);
      TENZIR_ADD_PLUGIN_TYPE(aggregation_function);
      TENZIR_ADD_PLUGIN_TYPE(aspect);
      TENZIR_ADD_PLUGIN_TYPE(component);
      TENZIR_ADD_PLUGIN_TYPE(command);
      TENZIR_ADD_PLUGIN_TYPE(context);
      TENZIR_ADD_PLUGIN_TYPE(loader_parser);
      TENZIR_ADD_PLUGIN_TYPE(loader_serialization);
      TENZIR_ADD_PLUGIN_TYPE(operator_parser);
      TENZIR_ADD_PLUGIN_TYPE(operator_serialization);
      TENZIR_ADD_PLUGIN_TYPE(rest_endpoint);
      TENZIR_ADD_PLUGIN_TYPE(parser_parser);
      TENZIR_ADD_PLUGIN_TYPE(parser_serialization);
      TENZIR_ADD_PLUGIN_TYPE(printer_parser);
      TENZIR_ADD_PLUGIN_TYPE(printer_serialization);
      TENZIR_ADD_PLUGIN_TYPE(saver_parser);
      TENZIR_ADD_PLUGIN_TYPE(saver_serialization);
      TENZIR_ADD_PLUGIN_TYPE(store);
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
