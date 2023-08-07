//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/adaptive_table_slice_builder.hpp>
#include <tenzir/argument_parser.hpp>
#include <tenzir/plugin.hpp>

namespace tenzir::plugins::plugins {

namespace {

class plugin final : public virtual aspect_plugin {
public:
  auto name() const -> std::string override {
    return "plugins";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto show(operator_control_plane&) const -> generator<table_slice> override {
    auto builder = adaptive_table_slice_builder{};
    for (const auto& plugin : tenzir::plugins::get()) {
      auto row = builder.push_row();
      auto err = row.push_field("name").add(plugin->name());
      TENZIR_ASSERT_CHEAP(not err);
      auto version
        = std::string{plugin.version() ? plugin.version() : "bundled"};
      err = row.push_field("version").add(version);
      TENZIR_ASSERT_CHEAP(not err);
      auto type = row.push_field("type");
      err = row.push_field("kind").add(fmt::to_string(plugin.type()));
      TENZIR_ASSERT_CHEAP(not err);
      auto types_field = row.push_field("types");
      auto types = types_field.push_list();
#define TENZIR_ADD_PLUGIN_TYPE(category)                                       \
  do {                                                                         \
    if (plugin.as<category##_plugin>()) {                                      \
      err = types.add(#category);                                              \
      TENZIR_ASSERT_CHEAP(not err);                                            \
    }                                                                          \
  } while (false)
      TENZIR_ADD_PLUGIN_TYPE(analyzer);
      TENZIR_ADD_PLUGIN_TYPE(aggregation_function);
      TENZIR_ADD_PLUGIN_TYPE(aspect);
      TENZIR_ADD_PLUGIN_TYPE(component);
      TENZIR_ADD_PLUGIN_TYPE(command);
      TENZIR_ADD_PLUGIN_TYPE(loader_parser);
      TENZIR_ADD_PLUGIN_TYPE(loader_serialization);
      TENZIR_ADD_PLUGIN_TYPE(operator_parser);
      TENZIR_ADD_PLUGIN_TYPE(operator_serialization);
      TENZIR_ADD_PLUGIN_TYPE(language);
      TENZIR_ADD_PLUGIN_TYPE(reader);
      TENZIR_ADD_PLUGIN_TYPE(rest_endpoint);
      TENZIR_ADD_PLUGIN_TYPE(parser_parser);
      TENZIR_ADD_PLUGIN_TYPE(parser_serialization);
      TENZIR_ADD_PLUGIN_TYPE(printer_parser);
      TENZIR_ADD_PLUGIN_TYPE(printer_serialization);
      TENZIR_ADD_PLUGIN_TYPE(saver_parser);
      TENZIR_ADD_PLUGIN_TYPE(saver_serialization);
      TENZIR_ADD_PLUGIN_TYPE(store);
      TENZIR_ADD_PLUGIN_TYPE(writer);
#undef TENZIR_ADD_PLUGIN_TYPE
    }
    auto result = builder.finish();
    auto renamed_schema
      = type{"tenzir.plugin", caf::get<record_type>(result.schema())};
    co_yield cast(std::move(result), renamed_schema);
  }
};

} // namespace

} // namespace tenzir::plugins::plugins

TENZIR_REGISTER_PLUGIN(tenzir::plugins::plugins::plugin)
