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
#include <tenzir/series_builder.hpp>

#include <algorithm>

namespace tenzir::plugins::connectors {

namespace {

class plugin final : public virtual aspect_plugin {
public:
  auto name() const -> std::string override {
    return "connectors";
  }

  auto show(operator_control_plane& ctrl) const
    -> generator<table_slice> override {
    TENZIR_UNUSED(ctrl);
    auto loaders = collect(plugins::get<loader_parser_plugin>());
    auto savers = collect(plugins::get<saver_parser_plugin>());
    auto connectors = std::set<std::string>{};
    for (const auto* plugin : loaders) {
      connectors.insert(plugin->name());
    }
    for (const auto* plugin : savers) {
      connectors.insert(plugin->name());
    }
    auto builder = series_builder{type{
      "tenzir.connector",
      record_type{
        {"name", string_type{}},
        {"loader", bool_type{}},
        {"saver", bool_type{}},
      },
    }};
    for (const auto& connector : connectors) {
      auto event = builder.record();
      event.field("name", connector);
      event.field("loader",
                  plugins::find<loader_parser_plugin>(connector) != nullptr);
      event.field("saver",
                  plugins::find<saver_parser_plugin>(connector) != nullptr);
    }
    co_yield builder.finish_assert_one_slice();
  }
};

} // namespace

} // namespace tenzir::plugins::connectors

TENZIR_REGISTER_PLUGIN(tenzir::plugins::connectors::plugin)
