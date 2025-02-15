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

namespace tenzir::plugins::formats {

namespace {

class plugin final : public virtual aspect_plugin {
public:
  auto name() const -> std::string override {
    return "formats";
  }

  auto show(operator_control_plane& ctrl) const
    -> generator<table_slice> override {
    TENZIR_UNUSED(ctrl);
    auto parsers = collect(plugins::get<parser_parser_plugin>());
    auto printers = collect(plugins::get<printer_parser_plugin>());
    auto formats = std::set<std::string>{};
    for (const auto* plugin : parsers) {
      formats.insert(plugin->name());
    }
    for (const auto* plugin : printers) {
      formats.insert(plugin->name());
    }
    auto builder = series_builder{type{
      "tenzir.format",
      record_type{
        {"name", string_type{}},
        {"printer", bool_type{}},
        {"parser", bool_type{}},
      },
    }};
    for (const auto& format : formats) {
      auto event = builder.record();
      event.field("name", format);
      event.field("printer",
                  plugins::find<printer_parser_plugin>(format) != nullptr);
      event.field("parser",
                  plugins::find<parser_parser_plugin>(format) != nullptr);
    }
    co_yield builder.finish_assert_one_slice();
  }
};

} // namespace

} // namespace tenzir::plugins::formats

TENZIR_REGISTER_PLUGIN(tenzir::plugins::formats::plugin)
