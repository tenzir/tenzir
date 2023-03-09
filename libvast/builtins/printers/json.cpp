//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/plugin.hpp>

namespace vast::plugins::json_printer {

class plugin : public virtual printer_plugin {
public:
  [[nodiscard]] auto
  make_printer(const record&, [[maybe_unused]] type input_schema,
               const operator_control_plane&) const
    -> caf::expected<printer_plugin::printer> override {
    return [](generator<table_slice>) -> generator<chunk_ptr> {
      co_return;
    };
  }

  [[nodiscard]] auto
  make_default_dumper(const record&, [[maybe_unused]] type input_schema,
                      const operator_control_plane&) const
    -> caf::expected<printer_plugin::dumper> override {
    return caf::make_error(ec::unimplemented,
                           "dumper currently not implemented");
  }

  [[nodiscard]] auto printer_allows_joining() const -> bool override {
    return true;
  };

  [[nodiscard]] auto name() const -> std::string override {
    return "json";
  }

  [[nodiscard]] auto initialize([[maybe_unused]] const record& plugin_config,
                                [[maybe_unused]] const record& global_config)
    -> caf::error override {
    return caf::none;
  }
};

} // namespace vast::plugins::json_printer

VAST_REGISTER_PLUGIN(vast::plugins::json_printer::plugin)
