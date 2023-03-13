//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/plugin.hpp>

namespace vast::plugins::stdout_dumper {

// -- loader plugin -----------------------------------------------------
class plugin : public virtual dumper_plugin {
public:
  [[nodiscard]] auto
  make_dumper(const record&, [[maybe_unused]] type input_schema,
              const operator_control_plane&) const
    -> caf::expected<dumper> override {
    return caf::make_error(ec::unimplemented,
                           "dumper currently not implemented");
  }

  [[nodiscard]] auto
  make_default_printer(const record&, [[maybe_unused]] type input_schema,
                       const operator_control_plane&) const
    -> caf::expected<printer> override {
    return caf::make_error(ec::unimplemented,
                           "printer currently not implemented");
  }

  [[nodiscard]] auto initialize([[maybe_unused]] const record& plugin_config,
                                [[maybe_unused]] const record& global_config)
    -> caf::error override {
    return caf::none;
  }

  [[nodiscard]] auto name() const -> std::string override {
    return "stdout";
  }

  [[nodiscard]] auto dumper_requires_joining() const -> bool override {
    return true;
  }
};

} // namespace vast::plugins::stdout_dumper

VAST_REGISTER_PLUGIN(vast::plugins::stdout_dumper::plugin)
