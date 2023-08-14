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

namespace tenzir::plugins::config {

namespace {

class plugin final : public virtual aspect_plugin {
public:
  auto initialize(const record& plugin_config, const record& global_config)
    -> caf::error override {
    (void)plugin_config;
    config_ = global_config;
    // We remove the CAF config section as it's prefilled with some config
    // settings that are irrelevant to users.
    config_.erase("caf");
    return {};
  }

  auto name() const -> std::string override {
    return "config";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto show(operator_control_plane& ctrl) const
    -> generator<table_slice> override {
    auto builder = adaptive_table_slice_builder{};
    if (auto err = builder.add_row(make_view(config_))) {
      diagnostic::error("failed to add config: {}", err)
        .emit(ctrl.diagnostics());
      co_return;
    }
    co_yield builder.finish("tenzir.config");
  }

private:
  record config_ = {};
};

} // namespace

} // namespace tenzir::plugins::config

TENZIR_REGISTER_PLUGIN(tenzir::plugins::config::plugin)
