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
    // TODO: The config does not yet include the plugin configuration, which is
    // a deficit in the `plugin::initialize` API. We should consider adding this
    // information as well.
    return {};
  }

  auto name() const -> std::string override {
    return "config";
  }

  auto show(operator_control_plane&) const -> generator<table_slice> override {
    auto builder = series_builder{};
    builder.data(make_view(config_));
    co_yield builder.finish_assert_one_slice("tenzir.config");
  }

private:
  record config_ = {};
};

} // namespace

} // namespace tenzir::plugins::config

TENZIR_REGISTER_PLUGIN(tenzir::plugins::config::plugin)
