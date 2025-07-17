//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/data.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::config {

namespace {

class plugin final : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "config";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto initialize(const record& plugin_config, const record& global_config)
    -> caf::error override {
    TENZIR_UNUSED(plugin_config);
    config_ = global_config;
    // This one's very noisy and not particularly user-facing, so we hide it.
    config_.erase("caf");
    // This one really shouldn't be exposed.
    if (auto* tenzir = get_if<record>(&config_, "tenzir")) {
      tenzir->erase("secrets");
      tenzir->erase("token");
    }
    // And neither these ones.
    if (auto* platform = get_if<record>(&config_, "plugins.platform")) {
      platform->erase("token");
      platform->erase("tenant-id");
      platform->erase("api-key");
    }
    return {};
  }

  auto make_function(function_plugin::invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    TRY(argument_parser2::function("config").parse(inv, ctx));
    return function_use::make(
      [this](evaluator eval, session ctx) -> multi_series {
        TENZIR_UNUSED(eval, ctx);
        auto builder = series_builder{};
        const auto view = make_view(config_);
        for (auto i = 0; i < eval.length(); ++i) {
          builder.data(view);
        }
        return builder.finish_assert_one_array();
      });
  }

private:
  record config_;
};

} // namespace

} // namespace tenzir::plugins::config

TENZIR_REGISTER_PLUGIN(tenzir::plugins::config::plugin)
