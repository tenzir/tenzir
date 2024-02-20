//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/metrics_collector.hpp>
#include <tenzir/node.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/type.hpp>

#include <caf/typed_event_based_actor.hpp>

#include <filesystem>

namespace tenzir::plugins::health_collector {

namespace defaults {

inline constexpr auto collection_interval = std::chrono::seconds{60};

} // namespace defaults

namespace {

class plugin final : public virtual component_plugin {
public:
  auto initialize(const record& /*global_config*/, const record& plugin_config)
    -> caf::error override {
    auto maybe_interval
      = try_get_or(plugin_config, "interval", defaults::collection_interval);
    if (!maybe_interval) {
      return maybe_interval.error();
    }
    collection_interval_ = *maybe_interval;
    return {};
  }

  auto name() const -> std::string override {
    return "metrics-collector";
  }

  auto make_component(node_actor::stateful_pointer<node_state> node) const
    -> component_plugin_actor override {
    auto [importer] = node->state.registry.find<importer_actor>();
    return node->spawn(tenzir::metrics_collector, collection_interval_, node,
                       std::move(importer));
  }

private:
  std::chrono::seconds collection_interval_ = {};
};

} // namespace

} // namespace tenzir::plugins::health_collector

TENZIR_REGISTER_PLUGIN(tenzir::plugins::health_collector::plugin)
