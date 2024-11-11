//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/collect.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/node.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/type.hpp>

#include <caf/typed_event_based_actor.hpp>

#include <filesystem>
#include <ranges>

namespace tenzir::plugins::metrics_collector {

namespace {

// The METRICS COLLECTOR component collects events from metrics plugins and
// forwards them to the IMPORTER actor. Every metrics plugin specifies the
// frequency with which it wants to be polled, and collected metrics are
// forwarded every 30 seconds to the importer.
using metrics_collector_actor
  = typed_actor_fwd<>::extend_with<component_plugin_actor>::unwrap;

struct metrics_collector_state {
  static constexpr auto name = "metrics-collector";

  struct instance {
    std::string name = {};
    series_builder builder = {};
    metrics_plugin::collector collector = {};

    auto run() -> caf::expected<void> {
      auto result = collector();
      if (not result) {
        return std::move(result.error());
      }
      auto event = builder.record();
      event.field("timestamp", time::clock::now());
      for (const auto& [name, data] : *result) {
        event.field(name, data);
      }
      return {};
    }
  };

  metrics_collector_actor::pointer self = {};
  importer_actor importer = {};
  std::vector<instance> instances = {};

  auto setup() -> caf::expected<void> {
    for (const auto* plugin : plugins::get<metrics_plugin>()) {
      auto ok = setup(*plugin);
      if (not ok) {
        return ok.error();
      }
    }
    if (instances.empty()) {
      return {};
    }
    detail::weak_run_delayed_loop(
      self, std::chrono::seconds{30},
      [this] {
        TENZIR_TRACE("{} sends out metrics", *self);
        for (auto& instance : instances) {
          for (auto&& slice : instance.builder.finish_as_table_slice()) {
            self->send(importer, std::move(slice));
          }
        }
      },
      false);
    return {};
  }

  auto setup(const metrics_plugin& plugin) -> caf::expected<void> {
    auto collector = plugin.make_collector();
    if (not collector) {
      TENZIR_WARN("{} failed to set up {} metrics: {}", *self, plugin.name(),
                  collector.error());
      return {};
    }
    const auto layout = plugin.metric_layout();
    TENZIR_ASSERT(layout.num_fields() > 0);
    const auto layout_with_timestamp = layout.transform(
      {{offset{0}, record_type::insert_before({{"timestamp", time_type{}}})}});
    TENZIR_ASSERT(layout_with_timestamp);
    auto schema = type{
      fmt::format("tenzir.metrics.{}", plugin.name()),
      *layout_with_timestamp,
      {{"internal", ""}},
    };
    const auto index = instances.size();
    instances.push_back({
      .name = plugin.name(),
      .builder = series_builder{std::move(schema)},
      .collector = std::move(*collector),
    });
    detail::weak_run_delayed_loop(
      self, plugin.metric_frequency(), [this, index] {
        auto& instance = instances[index];
        if (auto ok = instance.run(); not ok) {
          TENZIR_VERBOSE("{} failed to collect {} metrics: {}", *self,
                         instance.name, ok.error());
        }
      });
    return {};
  }
};

auto metrics_collector(
  metrics_collector_actor::stateful_pointer<metrics_collector_state> self,
  importer_actor importer) -> metrics_collector_actor::behavior_type {
  self->state.self = self;
  self->state.importer = std::move(importer);
  if (const auto ok = self->state.setup(); not ok) {
    self->quit(add_context(ok.error(), "failed to create {}", *self));
    return metrics_collector_actor::behavior_type::make_empty_behavior();
  }
  return {
    [](atom::ping) -> caf::result<void> {
      return {};
    },
  };
}

class plugin final : public virtual component_plugin {
public:
  auto name() const -> std::string override {
    return "metrics-collector";
  }

  auto make_component(node_actor::stateful_pointer<node_state> node) const
    -> component_plugin_actor override {
    auto [importer] = node->state.registry.find<importer_actor>();
    return node->spawn<caf::linked>(metrics_collector, std::move(importer));
  }
};

} // namespace

} // namespace tenzir::plugins::metrics_collector

TENZIR_REGISTER_PLUGIN(tenzir::plugins::metrics_collector::plugin)
