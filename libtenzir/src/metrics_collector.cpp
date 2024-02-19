//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/data.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/metrics_collector.hpp>
#include <tenzir/series_builder.hpp>

#include <ranges>

namespace tenzir {

auto metrics_collector_state::collect_and_import_metrics() -> void {
  TENZIR_ASSERT(importer != nullptr);
  // Use a consistent timestamp for all collected metrics.
  auto now = std::chrono::system_clock::now();
  for (auto& [name, collector] : collectors) {
    TENZIR_TRACE("running periodic metrics collection {}", name);
    auto schema_name = fmt::format("tenzir.metrics.{}", name);
    auto result = collector();
    if (!result) {
      return;
    }
    // We can cache the series builders in the state if constructing
    // them here proves expensive.
    auto b
      = series_builder{type{schema_name, record_type{}, {{"internal", ""}}}};
    auto r = b.record();
    r.field("timestamp", now);
    for (auto& [name, data] : *result) {
      r.field(name, data);
    }
    auto slice = b.finish_assert_one_slice();
    self->send(importer, std::move(slice));
  }
}

auto metrics_collector(
  metrics_collector_actor::stateful_pointer<metrics_collector_state> self,
  caf::timespan collection_interval, const node_actor& node,
  importer_actor importer) -> metrics_collector_actor::behavior_type {
  self->state.self = self;
  self->state.node = node;
  self->state.importer = std::move(importer);
  self->state.collection_interval = collection_interval;
  for (auto const* plugin : plugins::get<metrics_plugin>()) {
    auto name = plugin->metric_name();
    auto collector = plugin->make_collector();
    if (!collector) {
      TENZIR_VERBOSE("not activating collector {}: {}", name,
                     collector.error());
      continue;
    }
    self->state.collectors[name] = *collector;
  }
  TENZIR_VERBOSE("starting health collector measurement loop with interval {}",
                 self->state.collection_interval);
  self->send(self, atom::run_v);
  return {
    [self](atom::run) -> caf::result<void> {
      // Do a one-off import immediately.
      self->state.collect_and_import_metrics();
      // Start measurement loop.
      detail::weak_run_delayed_loop(self, self->state.collection_interval,
                                    [self] {
                                      self->state.collect_and_import_metrics();
                                    });
      return {};
    },
    [self](atom::status, status_verbosity, duration) -> caf::result<record> {
      auto result = record{};
      auto names = std::views::keys(self->state.collectors);
      result.emplace("interval", self->state.collection_interval);
      result.emplace("collectors", list{names.begin(), names.end()});
      return result;
    },
  };
}

} // namespace tenzir
