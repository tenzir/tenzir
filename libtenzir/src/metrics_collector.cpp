//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/data.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/import_stream.hpp>
#include <tenzir/metrics_collector.hpp>
#include <tenzir/series_builder.hpp>

#include <ranges>

namespace tenzir {

namespace {

auto import_metrics_from(const std::string& name,
                         metrics_plugin::collector const& collector,
                         import_stream& importer,
                         std::chrono::system_clock::time_point now) {
  TENZIR_TRACE("running periodic metrics collection {}", name);
  auto schema_name = fmt::format("tenzir.metrics.{}", name);
  auto result = collector();
  if (!result)
    return;
  // We can cache the series builders in the state if constructing
  // them here proves expensive.
  auto b = series_builder{type{schema_name, record_type{}, {{"internal", ""}}}};
  auto r = b.record();
  r.field("timestamp", now);
  for (auto& [name, data] : *result)
    r.field(name, data);
  auto slice = b.finish_assert_one_slice();
  importer.enqueue(std::move(slice));
}

} // namespace

auto metrics_collector_state::collect_and_import_metrics() -> void {
  TENZIR_ASSERT_CHEAP(importer != nullptr);
  // Use a consistent timestamp for all collected metrics.
  auto now = std::chrono::system_clock::now();
  for (auto& [name, collector] : collectors) {
    import_metrics_from(name, collector, *importer, now);
  }
}

auto metrics_collector(
  metrics_collector_actor::stateful_pointer<metrics_collector_state> self,
  caf::timespan collection_interval, const node_actor& node)
  -> metrics_collector_actor::behavior_type {
  self->state.node = node;
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
      // Build and import stream on first usage.
      auto stream = import_stream::make(self, self->state.node);
      if (!stream) {
        TENZIR_WARN("{} aborts because it failed to create import stream: {}",
                    *self, stream.error());
        return stream.error();
      }
      self->state.importer
        = std::make_unique<import_stream>(std::move(*stream));
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
