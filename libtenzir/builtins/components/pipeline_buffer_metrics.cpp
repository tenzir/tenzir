//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/node.hpp>
#include <tenzir/pipeline_buffer_stats.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/type.hpp>

#include <caf/typed_event_based_actor.hpp>

namespace tenzir::plugins::pipeline_buffer_metrics {

namespace {

// The PIPELINE BUFFER METRICS component polls the pipeline buffer registry
// every second and forwards metrics to the IMPORTER actor.
using pipeline_buffer_metrics_actor
  = typed_actor_fwd<>::extend_with<component_plugin_actor>::unwrap;

struct pipeline_buffer_metrics_state {
  static constexpr auto name = "pipeline-buffer-metrics";

  inline static const auto schema = type{
    "tenzir.metrics.operator_buffers",
    record_type{
      {"timestamp", time_type{}},
      {"pipeline_id", string_type{}},
      {"bytes", uint64_type{}},
      {"events", uint64_type{}},
    },
    {{"internal", ""}},
  };

  pipeline_buffer_metrics_actor::pointer self = {};
  importer_actor importer = {};

  auto emit_metrics() -> void {
    auto snapshot = pipeline_buffer_registry::instance().snapshot();
    if (snapshot.empty()) {
      return;
    }
    auto builder = series_builder{schema};
    const auto now = time::clock::now();
    for (const auto& [id, bytes, events] : snapshot) {
      auto event = builder.record();
      event.field("timestamp", now);
      event.field("pipeline_id", id);
      event.field("bytes", bytes);
      event.field("events", events);
    }
    for (auto&& slice : builder.finish_as_table_slice()) {
      self->mail(std::move(slice)).send(importer);
    }
  }
};

auto pipeline_buffer_metrics(
  pipeline_buffer_metrics_actor::stateful_pointer<pipeline_buffer_metrics_state>
    self,
  importer_actor importer) -> pipeline_buffer_metrics_actor::behavior_type {
  self->state().self = self;
  self->state().importer = std::move(importer);
  detail::weak_run_delayed_loop(self, std::chrono::seconds{1}, [self] {
    self->state().emit_metrics();
  });
  return {
    [](atom::status, status_verbosity, duration) -> caf::result<record> {
      return record{};
    },
  };
}

class plugin final : public virtual component_plugin {
public:
  auto name() const -> std::string override {
    return "pipeline-buffer-metrics";
  }

  auto make_component(node_actor::stateful_pointer<node_state> node) const
    -> component_plugin_actor override {
    auto [importer] = node->state().registry.find<importer_actor>();
    return node->spawn<caf::linked>(pipeline_buffer_metrics,
                                    std::move(importer));
  }
};

} // namespace

} // namespace tenzir::plugins::pipeline_buffer_metrics

TENZIR_REGISTER_PLUGIN(tenzir::plugins::pipeline_buffer_metrics::plugin)
