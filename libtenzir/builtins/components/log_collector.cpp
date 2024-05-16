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

namespace tenzir::plugins::log_collector {


struct buffer_log_sink : public log_sink {
    void handle(structured_log_msg const& msg) override {
        messages_.push_back()
    }

private:
  std::vector<structured_log_msg> messages_;
};

struct log_collector_sink : public log_sink {
    void handle(structured_log_msg const& msg) override {

    }

private:
    importer_actor importer_;
};

namespace {

// The LOG COLLECTOR component collects log messages and
// forwards them to the IMPORTER actor.
using log_collector_actor
  = typed_actor_fwd<>::extend_with<component_plugin_actor>::unwrap;

struct log_collector_state {
  static constexpr auto name = "log-collector";

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

  log_collector_actor::pointer self = {};
  importer_actor importer = {};
  std::vector<instance> instances = {};

  auto setup() -> caf::expected<void> {
    exchange_log_sink("log-collector-plugin", log_collector_sink{});
  }

  auto setup(const metrics_plugin& plugin) -> caf::expected<void> {
    // auto collector = plugin.make_collector();
    // if (not collector) {
    //   TENZIR_WARN("{} failed to set up {} metrics: {}", *self, plugin.name(),
    //               collector.error());
    //   return {};
    // }
    // const auto layout = plugin.metric_layout();
    // TENZIR_ASSERT(layout.num_fields() > 0);
    // const auto layout_with_timestamp = layout.transform(
    //   {{offset{0}, record_type::insert_before({{"timestamp", time_type{}}})}});
    // TENZIR_ASSERT(layout_with_timestamp);
    // auto schema = type{
    //   fmt::format("tenzir.metrics.{}", plugin.name()),
    //   *layout_with_timestamp,
    //   {{"internal", ""}},
    // };
    // const auto index = instances.size();
    // instances.push_back({
    //   .name = plugin.name(),
    //   .builder = series_builder{std::move(schema)},
    //   .collector = std::move(*collector),
    // });
    // detail::weak_run_delayed_loop(
    //   self, plugin.metric_frequency(), [this, index] {
    //     auto& instance = instances[index];
    //     if (auto ok = instance.run(); not ok) {
    //       TENZIR_VERBOSE("{} failed to collect {} metrics: {}", *self,
    //                      instance.name, ok.error());
    //     }
    //   });
    // return {};
  }
};

auto log_collector(
  log_collector_actor::stateful_pointer<log_collector_state> self,
  importer_actor importer) -> log_collector_actor::behavior_type {
  self->state.self = self;
  self->state.importer = std::move(importer);
  if (const auto ok = self->state.setup(); not ok) {
    self->quit(add_context(ok.error(), "failed to create {}", *self));
    return log_collector_actor::behavior_type::make_empty_behavior();
  }
  return {
    [](atom::status, status_verbosity, duration) -> caf::result<record> {
      // The `tenzir-ctl status` command is on its way out, so there is no need
      // to implement this.
      return record{};
    },
  };
}

class plugin final : public virtual component_plugin {
public:
  auto name() const -> std::string override {
    return "log-collector";
  }

  virtual auto
  initialize(const record& plugin_config, const record& global_config)
    -> caf::error {
    enabled_ = get_or(record, "log.enable-self-storage");
    // insert temporary buffer until the actor system is up
    if (enabled_)
      add_log_sink("log-collector-plugin", buffered_log_sink{});
    return {};
  }

  auto make_component(node_actor::stateful_pointer<node_state> node) const
    -> component_plugin_actor override {
    if (!enabled_) {
      return {};
    }
    auto [importer] = node->state.registry.find<importer_actor>();
    return node->spawn<caf::linked>(log_collector, std::move(importer));
  }

private:
  bool enabled_ = false;
};

} // namespace

} // namespace tenzir::plugins::log_collector

TENZIR_REGISTER_PLUGIN(tenzir::plugins::log_collector::plugin)
