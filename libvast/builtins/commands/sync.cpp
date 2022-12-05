//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/concept/printable/to_string.hpp>
#include <vast/concept/printable/vast/data.hpp>
#include <vast/data.hpp>
#include <vast/logger.hpp>
#include <vast/plugin.hpp>
#include <vast/system/connect_to_node.hpp>
#include <vast/system/node_control.hpp>
#include <vast/system/query_status.hpp>
#include <vast/system/report.hpp>
#include <vast/system/signal_monitor.hpp>
#include <vast/system/spawn_or_connect_to_node.hpp>
#include <vast/table_slice.hpp>

#include <caf/attach_continuous_stream_source.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>

#include <queue>

namespace vast::plugins::sync {

namespace {

// vast --endpoint=source-vast:port sync sink-vast:port

struct sink_and_source_state {
  static constexpr auto name = "sink-and-source";

  caf::event_based_actor* self = {};
  caf::stream_source_ptr<caf::broadcast_downstream_manager<table_slice>> source
    = {};
  caf::actor statistics_subscriber = {};
  // TODO: Actually update measurements.
  system::measurement measurement = {};
  std::queue<table_slice> buffer = {};

  void send_report() {
    auto r = system::performance_report{{{std::string{name}, measurement}}};
    measurement = {};
    if (statistics_subscriber)
      self->send(statistics_subscriber, r);
  }
};

caf::behavior sink_and_source(caf::stateful_actor<sink_and_source_state>* self,
                              system::importer_actor destination_importer) {
  self->state.self = self;
  self->state.source = caf::attach_continuous_stream_source(
    self,
    [](caf::unit_t&) {
      // init (nop)
    },
    [self](caf::unit_t&, caf::downstream<table_slice>& out, size_t num) {
      for (size_t i = 0; i < num; ++i) {
        if (self->state.buffer.empty())
          return;
        out.push(std::move(self->state.buffer.back()));
        self->state.buffer.pop();
      }
    },
    [](const caf::unit_t&) -> bool {
      // TODO actually shut down
      return false;
    });
  self->state.source->add_outbound_path(destination_importer);
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    self->state.send_report();
    self->quit(msg.reason);
  });
  return {
    [self](table_slice slice) {
      VAST_DEBUG("{} got: {} events from {}", *self, slice.rows(),
                 self->current_sender());
      self->state.buffer.push(std::move(slice));
    },
    [self](atom::limit, uint64_t max) {
      VAST_WARN("{} ignores limit of {}", *self, max);
    },
    [self](atom::statistics, const caf::actor& statistics_subscriber) {
      VAST_DEBUG("{} sets statistics subscriber to {}", *self,
                 statistics_subscriber);
      self->state.statistics_subscriber = statistics_subscriber;
    },
  };
}

caf::message sync_command(const invocation& inv, caf::actor_system& sys) {
  // Validate arguments.
  if (inv.arguments.size() != 1)
    return caf::make_message(
      caf::make_error(ec::invalid_argument,
                      fmt::format("vast sync [destination] accepts "
                                  "exactly one argument; got '{}' instead",
                                  fmt::join(inv.arguments, " "))));
  // Create a scoped actor for interaction with the actor system and connect to
  // the node.
  auto self = caf::scoped_actor{sys};
  // Get the source node actor.
  auto source_node_opt = system::spawn_or_connect_to_node(
    self, inv.options, content(sys.config()));
  if (auto* err = std::get_if<caf::error>(&source_node_opt))
    return caf::make_message(std::move(*err));
  const auto& source_node
    = std::holds_alternative<system::node_actor>(source_node_opt)
        ? std::get<system::node_actor>(source_node_opt)
        : std::get<scope_linked<system::node_actor>>(source_node_opt).get();
  // Get the destination node actor.
  auto destination_options = inv.options;
  caf::put(destination_options, "vast.endpoint", inv.arguments[0]);
  auto destination_node_opt
    = system::connect_to_node(self, destination_options);
  if (!destination_node_opt)
    return caf::make_message(std::move(destination_node_opt.error()));
  const auto& destination_node = *destination_node_opt;
  // Start signal monitor.
  std::thread sig_mon_thread;
  auto signal_guard = system::signal_monitor::run_guarded(
    sig_mon_thread, sys, defaults::system::signal_monitoring_interval, self);
  // Spawn a continuous unlimited exporter in the source node for all events.
  auto source_exporter_options = inv.options;
  caf::put(source_exporter_options, "vast.export.continuous", true);
  caf::put(source_exporter_options, "vast.export.max-events", size_t{0});
  auto source_spawn_exporter
    = invocation{source_exporter_options, "spawn exporter", {"#type != \"\""}};
  auto source_exporter_opt
    = system::spawn_at_node(self, source_node, source_spawn_exporter);
  if (!source_exporter_opt)
    return caf::make_message(std::move(source_exporter_opt.error()));
  auto source_exporter
    = caf::actor_cast<system::exporter_actor>(std::move(*source_exporter_opt));
  // Get the destination importer actor.
  auto destination_components
    = system::get_node_components<system::importer_actor>(self,
                                                          destination_node);
  if (!destination_components)
    return caf::make_message(std::move(destination_components.error()));
  auto [destination_importer] = std::move(*destination_components);
  // Create our local sink.
  // TODO: create sink and send destination importer so it can set up the
  // stream.
  auto local_sink_and_source
    = self->spawn(sink_and_source, destination_importer);
  // Link ourselves to the exporter until we know that the exporter monitors us
  // to avoid a dead window on ungraceful exits where we leave dangling exporter
  // actors in the node.
  self->link_to(source_exporter);
  caf::error err = caf::none;
  self
    ->request(source_exporter, caf::infinite, atom::sink_v,
              local_sink_and_source)
    .receive(
      [&]() {
        self->monitor(source_exporter);
        self->unlink_from(source_exporter);
      },
      [&](caf::error& error) {
        err = std::move(error);
      });
  if (err)
    return caf::make_message(std::move(err));
  // Register self as the statistics actor.
  self->send(source_exporter, atom::statistics_v, self);
  self->send(local_sink_and_source, atom::statistics_v, self);
  // Start the exporter.
  self->send(source_exporter, atom::run_v);
  // Start the receive-loop.
  auto result = caf::message{caf::none};
  auto waiting_for_final_report = false;
  auto stop = false;
  self
    ->do_receive(
      [&](atom::shutdown, const duration& timeout) {
        VAST_INFO("{} shuts down after {} timeout", inv.full_name,
                  to_string(timeout));
        self->send_exit(source_exporter, caf::exit_reason::user_shutdown);
        self->send_exit(local_sink_and_source, caf::exit_reason::user_shutdown);
        waiting_for_final_report = true;
        result = caf::make_message(caf::make_error(
          ec::timeout, fmt::format("{} shut down after {} timeout",
                                   inv.full_name, to_string(timeout))));
      },
      [&](caf::down_msg& msg) {
        // TODO: Figure out what we need to shut down in which order depending
        // on who we get a down message from.
        VAST_WARN("received DOWN from {}: {}", msg.source, msg.reason);
        stop = true;
      },
      [&]([[maybe_unused]] const system::performance_report& report) {
#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_INFO
        // Log a set of named measurements.
        for (const auto& [name, measurement, _] : report.data) {
          if (auto rate = measurement.rate_per_sec(); std::isfinite(rate))
            VAST_INFO("{} processed {} events at a rate of {} events/sec in {}",
                      name, measurement.events, static_cast<uint64_t>(rate),
                      to_string(measurement.duration));
          else
            VAST_INFO("{} processed {} events", name, measurement.events);
        }
#endif
      },
      [&]([[maybe_unused]] const std::string& name,
          [[maybe_unused]] const system::query_status& query_status) {
#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_INFO
        if (auto rate
            = system::measurement{query_status.runtime, query_status.processed}
                .rate_per_sec();
            std::isfinite(rate))
          VAST_INFO("{} processed {} candidates at a rate of {} candidates/sec "
                    "and shipped {} results in {}",
                    name, query_status.processed, static_cast<uint64_t>(rate),
                    query_status.shipped, to_string(query_status.runtime));
        else
          VAST_INFO("{} processed {} candidates and shipped {} results in {}",
                    name, query_status.processed, query_status.shipped,
                    to_string(query_status.runtime));
#endif
        if (waiting_for_final_report)
          stop = true;
      },
      [&](atom::signal, int signal) {
        VAST_DEBUG("{} got {}", inv.full_name, ::strsignal(signal));
        if (signal == SIGINT || signal == SIGTERM) {
          self->send_exit(source_exporter, caf::exit_reason::user_shutdown);
          self->send_exit(local_sink_and_source,
                          caf::exit_reason::user_shutdown);
        }
      })
    .until([&] {
      return stop;
    });
  return result;
}

class plugin final : public virtual command_plugin {
public:
  plugin() = default;
  ~plugin() override = default;

  caf::error initialize(data) override {
    return caf::none;
  }

  [[nodiscard]] const char* name() const override {
    return "sync";
  }

  [[nodiscard]] std::pair<std::unique_ptr<command>, command::factory>
  make_command() const override {
    auto sync = std::make_unique<command>("sync", "synchronizes two VAST nodes",
                                          command::opts("?vast.sync"));
    auto factory = command::factory{
      {"sync", sync_command},
    };
    return {std::move(sync), std::move(factory)};
  };
};

} // namespace

} // namespace vast::plugins::sync

VAST_REGISTER_PLUGIN(vast::plugins::sync::plugin)
