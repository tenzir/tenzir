//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/plugin.hpp>
#include <tenzir/type.hpp>

#include <caf/actor_system.hpp>
#include <caf/fwd.hpp>
#include <caf/telemetry/int_gauge.hpp>
#include <caf/telemetry/metric_registry.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace tenzir::plugins::caf_system {

namespace {

struct metric {
  struct system_t {
    int64_t running_actors = {};
    int64_t queued_messages = {};
    int64_t processed_messages = {};
    int64_t total_processed_messages = {};
    int64_t rejected_messages = {};
    int64_t total_rejected_messages = {};
  };

  struct middleman_t {
    int64_t inbound_messages_size = {};
    int64_t total_inbound_messages_size = {};
    int64_t outbound_messages_size = {};
    int64_t total_outbound_messages_size = {};
    duration serialization_time = {};
    duration total_serialization_time = {};
    duration deserialization_time = {};
    duration total_deserialization_time = {};
  };

  struct actor_t {
    duration processing_time = {};
    duration total_processing_time = {};
    duration mailbox_time = {};
    duration total_mailbox_time = {};
    int64_t mailbox_size = {};
  };

  system_t system = {};
  middleman_t middleman = {};
  std::unordered_map<std::string, actor_t> actors = {};
};

struct caf_collector {
  template <class T>
  void operator()(const caf::telemetry::metric_family* family,
                  const caf::telemetry::metric* metric, const T* impl) {
    if (family->prefix() == "caf.system") {
      if constexpr (std::same_as<T, caf::telemetry::int_gauge>) {
        if (family->name() == "running-actors") {
          result.system.running_actors = impl->value();
          return;
        }
        if (family->name() == "queued-messages") {
          result.system.queued_messages = impl->value();
          return;
        }
      }
      if constexpr (std::same_as<T, caf::telemetry::int_counter>) {
        if (family->name() == "processed-messages") {
          const auto value = impl->value();
          result.system.processed_messages
            = value - result.system.total_processed_messages;
          result.system.total_processed_messages = value;
          return;
        }
        if (family->name() == "rejected-messages") {
          const auto value = impl->value();
          result.system.rejected_messages
            = value - result.system.total_rejected_messages;
          result.system.total_rejected_messages = value;
          return;
        }
      }
    }
    if (family->prefix() == "caf.middleman") {
      if constexpr (std::same_as<T, caf::telemetry::int_histogram>) {
        if (family->name() == "inbound-messages-size") {
          const auto value = impl->sum();
          result.middleman.inbound_messages_size
            = value - result.middleman.total_inbound_messages_size;
          result.middleman.total_inbound_messages_size = value;
          return;
        }
        if (family->name() == "outbound-messages-size") {
          const auto value = impl->sum();
          result.middleman.outbound_messages_size
            = value - result.middleman.total_outbound_messages_size;
          result.middleman.total_outbound_messages_size = value;
          return;
        }
      }
      if constexpr (std::same_as<T, caf::telemetry::dbl_histogram>) {
        if (family->name() == "serialization-time") {
          const auto value = std::chrono::duration_cast<duration>(
            std::chrono::duration<double, std::chrono::seconds::period>{
              impl->sum()});
          result.middleman.serialization_time
            = value - result.middleman.total_serialization_time;
          result.middleman.total_serialization_time = value;
          return;
        }
        if (family->name() == "deserialization-time") {
          const auto value = std::chrono::duration_cast<duration>(
            std::chrono::duration<double, std::chrono::seconds::period>{
              impl->sum()});
          result.middleman.deserialization_time
            = value - result.middleman.total_deserialization_time;
          result.middleman.total_deserialization_time = value;
          return;
        }
      }
    }
    if (family->prefix() == "caf.actor") {
      TENZIR_ASSERT(metric->labels().size() == 1);
      TENZIR_ASSERT(metric->labels().back().name() == "name");
      auto& actor = result.actors[std::string{metric->labels().back().value()}];
      if constexpr (std::same_as<T, caf::telemetry::dbl_histogram>) {
        if (family->name() == "processing-time") {
          const auto value = std::chrono::duration_cast<duration>(
            std::chrono::duration<double, std::chrono::seconds::period>{
              impl->sum()});
          actor.processing_time = value - actor.total_processing_time;
          actor.total_processing_time = value;
          return;
        }
        if (family->name() == "mailbox-time") {
          const auto value = std::chrono::duration_cast<duration>(
            std::chrono::duration<double, std::chrono::seconds::period>{
              impl->sum()});
          actor.mailbox_time = value - actor.total_mailbox_time;
          actor.total_mailbox_time = value;
          return;
        }
      }
      if constexpr (std::same_as<T, caf::telemetry::int_gauge>) {
        if (family->name() == "mailbox-size") {
          actor.mailbox_size = impl->value();
          return;
        }
      }
    }
  }

  auto operator()() -> caf::expected<record> {
    system.metrics().collect(*this);
    auto system_metric = record{
      {"running_actors", result.system.running_actors},
      {"queued_messages", result.system.queued_messages},
      {"processed_messages", result.system.processed_messages},
      {"rejected_messages", result.system.rejected_messages},
    };
    auto middleman_metric = record{
      {"inbound_messages_size", result.middleman.inbound_messages_size},
      {"outbound_messages_size", result.middleman.outbound_messages_size},
      {"serialization_time", result.middleman.serialization_time},
      {"deserialization_time", result.middleman.deserialization_time},
    };
    auto actors_metric = list{};
    actors_metric.reserve(result.actors.size());
    for (const auto& [name, actor] : result.actors) {
      actors_metric.emplace_back(record{
        {"name", name},
        {"processing_time", actor.processing_time},
        {"mailbox_time", actor.mailbox_time},
        {"mailbox_size", actor.mailbox_size},
      });
    }
    return record{
      {"system", std::move(system_metric)},
      {"middleman", std::move(middleman_metric)},
      {"actors",
       actors_metric.empty() ? data{} : data{std::move(actors_metric)}},
    };
  }

  caf::actor_system& system;
  metric result = {};
};

class plugin final : public virtual metrics_plugin {
public:
  auto name() const -> std::string override {
    return "caf";
  }

  auto metric_name() const -> std::string override {
    return "caf";
  }

  auto make_collector(caf::actor_system& system) const
    -> caf::expected<collector> override {
    return caf_collector{system};
  }

  auto metric_layout() const -> record_type override {
    return record_type{
      {"system",
       record_type{
         {"running_actors", int64_type{}},
         {"queued_messages", int64_type{}},
         {"processed_messages", int64_type{}},
         {"rejected_messages", int64_type{}},
       }},
      {"middleman",
       record_type{
         {"inbound_messages_size", int64_type{}},
         {"outbound_messages_size", int64_type{}},
         {"serialization_time", duration_type{}},
         {"deserialization_time", duration_type{}},
       }},
      {"actors",
       list_type{
         record_type{
           {"name", string_type{}},
           {"processing_time", duration_type{}},
           {"mailbox_time", duration_type{}},
           {"mailbox_size", int64_type{}},
           // TODO: caf.actor.stream.* metrics are dysfunctional in CAF v1.0.2:
           // the metric families are set up, but no values are ever registered.
           // Additionally, it is not clear whether the `name` label is
           // referring to the actor's name or the stream's name. In the latter
           // case, this should be moved elsewhere.
           // {"streams",
           //  list_type{
           //    record_type{
           //      {"type", string_type{}},
           //      {"processed_elements", int64_type{}},
           //      {"pushed_elements", int64_type{}},
           //      {"input_buffer_size", int64_type{}},
           //      {"output_buffer_size", int64_type{}},
           //    },
           //  }},
         },
       }},
    };
  }
};

} // namespace

} // namespace tenzir::plugins::caf_system

TENZIR_REGISTER_PLUGIN(tenzir::plugins::caf_system::plugin)
