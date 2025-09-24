//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/heterogeneous_string_hash.hpp"

#include <tenzir/plugin.hpp>
#include <tenzir/type.hpp>

#include <caf/actor_system.hpp>
#include <caf/fwd.hpp>
#include <caf/telemetry/int_gauge.hpp>
#include <caf/telemetry/metric_registry.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <tsl/robin_map.h>

namespace tenzir::plugins::caf_system {

namespace {

class gauge_t {
public:
  auto add(int64_t value) -> void {
    total_ += value;
  }
  auto rotate() -> int64_t {
    return std::exchange(total_, 0);
  }

private:
  int64_t total_ = {};
};

class message_metric_t {
public:
  auto add(int64_t value) -> void {
    total_ += value;
  }
  auto rotate() -> int64_t {
    const auto ret = total_ - last_total_;
    last_total_ = total_;
    total_ = 0;
    return ret;
  }
  auto last_total() -> int64_t {
    return last_total_;
  }

private:
  int64_t last_total_ = {};
  int64_t total_ = {};
};

struct bundled_message_metric_t {
  message_metric_t processed;
  message_metric_t rejected;
};

struct metric {
  struct system_t {
    gauge_t running_actors;
    tsl::robin_map<std::string, gauge_t, detail::heterogeneous_string_hash,
                   detail::heterogeneous_string_equal>
      running_by_name;
    /// Message Metrics for all messages (unnamed + per actor)
    bundled_message_metric_t all_messages;
    /// Message Metrics that did not provide a name label
    bundled_message_metric_t unnamed_messages;
    /// Message Metrics per actor name
    tsl::robin_map<std::string, bundled_message_metric_t,
                   detail::heterogeneous_string_hash,
                   detail::heterogeneous_string_equal>
      messages_by_name;
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

auto get_actor_name(const caf::telemetry::metric* metric)
  -> std::optional<std::string_view> {
  constexpr static auto get_value = [](const auto& label) -> std::string_view {
    return label.name();
  };
  const auto actor_name_it
    = std::ranges::find(metric->labels(), std::string_view{"name"}, get_value);
  if (actor_name_it == metric->labels().end()) {
    return std::nullopt;
  }
  return actor_name_it->value();
}

struct caf_collector {
  void collect_system_counter(const caf::telemetry::metric_family* family,
                              const caf::telemetry::metric* metric,
                              const caf::telemetry::int_counter* impl) {
    message_metric_t bundled_message_metric_t::* member = nullptr;
    if (family->name() == "processed-messages") {
      member = &bundled_message_metric_t::processed;
    } else if (family->name() == "rejected-messages") {
      member = &bundled_message_metric_t::rejected;
    }
    if (member == nullptr) {
      return;
    }
    const auto value = impl->value();
    (result.system.all_messages.*member).add(value);
    const auto actor_name = get_actor_name(metric);
    if (not actor_name) {
      (result.system.unnamed_messages.*member).add(value);
      return;
    }
    auto it = result.system.messages_by_name.find(*actor_name);
    if (it == result.system.messages_by_name.end()) {
      auto inserted = false;
      std::tie(it, inserted)
        = result.system.messages_by_name.try_emplace(std::string{*actor_name});
      TENZIR_ASSERT(inserted);
    }
    auto& m = it.value();
    (m.*member).add(value);
  }

  void collect_system_gauge(const caf::telemetry::metric_family* family,
                            const caf::telemetry::metric* metric,
                            const caf::telemetry::int_gauge* impl) {
    if (family->name() != "running-actors") {
      return;
    }
    const auto value = impl->value();
    result.system.running_actors.add(value);
    const auto actor_name = get_actor_name(metric);
    TENZIR_ASSERT(actor_name);
    auto it = result.system.running_by_name.find(*actor_name);
    if (it == result.system.running_by_name.end()) {
      auto inserted = false;
      std::tie(it, inserted)
        = result.system.running_by_name.try_emplace(std::string{*actor_name});
      TENZIR_ASSERT(inserted);
    }
    it.value().add(value);
  }

  template <class T>
  void operator()(const caf::telemetry::metric_family* family,
                  const caf::telemetry::metric* metric, const T* impl) {
    if (family->prefix() == "caf.system") {
      if constexpr (std::same_as<T, caf::telemetry::int_gauge>) {
        collect_system_gauge(family, metric, impl);
      }
      if constexpr (std::same_as<T, caf::telemetry::int_counter>) {
        collect_system_counter(family, metric, impl);
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

  auto messages_by_actor() -> list {
    auto res = list{};
    res.reserve(result.system.messages_by_name.size() + 1);
    auto& r = as<record>(res.emplace_back(record{}));
    r.reserve(3);
    r.try_emplace("name", caf::none);
    r.try_emplace("processed",
                  result.system.unnamed_messages.processed.rotate());
    r.try_emplace("rejected", result.system.unnamed_messages.rejected.rotate());
    for (auto it = result.system.messages_by_name.begin();
         it != result.system.messages_by_name.end(); ++it) {
      auto& k = it.key();
      auto& v = it.value();
      const auto processed = v.processed.rotate();
      const auto rejected = v.rejected.rotate();
      if (processed == 0 and rejected == 0) {
        continue;
      }
      auto& r = as<record>(res.emplace_back(record{}));
      r.reserve(3);
      r.try_emplace("name", k);
      r.try_emplace("processed", processed);
      r.try_emplace("rejected", rejected);
    }
    return res;
  }

  auto running_by_actor() -> list {
    auto res = list{};
    res.reserve(result.system.running_by_name.size());
    for (auto it = result.system.running_by_name.begin();
         it != result.system.running_by_name.end(); ++it) {
      const auto& k = it.key();
      const auto value = it.value().rotate();
      if (value == 0) {
        continue;
      }
      auto& r = as<record>(res.emplace_back(record{}));
      r.reserve(2);
      r.try_emplace("name", k);
      r.try_emplace("count", value);
    }
    return res;
  }

  auto operator()() -> caf::expected<record> {
    system.metrics().collect(*this);
    auto system_metric = record{
      {"running_actors", result.system.running_actors.rotate()},
      {"running_actors_by_name", running_by_actor()},
      {"all_messages",
       record{
         {"processed", result.system.all_messages.processed.rotate()},
         {"rejected", result.system.all_messages.rejected.rotate()},
       }},
      {"messages_by_actor", messages_by_actor()},
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
         {
           "running_actors_by_name",
           list_type{record_type{
             {"name", string_type{}},
             {"count", int64_type{}},
           }},
         },
         {"all_messages",
          record_type{
            {"processed", int64_type{}},
            {"rejected", int64_type{}},
          }},
         {
           "messages_by_actor",
           list_type{record_type{
             {"name", string_type{}},
             {"processed", int64_type{}},
             {"rejected", int64_type{}},
           }},
         },
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
