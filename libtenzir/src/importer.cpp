//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/importer.hpp"

#include "tenzir/fwd.hpp"

#include "tenzir/atoms.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/actor_metrics.hpp"
#include "tenzir/detail/weak_run_delayed.hpp"
#include "tenzir/retention_policy.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/status.hpp"
#include "tenzir/table_slice.hpp"

#include <caf/config_value.hpp>
#include <caf/settings.hpp>

namespace tenzir {

importer::importer(importer_actor::pointer self, index_actor index)
  : self{self}, index{std::move(index)} {
}

importer::~importer() noexcept {
  flush();
}

void importer::handle_slice(table_slice&& slice) {
  const auto rows = slice.rows();
  TENZIR_ASSERT(rows > 0);
  // If we're in "unbuffered" mode, we flush immediately.
  if (import_buffer_timeout == duration::zero()) {
    flush();
    return;
  }
  // Otherwise, we buffer the slice first, and register it for flushing after
  // some timeout.
  auto schema = slice.schema();
  auto it = unpersisted_events.find(schema);
  if (it == unpersisted_events.end()) {
    it
      = unpersisted_events.emplace_hint(it, schema, std::vector<table_slice>{});
    self->run_delayed_weak(import_buffer_timeout,
                           [this, schema = std::move(schema)]() mutable {
                             flush(std::move(schema));
                           });
  }
  it->second.push_back(std::move(slice));
}

void importer::flush(std::optional<type> schema) {
  const auto do_flush
    = [&](std::vector<table_slice> events, const bool is_internal) {
        auto concat_buffer_size = size_t{0};
        auto concat_buffer = std::vector<table_slice>{};
        const auto rotate_buffer = [&] {
          auto events = concatenate(std::move(concat_buffer));
          TENZIR_ASSERT(events.rows() > 0);
          events.import_time(time::clock::now());
          if (not is_internal) {
            schema_counters[events.schema()] += events.rows();
          }
          for (const auto& [subscriber, wants_internal] : subscribers) {
            if (is_internal == wants_internal) {
              self->mail(events).send(subscriber);
            }
          }
          if (retention_policy.should_be_persisted(events)) {
            self->mail(events).send(index);
          }
          recent_events.push_back(std::move(events));
          concat_buffer_size = 0;
          concat_buffer.clear();
        };
        for (auto& slice : events) {
          concat_buffer_size += slice.rows();
          concat_buffer.push_back(std::move(slice));
          if (concat_buffer_size >= defaults::import::table_slice_size) {
            rotate_buffer();
          }
        }
        if (concat_buffer_size > 0) {
          rotate_buffer();
        }
      };
  if (schema) {
    const auto it = unpersisted_events.find(*schema);
    if (it != unpersisted_events.end()) {
      do_flush(std::move(it->second),
               it->first.attribute("internal").has_value());
      unpersisted_events.erase(it);
    }
    return;
  }
  for (auto& [schema, events] : unpersisted_events) {
    do_flush(std::move(events), schema.attribute("internal").has_value());
  }
  unpersisted_events.clear();
}

auto importer::make_behavior() -> importer_actor::behavior_type {
  const auto config = check(to<record>(content(self->config())));
  if (auto policy = retention_policy::make(config)) {
    retention_policy = std::move(*policy);
  } else {
    self->quit(std::move(policy.error()));
    return importer_actor::behavior_type::make_empty_behavior();
  }
  if (auto timeout
      = try_get_only<duration>(config, "tenzir.import-buffer-timeout")) {
    if (*timeout) {
      if (**timeout < duration::zero()) {
        self->quit(diagnostic::error("`tenzir.import-buffer-timeout` must be a "
                                     "positive duration")
                     .to_error());
        return importer_actor::behavior_type::make_empty_behavior();
      }
      import_buffer_timeout = std::move(**timeout);
    }
  } else {
    self->quit(std::move(timeout.error()));
    return importer_actor::behavior_type::make_empty_behavior();
  }
  // We call the metrics "ingest" to distinguish them from the "import" metrics;
  // these will disappear again in the future when we rewrite the database
  // component.
  auto builder = series_builder{type{
    "tenzir.metrics.ingest",
    record_type{
      {"timestamp", time_type{}},
      {"schema", string_type{}},
      {"schema_id", string_type{}},
      {"events", uint64_type{}},
    },
    {{"internal"}},
  }};
  detail::weak_run_delayed_loop(
    self, defaults::metrics_interval,
    [this, builder = std::move(builder),
     actor_metrics_builder = detail::make_actor_metrics_builder()]() mutable {
      handle_slice(detail::generate_actor_metrics(actor_metrics_builder, self));
      const auto now = time::clock::now();
      for (const auto& [schema, count] : schema_counters) {
        auto event = builder.record();
        event.field("timestamp", now);
        event.field("schema", schema.name());
        event.field("schema_id", schema.make_fingerprint());
        event.field("events", count);
      }
      schema_counters.clear();
      auto slice = builder.finish_assert_one_slice();
      if (slice.rows() == 0) {
        return;
      }
      handle_slice(std::move(slice));
    });
  // Clean up unpersisted events every second.
  const auto active_partition_timeout
    = caf::get_or(content(self->system().config()),
                  "tenzir.active-partition-timeout",
                  defaults::active_partition_timeout);
  if (active_partition_timeout > std::chrono::minutes{1}) {
    TENZIR_WARN("high `active-partition-timeout` detected: this can lead to "
                "memory usage problems");
  }
  detail::weak_run_delayed_loop(
    self, std::chrono::seconds{1},
    [this, active_partition_timeout] {
      // We clear everything that's older than the active partition timeout plus
      // a fixed 10 seconds to allow some processing to happen. This is an
      // estimate, and it's definitely not a perfect solution, but it's good
      // enough hopefully.
      const auto cutoff = time::clock::now() - active_partition_timeout
                          - std::chrono::seconds{10};
      const auto it
        = std::ranges::find_if(recent_events, [&](const auto& slice) {
            return slice.import_time() > cutoff;
          });
      recent_events.erase(recent_events.begin(), it);
    },
    false);
  return {
    [this](atom::flush) -> caf::result<void> {
      flush();
      return self->mail(atom::flush_v).delegate(index);
    },
    [this](table_slice& slice) -> caf::result<void> {
      handle_slice(std::move(slice));
      return {};
    },
    [this](atom::subscribe, receiver_actor<table_slice>& subscriber,
           bool internal) -> std::vector<table_slice> {
      self->monitor(
        subscriber, [this, source = subscriber->address()](const caf::error&) {
          const auto subscriber
            = std::remove_if(subscribers.begin(), subscribers.end(),
                             [&](const auto& subscriber) {
                               return subscriber.first.address() == source;
                             });
          subscribers.erase(subscriber, subscribers.end());
        });
      subscribers.emplace_back(std::move(subscriber), internal);
      return recent_events;
    },
    // -- status_client_actor --------------------------------------------------
    [](atom::status, status_verbosity, duration) { //
      return record{};
    },
    [this](const caf::exit_msg& msg) {
      self->quit(msg.reason);
    },
  };
}

} // namespace tenzir
