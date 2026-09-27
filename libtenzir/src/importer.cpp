//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/importer.hpp"

#include "tenzir/fwd.hpp"

#include "tenzir/atoms.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/actor_metrics.hpp"
#include "tenzir/detail/weak_run_delayed.hpp"
#include "tenzir/import_conversion.hpp"
#include "tenzir/import_routing.hpp"
#include "tenzir/import_wire.hpp"
#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/recent_snapshot.hpp"
#include "tenzir/retention_policy.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/status.hpp"
#include "tenzir/table_slice.hpp"

#include <caf/config_value.hpp>
#include <caf/settings.hpp>

#include <algorithm>

namespace tenzir {

importer::importer(importer_actor::pointer self, index_actor index)
  : self{self}, index{std::move(index)} {
}

importer::~importer() noexcept {
  flush();
  for (auto& events : unpersisted_nova_events) {
    for (auto row : nova::storage::true_bits(events.mask)) {
      if (retention_policy.should_be_persisted(
            *events.meta.name.get(row), *events.meta.internal.get(row))) {
        self->mail(std::move(events)).send(index);
      }
      break;
    }
  }
  for (auto token : snapshot_barriers) {
    self->mail(atom::resume_v, token).send(index);
  }
}

void importer::handle_slice(table_slice&& slice) {
  const auto rows = slice.rows();
  TENZIR_ASSERT(rows > 0);
  auto record_batch = to_record_batch(slice);
  if (type::from_arrow(*record_batch->schema()) != slice.schema()) {
    TENZIR_ERROR("{} drops malformed slice because its Arrow types differ "
                 "from its table slice schema",
                 *self);
    return;
  }
  auto validation = record_batch->ToStructArray();
  if (not validation.ok()) {
    TENZIR_ERROR("{} drops malformed slice because it failed validation: {}",
                 *self, validation.status().ToStringWithoutContextLines());
    return;
  }
  const auto is_internal = slice.schema().attribute("internal").has_value();
  auto events = Option<table_slice>{};
  for (const auto& subscriber : subscribers) {
    if (subscriber.eager and subscriber.internal == is_internal) {
      if (not events) {
        events.emplace(slice);
        events->import_time(time::clock::now());
      }
      self->mail(*events).send(subscriber.receiver);
    }
  }
  for (auto const& subscriber : nova_subscribers) {
    if (subscriber.eager and subscriber.internal == is_internal) {
      if (not events) {
        events.emplace(slice);
        events->import_time(time::clock::now());
      }
      auto converted = import_table_slice(*events);
      if (converted) {
        self->mail(std::move(converted).unwrap()).send(subscriber.receiver);
      } else {
        TENZIR_ERROR("{} cannot adapt imported slice for event subscriber: {}",
                     *self, converted.unwrap_err());
      }
    }
  }
  auto schema = slice.schema();
  auto it = unpersisted_events.find(schema);
  if (it == unpersisted_events.end()) {
    it
      = unpersisted_events.emplace_hint(it, schema, std::vector<table_slice>{});
    if (import_buffer_timeout != duration::zero()) {
      self->run_delayed_weak(import_buffer_timeout,
                             [this, schema = std::move(schema)]() mutable {
                               flush(std::move(schema));
                             });
    }
  }
  it->second.push_back(std::move(slice));
  if (import_buffer_timeout == duration::zero()) {
    flush(std::move(schema));
  }
}

void importer::flush(Option<type> schema) {
  const auto do_flush = [&](std::vector<table_slice> events,
                            const bool is_internal) {
    auto concat_buffer_size = size_t{0};
    auto concat_buffer = std::vector<table_slice>{};
    const auto rotate_buffer = [&] {
      auto events_result = try_concatenate(std::move(concat_buffer));
      if (not events_result) {
        // At least one buffered slice is malformed (e.g. its record batch
        // disagrees with its schema). We cannot tell which one, so we
        // drop this buffer: losing one buffer of events is preferable to
        // terminating the importer and with it the whole node.
        TENZIR_ERROR("{} drops buffer of {} events because they failed "
                     "to concatenate: {}",
                     *self, concat_buffer_size, events_result.error());
        concat_buffer_size = 0;
        concat_buffer.clear();
        return;
      }
      auto events = std::move(*events_result);
      TENZIR_ASSERT(events.rows() > 0);
      events.import_time(time::clock::now());
      if (not is_internal) {
        schema_counters[events.schema()] += events.rows();
      }
      for (const auto& subscriber : subscribers) {
        if (not subscriber.eager and is_internal == subscriber.internal) {
          self->mail(events).send(subscriber.receiver);
        }
      }
      for (auto const& subscriber : nova_subscribers) {
        if (not subscriber.eager and subscriber.internal == is_internal) {
          auto converted = import_table_slice(events);
          if (converted) {
            self->mail(std::move(converted).unwrap()).send(subscriber.receiver);
          } else {
            TENZIR_ERROR("{} cannot adapt buffered slice for event "
                         "subscriber: {}",
                         *self, converted.unwrap_err());
          }
        }
      }
      if (retention_policy.should_be_persisted(events)) {
        self->mail(events).send(index);
      }
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

void importer::flush_nova() {
  if (unpersisted_nova_events.empty()) {
    return;
  }
  ++nova_buffer_generation;
  auto buffered
    = std::exchange(unpersisted_nova_events, std::vector<nova::Events>{});
  unpersisted_nova_bytes = 0;
  for (auto& events : buffered) {
    auto first = std::string{};
    auto internal = false;
    for (auto row : nova::storage::true_bits(events.mask)) {
      first = std::string{*events.meta.name.get(row)};
      internal = *events.meta.internal.get(row);
      break;
    }
    for (auto const& subscriber : nova_subscribers) {
      if (not subscriber.eager and subscriber.internal == internal) {
        self->mail(events).send(subscriber.receiver);
      }
    }
    if (not retention_policy.should_be_persisted(first, internal)) {
      continue;
    }
    ++pending_nova_requests;
    self->mail(std::move(events))
      .request(index, caf::infinite)
      .then(
        [this] {
          --pending_nova_requests;
          finish_nova_flush();
        },
        [this](caf::error error) {
          if (not nova_error.valid()) {
            nova_error = std::move(error);
          }
          --pending_nova_requests;
          finish_nova_flush();
        });
  }
}

void importer::finish_nova_flush() {
  if (pending_nova_requests != 0) {
    return;
  }
  for (auto& waiter : nova_accept_waiters) {
    if (nova_error.valid()) {
      waiter.deliver(nova_error);
    } else {
      waiter.deliver();
    }
  }
  nova_accept_waiters.clear();
  if (nova_flush_waiters.empty()) {
    return;
  }
  auto waiters
    = std::make_shared<std::vector<caf::typed_response_promise<void>>>(
      std::exchange(nova_flush_waiters,
                    std::vector<caf::typed_response_promise<void>>{}));
  auto error = std::exchange(nova_error, caf::error{});
  self->mail(atom::flush_v)
    .request(index, caf::infinite)
    .then(
      [waiters, error]() mutable {
        for (auto& waiter : *waiters) {
          if (error.valid()) {
            waiter.deliver(error);
          } else {
            waiter.deliver();
          }
        }
      },
      [waiters](caf::error error) mutable {
        for (auto& waiter : *waiters) {
          waiter.deliver(error);
        }
      });
}

void importer::release_snapshot_barrier(uuid token) {
  if (snapshot_barriers.erase(token) != 0) {
    self->mail(atom::resume_v, token).send(index);
  }
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
  if (auto limit = try_get_only<int64_t>(config, "tenzir.max-buffered-bytes")) {
    auto bytes = *limit ? **limit : int64_t{1} << 30;
    if (bytes <= 0) {
      self->quit(diagnostic::error("`tenzir.max-buffered-bytes` must be "
                                   "positive")
                   .to_error());
      return importer_actor::behavior_type::make_empty_behavior();
    }
    max_unpersisted_nova_bytes = static_cast<size_t>(
      std::min<int64_t>(int64_t{16} * 1024 * 1024, bytes / 2));
  } else {
    self->quit(std::move(limit.error()));
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
      for (auto const& [name, count] : nova_name_counters) {
        auto event = builder.record();
        event.field("timestamp", now);
        event.field("schema", name);
        event.field("schema_id", "");
        event.field("events", count);
      }
      nova_name_counters.clear();
      auto slice = builder.finish_assert_one_slice();
      if (slice.rows() == 0) {
        return;
      }
      handle_slice(std::move(slice));
    });
  return {
    [this](atom::flush) -> caf::result<void> {
      flush();
      flush_nova();
      auto rp = self->make_response_promise<void>();
      nova_flush_waiters.push_back(rp);
      finish_nova_flush();
      return rp;
    },
    [this](table_slice& slice) -> caf::result<void> {
      handle_slice(std::move(slice));
      return {};
    },
    [this](ImportWireBatch& batch) -> caf::result<void> {
      auto const now = time::clock::now();
      auto converted = from_import_wire(batch);
      if (not converted) {
        return caf::make_error(ec::type_clash,
                               std::move(converted).unwrap_err());
      }
      auto events = std::move(converted).unwrap();
      auto times = nova::ArrayBuilder<nova::Time>{};
      for (auto i = nova::storage::Index{0}; i < events.length(); ++i) {
        auto timestamp = *events.meta.import_time.get(i);
        times.data(timestamp == time{} ? now : timestamp);
      }
      events.meta.import_time = times.finish();
      auto grouped = group_import_shapes(events);
      if (not grouped) {
        return caf::make_error(ec::type_clash, std::move(grouped).unwrap_err());
      }
      for (auto& group : grouped.unwrap()) {
        if (group.key.fields.empty()) {
          continue;
        }
        if (not group.key.internal) {
          nova_name_counters[group.key.name] += group.mask.true_count();
        }
        auto selected = events;
        selected.mask = std::move(group.mask);
        for (auto const& subscriber : nova_subscribers) {
          if (subscriber.eager and subscriber.internal == group.key.internal) {
            self->mail(selected).send(subscriber.receiver);
          }
        }
        if (unpersisted_nova_events.empty()
            and import_buffer_timeout != duration::zero()) {
          auto generation = nova_buffer_generation;
          self->run_delayed_weak(import_buffer_timeout, [this, generation] {
            if (nova_buffer_generation == generation) {
              flush_nova();
            }
          });
        }
        unpersisted_nova_events.push_back(std::move(selected));
        unpersisted_nova_bytes += unpersisted_nova_events.back().approx_bytes();
      }
      if (import_buffer_timeout == duration::zero()
          or unpersisted_nova_bytes >= max_unpersisted_nova_bytes) {
        flush_nova();
      }
      if (pending_nova_requests > 0) {
        auto rp = self->make_response_promise<void>();
        nova_accept_waiters.push_back(rp);
        return rp;
      }
      if (nova_error.valid()) {
        return nova_error;
      }
      return {};
    },
    [this](atom::get, receiver_actor<table_slice>& subscriber, bool internal,
           bool live, bool recent,
           bool eager) -> caf::result<std::vector<table_slice>> {
      auto rp = self->make_response_promise<std::vector<table_slice>>();
      if (live) {
        self->monitor(subscriber, [this, source = subscriber->address()](
                                    const caf::error&) {
          const auto it = std::remove_if(
            subscribers.begin(), subscribers.end(), [&](const auto& sub) {
              return sub.receiver.address() == source;
            });
          subscribers.erase(it, subscribers.end());
        });
        subscribers.emplace_back(std::move(subscriber), internal, eager);
        if (eager) {
          for (const auto& [schema, buffered] : unpersisted_events) {
            if (schema.attribute("internal").has_value() != internal) {
              continue;
            }
            for (const auto& slice : buffered) {
              auto events = slice;
              events.import_time(time::clock::now());
              self->mail(events).send(subscribers.back().receiver);
            }
          }
        }
      }
      // We must call the index ourselves here in order to return a consistent
      // state if both `live` and `recent` are set.
      if (recent) {
        self->mail(atom::get_v, internal)
          .request(index, caf::infinite)
          .then(
            [rp](std::vector<table_slice>& events) mutable {
              rp.deliver(std::move(events));
            },
            [rp](const caf::error& err) mutable {
              rp.deliver(err);
            });
      } else {
        rp.deliver(std::vector<table_slice>{});
      }
      return rp;
    },
    [this](atom::get, atom::snapshot, receiver_actor<table_slice>& subscriber,
           bool internal, bool live, bool recent,
           bool eager) -> caf::result<recent_snapshot> {
      if (not recent) {
        return caf::make_error(ec::logic_error,
                               "snapshot subscription requires recent data");
      }
      auto rp = self->make_response_promise<recent_snapshot>();
      auto buffered = std::vector<table_slice>{};
      for (auto const& [schema, slices] : unpersisted_events) {
        auto is_internal = schema.attribute("internal").has_value();
        if (is_internal == internal
            and not retention_policy.should_be_persisted(schema.name(),
                                                         is_internal)) {
          buffered.insert(buffered.end(), slices.begin(), slices.end());
        }
      }
      flush();
      flush_nova();
      auto token = uuid::random();
      self->mail(atom::pause_v, token)
        .request(index, caf::infinite)
        .await(
          [this, rp, token, subscriber, internal, live, eager,
           buffered = std::move(buffered)]() mutable {
            snapshot_barriers.insert(token);
            self->monitor(subscriber,
                          [this, token,
                           source = subscriber.address()](caf::error const&) {
                            std::erase_if(subscribers, [&](auto const& sub) {
                              return sub.receiver.address() == source;
                            });
                            release_snapshot_barrier(token);
                          });
            if (live) {
              subscribers.emplace_back(subscriber, internal, eager);
            }
            auto snapshot = std::make_shared<recent_snapshot>(
              recent_snapshot{std::move(buffered), token});
            self->mail(atom::get_v, internal)
              .request(index, caf::infinite)
              .then(
                [rp, snapshot](std::vector<table_slice> events) mutable {
                  snapshot->events.insert(
                    snapshot->events.end(),
                    std::make_move_iterator(events.begin()),
                    std::make_move_iterator(events.end()));
                  rp.deliver(std::move(*snapshot));
                },
                [this, rp, token](caf::error error) mutable {
                  release_snapshot_barrier(token);
                  rp.deliver(std::move(error));
                });
          },
          [rp](caf::error error) mutable {
            rp.deliver(std::move(error));
          });
      return rp;
    },
    [this](atom::get, atom::snapshot, receiver_actor<nova::Events>& subscriber,
           bool internal, bool live, bool recent,
           bool eager) -> caf::result<NovaRecentSnapshot> {
      if (not recent) {
        return caf::make_error(ec::logic_error,
                               "snapshot subscription requires recent data");
      }
      auto rp = self->make_response_promise<NovaRecentSnapshot>();
      auto buffered = std::vector<nova::Events>{};
      for (auto const& [schema, slices] : unpersisted_events) {
        auto is_internal = schema.attribute("internal").has_value();
        if (is_internal != internal
            or retention_policy.should_be_persisted(schema.name(),
                                                    is_internal)) {
          continue;
        }
        for (auto const& slice : slices) {
          auto converted = import_table_slice(slice);
          if (not converted) {
            return caf::make_error(ec::type_clash,
                                   std::move(converted).unwrap_err());
          }
          buffered.push_back(std::move(converted).unwrap());
        }
      }
      for (auto const& events : unpersisted_nova_events) {
        for (auto row : nova::storage::true_bits(events.mask)) {
          if (*events.meta.internal.get(row) == internal
              and not retention_policy.should_be_persisted(
                *events.meta.name.get(row), internal)) {
            buffered.push_back(events);
          }
          break;
        }
      }
      flush();
      flush_nova();
      auto token = uuid::random();
      self->mail(atom::pause_v, token)
        .request(index, caf::infinite)
        .await(
          [this, rp, token, subscriber, internal, live, eager,
           buffered = std::move(buffered)]() mutable {
            snapshot_barriers.insert(token);
            self->monitor(
              subscriber,
              [this, token, source = subscriber.address()](caf::error const&) {
                std::erase_if(nova_subscribers, [&](auto const& sub) {
                  return sub.receiver.address() == source;
                });
                release_snapshot_barrier(token);
              });
            if (live) {
              nova_subscribers.emplace_back(subscriber, internal, eager);
            }
            auto snapshot = std::make_shared<NovaRecentSnapshot>(
              NovaRecentSnapshot{std::move(buffered), token});
            auto pending = std::make_shared<size_t>(2);
            auto first_error = std::make_shared<caf::error>();
            auto finish
              = [this, rp, token, snapshot, pending, first_error]() mutable {
                  if (--*pending != 0) {
                    return;
                  }
                  if (first_error->valid()) {
                    release_snapshot_barrier(token);
                    rp.deliver(*first_error);
                  } else {
                    rp.deliver(std::move(*snapshot));
                  }
                };
            self->mail(atom::get_v, atom::internal_v, internal)
              .request(index, caf::infinite)
              .then(
                [snapshot, finish](std::vector<nova::Events> events) mutable {
                  snapshot->events.insert(
                    snapshot->events.end(),
                    std::make_move_iterator(events.begin()),
                    std::make_move_iterator(events.end()));
                  finish();
                },
                [first_error, finish](caf::error error) mutable {
                  *first_error = std::move(error);
                  finish();
                });
            // Actor metrics still originate as Arrow slices, independently of
            // the retained event batches above.
            self->mail(atom::get_v, internal)
              .request(index, caf::infinite)
              .then(
                [snapshot, first_error,
                 finish](std::vector<table_slice> slices) mutable {
                  for (auto const& slice : slices) {
                    auto converted = import_table_slice(slice);
                    if (not converted) {
                      *first_error = caf::make_error(
                        ec::type_clash, std::move(converted).unwrap_err());
                      break;
                    }
                    snapshot->events.push_back(std::move(converted).unwrap());
                  }
                  finish();
                },
                [first_error, finish](caf::error error) mutable {
                  *first_error = std::move(error);
                  finish();
                });
          },
          [rp](caf::error error) mutable {
            rp.deliver(std::move(error));
          });
      return rp;
    },
    [this](atom::resume, uuid token) -> caf::result<void> {
      if (not snapshot_barriers.contains(token)) {
        return caf::make_error(ec::logic_error,
                               "unknown importer snapshot barrier");
      }
      release_snapshot_barrier(token);
      return {};
    },
    [this](atom::get, atom::internal, receiver_actor<nova::Events>& subscriber,
           bool internal, bool live, bool recent,
           bool eager) -> caf::result<std::vector<nova::Events>> {
      auto rp = self->make_response_promise<std::vector<nova::Events>>();
      if (live) {
        self->monitor(subscriber, [this, source = subscriber->address()](
                                    caf::error const&) {
          auto it
            = std::remove_if(nova_subscribers.begin(), nova_subscribers.end(),
                             [&](auto const& sub) {
                               return sub.receiver.address() == source;
                             });
          nova_subscribers.erase(it, nova_subscribers.end());
        });
        nova_subscribers.emplace_back(std::move(subscriber), internal, eager);
        if (eager) {
          for (auto const& batch : unpersisted_nova_events) {
            for (auto row : nova::storage::true_bits(batch.mask)) {
              if (*batch.meta.internal.get(row) == internal) {
                self->mail(batch).send(nova_subscribers.back().receiver);
              }
              break;
            }
          }
          for (auto const& [schema, buffered] : unpersisted_events) {
            if (schema.attribute("internal").has_value() != internal) {
              continue;
            }
            for (auto const& slice : buffered) {
              auto converted = import_table_slice(slice);
              if (converted) {
                self->mail(std::move(converted).unwrap())
                  .send(nova_subscribers.back().receiver);
              }
            }
          }
        }
      }
      if (not recent) {
        rp.deliver(std::vector<nova::Events>{});
        return rp;
      }
      struct Snapshot {
        std::vector<nova::Events> events;
        size_t pending = 2;
        caf::error error = caf::none;
      };
      auto snapshot = std::make_shared<Snapshot>();
      for (auto const& batch : unpersisted_nova_events) {
        for (auto row : nova::storage::true_bits(batch.mask)) {
          if (*batch.meta.internal.get(row) == internal) {
            snapshot->events.push_back(batch);
          }
          break;
        }
      }
      for (auto const& [schema, buffered] : unpersisted_events) {
        if (schema.attribute("internal").has_value() != internal) {
          continue;
        }
        for (auto const& slice : buffered) {
          auto converted = import_table_slice(slice);
          if (not converted) {
            rp.deliver(caf::make_error(ec::type_clash,
                                       std::move(converted).unwrap_err()));
            return rp;
          }
          snapshot->events.push_back(std::move(converted).unwrap());
        }
      }
      auto finish = [rp, snapshot]() mutable {
        if (--snapshot->pending != 0) {
          return;
        }
        if (snapshot->error.valid()) {
          rp.deliver(snapshot->error);
        } else {
          rp.deliver(std::move(snapshot->events));
        }
      };
      self->mail(atom::get_v, atom::internal_v, internal)
        .request(index, caf::infinite)
        .then(
          [snapshot, finish](std::vector<nova::Events> events) mutable {
            snapshot->events.insert(snapshot->events.end(),
                                    std::make_move_iterator(events.begin()),
                                    std::make_move_iterator(events.end()));
            finish();
          },
          [snapshot, finish](caf::error error) mutable {
            snapshot->error = std::move(error);
            finish();
          });
      self->mail(atom::get_v, internal)
        .request(index, caf::infinite)
        .then(
          [snapshot, finish](std::vector<table_slice> slices) mutable {
            for (auto const& slice : slices) {
              auto converted = import_table_slice(slice);
              if (not converted) {
                snapshot->error = caf::make_error(
                  ec::type_clash, std::move(converted).unwrap_err());
                break;
              }
              snapshot->events.push_back(std::move(converted).unwrap());
            }
            finish();
          },
          [snapshot, finish](caf::error error) mutable {
            snapshot->error = std::move(error);
            finish();
          });
      return rp;
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
