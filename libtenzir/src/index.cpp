//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/index.hpp"

#include "tenzir/fwd.hpp"

#include "tenzir/active_partition.hpp"
#include "tenzir/catalog.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/concept/parseable/tenzir/uuid.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/concept/printable/tenzir/bitmap.hpp"
#include "tenzir/concept/printable/tenzir/error.hpp"
#include "tenzir/concept/printable/tenzir/expression.hpp"
#include "tenzir/concept/printable/tenzir/table_slice.hpp"
#include "tenzir/concept/printable/tenzir/uuid.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/data.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/actor_metrics.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/fanout_counter.hpp"
#include "tenzir/detail/fill_status_map.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/weak_run_delayed.hpp"
#include "tenzir/error.hpp"
#include "tenzir/fbs/index.hpp"
#include "tenzir/fbs/partition.hpp"
#include "tenzir/fbs/partition_transform.hpp"
#include "tenzir/fbs/utils.hpp"
#include "tenzir/fbs/uuid.hpp"
#include "tenzir/flatbuffer.hpp"
#include "tenzir/ids.hpp"
#include "tenzir/io/read.hpp"
#include "tenzir/io/save.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/modules.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/plugin/store.hpp"
#include "tenzir/shutdown.hpp"
#include "tenzir/status.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/uuid.hpp"

#include <caf/actor_registry.hpp>
#include <caf/error.hpp>
#include <caf/make_copy_on_write.hpp>
#include <caf/response_promise.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/send.hpp>
#include <flatbuffers/flatbuffers.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <ctime>
#include <deque>
#include <filesystem>
#include <memory>
#include <numeric>
#include <span>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

// clang-format off
//
// # Import
//
// The index splits the "stream" of incoming table slices by schema and forwards
// them to active partitions. It rotates the active partition for each schema
// when the active partition timeout is hit or the partition reached its maximum
// size.
//
//              table slice              table slice                      table slice column
//   importer ----------------> index ---------------> active partition ------------------------> indexer
//                                                                      ------------------------> indexer
//                                                                                ...
//
// # Erase
//
// We currently have two distinct erasure code paths: One externally driven by
// the catalog, which looks at the file system and identifies those partitions
// that shall be removed. This is done by the `atom::erase` handler.
//
// clang-format on

namespace tenzir {

// -- index_state --------------------------------------------------------------

index_state::index_state(index_actor::pointer self) : self{self} {
}

// -- inbound path -----------------------------------------------------------

void index_state::handle_slice(table_slice x) {
  const auto& schema = x.schema();
  auto active_partition = active_partitions.find(schema);
  if (active_partition == active_partitions.end()) {
    auto part = create_active_partition(schema);
    if (not part) {
      self->quit(caf::make_error(ec::logic_error,
                                 fmt::format("{} failed to create active "
                                             "partition: {}",
                                             *self, part.error())));
      return;
    }
    active_partition = *part;
  } else if (active_partition->second.events + x.rows() > partition_capacity) {
    TENZIR_TRACE("{} flushes active partition {} with {}/{} events due to {} "
                 "incoming events",
                 *self, schema, active_partition->second.events,
                 partition_capacity, x.rows());
    decommission_active_partition(schema, {});
    auto part = create_active_partition(schema);
    if (not part) {
      self->quit(caf::make_error(ec::logic_error,
                                 fmt::format("{} failed to create active "
                                             "partition: {}",
                                             *self, part.error())));
      return;
    }
    active_partition = *part;
  }
  TENZIR_ASSERT(active_partition->second.actor);
  buffered_events += x.rows();
  active_partition->second.events += x.rows();
  self->mail(x).send(active_partition->second.actor);
  // Flush the partition that was written to if it exceeds the capacity. We
  // already check above whether the write would exceed the capacity and then
  // flush ahead of time, but this can still happen if the single table slice we
  // just got already exceeds it.
  if (active_partition->second.events >= partition_capacity) {
    TENZIR_TRACE("{} flushes active partition {} with {}/{} events due to {} "
                 "incoming events directly exceeding capacity",
                 *self, schema, active_partition->second.events,
                 partition_capacity, x.rows());
    decommission_active_partition(schema, {});
  }
  // When the total number of events in active partitions exceeds the configured
  // limit, we flush the largest partition repeatedly until we are below it.
  // Note that this might be suboptimal in some cases, for example if the limit
  // is 1000, we have 9 partitions with 100 events, and the final partition is
  // the only one being written to. We then always flush it, limiting it to 100
  // events at a time. Another strategy to try here would be LRU, but that could
  // potentially require flushing many smaller partitions before dropping below
  // the limit.
  while (buffered_events > max_buffered_events) {
    TENZIR_ASSERT(not active_partitions.empty());
    auto max = std::ranges::max_element(active_partitions, std::less<>{},
                                        [](auto& entry) {
                                          return entry.second.events;
                                        });
    TENZIR_VERBOSE("{} flushes active partition {} with {}/{} events due to "
                   "{}/{} buffered events",
                   *self, max->first, max->second.events, partition_capacity,
                   buffered_events, max_buffered_events);
    decommission_active_partition(max->first, {});
  }
}

// -- partition handling -----------------------------------------------------

caf::expected<std::unordered_map<type, active_partition_info>::iterator>
index_state::create_active_partition(const type& schema) {
  TENZIR_ASSERT(taxonomies);
  TENZIR_ASSERT(schema);
  auto id = uuid::random();
  const auto [active_partition, inserted]
    = active_partitions.emplace(schema, active_partition_info{});
  TENZIR_ASSERT(inserted);
  TENZIR_ASSERT(active_partition != active_partitions.end());
  active_partition->second.actor
    = self->spawn(::tenzir::active_partition, schema, id, filesystem,
                  index_opts, synopsis_opts, store_actor_plugin, taxonomies);
  active_partition->second.id = id;
  detail::weak_run_delayed(self, active_partition_timeout, [schema, id, this] {
    const auto it = active_partitions.find(schema);
    if (it == active_partitions.end() or it->second.id != id) {
      // If the partition was already rotated then there's nothing to do for us.
      return;
    }
    TENZIR_TRACE("{} flushes active partition {} with {}/{} {} events "
                 "after {} timeout",
                 *self, it->second.id, it->second.events, partition_capacity,
                 schema, data{active_partition_timeout});
    decommission_active_partition(schema, [this, schema,
                                           id](const caf::error& err) mutable {
      if (err.valid()) {
        TENZIR_WARN("{} failed to flush active partition {} ({}) after {} "
                    "timeout: {}",
                    *self, id, schema, data{active_partition_timeout}, err);
      }
    });
  });
  TENZIR_TRACE("{} created new partition {}", *self, id);
  return active_partition;
}

void index_state::decommission_active_partition(
  type schema, std::function<void(const caf::error&)> completion) {
  // We need to take `schema` by value here because it could be derived from the
  // key in the map which we are now going to erase.
  const auto active_partition = active_partitions.find(schema);
  TENZIR_ASSERT(active_partition != active_partitions.end());
  TENZIR_ASSERT(buffered_events >= active_partition->second.events);
  buffered_events -= active_partition->second.events;
  const auto id = active_partition->second.id;
  const auto actor = std::exchange(active_partition->second.actor, {});
  const auto type = active_partition->first;
  // Move the active partition to the list of unpersisted partitions.
  TENZIR_ASSERT_EXPENSIVE(not unpersisted.contains(id));
  unpersisted.emplace(id, unpersisted_partition_info{
                            .schema = type,
                            .actor = actor,
                            .ref_count = 1,
                            .visible_for_recent = true,
                            .exit_sent = false,
                            .exit_reason = caf::none,
                          });
  active_partitions.erase(active_partition);
  // Persist active partition asynchronously.
  const auto part_path = paths.partition(id);
  const auto synopsis_path = paths.synopsis(id);
  TENZIR_TRACE("{} persists active partition {} to {}", *self, schema,
               part_path);
  self->mail(atom::persist_v, part_path, synopsis_path)
    .request(actor, caf::infinite)
    .then(
      [=, this](partition_synopsis_ptr& ps) {
        TENZIR_TRACE("{} successfully persisted partition {} {}", *self, schema,
                     id);
        // The catalog expects to own the partition synopsis it receives,
        // so we make a copy for the listeners.
        // TODO: We should skip this continuation if we're currently shutting
        // down.
        auto apsv = std::vector<partition_synopsis_pair>{{id, ps}};
        self->mail(atom::merge_v, std::move(apsv))
          .request(catalog, caf::infinite)
          .then(
            [=, this](atom::ok) {
              TENZIR_TRACE("{} inserted partition {} {} to the catalog", *self,
                           schema, id);
              retire_partition(id, caf::exit_reason::normal);
              if (completion) {
                completion(caf::none);
              }
            },
            [=, this](const caf::error& err) {
              TENZIR_ERROR("{} failed to commit partition {} {} to the "
                           "catalog, "
                           "the contained data will not be available for "
                           "queries: {}",
                           *self, schema, id, err);
              retire_partition(id, err);
              if (completion) {
                completion(err);
              }
            });
      },
      [=, this](caf::error& err) {
        TENZIR_ERROR("{} failed to persist partition {} {} and evicts data "
                     "from "
                     "memory to preserve process integrity: {}",
                     *self, schema, id, err);
        retire_partition(id, err);
        if (completion) {
          completion(err);
        }
      });
}

auto index_state::flush() -> caf::typed_response_promise<void> {
  // If we've got nothing to flush we can just exit immediately.
  auto rp = self->make_response_promise<void>();
  if (active_partitions.empty()) {
    rp.deliver();
    return rp;
  }
  auto counter = detail::make_fanout_counter(
    active_partitions.size(),
    [rp]() mutable {
      rp.deliver();
    },
    [rp](caf::error error) mutable {
      rp.deliver(std::move(error));
    });
  // We gather the schemas first before we call decomission active partition
  // on every active partition to avoid iterator invalidation.
  auto schemas = std::vector<type>{};
  schemas.reserve(active_partitions.size());
  for (const auto& [schema, _] : active_partitions) {
    schemas.push_back(schema);
  }
  for (const auto& schema : schemas) {
    decommission_active_partition(schema,
                                  [counter](const caf::error& err) mutable {
                                    if (err.valid()) {
                                      counter->receive_error(err);
                                    } else {
                                      counter->receive_success();
                                    }
                                  });
  }
  return rp;
}

void index_state::pin_recent_partition(const uuid& id) {
  const auto it = unpersisted.find(id);
  TENZIR_ASSERT(it != unpersisted.end());
  TENZIR_ASSERT(it->second.visible_for_recent);
  ++it->second.ref_count;
}

void index_state::unpin_recent_partition(const uuid& id) {
  const auto it = unpersisted.find(id);
  TENZIR_ASSERT(it != unpersisted.end());
  TENZIR_ASSERT(it->second.ref_count > 0);
  --it->second.ref_count;
  if (it->second.ref_count != 0) {
    return;
  }
  if (not it->second.exit_sent) {
    if (it->second.exit_reason.valid()) {
      self->send_exit(it->second.actor, it->second.exit_reason);
    } else {
      self->send_exit(it->second.actor, caf::exit_reason::normal);
    }
  }
  unpersisted.erase(it);
}

void index_state::retire_partition(const uuid& id, caf::error reason) {
  const auto it = unpersisted.find(id);
  TENZIR_ASSERT(it != unpersisted.end());
  it->second.visible_for_recent = false;
  it->second.exit_reason = std::move(reason);
  TENZIR_ASSERT(it->second.ref_count > 0);
  --it->second.ref_count;
  if (it->second.ref_count != 0) {
    return;
  }
  if (not it->second.exit_sent) {
    if (it->second.exit_reason.valid()) {
      self->send_exit(it->second.actor, it->second.exit_reason);
    } else {
      self->send_exit(it->second.actor, caf::exit_reason::normal);
    }
  }
  unpersisted.erase(it);
}

void index_state::drain_retired_partitions(caf::error reason) {
  for (auto& [_, partition] : unpersisted) {
    partition.visible_for_recent = false;
    auto shutdown_reason = reason.valid() ? reason : partition.exit_reason;
    if (shutdown_reason.valid()) {
      self->send_exit(partition.actor, shutdown_reason);
      partition.exit_reason = shutdown_reason;
    } else {
      self->send_exit(partition.actor, caf::exit_reason::normal);
      partition.exit_reason = caf::exit_reason::normal;
    }
    partition.exit_sent = true;
  }
}

// -- introspection ----------------------------------------------------------

std::size_t index_state::memusage() const {
  auto usage = std::size_t{sizeof(*this)};
  for (const auto& [type, partition_info] : active_partitions) {
    usage += as_bytes(type).size() + sizeof(partition_info);
  }
  for (const auto& [id, partition] : unpersisted) {
    usage += sizeof(id) + as_bytes(partition.schema).size() + sizeof(partition);
  }
  return usage;
}

index_actor::behavior_type
index(index_actor::stateful_pointer<index_state> self,
      filesystem_actor filesystem, catalog_actor catalog,
      const std::filesystem::path& dir, std::string store_backend,
      size_t max_buffered_events, size_t partition_capacity,
      duration active_partition_timeout,
      const std::filesystem::path& catalog_dir, index_config index_config) {
  TENZIR_TRACE("index {} {} {} {} {} {}", TENZIR_ARG(self->id()),
               TENZIR_ARG(filesystem), TENZIR_ARG(dir),
               TENZIR_ARG(partition_capacity),
               TENZIR_ARG(active_partition_timeout), TENZIR_ARG(catalog_dir),
               TENZIR_ARG(index_config));
  if (self->getf(caf::scheduled_actor::is_detached_flag)) {
    caf::detail::set_thread_name("tnz.index");
  }
  TENZIR_VERBOSE("{} initializes index in {} with a maximum partition size of "
                 "{} events",
                 *self, dir, partition_capacity);
  self->state().index_opts["cardinality"] = partition_capacity;
  self->state().synopsis_opts = std::move(index_config);
  if (dir != catalog_dir) {
    TENZIR_VERBOSE("{} uses {} for catalog data", *self, catalog_dir);
  }
  // Set members.
  self->state().self = self;
  self->state().store_actor_plugin
    = plugins::find<store_actor_plugin>(store_backend);
  if (not self->state().store_actor_plugin) {
    auto error = caf::make_error(ec::invalid_configuration,
                                 fmt::format("could not find "
                                             "store plugin '{}'",
                                             store_backend));
    TENZIR_ERROR("{}", render(error));
    self->quit(error);
    return index_actor::behavior_type::make_empty_behavior();
  }
  self->state().filesystem = std::move(filesystem);
  self->state().catalog = std::move(catalog);
  self->state().taxonomies = std::make_shared<tenzir::taxonomies>();
  self->state().taxonomies->concepts = modules::concepts();
  self->state().paths = {
    .index_dir = dir,
    .synopsis_dir = catalog_dir,
    .markers_dir = dir / "markers",
    .archive_dir = dir / ".." / "archive",
  };
  self->state().partition_capacity = partition_capacity;
  self->state().max_buffered_events = max_buffered_events;
  self->state().active_partition_timeout = active_partition_timeout;
  detail::weak_run_delayed_loop(
    self, defaults::metrics_interval,
    [self, actor_metrics_builder
           = detail::make_actor_metrics_builder()]() mutable {
      const auto importer
        = self->system().registry().get<importer_actor>("tenzir.importer");
      // There exists a very unlikely scenario where the importer was not
      // spawned within the metrics interval after the index was spawned. The
      // importer requires a handle to the index on startup, and the index
      // needs a handle to the index for forwarding metrics, so we cannot just
      // reverse the startup order here. Instead, we just delay the first
      // metrics until the importer is ready.
      if (not importer) [[unlikely]] {
        return;
      }
      self->mail(detail::generate_actor_metrics(actor_metrics_builder, self))
        .send(importer);
    });
  return {
    [self](table_slice& slice) {
      self->state().handle_slice(std::move(slice));
    },
    [self](atom::get, bool internal) -> caf::result<std::vector<table_slice>> {
      auto rp = self->make_response_promise<std::vector<table_slice>>();
      auto result = std::make_shared<std::vector<table_slice>>();
      auto pending = std::make_shared<size_t>(0);
      auto first_error = std::make_shared<caf::error>();
      auto pinned_partitions = std::make_shared<std::vector<uuid>>();
      auto finish = [result, pending, rp, first_error, pinned_partitions,
                     &state = self->state()]() mutable {
        if (*pending != 0) {
          return;
        }
        for (const auto& id : *pinned_partitions) {
          state.unpin_recent_partition(id);
        }
        pinned_partitions->clear();
        if (first_error->valid()) {
          rp.deliver(*first_error);
          return;
        }
        rp.deliver(std::move(*result));
      };
      auto track_unpersisted_partition
        = [pending, pinned_partitions, &state = self->state()](const uuid& id) {
            state.pin_recent_partition(id);
            pinned_partitions->push_back(id);
            *pending += 1;
          };
      // Collect from active partitions.
      for (const auto& [schema, info] : self->state().active_partitions) {
        if (schema.attribute("internal").has_value() != internal) {
          continue;
        }
        *pending += 1;
        self->mail(atom::get_v)
          .request(info.actor, caf::infinite)
          .then(
            [result, pending,
             finish](std::vector<table_slice>& slices) mutable {
              result->insert(result->end(),
                             std::make_move_iterator(slices.begin()),
                             std::make_move_iterator(slices.end()));
              *pending -= 1;
              finish();
            },
            [pending, first_error, finish](const caf::error& err) mutable {
              if (not first_error->valid()) {
                *first_error = err;
              }
              *pending -= 1;
              finish();
            });
      }
      // Collect from unpersisted partitions (being written to disk).
      for (const auto& [uuid, partition] : self->state().unpersisted) {
        if (not partition.visible_for_recent
            or partition.schema.attribute("internal").has_value() != internal) {
          continue;
        }
        track_unpersisted_partition(uuid);
        self->mail(atom::get_v)
          .request(partition.actor, caf::infinite)
          .then(
            [result, pending,
             finish](std::vector<table_slice>& slices) mutable {
              result->insert(result->end(),
                             std::make_move_iterator(slices.begin()),
                             std::make_move_iterator(slices.end()));
              *pending -= 1;
              finish();
            },
            [pending, first_error, finish](const caf::error& err) mutable {
              if (not first_error->valid()) {
                *first_error = err;
              }
              *pending -= 1;
              finish();
            });
      }
      // If no partitions, return immediately
      finish();
      return rp;
    },
    [self](atom::flush) -> caf::result<void> {
      TENZIR_DEBUG("{} got a flush request from {}", *self,
                   self->current_sender());
      if (self->state().active_partitions.empty()) {
        return {};
      }
      return self->state().flush();
    },
    // -- status_client_actor --------------------------------------------------
    [](atom::status, status_verbosity, duration) -> record {
      return {};
    },
    [self](const caf::exit_msg& msg) {
      TENZIR_VERBOSE("{} received EXIT from {} with reason: {}", *self,
                     msg.source, msg.reason);
      auto perform_shutdown = [self](auto reason) {
        self->state().drain_retired_partitions(reason);
        shutdown<policy::parallel>(self, std::vector<caf::actor>{}, reason);
      };
      self->state().shutting_down = true;
      self->mail(atom::flush_v)
        .request(static_cast<index_actor>(self), std::chrono::minutes{10})
        .then(
          [perform_shutdown, reason = msg.reason]() {
            perform_shutdown(reason);
          },
          [perform_shutdown](caf::error& err) {
            auto diag
              = diagnostic::error(std::move(err)).note("while shutting down");
            if (err == caf::sec::request_timeout) {
              diag
                = std::move(diag).note("shutdown timeout: risk of data loss!");
            }
            perform_shutdown(std::move(diag).to_error());
          });
    },
  };
}

} // namespace tenzir
