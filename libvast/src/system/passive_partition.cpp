//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/passive_partition.hpp"

#include "vast/fwd.hpp"

#include "vast/address_synopsis.hpp"
#include "vast/aliases.hpp"
#include "vast/chunk.hpp"
#include "vast/concept/hashable/xxhash.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/table_slice.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/notifying_stream_manager.hpp"
#include "vast/detail/partition_common.hpp"
#include "vast/detail/settings.hpp"
#include "vast/detail/tracepoint.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/fbs/partition.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/fbs/uuid.hpp"
#include "vast/ids.hpp"
#include "vast/legacy_type.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/qualified_record_field.hpp"
#include "vast/synopsis.hpp"
#include "vast/system/indexer.hpp"
#include "vast/system/local_segment_store.hpp"
#include "vast/system/shutdown.hpp"
#include "vast/system/status.hpp"
#include "vast/system/terminate.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_column.hpp"
#include "vast/time.hpp"
#include "vast/value_index.hpp"

#include <caf/attach_continuous_stream_stage.hpp>
#include <caf/broadcast_downstream_manager.hpp>
#include <caf/deserializer.hpp>
#include <caf/error.hpp>
#include <caf/sec.hpp>
#include <flatbuffers/base.h> // FLATBUFFERS_MAX_BUFFER_SIZE
#include <flatbuffers/flatbuffers.h>

#include <filesystem>
#include <memory>
#include <span>

namespace vast::system {

/// Gets the INDEXER at a certain position.
indexer_actor passive_partition_state::indexer_at(size_t position) const {
  VAST_ASSERT(position < indexers.size());
  auto& indexer = indexers[position];
  // Deserialize the value index and spawn a passive_indexer lazily when it is
  // requested for the first time.
  if (!indexer) {
    auto qualified_index = flatbuffer->indexes()->Get(position);
    auto index = qualified_index->index();
    auto data = index->data();
    value_index_ptr state_ptr;
    if (auto error = fbs::deserialize_bytes(data, state_ptr)) {
      VAST_ERROR("{} failed to deserialize indexer at {} with error: "
                 "{}",
                 *self, position, render(error));
      return {};
    }
    indexer = self->spawn(passive_indexer, id, std::move(state_ptr));
  }
  return indexer;
}

const vast::legacy_record_type&
passive_partition_state::combined_layout() const {
  return combined_layout_;
}

const std::unordered_map<std::string, ids>&
passive_partition_state::type_ids() const {
  return type_ids_;
}

caf::error
unpack(const fbs::partition::v0& partition, passive_partition_state& state) {
  // Check that all fields exist.
  if (!partition.uuid())
    return caf::make_error(ec::format_error, //
                           "missing 'uuid' field in partition flatbuffer");
  auto combined_layout = partition.combined_layout();
  if (!combined_layout)
    return caf::make_error(ec::format_error, //
                           "missing 'layouts' field in partition flatbuffer");
  auto store_header = partition.store();
  // If no store_id is set, use the global store for backwards compatibility.
  if (store_header && !store_header->id())
    return caf::make_error(ec::format_error, //
                           "missing 'id' field in partition store header");
  if (store_header && !store_header->data())
    return caf::make_error(ec::format_error, //
                           "missing 'data' field in partition store header");
  state.store_id
    = store_header ? store_header->id()->str() : std::string{"legacy_archive"};
  if (store_header && store_header->data())
    state.store_header = std::span{
      reinterpret_cast<const std::byte*>(store_header->data()->data()),
      store_header->data()->size()};
  auto indexes = partition.indexes();
  if (!indexes)
    return caf::make_error(ec::format_error, //
                           "missing 'indexes' field in partition flatbuffer");
  for (auto qualified_index : *indexes) {
    if (!qualified_index->field_name())
      return caf::make_error(ec::format_error, //
                             "missing field name in qualified index");
    auto index = qualified_index->index();
    if (!index)
      return caf::make_error(ec::format_error, //
                             "missing index name in qualified index");
    if (!index->data())
      return caf::make_error(ec::format_error, "missing data in index");
  }
  if (auto error = unpack(*partition.uuid(), state.id))
    return error;
  state.events = partition.events();
  state.offset = partition.offset();
  state.name = "partition-" + to_string(state.id);
  if (auto error
      = fbs::deserialize_bytes(combined_layout, state.combined_layout_))
    return error;
  // This condition should be '!=', but then we cant deserialize in unit tests
  // anymore without creating a bunch of index actors first. :/
  if (state.combined_layout_.fields.size() < indexes->size()) {
    VAST_ERROR("{} found incoherent number of indexers in deserialized state; "
               "{} fields for {} indexes",
               state.name, state.combined_layout_.fields.size(),
               indexes->size());
    return caf::make_error(ec::format_error, "incoherent number of indexers");
  }
  // We only create dummy entries here, since the positions of the `indexers`
  // vector must be the same as in `combined_layout`. The actual indexers are
  // deserialized and spawned lazily on demand.
  state.indexers.resize(indexes->size());
  VAST_DEBUG("{} found {} indexers for partition {}", state.name,
             indexes->size(), state.id);
  auto type_ids = partition.type_ids();
  for (size_t i = 0; i < type_ids->size(); ++i) {
    auto type_ids_tuple = type_ids->Get(i);
    auto name = type_ids_tuple->name();
    auto ids_data = type_ids_tuple->ids();
    auto& ids = state.type_ids_[name->str()];
    if (auto error = fbs::deserialize_bytes(ids_data, ids))
      return error;
  }
  VAST_DEBUG("{} restored {} type-to-ids mapping for partition {}", state.name,
             state.type_ids_.size(), state.id);
  return caf::none;
}

caf::error unpack(const fbs::partition::v0& x, partition_synopsis& ps) {
  if (!x.partition_synopsis())
    return caf::make_error(ec::format_error, "missing partition synopsis");
  if (!x.type_ids())
    return caf::make_error(ec::format_error, "missing type_ids");
  // The id_range was only added in VAST 2021.08.26, so we fill it
  // from the data in the partition if it does not exist.
  if (!x.partition_synopsis()->id_range())
    return unpack(*x.partition_synopsis(), ps, x.offset(), x.events());
  return unpack(*x.partition_synopsis(), ps);
}

partition_actor::behavior_type passive_partition(
  partition_actor::stateful_pointer<passive_partition_state> self, uuid id,
  store_actor legacy_archive, filesystem_actor filesystem,
  const std::filesystem::path& path) {
  auto id_string = to_string(id);
  self->state.self = self;
  self->state.path = path;
  self->state.archive = legacy_archive;
  self->state.filesystem = filesystem;
  self->state.name = "partition-" + id_string;
  VAST_TRACEPOINT(passive_partition_spawned, id_string.c_str());
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    VAST_DEBUG("{} received EXIT from {} with reason: {}", *self, msg.source,
               msg.reason);
    // Receiving an EXIT message does not need to coincide with the state
    // being destructed, so we explicitly clear the vector to release the
    // references.
    // TODO: We must actor_cast to caf::actor here because 'terminate'
    // operates on 'std::vector<caf::actor>' only. That should probably be
    // generalized in the future.
    auto indexers = std::vector<caf::actor>{};
    indexers.reserve(self->state.indexers.size());
    for (auto&& indexer : std::exchange(self->state.indexers, {}))
      indexers.push_back(caf::actor_cast<caf::actor>(std::move(indexer)));
    if (msg.reason != caf::exit_reason::user_shutdown) {
      self->quit(msg.reason);
      return;
    }
    // When the shutdown was requested by the user (as opposed to the partition
    // just dropping out of the LRU cache), pro-actively remove the indexers.
    terminate<policy::parallel>(self, std::move(indexers))
      .then(
        [=](atom::done) {
          VAST_DEBUG("{} shut down all indexers successfully", *self);
          self->quit();
        },
        [=](const caf::error& err) {
          VAST_ERROR("{} failed to shut down all indexers: {}", *self,
                     render(err));
          self->quit(err);
        });
  });
  // We send a "read" to the fs actor and upon receiving the result deserialize
  // the flatbuffer and switch to the "normal" partition behavior for responding
  // to queries.
  self->request(self->state.filesystem, caf::infinite, atom::mmap_v, path)
    .then(
      [=](chunk_ptr chunk) {
        VAST_TRACE_SCOPE("{} {}", *self, VAST_ARG(chunk));
        VAST_TRACEPOINT(passive_partition_loaded, id_string.c_str());
        if (self->state.partition_chunk) {
          VAST_WARN("{} ignores duplicate chunk", *self);
          return;
        }
        if (!chunk) {
          VAST_ERROR("{} got invalid chunk", *self);
          self->quit();
          return;
        }
        // FlatBuffers <= 1.11 does not correctly use '::flatbuffers::soffset_t'
        // over 'soffset_t' in FLATBUFFERS_MAX_BUFFER_SIZE.
        using ::flatbuffers::soffset_t;
        if (chunk->size() >= FLATBUFFERS_MAX_BUFFER_SIZE) {
          VAST_ERROR("failed to load partition at {} because its size of {} "
                     "exceeds the "
                     "maximum allowed size of {}",
                     path, chunk->size(), FLATBUFFERS_MAX_BUFFER_SIZE);
          return self->quit();
        }
        // Deserialize chunk from the filesystem actor
        auto partition = fbs::GetPartition(chunk->data());
        if (partition->partition_type() != fbs::partition::Partition::v0) {
          VAST_ERROR("{} found partition with invalid version of type: {}",
                     *self, partition->GetFullyQualifiedName());
          self->quit();
          return;
        }
        auto partition_v0 = partition->partition_as_v0();
        self->state.partition_chunk = chunk;
        self->state.flatbuffer = partition_v0;
        if (auto error = unpack(*self->state.flatbuffer, self->state)) {
          VAST_ERROR("{} failed to unpack partition: {}", *self, render(error));
          self->quit(std::move(error));
          return;
        }
        if (self->state.id != id) {
          VAST_ERROR("unexpected ID for passive partition: expected {}, got {}",
                     id, self->state.id);
          self->quit();
          return;
        }
        if (self->state.store_id == "legacy_archive") {
          self->state.store = self->state.archive;
        } else {
          const auto* plugin
            = plugins::find<store_plugin>(self->state.store_id);
          if (!plugin) {
            auto error = caf::make_error(ec::format_error,
                                         "encountered unhandled store backend");
            VAST_ERROR("{} encountered unknown store backend '{}'", *self,
                       self->state.store_id);
            self->quit(std::move(error));
            return;
          }
          auto store = plugin->make_store(filesystem, self->state.store_header);
          if (!store) {
            VAST_ERROR("{} failed to spawn store: {}", *self,
                       render(store.error()));
            self->quit(caf::make_error(ec::system_error, "failed to spawn "
                                                         "store"));
            return;
          }
          self->state.store = *store;
        }
        if (id != self->state.id)
          VAST_WARN("{} encountered partition ID mismatch: restored {}"
                    "from disk, expected {}",
                    *self, self->state.id, id);
        // Delegate all deferred evaluations now that we have the partition chunk.
        VAST_DEBUG("{} delegates {} deferred evaluations", *self,
                   self->state.deferred_evaluations.size());
        for (auto&& [expr, rp] :
             std::exchange(self->state.deferred_evaluations, {}))
          rp.delegate(static_cast<partition_actor>(self), std::move(expr));
      },
      [=](caf::error err) {
        VAST_ERROR("{} failed to load partition: {}", *self, render(err));
        // Deliver the error for all deferred evaluations.
        for (auto&& [expr, rp] :
             std::exchange(self->state.deferred_evaluations, {})) {
          // Because of a deficiency in the typed_response_promise API, we must
          // access the underlying response_promise to deliver the error.
          caf::response_promise& untyped_rp = rp;
          untyped_rp.deliver(static_cast<partition_actor>(self), err);
        }
        // Quit the partition.
        self->quit(std::move(err));
      });
  return {
    [self](vast::query query) -> caf::result<atom::done> {
      VAST_TRACE_SCOPE("{} {}", *self, VAST_ARG(query));
      if (!self->state.partition_chunk)
        return std::get<1>(self->state.deferred_evaluations.emplace_back(
          std::move(query), self->make_response_promise<atom::done>()));
      // We can safely assert that if we have the partition chunk already, all
      // deferred evaluations were taken care of.
      VAST_ASSERT(self->state.deferred_evaluations.empty());
      // Don't handle queries after we already received an exit message, while
      // the terminator is running. Since we require every partition to have at
      // least one indexer, we can use this to check.
      if (self->state.indexers.empty())
        return caf::make_error(ec::system_error, "can not handle query because "
                                                 "shutdown was requested");
      auto rp = self->make_response_promise<atom::done>();
      // Don't bother with the indexers etc. if we already know the ids
      // we want to retrieve.
      if (!query.ids.empty()) {
        if (query.expr != vast::expression{})
          return caf::make_error(ec::invalid_argument, "query may only contain "
                                                       "either expression or "
                                                       "ids");
        rp.delegate(self->state.store, query);
        return rp;
      }
      auto triples = detail::evaluate(self->state, query.expr);
      if (triples.empty())
        return atom::done_v;
      auto eval = self->spawn(evaluator, query.expr, triples);
      self->request(eval, caf::infinite, atom::run_v)
        .then(
          [self, rp, query = std::move(query)](const ids& hits) mutable {
            // TODO: Use the first path if the expression can be evaluated
            // exactly.
            auto* count = caf::get_if<query::count>(&query.cmd);
            if (count && count->mode == query::count::estimate) {
              self->send(count->sink, rank(hits));
              rp.deliver(atom::done_v);
            } else {
              query.ids = hits;
              rp.delegate(self->state.store, std::move(query));
            }
          },
          [rp](caf::error& err) mutable {
            rp.deliver(std::move(err));
          });
      return rp;
    },
    [self](atom::erase) -> caf::result<atom::done> {
      if (!self->state.partition_chunk) {
        VAST_DEBUG("{} skips an erase request", *self);
        return caf::skip;
      }
      VAST_DEBUG("{} received an erase message and deletes {}", *self,
                 self->state.path);
      self
        ->request(self->state.filesystem, caf::infinite, atom::erase_v,
                  self->state.path)
        .then([](atom::done) {},
              [self](const caf::error& err) {
                VAST_WARN("{} failed to delete {}: {}; try deleting manually",
                          *self, self->state.path, err);
              });
      vast::ids all_ids;
      for (const auto& kv : self->state.type_ids_) {
        all_ids |= kv.second;
      }
      return self->delegate(self->state.store, atom::erase_v,
                            std::move(all_ids));
    },
    [self](atom::status, status_verbosity /*v*/) -> record {
      record result;
      if (!self->state.partition_chunk) {
        result["state"] = "waiting for chunk";
        return result;
      }
      result["size"] = self->state.partition_chunk->size();
      size_t mem_indexers = 0;
      for (size_t i = 0; i < self->state.indexers.size(); ++i) {
        if (self->state.indexers[i])
          mem_indexers += sizeof(indexer_state)
                          + self->state.flatbuffer->indexes()
                              ->Get(i)
                              ->index()
                              ->data()
                              ->size();
      }
      result["memory-usage-indexers"] = mem_indexers;
      auto x = self->state.partition_chunk->incore();
      if (!x) {
        result["memory-usage-incore"] = render(x.error());
        result["memory-usage"] = self->state.partition_chunk->size()
                                 + mem_indexers + sizeof(self->state);
      } else {
        result["memory-usage-incore"] = *x;
        result["memory-usage"] = *x + mem_indexers + sizeof(self->state);
      }
      return result;
    },
  };
}

} // namespace vast::system
