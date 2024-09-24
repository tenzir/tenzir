//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/passive_partition.hpp"

#include "tenzir/fwd.hpp"

#include "tenzir/aliases.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/partition_common.hpp"
#include "tenzir/detail/tracepoint.hpp"
#include "tenzir/fbs/partition.hpp"
#include "tenzir/fbs/utils.hpp"
#include "tenzir/ids.hpp"
#include "tenzir/indexer.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/shutdown.hpp"
#include "tenzir/status.hpp"
#include "tenzir/terminate.hpp"
#include "tenzir/type.hpp"
#include "tenzir/value_index.hpp"

#include <caf/deserializer.hpp>
#include <caf/error.hpp>
#include <caf/sec.hpp>
#include <flatbuffers/base.h> // FLATBUFFERS_MAX_BUFFER_SIZE
#include <flatbuffers/flatbuffers.h>

#include <filesystem>
#include <memory>
#include <span>

namespace tenzir {

namespace {
void delegate_deferred_requests(passive_partition_state& state) {
  for (auto&& [expr, rp] : std::exchange(state.deferred_evaluations, {}))
    rp.delegate(static_cast<partition_actor>(state.self), atom::query_v,
                std::move(expr));
  for (auto&& rp : std::exchange(state.deferred_erasures, {}))
    rp.delegate(static_cast<partition_actor>(state.self), atom::erase_v);
}

void deliver_error_to_deferred_requests(passive_partition_state& state,
                                        const caf::error& err) {
  for (auto&& [expr, rp] : std::exchange(state.deferred_evaluations, {})) {
    // Because of a deficiency in the typed_response_promise API, we must
    // access the underlying response_promise to deliver the error.
    rp.deliver(err);
  }
  for (auto&& rp : std::exchange(state.deferred_erasures, {})) {
    rp.deliver(err);
  }
}

caf::expected<tenzir::record_type>
unpack_schema(const fbs::partition::LegacyPartition& partition) {
  if (auto const* data = partition.combined_schema_caf_0_17()) {
    auto lrt = legacy_record_type{};
    if (auto error = fbs::deserialize_bytes(data, lrt))
      return error;
    return caf::get<record_type>(type::from_legacy_type(lrt));
  }
  if (auto const* data = partition.schema()) {
    auto chunk = chunk::copy(as_bytes(*data));
    auto t = type{std::move(chunk)};
    auto* schema = caf::get_if<record_type>(&t);
    if (!schema)
      return caf::make_error(ec::format_error, "schema field contained "
                                               "unexpected type");
    return std::move(*schema);
  }
  return caf::make_error(ec::format_error, "missing 'schemas' field in "
                                           "partition flatbuffer");
}

} // namespace

value_index_ptr
unpack_value_index(const fbs::value_index::detail::LegacyValueIndex& index_fbs,
                   const fbs::flatbuffer_container& container) {
  // If an external idx was specified, the data is not stored inline in
  // the flatbuffer but in a separate segment of this file.
  auto uncompress = [&index_fbs]<class T>(T&& index_data) {
    auto data_view = as_bytes(std::forward<T>(index_data));
    auto uncompressed_data
      = index_fbs.decompressed_size() != 0
          ? chunk::decompress(data_view, index_fbs.decompressed_size())
          : chunk::make(data_view, []() noexcept {});
    TENZIR_ASSERT(uncompressed_data);
    return uncompressed_data;
  };
  if (const auto* data = index_fbs.caf_0_18_data()) {
    auto uncompressed_data = uncompress(*data);
    auto bytes = as_bytes(*uncompressed_data);
    caf::binary_deserializer sink{nullptr, bytes.data(), bytes.size()};
    value_index_ptr state_ptr;
    if (!sink.apply(state_ptr) || !state_ptr)
      return {};
    return state_ptr;
  }
  if (const auto* data = index_fbs.caf_0_17_data()) {
    auto uncompressed_data = uncompress(*data);
    detail::legacy_deserializer sink(as_bytes(*uncompressed_data));
    value_index_ptr state_ptr;
    if (!sink(state_ptr) || !state_ptr)
      return {};
    return state_ptr;
  }
  if (auto ext_index = index_fbs.caf_0_17_external_container_idx()) {
    auto uncompressed_data = uncompress(container.get_raw(ext_index));
    detail::legacy_deserializer sink(as_bytes(*uncompressed_data));
    value_index_ptr state_ptr;
    if (!sink(state_ptr) || !state_ptr)
      return {};
    return state_ptr;
  }
  if (auto ext_index = index_fbs.caf_0_18_external_container_idx()) {
    auto uncompressed_data = uncompress(container.get_raw(ext_index));
    auto bytes = as_bytes(*uncompressed_data);
    caf::binary_deserializer sink{nullptr, bytes.data(), bytes.size()};
    value_index_ptr state_ptr;
    if (!sink.apply(state_ptr) || !state_ptr)
      return {};
    return state_ptr;
  }
  return {};
}

/// Gets the INDEXER at a certain position.
indexer_actor passive_partition_state::indexer_at(size_t position) const {
  TENZIR_ASSERT(position < indexers.size());
  auto& indexer = indexers[position];
  if (indexer)
    return indexer;
  // Deserialize the value index and spawn a passive_indexer lazily when it is
  // requested for the first time.
  const auto* qualified_index = flatbuffer->indexes()->Get(position);
  if (!qualified_index || !qualified_index->index())
    return {};
  if (auto value_index
      = unpack_value_index(*qualified_index->index(), *container)) {
    indexer = self->spawn(passive_indexer, id, std::move(value_index));
    return indexer;
  }
  TENZIR_DEBUG("passive-partition {} has no index or failed to index for field "
               "{}",
               id, qualified_index->field_name()->string_view());
  return {};
}

const std::optional<tenzir::record_type>&
passive_partition_state::combined_schema() const {
  return combined_schema_;
}

const std::unordered_map<std::string, ids>&
passive_partition_state::type_ids() const {
  return type_ids_;
}

caf::error unpack(const fbs::partition::LegacyPartition& partition,
                  passive_partition_state& state) {
  // Check that all fields exist.
  if (!partition.uuid())
    return caf::make_error(ec::format_error, //
                           "missing 'uuid' field in partition flatbuffer");
  auto const* store_header = partition.store();
  // If no store_id is set, use the global store for backwards compatibility.
  if (store_header && !store_header->id())
    return caf::make_error(ec::format_error, //
                           "missing 'id' field in partition store header");
  if (store_header && !store_header->data())
    return caf::make_error(ec::format_error, //
                           "missing 'data' field in partition store header");
  state.store_id = store_header->id()->str();
  if (store_header && store_header->data())
    state.store_header = std::span{
      reinterpret_cast<const std::byte*>(store_header->data()->data()),
      store_header->data()->size()};
  auto const* indexes = partition.indexes();
  if (!indexes)
    return caf::make_error(ec::format_error, //
                           "missing 'indexes' field in partition flatbuffer");
  for (auto const* qualified_index : *indexes) {
    if (!qualified_index->field_name())
      return caf::make_error(ec::format_error, //
                             "missing field name in qualified index");
    auto const* index = qualified_index->index();
    if (!index)
      return caf::make_error(ec::format_error, //
                             "missing index field in qualified index");
  }
  if (auto error = unpack(*partition.uuid(), state.id))
    return error;
  state.events = partition.events();
  if (auto schema = unpack_schema(partition))
    state.combined_schema_ = std::move(*schema);
  else
    return schema.error();
  // This condition should be '!=', but then we cant deserialize in unit tests
  // anymore without creating a bunch of index actors first. :/
  if (state.combined_schema_->num_fields() < indexes->size()) {
    TENZIR_ERROR(
      "{} found incoherent number of indexers in deserialized state; "
      "{} fields for {} indexes",
      state.name, state.combined_schema_->num_fields(), indexes->size());
    return caf::make_error(ec::format_error, "incoherent number of indexers");
  }
  // We only create dummy entries here, since the positions of the `indexers`
  // vector must be the same as in `combined_schema`. The actual indexers are
  // deserialized and spawned lazily on demand.
  state.indexers.resize(indexes->size());
  TENZIR_DEBUG("{} found {} indexers for partition {}", state.name,
               indexes->size(), state.id);
  auto const* type_ids = partition.type_ids();
  for (size_t i = 0; i < type_ids->size(); ++i) {
    auto const* type_ids_tuple = type_ids->Get(i);
    auto const* name = type_ids_tuple->name();
    auto const* ids_data = type_ids_tuple->ids();
    auto& ids = state.type_ids_[name->str()];
    if (auto error = fbs::deserialize_bytes(ids_data, ids))
      return error;
  }
  TENZIR_DEBUG("{} restored {} type-to-ids mapping for partition {}",
               state.name, state.type_ids_.size(), state.id);
  return caf::none;
}

caf::error
unpack(const fbs::partition::LegacyPartition& x, partition_synopsis& ps) {
  if (!x.partition_synopsis())
    return caf::make_error(ec::format_error, "missing partition synopsis");
  if (!x.type_ids())
    return caf::make_error(ec::format_error, "missing type_ids");
  return unpack(*x.partition_synopsis(), ps);
}

caf::expected<const tenzir::fbs::Partition*>
partition_chunk::get_flatbuffer(tenzir::chunk_ptr chunk) {
  if (flatbuffers::BufferHasIdentifier(chunk->data(),
                                       fbs::PartitionIdentifier())) {
    // FlatBuffers <= 1.11 does not correctly use '::flatbuffers::soffset_t'
    // over 'soffset_t' in FLATBUFFERS_MAX_BUFFER_SIZE.
    using ::flatbuffers::soffset_t;
    if (chunk->size() >= FLATBUFFERS_MAX_BUFFER_SIZE) {
      return caf::make_error(ec::format_error, "partition exceeds the maximum "
                                               "flatbuffer size");
    }
    return fbs::GetPartition(chunk->data());
  } else if (flatbuffers::BufferHasIdentifier(
               chunk->data(), fbs::SegmentedFileHeaderIdentifier())) {
    auto container = fbs::flatbuffer_container(chunk);
    if (!container)
      return caf::make_error(ec::format_error, "invalid flatbuffer container");
    return container.as_flatbuffer<fbs::Partition>(0);
  } else {
    return caf::make_error(ec::format_error, "unknown identifier {}",
                           flatbuffers::GetBufferIdentifier(chunk->data()));
  }
}

caf::error
passive_partition_state::initialize_from_chunk(const tenzir::chunk_ptr& chunk) {
  // For partitions written prior to Tenzir 2.3, the chunk contains the
  // partition as top-level flatbuffer.
  if (flatbuffers::BufferHasIdentifier(chunk->data(),
                                       fbs::PartitionIdentifier())) {
    // FlatBuffers <= 1.11 does not correctly use '::flatbuffers::soffset_t'
    // over 'soffset_t' in FLATBUFFERS_MAX_BUFFER_SIZE.
    using ::flatbuffers::soffset_t;
    if (chunk->size() >= FLATBUFFERS_MAX_BUFFER_SIZE) {
      return caf::make_error(
        ec::format_error,
        fmt::format("failed to load partition because its size of {} "
                    "exceeds the "
                    "maximum allowed size of {}",
                    chunk->size(), FLATBUFFERS_MAX_BUFFER_SIZE));
    }
    auto partition = fbs::GetPartition(chunk->data());
    if (partition->partition_type() != fbs::partition::Partition::legacy) {
      return caf::make_error(
        ec::format_error,
        fmt::format("unknown partition version {}",
                    static_cast<uint8_t>(partition->partition_type())));
    }
    this->partition_chunk = chunk;
    this->flatbuffer = partition->partition_as_legacy();
  } else if (flatbuffers::BufferHasIdentifier(
               chunk->data(), fbs::SegmentedFileHeaderIdentifier())) {
    this->partition_chunk = chunk;
    this->container = fbs::flatbuffer_container(chunk);
    if (!this->container)
      return caf::make_error(ec::format_error, "invalid flatbuffer container");
    auto partition = container->as_flatbuffer<fbs::Partition>(0);
    if (partition->partition_type() != fbs::partition::Partition::legacy)
      return caf::make_error(
        ec::format_error,
        fmt::format("unknown partition version {}",
                    static_cast<uint8_t>(partition->partition_type())));
    this->flatbuffer = partition->partition_as_legacy();
  } else {
    return caf::make_error(ec::format_error,
                           "partition at contains unknown identifier {}",
                           flatbuffers::GetBufferIdentifier(chunk->data()));
  }
  if (auto error = unpack(*flatbuffer, *this))
    return caf::make_error(
      ec::format_error, fmt::format("failed to unpack partition: {}", error));
  return {};
}

partition_actor::behavior_type passive_partition(
  partition_actor::stateful_pointer<passive_partition_state> self, uuid id,
  filesystem_actor filesystem, const std::filesystem::path& path) {
  auto id_string = fmt::to_string(id);
  self->state.self = self;
  self->state.path = path;
  self->state.filesystem = std::move(filesystem);
  TENZIR_TRACEPOINT(passive_partition_spawned, id_string.c_str());
  self->set_down_handler([=](const caf::down_msg& msg) {
    if (msg.source != self->state.store.address()) {
      TENZIR_WARN("{} ignores DOWN from unexpected sender: {}", *self,
                  msg.reason);
      return;
    }
    TENZIR_ERROR("{} shuts down after DOWN from {} store: {}", *self,
                 self->state.store_id, msg.reason);
    self->quit(msg.reason);
  });
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    TENZIR_DEBUG("{} received EXIT from {} with reason: {}", *self, msg.source,
                 msg.reason);
    self->demonitor(self->state.store->address());
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
          TENZIR_DEBUG("{} shut down all indexers successfully", *self);
          self->quit();
        },
        [=](const caf::error& err) {
          TENZIR_ERROR("{} failed to shut down all indexers: {}", *self, err);
          self->quit(err);
        });
  });
  // We send a "read" to the fs actor and upon receiving the result deserialize
  // the flatbuffer and switch to the "normal" partition behavior for responding
  // to queries.
  self->request(self->state.filesystem, caf::infinite, atom::mmap_v, path)
    .then(
      [=](chunk_ptr chunk) {
        TENZIR_TRACE_SCOPE("{} {}", *self, TENZIR_ARG(chunk));
        TENZIR_TRACEPOINT(passive_partition_loaded, id_string.c_str());
        TENZIR_ASSERT(!self->state.partition_chunk);
        if (!chunk) {
          TENZIR_ERROR("{} got invalid chunk", *self);
          self->quit();
          return;
        }
        if (auto err = self->state.initialize_from_chunk(chunk)) {
          TENZIR_ERROR("{} failed to initialize passive partition from file "
                       "{}: "
                       "{}",
                       *self, path, err);
          self->quit();
          return;
        }
        if (self->state.id != id) {
          TENZIR_ERROR("unexpected ID for passive partition: expected {}, got "
                       "{}",
                       id, self->state.id);
          self->quit();
          return;
        }
        const auto* plugin
          = plugins::find<store_actor_plugin>(self->state.store_id);
        if (!plugin) {
          auto error = caf::make_error(ec::format_error,
                                       "encountered unhandled store backend");
          TENZIR_ERROR("{} encountered unknown store backend '{}'", *self,
                       self->state.store_id);
          self->quit(std::move(error));
          return;
        }
        auto store = plugin->make_store(self->state.filesystem,
                                        self->state.store_header);
        if (!store) {
          TENZIR_ERROR("{} failed to spawn store: {}", *self, store.error());
          self->quit(caf::make_error(ec::system_error, "failed to spawn "
                                                       "store"));
          return;
        }
        self->state.store = *store;
        self->monitor(self->state.store);
        // Delegate all deferred evaluations now that we have the partition chunk.
        TENZIR_DEBUG("{} delegates {} deferred evaluations", *self,
                     self->state.deferred_evaluations.size());
        delegate_deferred_requests(self->state);
      },
      [=](caf::error err) {
        TENZIR_ERROR("{} failed to load partition: {}", *self, err);
        deliver_error_to_deferred_requests(self->state, err);
        // Quit the partition.
        self->quit(std::move(err));
      });
  return {
    [self](atom::query,
           tenzir::query_context query_context) -> caf::result<uint64_t> {
      TENZIR_DEBUG("{} received query {}", *self, query_context);
      if (!self->state.partition_chunk) {
        TENZIR_DEBUG("{} waits for its state", *self);
        return std::get<1>(self->state.deferred_evaluations.emplace_back(
          std::move(query_context), self->make_response_promise<uint64_t>()));
      }
      // We can safely assert that if we have the partition chunk already, all
      // deferred evaluations were taken care of.
      TENZIR_ASSERT(self->state.deferred_evaluations.empty());
      // Don't handle queries after we already received an exit message, while
      // the terminator is running. Since we require every partition to have at
      // least one indexer, we can use this to check.
      if (self->state.indexers.empty())
        return caf::make_error(ec::system_error, "can not handle query because "
                                                 "shutdown was requested");
      auto rp = self->make_response_promise<uint64_t>();
      // Don't bother with the indexers etc. if we already know the ids
      // we want to retrieve.
      if (!query_context.ids.empty()) {
        if (query_context.expr != tenzir::expression{})
          return caf::make_error(ec::invalid_argument, "query may only contain "
                                                       "either expression or "
                                                       "ids");
        rp.delegate(self->state.store, atom::query_v, query_context);
        return rp;
      }
      auto triples = detail::evaluate(self->state, query_context.expr);
      if (triples.empty()) {
        rp.deliver(uint64_t{0});
        return rp;
      }
      auto ids_for_evaluation
        = detail::get_ids_for_evaluation(self->state.type_ids(), triples);
      auto eval = self->spawn(evaluator, query_context.expr, std::move(triples),
                              std::move(ids_for_evaluation));
      self->request(eval, caf::infinite, atom::run_v)
        .then(
          [self, rp,
           query_context = std::move(query_context)](const ids& hits) mutable {
            if (!hits.empty() && hits.size() != self->state.events) {
              // FIXME: We run into this for at least the IP index following the
              // quickstart guide in the documentation, indicating that the IP
              // index returns an undersized bitmap whose length does not match
              // the number of events in this partition. This _can_ cause subtle
              // issues downstream because you need to very carefully handle
              // this scenario, which is easy to overlook as a developer. We
              // should fix this issue.
              TENZIR_DEBUG("{} received evaluator results with wrong length: "
                           "expected {}, got {}",
                           *self, self->state.events, hits.size());
            }
            TENZIR_DEBUG("{} received results from the evaluator", *self);
            // TODO: Use the first path if the expression can be evaluated
            // exactly.
            query_context.ids = hits;
            rp.delegate(self->state.store, atom::query_v,
                        std::move(query_context));
          },
          [rp](caf::error& err) mutable {
            rp.deliver(std::move(err));
          });
      return rp;
    },
    [self](atom::erase) -> caf::result<atom::done> {
      auto rp = self->make_response_promise<atom::done>();
      if (!self->state.partition_chunk) {
        TENZIR_DEBUG("{} skips an erase request", *self);
        return self->state.deferred_erasures.emplace_back(std::move(rp));
      }
      TENZIR_DEBUG("{} received an erase message and deletes {}", *self,
                   self->state.path);
      self
        ->request(self->state.filesystem, caf::infinite, atom::erase_v,
                  self->state.path)
        .then([](atom::done) {},
              [self](const caf::error& err) {
                TENZIR_WARN("{} failed to delete {}: {}; try deleting manually",
                            *self, self->state.path, err);
              });
      tenzir::ids all_ids;
      for (const auto& kv : self->state.type_ids_) {
        all_ids |= kv.second;
      }
      self
        ->request(self->state.store, caf::infinite, atom::erase_v,
                  std::move(all_ids))
        .then(
          [rp](uint64_t) mutable {
            rp.deliver(atom::done_v);
          },
          [rp](caf::error& err) mutable {
            rp.deliver(std::move(err));
          });
      return rp;
    },
    [self](atom::status, status_verbosity, duration) -> record {
      record result;
      if (!self->state.partition_chunk) {
        result["state"] = "waiting for chunk";
        return result;
      }
      result["size"] = self->state.partition_chunk->size();
      size_t mem_indexers = 0;
      for (size_t i = 0; i < self->state.indexers.size(); ++i)
        if (self->state.indexers[i])
          mem_indexers += sizeof(indexer_state)
                          + self->state.flatbuffer->indexes()
                              ->Get(i)
                              ->index()
                              ->decompressed_size();
      result["memory-usage-indexers"] = mem_indexers;
      auto x = self->state.partition_chunk->incore();
      if (!x) {
        result["memory-usage-incore"] = fmt::to_string(x.error());
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

} // namespace tenzir
