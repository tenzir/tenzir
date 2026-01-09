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
#include "tenzir/detail/tracepoint.hpp"
#include "tenzir/fbs/partition.hpp"
#include "tenzir/fbs/utils.hpp"
#include "tenzir/ids.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/shutdown.hpp"
#include "tenzir/status.hpp"
#include "tenzir/terminate.hpp"
#include "tenzir/type.hpp"

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
  for (auto&& [expr, rp] : std::exchange(state.deferred_evaluations, {})) {
    rp.delegate(static_cast<partition_actor>(state.self), atom::query_v,
                std::move(expr));
  }
  for (auto&& rp : std::exchange(state.deferred_erasures, {})) {
    rp.delegate(static_cast<partition_actor>(state.self), atom::erase_v);
  }
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
  if (auto const* data = partition.schema()) {
    auto chunk = chunk::copy(as_bytes(*data));
    auto t = type{std::move(chunk)};
    auto* schema = try_as<record_type>(&t);
    if (! schema) {
      return caf::make_error(ec::format_error, "schema field contained "
                                               "unexpected type");
    }
    return std::move(*schema);
  }
  return caf::make_error(ec::format_error, "missing 'schemas' field in "
                                           "partition flatbuffer");
}

} // namespace

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
  if (! partition.uuid()) {
    return caf::make_error(ec::format_error, //
                           "missing 'uuid' field in partition flatbuffer");
  }
  auto const* store_header = partition.store();
  // If no store_id is set, use the global store for backwards compatibility.
  if (store_header && ! store_header->id()) {
    return caf::make_error(ec::format_error, //
                           "missing 'id' field in partition store header");
  }
  if (store_header && ! store_header->data()) {
    return caf::make_error(ec::format_error, //
                           "missing 'data' field in partition store header");
  }
  state.store_id = store_header->id()->str();
  if (store_header && store_header->data()) {
    state.store_header = std::span{
      reinterpret_cast<const std::byte*>(store_header->data()->data()),
      store_header->data()->size()};
  }
  if (auto error = unpack(*partition.uuid(), state.id); error.valid()) {
    return error;
  }
  state.events = partition.events();
  if (auto schema = unpack_schema(partition)) {
    state.combined_schema_ = std::move(*schema);
  } else {
    return schema.error();
  }
  auto const* type_ids = partition.type_ids();
  for (size_t i = 0; i < type_ids->size(); ++i) {
    auto const* type_ids_tuple = type_ids->Get(i);
    auto const* name = type_ids_tuple->name();
    auto const* ids_data = type_ids_tuple->ids();
    auto& ids = state.type_ids_[name->str()];
    if (auto error = fbs::deserialize_bytes(ids_data, ids); error.valid()) {
      return error;
    }
  }
  return caf::none;
}

caf::error
unpack(const fbs::partition::LegacyPartition& x, partition_synopsis& ps) {
  if (! x.partition_synopsis()) {
    return caf::make_error(ec::format_error, "missing partition synopsis");
  }
  if (! x.type_ids()) {
    return caf::make_error(ec::format_error, "missing type_ids");
  }
  return unpack(*x.partition_synopsis(), ps);
}

caf::expected<const tenzir::fbs::Partition*>
partition_chunk::get_flatbuffer(tenzir::chunk_ptr chunk) {
  // FlatBuffers <= 1.11 does not correctly use '::flatbuffers::soffset_t'
  // over 'soffset_t' in FLATBUFFERS_MAX_BUFFER_SIZE.
  using ::flatbuffers::soffset_t;
  using ::flatbuffers::uoffset_t;
  if (chunk->size() < FLATBUFFERS_MIN_BUFFER_SIZE) {
    return caf::make_error(ec::format_error, "partition was smaller than the "
                                             "minimum flatbuffer size");
  }
  if (flatbuffers::BufferHasIdentifier(chunk->data(),
                                       fbs::PartitionIdentifier())) {
    if (chunk->size() >= FLATBUFFERS_MAX_BUFFER_SIZE) {
      return caf::make_error(ec::format_error, "partition exceeds the maximum "
                                               "flatbuffer size");
    }
    return fbs::GetPartition(chunk->data());
  } else if (flatbuffers::BufferHasIdentifier(
               chunk->data(), fbs::SegmentedFileHeaderIdentifier())) {
    auto container = fbs::flatbuffer_container(chunk);
    if (! container) {
      return caf::make_error(ec::format_error, "invalid flatbuffer container");
    }
    return container.as_flatbuffer<fbs::Partition>(0);
  } else {
    return caf::make_error(ec::format_error, "unknown identifier {}",
                           flatbuffers::GetBufferIdentifier(chunk->data()));
  }
}

caf::error
passive_partition_state::initialize_from_chunk(const tenzir::chunk_ptr& chunk) {
  using flatbuffers::soffset_t;
  using flatbuffers::uoffset_t;
  if (! chunk || chunk->size() < FLATBUFFERS_MIN_BUFFER_SIZE) {
    return caf::make_error(ec::format_error, "flatbuffer failed to load or was "
                                             "smaller than the mininum size");
  }
  // For partitions written prior to VAST 2.3, the chunk contains the
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
    if (! this->container) {
      return caf::make_error(ec::format_error, "invalid flatbuffer container");
    }
    auto partition = container->as_flatbuffer<fbs::Partition>(0);
    if (partition->partition_type() != fbs::partition::Partition::legacy) {
      return caf::make_error(
        ec::format_error,
        fmt::format("unknown partition version {}",
                    static_cast<uint8_t>(partition->partition_type())));
    }
    this->flatbuffer = partition->partition_as_legacy();
  } else {
    return caf::make_error(ec::format_error,
                           "partition at contains unknown identifier {}",
                           flatbuffers::GetBufferIdentifier(chunk->data()));
  }
  if (auto error = unpack(*flatbuffer, *this); error.valid()) {
    return caf::make_error(
      ec::format_error, fmt::format("failed to unpack partition: {}", error));
  }
  return {};
}

partition_actor::behavior_type passive_partition(
  partition_actor::stateful_pointer<passive_partition_state> self, uuid id,
  filesystem_actor filesystem, const std::filesystem::path& path) {
  auto id_string = fmt::to_string(id);
  self->state().self = self;
  self->state().path = path;
  self->state().filesystem = std::move(filesystem);
  TENZIR_TRACEPOINT(passive_partition_spawned, id_string.c_str());
  // We send a "read" to the fs actor and upon receiving the result deserialize
  // the flatbuffer and switch to the "normal" partition behavior for responding
  // to queries.
  self->mail(atom::mmap_v, path)
    .request(self->state().filesystem, caf::infinite)
    .then(
      [=](chunk_ptr chunk) {
        TENZIR_TRACE("{} {}", *self, TENZIR_ARG(chunk));
        TENZIR_TRACEPOINT(passive_partition_loaded, id_string.c_str());
        TENZIR_ASSERT(! self->state().partition_chunk);
        if (auto err = self->state().initialize_from_chunk(chunk);
            err.valid()) {
          TENZIR_ERROR("{} failed to initialize passive partition from file "
                       "{}: "
                       "{}",
                       *self, path, err);
          self->quit();
          return;
        }
        if (self->state().id != id) {
          TENZIR_ERROR("unexpected ID for passive partition: expected {}, got "
                       "{}",
                       id, self->state().id);
          self->quit();
          return;
        }
        const auto* plugin
          = plugins::find<store_actor_plugin>(self->state().store_id);
        if (! plugin) {
          auto error = caf::make_error(ec::format_error,
                                       "encountered unhandled store backend");
          TENZIR_ERROR("{} encountered unknown store backend '{}'", *self,
                       self->state().store_id);
          self->quit(std::move(error));
          return;
        }
        auto store = plugin->make_store(self->state().filesystem,
                                        self->state().store_header);
        if (! store) {
          TENZIR_ERROR("{} failed to spawn store: {}", *self, store.error());
          self->quit(caf::make_error(ec::system_error, "failed to spawn "
                                                       "store"));
          return;
        }
        self->state().store = *store;
        self->monitor(self->state().store, [=](caf::error err) {
          TENZIR_ERROR("{} shuts down after DOWN from {} store: {}", *self,
                       self->state().store_id, err);
          self->quit(std::move(err));
        });
        // Delegate all deferred evaluations now that we have the partition chunk.
        delegate_deferred_requests(self->state());
      },
      [=](caf::error err) {
        // This error is nicely printed at the export operator as a warning. No
        // need to print it as an error here already.
        TENZIR_WARN("{} failed to load partition: {}", *self, err);
        deliver_error_to_deferred_requests(self->state(), err);
        // Quit the partition.
        self->quit(std::move(err));
      });
  return {
    [self](atom::query,
           tenzir::query_context query_context) -> caf::result<uint64_t> {
      TENZIR_TRACE("{} received query {}", *self, query_context);
      if (! self->state().partition_chunk) {
        return std::get<1>(self->state().deferred_evaluations.emplace_back(
          std::move(query_context), self->make_response_promise<uint64_t>()));
      }
      // We can safely assert that if we have the partition chunk already, all
      // deferred evaluations were taken care of.
      TENZIR_ASSERT(self->state().deferred_evaluations.empty());
      return self->mail(atom::query_v, std::move(query_context))
        .delegate(self->state().store);
    },
    [self](atom::erase) -> caf::result<atom::done> {
      auto rp = self->make_response_promise<atom::done>();
      if (! self->state().partition_chunk) {
        TENZIR_TRACE("{} skips an erase request", *self);
        return self->state().deferred_erasures.emplace_back(std::move(rp));
      }
      TENZIR_TRACE("{} received an erase message and deletes {}", *self,
                   self->state().path);
      self->mail(atom::erase_v, self->state().path)
        .request(self->state().filesystem, caf::infinite)
        .then([](atom::done) {},
              [self](const caf::error& err) {
                TENZIR_WARN("{} failed to delete {}: {}; try deleting manually",
                            *self, self->state().path, err);
              });
      tenzir::ids all_ids;
      for (const auto& kv : self->state().type_ids_) {
        all_ids |= kv.second;
      }
      self->mail(atom::erase_v, std::move(all_ids))
        .request(self->state().store, caf::infinite)
        .then(
          [rp](uint64_t) mutable {
            rp.deliver(atom::done_v);
          },
          [rp](caf::error& err) mutable {
            rp.deliver(std::move(err));
          });
      return rp;
    },
    [](atom::status, status_verbosity, duration) -> record {
      return {};
    },
    [self](const caf::exit_msg& msg) {
      TENZIR_TRACE("{} received EXIT from {} with reason: {}", *self,
                   msg.source, msg.reason);
      self->quit(std::move(msg.reason));
    },
  };
}

} // namespace tenzir
