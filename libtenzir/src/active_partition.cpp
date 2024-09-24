//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/active_partition.hpp"

#include "tenzir/fwd.hpp"

#include "tenzir/aliases.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/concept/printable/tenzir/expression.hpp"
#include "tenzir/concept/printable/tenzir/table_slice.hpp"
#include "tenzir/concept/printable/tenzir/uuid.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/fill_status_map.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/notifying_stream_manager.hpp"
#include "tenzir/detail/partition_common.hpp"
#include "tenzir/detail/settings.hpp"
#include "tenzir/detail/shutdown_stream_stage.hpp"
#include "tenzir/detail/tracepoint.hpp"
#include "tenzir/detail/weak_run_delayed.hpp"
#include "tenzir/expression_visitors.hpp"
#include "tenzir/fbs/partition.hpp"
#include "tenzir/fbs/utils.hpp"
#include "tenzir/fbs/uuid.hpp"
#include "tenzir/hash/xxhash.hpp"
#include "tenzir/ids.hpp"
#include "tenzir/indexer.hpp"
#include "tenzir/ip_synopsis.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/qualified_record_field.hpp"
#include "tenzir/resource.hpp"
#include "tenzir/shutdown.hpp"
#include "tenzir/status.hpp"
#include "tenzir/synopsis.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/taxonomies.hpp"
#include "tenzir/terminate.hpp"
#include "tenzir/time.hpp"
#include "tenzir/type.hpp"
#include "tenzir/value_index.hpp"
#include "tenzir/value_index_factory.hpp"

#include <caf/attach_continuous_stream_stage.hpp>
#include <caf/broadcast_downstream_manager.hpp>
#include <caf/deserializer.hpp>
#include <caf/error.hpp>
#include <caf/make_copy_on_write.hpp>
#include <caf/sec.hpp>
#include <flatbuffers/base.h> // FLATBUFFERS_MAX_BUFFER_SIZE
#include <flatbuffers/flatbuffers.h>

#include <filesystem>
#include <memory>
#include <span>

namespace tenzir {

namespace {

chunk_ptr serialize_partition_synopsis(const partition_synopsis& synopsis) {
  flatbuffers::FlatBufferBuilder synopsis_builder;
  const auto ps = pack(synopsis_builder, synopsis);
  if (!ps)
    return {};
  fbs::PartitionSynopsisBuilder ps_builder(synopsis_builder);
  ps_builder.add_partition_synopsis_type(
    fbs::partition_synopsis::PartitionSynopsis::legacy);
  ps_builder.add_partition_synopsis(ps->Union());
  auto ps_offset = ps_builder.Finish();
  fbs::FinishPartitionSynopsisBuffer(synopsis_builder, ps_offset);
  return fbs::release(synopsis_builder);
}

/// Delivers persistance promise and calculates indexer_chunks
void serialize(
  active_partition_actor::stateful_pointer<active_partition_state> self) {
  auto& mutable_synopsis = self->state.data.synopsis.unshared();
  // Shrink synopses for addr fields to optimal size.
  mutable_synopsis.shrink();
  // TODO: It would probably make more sense if the partition
  // synopsis keeps track of offset/events internally.
  mutable_synopsis.events = self->state.data.events;
  for (auto& [qf, actor] : self->state.indexers) {
    if (actor == nullptr) {
      self->state.data.indexer_chunks.emplace_back(qf.name(), nullptr);
      continue;
    }
    auto actor_id = actor.id();
    auto chunk_it = self->state.chunks.find(actor_id);
    if (chunk_it == self->state.chunks.end()) {
      auto error = caf::make_error(ec::logic_error, "no chunk for for actor id "
                                                      + to_string(actor_id));
      TENZIR_ERROR("{} failed to serialize: {}", *self, render(error));
      self->state.persistence_promise.deliver(error);
      return;
    }
    // TODO: Consider storing indexer chunks by the fully qualified
    // field instead of just its fully qualified name in a future
    // partition version. As-is, this breaks if multiple fields with
    // the same fully qualified name but different types exist in
    // the same partition.
    self->state.data.indexer_chunks.emplace_back(
      std::make_pair(qf.name(), chunk_it->second));
  }
  // Create the partition flatbuffer.
  auto combined_schema = self->state.combined_schema();
  if (!combined_schema) {
    auto err = caf::make_error(ec::logic_error, "unable to create "
                                                "combined schema");
    TENZIR_ERROR("{} failed to serialize {} with error: {}", *self, *self, err);
    self->state.persistence_promise.deliver(err);
    return;
  }
  auto partition = pack_full(self->state.data, *combined_schema);
  if (!partition) {
    TENZIR_ERROR("{} failed to serialize {} with error: {}", *self, *self,
                 partition.error());
    self->state.persistence_promise.deliver(partition.error());
    return;
  }
  TENZIR_ASSERT(self->state.persist_path);
  TENZIR_ASSERT(self->state.synopsis_path);
  // Note that this is a performance optimization: We used to store
  // the partition synopsis inside the `Partition` flatbuffer, and
  // then on startup the index would mmap all partitions and read
  // the relevant part of the flatbuffer. However, due to the way
  // the flatbuffer file format is structured this still needs three
  // random file accesses: At the beginning to read vtable offset
  // and file identifier, at the end to read the actual vtable, and
  // finally at the actual data. On systems with aggressive
  // readahead (ie., btrfs defaults to 4MiB), this can increase the
  // i/o at startup and thus the time to boot by more than 10x.
  //
  // Since the synopsis should be small compared to the actual data,
  // we store a redundant copy in the partition itself so we can
  // regenerate the synopses as needed. This also means we don't
  // need to handle errors here, since Tenzir can still start
  // correctly (if a bit slower) when the write fails.
  if (auto ps_chunk
      = serialize_partition_synopsis(*self->state.data.synopsis)) {
    self->state.data.synopsis.unshared().sketches_file = {
      .url = fmt::format("file://{}", *self->state.synopsis_path),
      .size = ps_chunk->size(),
    };
    self
      ->request(self->state.filesystem, caf::infinite, atom::write_v,
                *self->state.synopsis_path, std::move(ps_chunk))
      .then(
        [=](atom::ok) {
          TENZIR_DEBUG("{} persisted partition synopsis", *self);
        },
        [=](const caf::error& err) {
          TENZIR_WARN("{} failed to persist partition synopsis to {} and will "
                      "attempt to restore it on the next start: {}",
                      *self, *self->state.synopsis_path, err);
        });
  } else {
    TENZIR_WARN("{} failed to serialize partition synopsis and will attempt to "
                "restore it on the next start",
                *self);
  }
  TENZIR_DEBUG("{} persists partition with a total size of "
               "{} bytes",
               *self, (*partition)->size());
  self->state.data.synopsis.unshared().indexes_file = {
    .url = fmt::format("file://{}", *self->state.persist_path),
    .size = (*partition)->size(),
  };
  // TODO: Add a proper timeout.
  self
    ->request(self->state.filesystem, caf::infinite, atom::write_v,
              *self->state.persist_path, std::move(*partition))
    .then(
      [=](atom::ok) {
        self->state.persistence_promise.deliver(self->state.data.synopsis);
      },
      [=](caf::error e) {
        self->state.persistence_promise.deliver(std::move(e));
      });
}

} // namespace

bool should_skip_index_creation(const type& type,
                                const qualified_record_field& qf,
                                const std::vector<index_config::rule>& rules) {
  // We no longer build dense indexes as of Tenzir v4.3. Over time, they've lost
  // much of their appeal with partition sizes growing and columnar scanning of
  // stores becoming more effective.
  // TODO: Rip out the parts of the code base relating to value indexes, the
  // value index factory, and active and passive indexer actors.
  (void)type;
  (void)qf;
  (void)rules;
  return true;
}

/// Gets the ACTIVE INDEXER at a certain position.
active_indexer_actor active_partition_state::indexer_at(size_t position) const {
  TENZIR_ASSERT(position < indexers.size());
  return as_vector(indexers)[position].second;
}

std::optional<record_type> active_partition_state::combined_schema() const {
  if (indexers.empty())
    return {};
  auto fields = std::vector<struct record_type::field>{};
  fields.reserve(indexers.size());
  for (const auto& [qf, _] : indexers)
    fields.push_back({std::string{qf.name()}, qf.type()});
  return record_type{fields};
}

const std::unordered_map<std::string, ids>&
active_partition_state::type_ids() const {
  return data.type_ids;
}

void active_partition_state::handle_slice(table_slice x) {
  TENZIR_TRACE_SCOPE("partition {} got table slice {}", data.id, TENZIR_ARG(x));
  x.offset(data.events);
  // Adjust the import time range iff necessary.
  auto& mutable_synopsis = data.synopsis.unshared();
  mutable_synopsis.min_import_time
    = std::min(data.synopsis->min_import_time, x.import_time());
  mutable_synopsis.max_import_time
    = std::max(data.synopsis->max_import_time, x.import_time());
  // We rely on `invalid_id` actually being the highest possible id
  // when using `min()` below.
  static_assert(invalid_id == std::numeric_limits<tenzir::id>::max());
  auto first = x.offset();
  auto last = x.offset() + x.rows();
  const auto& schema = x.schema();
  TENZIR_ASSERT(!schema.name().empty());
  auto it = data.type_ids.emplace(schema.name(), ids{}).first;
  auto& ids = it->second;
  TENZIR_ASSERT(first >= ids.size());
  // Mark the ids of this table slice for the current type.
  ids.append_bits(false, first - ids.size());
  ids.append_bits(true, last - first);
  data.events += x.rows();
  data.synopsis.unshared().add(x, partition_capacity, synopsis_index_config);
  for (const auto& [field, offset] : caf::get<record_type>(schema).leaves()) {
    // TODO: The qualified record field is a leftover from heterogeneous
    // partitions, the indexers can be indexed by the offset instead.
    const auto qf = qualified_record_field{schema, offset};
    indexers.emplace(qf, active_indexer_actor{});
  }
  self->send(store_builder, x);
}

void active_partition_state::add_flush_listener(flush_listener_actor listener) {
  TENZIR_DEBUG("{} adds a new 'flush' subscriber: {}", *self, listener);
  flush_listeners.emplace_back(std::move(listener));
  detail::notify_listeners_if_clean(*this, *stage);
}

void active_partition_state::notify_flush_listeners() {
  TENZIR_DEBUG("{} sends 'flush' messages to {} listeners", *self,
               flush_listeners.size());
  for (auto& listener : flush_listeners)
    self->send(listener, atom::flush_v);
  flush_listeners.clear();
}

caf::expected<tenzir::chunk_ptr>
pack_full(const active_partition_state::serialization_data& x,
          const record_type& combined_schema) {
  flatbuffers::FlatBufferBuilder builder;
  auto uuid = pack(builder, x.id);
  if (!uuid)
    return uuid.error();
  std::vector<flatbuffers::Offset<fbs::value_index::LegacyQualifiedValueIndex>>
    indices;
  std::vector<tenzir::chunk_ptr> external_indices;
  // Note that the deserialization code relies on the order of indexers within
  // the flatbuffers being preserved.
  for (const auto& [name, chunk] : x.indexer_chunks) {
    auto fieldname = builder.CreateString(name);
    auto data = flatbuffers::Offset<flatbuffers::Vector<uint8_t>>{};
    auto size = size_t{0};
    auto external_idx = size_t{0};
    if (chunk) {
      auto compressed_chunk = chunk::compress(as_bytes(chunk));
      if (!compressed_chunk)
        return compressed_chunk.error();
      size = (*compressed_chunk)->size();
      // This threshold is an educated guess to keep tiny indices inline
      // to reduce additional page loads and huge indices out of the way.
      constexpr auto INDEXER_INLINE_THRESHOLD = 4096ull;
      if (size < INDEXER_INLINE_THRESHOLD) {
        data = builder.CreateVector(
          reinterpret_cast<const uint8_t*>((*compressed_chunk)->data()), size);
      } else {
        external_indices.emplace_back(std::move(*compressed_chunk));
        // The index into the flatbuffer_container is 1 + index into
        // `external_indices`.
        external_idx = external_indices.size();
      }
    }
    fbs::value_index::detail::LegacyValueIndexBuilder vbuilder(builder);
    if (chunk)
      vbuilder.add_decompressed_size(chunk->size());
    if (external_idx > 0)
      vbuilder.add_caf_0_18_external_container_idx(external_idx);
    else
      vbuilder.add_caf_0_18_data(data);
    auto vindex = vbuilder.Finish();
    fbs::value_index::LegacyQualifiedValueIndexBuilder qbuilder(builder);
    qbuilder.add_field_name(fieldname);
    qbuilder.add_index(vindex);
    auto qindex = qbuilder.Finish();
    indices.push_back(qindex);
  }
  auto indexes = builder.CreateVector(indices);
  // Serialize schema.
  auto schema_bytes = as_bytes(combined_schema);
  auto schema_offset = builder.CreateVector(
    reinterpret_cast<const uint8_t*>(schema_bytes.data()), schema_bytes.size());
  std::vector<flatbuffers::Offset<fbs::partition::detail::LegacyTypeIDs>> tids;
  for (const auto& kv : x.type_ids) {
    auto name = builder.CreateString(kv.first);
    auto ids = fbs::serialize_bytes(builder, kv.second);
    if (!ids)
      return ids.error();
    fbs::partition::detail::LegacyTypeIDsBuilder tids_builder(builder);
    tids_builder.add_name(name);
    tids_builder.add_ids(*ids);
    tids.push_back(tids_builder.Finish());
  }
  auto type_ids = builder.CreateVector(tids);
  // Serialize synopses.
  auto maybe_ps = pack(builder, *x.synopsis);
  if (!maybe_ps)
    return maybe_ps.error();
  flatbuffers::Offset<fbs::partition::detail::StoreHeader> store_header = {};
  auto store_name = builder.CreateString(x.store_id);
  auto store_data = builder.CreateVector(
    reinterpret_cast<const uint8_t*>(x.store_header->data()),
    x.store_header->size());
  fbs::partition::detail::StoreHeaderBuilder store_builder(builder);
  store_builder.add_id(store_name);
  store_builder.add_data(store_data);
  store_header = store_builder.Finish();
  fbs::partition::LegacyPartitionBuilder legacy_builder(builder);
  legacy_builder.add_uuid(*uuid);
  legacy_builder.add_events(x.events);
  legacy_builder.add_indexes(indexes);
  legacy_builder.add_partition_synopsis(*maybe_ps);
  legacy_builder.add_schema(schema_offset);
  legacy_builder.add_type_ids(type_ids);
  legacy_builder.add_store(store_header);
  auto partition_v0 = legacy_builder.Finish();
  fbs::PartitionBuilder partition_builder(builder);
  partition_builder.add_partition_type(fbs::partition::Partition::legacy);
  partition_builder.add_partition(partition_v0.Union());
  auto partition = partition_builder.Finish();
  FinishPartitionBuffer(builder, partition);
  auto chunk = chunk::make(builder.Release());
  // To keep things simple we always write a `SegmentedFile`,
  // even if all indices are inline.
  fbs::flatbuffer_container_builder cbuilder;
  cbuilder.add(as_bytes(chunk));
  for (auto const& index : external_indices)
    cbuilder.add(as_bytes(index));
  auto container = std::move(cbuilder).finish(fbs::PartitionIdentifier());
  return std::move(container).dissolve();
}

active_partition_actor::behavior_type active_partition(
  active_partition_actor::stateful_pointer<active_partition_state> self,
  type schema, uuid id, filesystem_actor filesystem, caf::settings index_opts,
  const index_config& synopsis_opts, const store_actor_plugin* store_plugin,
  std::shared_ptr<tenzir::taxonomies> taxonomies) {
  TENZIR_TRACE_SCOPE("active partition {} {}", TENZIR_ARG(self->id()),
                     TENZIR_ARG(id));
  self->state.self = self;
  self->state.filesystem = std::move(filesystem);
  self->state.streaming_initiated = false;
  self->state.data.id = id;
  self->state.data.events = 0;
  self->state.data.synopsis = caf::make_copy_on_write<partition_synopsis>();
  self->state.data.synopsis.unshared().schema = std::move(schema);
  self->state.partition_capacity
    = get_or(index_opts, "cardinality", defaults::max_partition_size);
  self->state.synopsis_index_config = synopsis_opts;
  self->state.store_plugin = store_plugin;
  self->state.taxonomies = taxonomies;
  auto make_stage = [&] {
    return detail::attach_notifying_stream_stage(
      self, false,
      [=](caf::unit_t&) {
        // nop
      },
      [=](caf::unit_t&, caf::downstream<table_slice>& out, table_slice x) {
        TENZIR_TRACE_SCOPE("partition {} got table slice {} {}",
                           self->state.data.id, TENZIR_ARG(out), TENZIR_ARG(x));
        x.offset(self->state.data.events);
        // Adjust the import time range iff necessary.
        auto& mutable_synopsis = self->state.data.synopsis.unshared();
        mutable_synopsis.min_import_time = std::min(
          self->state.data.synopsis->min_import_time, x.import_time());
        mutable_synopsis.max_import_time = std::max(
          self->state.data.synopsis->max_import_time, x.import_time());
        // We rely on `invalid_id` actually being the highest possible id
        // when using `min()` below.
        static_assert(invalid_id == std::numeric_limits<tenzir::id>::max());
        auto first = x.offset();
        auto last = x.offset() + x.rows();
        const auto& schema = x.schema();
        TENZIR_ASSERT(!schema.name().empty());
        auto it = self->state.data.type_ids.emplace(schema.name(), ids{}).first;
        auto& ids = it->second;
        TENZIR_ASSERT(first >= ids.size());
        // Mark the ids of this table slice for the current type.
        ids.append_bits(false, first - ids.size());
        ids.append_bits(true, last - first);
        self->state.data.events += x.rows();
        self->state.data.synopsis.unshared().add(
          x, self->state.partition_capacity, self->state.synopsis_index_config);
        size_t column_idx = -1;
        for (const auto& [field, offset] :
             caf::get<record_type>(schema).leaves()) {
          column_idx++;
          // TODO: The qualified record field is a leftover from heterogeneous
          // partitions, the indexers can be indexed by the offset instead.
          const auto qf = qualified_record_field{schema, offset};
          auto& idx = self->state.indexers[qf];
          if (idx)
            continue;
          if (should_skip_index_creation(
                field.type, qf, self->state.synopsis_index_config.rules))
            continue;
          auto value_index
            = factory<tenzir::value_index>::make(field.type, index_opts);
          if (!value_index) {
            TENZIR_WARN("{} failed to spawn active indexer with options {} for "
                        "field {}: value index missing",
                        *self, index_opts, field);
            continue;
          }
          idx = self->spawn(active_indexer, column_idx, std::move(value_index));
          auto slot = self->state.stage->add_outbound_path(idx);
          TENZIR_DEBUG("{} spawned new active indexer for field {} at slot {}",
                       *self, field.name, slot);
        }
        out.push(x);
      },
      [=](caf::unit_t&, const caf::error& err) {
        TENZIR_DEBUG("active partition {} finalized streaming {}", id,
                     render(err));
        // We get an 'unreachable' error when the stream becomes unreachable
        // because the actor was destroyed; in this case the state was already
        // destroyed during `local_actor::on_exit()`.
        if (err && err != caf::exit_reason::unreachable
            && err != ec::end_of_input) {
          TENZIR_ERROR("{} aborts with error: {}", *self, err);
          return;
        }
      },
      caf::policy::arg<caf::broadcast_downstream_manager<table_slice>>{});
  };
  self->state.stage = make_stage();
  self->state.data.store_id = self->state.store_plugin->name();
  auto builder_and_header = self->state.store_plugin->make_store_builder(
    self->state.filesystem, self->state.data.id);
  if (!builder_and_header) {
    TENZIR_ERROR("{} failed to create a store builder: {}", *self,
                 builder_and_header.error());
    return active_partition_actor::behavior_type::make_empty_behavior();
  }
  auto& [builder, header] = *builder_and_header;
  self->state.data.store_header = chunk::make_empty();
  self->state.data.store_header = header;
  self->state.store_builder = builder;
  auto slot = self->state.stage->add_outbound_path(builder);
  dynamic_cast<decltype(make_stage())::pointer>(self->state.stage.get())
    ->set_notification_slot(slot);
  TENZIR_DEBUG("{} spawned new active store at slot {}", *self, slot);
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    TENZIR_DEBUG("{} received EXIT from {} with reason: {}", *self, msg.source,
                 msg.reason);
    if (self->state.streaming_initiated && self->state.stage) {
      detail::shutdown_stream_stage(self->state.stage);
    }
    // Delay shutdown if we're currently in the process of persisting.
    if (self->state.persistence_promise.pending()) {
      std::call_once(self->state.shutdown_once, [=] {
        TENZIR_DEBUG("{} delays partition shutdown because it is still "
                     "writing to disk",
                     *self);
      });
      using namespace std::chrono_literals;
      // Ideally, we would use a self->delayed_delegate(self, ...) here, but CAF
      // does not have this functionality. Since we do not care about the return
      // value of the partition outselves, and the handler we delegate to
      // already uses a response promise, we send the message anonymously. We
      // also need to actor_cast self, since sending an exit message to a typed
      // actor without using self->send_exit is not supported.
      caf::delayed_anon_send(caf::actor_cast<caf::actor>(self), 100ms, msg);
      return;
    }
    TENZIR_VERBOSE("{} shuts down after persisting partition state", *self);
    // TODO: We must actor_cast to caf::actor here because 'shutdown' operates
    // on 'std::vector<caf::actor>' only. That should probably be generalized
    // in the future.
    auto indexers = std::vector<caf::actor>{};
    indexers.reserve(self->state.indexers.size());
    auto copy = std::exchange(self->state.indexers, {});
    for ([[maybe_unused]] auto&& [qf, indexer] : std::move(copy))
      indexers.push_back(caf::actor_cast<caf::actor>(std::move(indexer)));
    shutdown<policy::parallel>(self, std::move(indexers));
  });
  return {
    [self](atom::erase) -> caf::result<atom::done> {
      // Erase is sent by the disk monitor to erase this partition
      // from disk, but an active partition does not have any files
      // on disk, so it should never get selected for deletion.
      TENZIR_WARN("{} got erase atom as an active partition", *self);
      return caf::make_error(ec::logic_error, "can not erase the active "
                                              "partition");
    },
    [self](caf::stream<table_slice> in) {
      self->state.streaming_initiated = true;
      return self->state.stage->add_inbound_path(in);
    },
    [self](table_slice& slice) {
      self->state.handle_slice(std::move(slice));
    },
    [self](atom::subscribe, atom::flush, const flush_listener_actor& listener) {
      self->state.add_flush_listener(listener);
    },
    [self](atom::persist, const std::filesystem::path& part_path,
           const std::filesystem::path& synopsis_path)
      -> caf::result<partition_synopsis_ptr> {
      TENZIR_DEBUG("{} got persist atom", *self);
      // Ensure that the response promise has not already been initialized.
      TENZIR_ASSERT(!self->state.persistence_promise.source());
      self->state.persist_path = part_path;
      self->state.synopsis_path = synopsis_path;
      self->state.persisted_indexers = 0;
      self->state.persistence_promise
        = self->make_response_promise<partition_synopsis_ptr>();
      self->request(self->state.store_builder, caf::infinite, atom::persist_v)
        .then(
          [self](resource& store_file) {
            self->state.data.synopsis.unshared().store_file
              = std::move(store_file);
            auto& indexers = self->state.indexers;
            auto valid_count
              = std::count_if(indexers.begin(), indexers.end(), [](auto& idx) {
                  return idx.second != nullptr;
                });
            if (self->state.persistence_promise.pending()
                && self->state.persisted_indexers
                     == detail::narrow_cast<size_t>(valid_count)) {
              serialize(self);
            }
          },
          [self](caf::error err) {
            TENZIR_ERROR("{} failed to get the store info {}", *self, err);
            if (self->state.persistence_promise.pending()) {
              self->state.persistence_promise.deliver(std::move(err));
            }
          });
      self->send(self, atom::internal_v, atom::persist_v, atom::resume_v);
      return self->state.persistence_promise;
    },
    [self](atom::internal, atom::persist, atom::resume) -> caf::result<void> {
      TENZIR_TRACE("{} resumes persist atom {}", *self,
                   self->state.indexers.size());
      if (self->state.streaming_initiated && self->state.stage) {
        if (self->state.stage->inbound_paths().empty()) {
          detail::shutdown_stream_stage(self->state.stage);
        } else {
          using namespace std::chrono_literals;
          auto rp = self->make_response_promise<void>();
          detail::weak_run_delayed(self, 50ms, [self, rp]() mutable {
            rp.delegate(static_cast<active_partition_actor>(self),
                        atom::internal_v, atom::persist_v, atom::resume_v);
          });
          return rp;
        }
      }
      return {};
      if (self->state.indexers.empty()) {
        self->state.persistence_promise.deliver(
          caf::make_error(ec::logic_error, "partition has no indexers"));
        return {};
      }
      auto& indexers = self->state.indexers;
      auto valid_count
        = std::count_if(indexers.begin(), indexers.end(), [](auto& idx) {
            return idx.second != nullptr;
          });

      if (0u == valid_count) {
        // We call serialize from the response handler of persist request to the
        // store in this case.
        return {};
      }
      TENZIR_DEBUG("{} sends 'snapshot' to {} indexers", *self, valid_count);
      for (auto& [field, indexer] : self->state.indexers) {
        if (indexer == nullptr)
          continue;
        self->request(indexer, caf::infinite, atom::snapshot_v)
          .then(
            [=](chunk_ptr chunk) {
              ++self->state.persisted_indexers;
              if (!self->state.persistence_promise.pending()) {
                TENZIR_WARN("{} ignores persisted indexer because the "
                            "persistence promise is already fulfilled",
                            *self);
                return;
              }
              auto sender = self->current_sender()->id();
              if (!chunk) {
                TENZIR_ERROR("{} failed to persist indexer {}", *self, sender);
                self->state.persistence_promise.deliver(caf::make_error(
                  ec::unspecified, "failed to persist indexer", sender));
                return;
              }
              TENZIR_DEBUG("{} got chunk from {}", *self, sender);
              self->state.chunks.emplace(sender, chunk);
              if (self->state.persisted_indexers
                  < detail::narrow_cast<size_t>(valid_count)) {
                TENZIR_DEBUG(
                  "{} waits for more chunks after receiving {} out of "
                  "{}",
                  *self, self->state.persisted_indexers, valid_count);
                return;
              }
              if (self->state.data.synopsis->store_file.url.empty()) {
                TENZIR_DEBUG("{} waits for the store to persist", *self);
                return;
              }
              serialize(self);
            },
            [=, field_ = field](caf::error err) {
              TENZIR_ERROR("{} failed to persist indexer for {} with error: {}",
                           *self, field_.name(), err);
              ++self->state.persisted_indexers;
              if (!self->state.persistence_promise.pending())
                self->state.persistence_promise.deliver(std::move(err));
            });
      }
      return {};
    },
    [self](atom::query, query_context query_context) -> caf::result<uint64_t> {
      if (!self->state.data.synopsis->schema)
        return caf::make_error(ec::logic_error,
                               "active partition must have a schema");
      auto resolved = resolve(*self->state.taxonomies, query_context.expr,
                              self->state.data.synopsis->schema);
      if (!resolved)
        return std::move(resolved.error());
      query_context.expr = std::move(*resolved);
      return self->delegate(self->state.store_builder, atom::query_v,
                            std::move(query_context));
    },
    [self](atom::status, status_verbosity v,
           duration d) -> caf::typed_response_promise<record> {
      struct extra_state {
        size_t memory_usage = 0;
        void deliver(caf::typed_response_promise<record>&& promise,
                     record&& content) {
          content["memory-usage"] = uint64_t{memory_usage};
          promise.deliver(std::move(content));
        }
      };
      auto rs = make_status_request_state<extra_state>(self);
      auto indexer_states = list{};
      // Reservation is necessary to make sure the entries don't get relocated
      // as the underlying vector grows - `ps` would refer to the wrong memory
      // otherwise.
      const auto timeout = d / 10 * 9;
      indexer_states.reserve(self->state.indexers.size());
      for (auto& i : self->state.indexers) {
        if (!i.second)
          continue;
        auto& ps = caf::get<record>(indexer_states.emplace_back(record{}));
        collect_status(
          rs, timeout, v, i.second,
          [rs, v, &ps, &field = i.first](record& response) {
            ps["field"] = field.name();
            ps["type"] = fmt::to_string(field.type());
            auto it = response.find("memory-usage");
            if (it != response.end()) {
              if (const auto* s = caf::get_if<uint64_t>(&it->second))
                rs->memory_usage += *s;
            }
            if (v >= status_verbosity::debug)
              merge(response, ps, policy::merge_lists::no);
          },
          [rs, &ps, &field = i.first](caf::error& err) {
            TENZIR_WARN("{} failed to retrieve status from {}: {}", *rs->self,
                        field.name(), err);
            ps["error"] = fmt::to_string(err);
          });
      }
      rs->content["indexers"] = std::move(indexer_states);
      if (v >= status_verbosity::debug)
        detail::fill_status_map(rs->content, self);
      return rs->promise;
    },
  };
}

} // namespace tenzir
