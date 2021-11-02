//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/active_partition.hpp"

#include "vast/fwd.hpp"

#include "vast/address_synopsis.hpp"
#include "vast/aliases.hpp"
#include "vast/chunk.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/table_slice.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/fill_status_map.hpp"
#include "vast/detail/notifying_stream_manager.hpp"
#include "vast/detail/partition_common.hpp"
#include "vast/detail/settings.hpp"
#include "vast/detail/tracepoint.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/fbs/partition.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/fbs/uuid.hpp"
#include "vast/hash/xxhash.hpp"
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

/// Gets the ACTIVE INDEXER at a certain position.
active_indexer_actor active_partition_state::indexer_at(size_t position) const {
  VAST_ASSERT(position < indexers.size());
  return as_vector(indexers)[position].second;
}

const vast::legacy_record_type&
active_partition_state::combined_layout() const {
  return data.combined_layout;
}

const std::unordered_map<std::string, ids>&
active_partition_state::type_ids() const {
  return data.type_ids;
}

void active_partition_state::add_flush_listener(flush_listener_actor listener) {
  VAST_DEBUG("{} adds a new 'flush' subscriber: {}", *self, listener);
  flush_listeners.emplace_back(std::move(listener));
  detail::notify_listeners_if_clean(*this, *stage);
}

void active_partition_state::notify_flush_listeners() {
  VAST_DEBUG("{} sends 'flush' messages to {} listeners", *self,
             flush_listeners.size());
  for (auto& listener : flush_listeners)
    self->send(listener, atom::flush_v);
  flush_listeners.clear();
}

bool partition_selector::operator()(const qualified_record_field& filter,
                                    const table_slice_column& column) const {
  return filter == column.field();
}

caf::expected<flatbuffers::Offset<fbs::Partition>>
pack(flatbuffers::FlatBufferBuilder& builder,
     const active_partition_state::serialization_data& x) {
  auto uuid = pack(builder, x.id);
  if (!uuid)
    return uuid.error();
  std::vector<flatbuffers::Offset<fbs::qualified_value_index::v0>> indices;
  // Note that the deserialization code relies on the order of indexers within
  // the flatbuffers being preserved.
  for (const auto& [name, chunk] : x.indexer_chunks) {
    auto data = builder.CreateVector(
      reinterpret_cast<const uint8_t*>(chunk->data()), chunk->size());
    auto fieldname = builder.CreateString(name);
    fbs::value_index::v0Builder vbuilder(builder);
    vbuilder.add_data(data);
    auto vindex = vbuilder.Finish();
    fbs::qualified_value_index::v0Builder qbuilder(builder);
    qbuilder.add_field_name(fieldname);
    qbuilder.add_index(vindex);
    auto qindex = qbuilder.Finish();
    indices.push_back(qindex);
  }
  auto indexes = builder.CreateVector(indices);
  // Serialize layout.
  auto combined_layout = fbs::serialize_bytes(builder, x.combined_layout);
  if (!combined_layout)
    return combined_layout.error();
  std::vector<flatbuffers::Offset<fbs::type_ids::v0>> tids;
  for (const auto& kv : x.type_ids) {
    auto name = builder.CreateString(kv.first);
    auto ids = fbs::serialize_bytes(builder, kv.second);
    if (!ids)
      return ids.error();
    fbs::type_ids::v0Builder tids_builder(builder);
    tids_builder.add_name(name);
    tids_builder.add_ids(*ids);
    tids.push_back(tids_builder.Finish());
  }
  auto type_ids = builder.CreateVector(tids);
  // Serialize synopses.
  auto maybe_ps = pack(builder, *x.synopsis);
  if (!maybe_ps)
    return maybe_ps.error();
  flatbuffers::Offset<fbs::partition::store_header::v0> store_header = {};
  auto store_name = builder.CreateString(x.store_id);
  auto store_data = builder.CreateVector(
    reinterpret_cast<const uint8_t*>(x.store_header->data()),
    x.store_header->size());
  fbs::partition::store_header::v0Builder store_builder(builder);
  store_builder.add_id(store_name);
  store_builder.add_data(store_data);
  store_header = store_builder.Finish();
  fbs::partition::v0Builder v0_builder(builder);
  v0_builder.add_uuid(*uuid);
  v0_builder.add_offset(x.offset);
  v0_builder.add_events(x.events);
  v0_builder.add_indexes(indexes);
  v0_builder.add_partition_synopsis(*maybe_ps);
  v0_builder.add_combined_layout(*combined_layout);
  v0_builder.add_type_ids(type_ids);
  v0_builder.add_store(store_header);
  auto partition_v0 = v0_builder.Finish();
  fbs::PartitionBuilder partition_builder(builder);
  partition_builder.add_partition_type(fbs::partition::Partition::v0);
  partition_builder.add_partition(partition_v0.Union());
  auto partition = partition_builder.Finish();
  fbs::FinishPartitionBuffer(builder, partition);
  return partition;
}

active_partition_actor::behavior_type active_partition(
  active_partition_actor::stateful_pointer<active_partition_state> self,
  uuid id, filesystem_actor filesystem, caf::settings index_opts,
  caf::settings synopsis_opts, store_actor store, std::string store_id,
  chunk_ptr header) {
  self->state.self = self;
  self->state.name = "partition-" + to_string(id);
  self->state.filesystem = std::move(filesystem);
  self->state.streaming_initiated = false;
  self->state.synopsis_opts = std::move(synopsis_opts);
  self->state.data.id = id;
  self->state.data.offset = invalid_id;
  self->state.data.events = 0;
  self->state.data.synopsis = std::make_shared<partition_synopsis>();
  self->state.data.store_id = store_id;
  self->state.data.store_header = std::move(header);
  self->state.partition_local_stores = store_id != "archive";
  self->state.store = std::move(store);
  put(self->state.synopsis_opts, "buffer-input-data", true);
  // The active partition stage is a caf stream stage that takes
  // a stream of `table_slice` as input and produces several
  // streams of `table_slice_column` as output.
  self->state.stage = detail::attach_notifying_stream_stage(
    self, true,
    [=](caf::unit_t&) {
      // nop
    },
    [=](caf::unit_t&, caf::downstream<table_slice_column>& out, table_slice x) {
      VAST_TRACE_SCOPE("partition {} got table slice {} {}",
                       self->state.data.id, VAST_ARG(out), VAST_ARG(x));
      // We rely on `invalid_id` actually being the highest possible id
      // when using `min()` below.
      static_assert(invalid_id == std::numeric_limits<vast::id>::max());
      auto first = x.offset();
      auto last = x.offset() + x.rows();
      auto layout = flatten(x.layout());
      auto it = self->state.data.type_ids.emplace(layout.name(), ids{}).first;
      auto& ids = it->second;
      VAST_ASSERT(first >= ids.size());
      // Mark the ids of this table slice for the current type.
      ids.append_bits(false, first - ids.size());
      ids.append_bits(true, last - first);
      self->state.data.offset = std::min(x.offset(), self->state.data.offset);
      self->state.data.events += x.rows();
      self->state.data.synopsis->add(x, self->state.synopsis_opts);
      size_t col = 0;
      VAST_ASSERT(!layout.fields.empty());
      for (auto& field : layout.fields) {
        auto qf = qualified_record_field{layout.name(), field};
        auto& idx = self->state.indexers[qf];
        if (!idx) {
          self->state.data.combined_layout.fields.push_back(
            as_record_field(qf));
          idx = self->spawn(active_indexer, field.type, index_opts);
          auto slot = self->state.stage->add_outbound_path(idx);
          self->state.stage->out().set_filter(slot, qf);
          VAST_DEBUG("{} spawned new indexer for field {} at slot {}", *self,
                     field.name, slot);
        }
        out.push(table_slice_column{x, col++, qf});
      }
    },
    [=](caf::unit_t&, const caf::error& err) {
      VAST_DEBUG("active partition {} finalized streaming {}", id, render(err));
      // We get an 'unreachable' error when the stream becomes unreachable
      // because the actor was destroyed; in this case the state was already
      // destroyed during `local_actor::on_exit()`.
      if (err && err != caf::exit_reason::unreachable) {
        VAST_ERROR("{} aborts with error: {}", *self, render(err));
        return;
      }
    },
    // Every "outbound path" has a path_state, which consists of a "Filter"
    // and a vector of "T", the output buffer. In the case of a partition,
    // we have:
    //
    //   T:      vast::table_slice_column
    //   Filter: vast::qualified_record_field
    //   Select: vast::system::partition_selector
    //
    // NOTE: The broadcast_downstream_manager has to iterate over all
    // indexers, and compute the qualified record field name for each. A
    // specialized downstream manager could optimize this by using e.g. a map
    // from qualified record fields to downstream indexers.
    caf::policy::arg<caf::broadcast_downstream_manager<
      table_slice_column, vast::qualified_record_field, partition_selector>>{});
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    VAST_DEBUG("{} received EXIT from {} with reason: {}", *self, msg.source,
               msg.reason);
    if (self->state.streaming_initiated
        && self->state.stage->inbound_paths().empty()) {
      self->state.stage->out().fan_out_flush();
      self->state.stage->out().close();
      self->state.stage->out().force_emit_batches();
    }
    // Delay shutdown if we're currently in the process of persisting.
    if (self->state.persistence_promise.pending()) {
      std::call_once(self->state.shutdown_once, [=] {
        VAST_DEBUG("{} delays partition shutdown because it is still "
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
    VAST_VERBOSE("{} shuts down after persisting partition state", *self);
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
      VAST_WARN("{} got erase atom as an active partition", *self);
      return caf::make_error(ec::logic_error, "can not erase the active "
                                              "partition");
    },
    [self](caf::stream<table_slice> in) {
      self->state.streaming_initiated = true;
      return self->state.stage->add_inbound_path(in);
    },
    [self](atom::subscribe, atom::flush, const flush_listener_actor& listener) {
      self->state.add_flush_listener(listener);
    },
    [self](atom::persist, const std::filesystem::path& part_dir,
           const std::filesystem::path& synopsis_dir) {
      VAST_DEBUG("{} got persist atom", *self);
      // Ensure that the response promise has not already been initialized.
      VAST_ASSERT(
        !static_cast<caf::response_promise&>(self->state.persistence_promise)
           .source());
      self->state.persist_path = part_dir;
      self->state.synopsis_path = synopsis_dir;
      self->state.persisted_indexers = 0;
      self->state.persistence_promise
        = self->make_response_promise<std::shared_ptr<partition_synopsis>>();
      self->send(self, atom::internal_v, atom::persist_v, atom::resume_v);
      return self->state.persistence_promise;
    },
    [self](atom::internal, atom::persist, atom::resume) {
      VAST_TRACE("{} resumes persist atom {}", *self,
                 self->state.indexers.size());
      if (self->state.streaming_initiated
          && self->state.stage->inbound_paths().empty()) {
        self->state.stage->out().fan_out_flush();
        self->state.stage->out().close();
        self->state.stage->out().force_emit_batches();
      } else {
        using namespace std::chrono_literals;
        self->delayed_send(self, 50ms, atom::internal_v, atom::persist_v,
                           atom::resume_v);
        return;
      }
      if (self->state.indexers.empty()) {
        self->state.persistence_promise.deliver(
          caf::make_error(ec::logic_error, "partition has no indexers"));
        return;
      }
      VAST_DEBUG("{} sends 'snapshot' to {} indexers", *self,
                 self->state.indexers.size());
      for (auto& kv : self->state.indexers) {
        self->request(kv.second, caf::infinite, atom::snapshot_v)
          .then(
            [=](chunk_ptr chunk) {
              ++self->state.persisted_indexers;
              if (!self->state.persistence_promise.pending()) {
                VAST_WARN("{} ignores persisted indexer because the "
                          "persistence promise is already fulfilled",
                          *self);
                return;
              }
              auto sender = self->current_sender()->id();
              if (!chunk) {
                VAST_ERROR("{} failed to persist indexer {}", *self, sender);
                self->state.persistence_promise.deliver(caf::make_error(
                  ec::unspecified, "failed to persist indexer", sender));
                return;
              }
              VAST_DEBUG("{} got chunk from {}", *self, sender);
              self->state.chunks.emplace(sender, chunk);
              if (self->state.persisted_indexers
                  < self->state.indexers.size()) {
                VAST_DEBUG("{} waits for more chunks after receiving {} out of "
                           "{}",
                           *self, self->state.persisted_indexers,
                           self->state.indexers.size());
                return;
              }
              // Shrink synopses for addr fields to optimal size.
              self->state.data.synopsis->shrink();
              // TODO: It would probably make more sense if the partition
              // synopsis keeps track of offset/events internally.
              self->state.data.synopsis->offset = self->state.data.offset;
              self->state.data.synopsis->events = self->state.data.events;
              for (auto& [qf, actor] : self->state.indexers) {
                auto actor_id = actor.id();
                auto chunk_it = self->state.chunks.find(actor_id);
                if (chunk_it == self->state.chunks.end()) {
                  auto error = caf::make_error(ec::logic_error,
                                               "no chunk for for actor id "
                                                 + to_string(actor_id));
                  VAST_ERROR("{} failed to serialize: {}", self->state.name,
                             render(error));
                  self->state.persistence_promise.deliver(error);
                  return;
                }
                self->state.data.indexer_chunks.push_back(
                  std::make_pair(qf.field_name, chunk_it->second));
              }
              // Create the partition flatbuffer.
              flatbuffers::FlatBufferBuilder builder;
              auto partition = pack(builder, self->state.data);
              if (!partition) {
                VAST_ERROR("{} failed to serialize {} with error: {}", *self,
                           self->state.name, render(partition.error()));
                self->state.persistence_promise.deliver(partition.error());
                return;
              }
              VAST_ASSERT(self->state.persist_path);
              VAST_ASSERT(self->state.synopsis_path);
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
              // need to handle errors here, since VAST can still start
              // correctly (if a bit slower) when the write fails.
              flatbuffers::FlatBufferBuilder synopsis_builder;
              if (auto ps
                  = pack(synopsis_builder, *self->state.data.synopsis)) {
                fbs::PartitionSynopsisBuilder ps_builder(synopsis_builder);
                ps_builder.add_partition_synopsis_type(
                  fbs::partition_synopsis::PartitionSynopsis::v0);
                ps_builder.add_partition_synopsis(ps->Union());
                auto ps_offset = ps_builder.Finish();
                fbs::FinishPartitionSynopsisBuffer(synopsis_builder, ps_offset);
                auto ps_chunk = fbs::release(synopsis_builder);
                self
                  ->request(self->state.filesystem, caf::infinite,
                            atom::write_v, *self->state.synopsis_path, ps_chunk)
                  .then([=](atom::ok) {}, [=](caf::error) {});
              }
              auto fbchunk = fbs::release(builder);
              VAST_DEBUG("{} persists partition with a total size of "
                         "{} bytes",
                         *self, fbchunk->size());
              // TODO: Add a proper timeout.
              self
                ->request(self->state.filesystem, caf::infinite, atom::write_v,
                          *self->state.persist_path, fbchunk)
                .then(
                  [=](atom::ok) {
                    // Relinquish ownership and send the shrunken synopsis to
                    // the index.
                    self->state.persistence_promise.deliver(
                      self->state.data.synopsis);
                    self->state.data.synopsis.reset();
                  },
                  [=](caf::error e) {
                    self->state.persistence_promise.deliver(std::move(e));
                  });
              return;
            },
            [=](caf::error err) {
              VAST_ERROR("{} failed to persist indexer for {} with error: {}",
                         *self, kv.first.fqn(), render(err));
              ++self->state.persisted_indexers;
              if (!self->state.persistence_promise.pending())
                self->state.persistence_promise.deliver(std::move(err));
            });
      }
    },
    [self](vast::query query) -> caf::result<atom::done> {
      auto rp = self->make_response_promise<atom::done>();
      // Don't bother with with indexers, etc. if we already have an id set.
      if (!query.ids.empty()) {
        // TODO: Depending on the selectivity of the query and the rank of the
        // ids, it may still be beneficial to load some of the indexers to prune
        // the ids before hitting the store.
        rp.delegate(self->state.store, std::move(query));
        return rp;
      }
      // TODO: We should do a candidate check using `self->state.synopsis` and
      // return early if that doesn't yield any results.
      auto triples = detail::evaluate(self->state, query.expr);
      if (triples.empty()) {
        rp.deliver(atom::done_v);
        return rp;
      }
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
    [self](atom::status,
           status_verbosity v) -> caf::typed_response_promise<record> {
      struct extra_state {
        size_t memory_usage = 0;
        void deliver(caf::typed_response_promise<record>&& promise,
                     record&& content) {
          content["memory-usage"] = count{memory_usage};
          promise.deliver(std::move(content));
        }
      };
      auto rs = make_status_request_state<extra_state>(self);
      auto indexer_states = list{};
      // Reservation is necessary to make sure the entries don't get relocated
      // as the underlying vector grows - `ps` would refer to the wrong memory
      // otherwise.
      const auto timeout = defaults::system::initial_request_timeout / 5 * 3;
      indexer_states.reserve(self->state.indexers.size());
      for (auto& i : self->state.indexers) {
        auto& ps = caf::get<record>(indexer_states.emplace_back(record{}));
        collect_status(
          rs, timeout, v, i.second,
          [rs, v, &ps, &field = i.first](record& response) {
            ps["field"] = field.fqn();
            auto it = response.find("memory-usage");
            if (it != response.end()) {
              if (const auto* s = caf::get_if<count>(&it->second))
                rs->memory_usage += *s;
            }
            if (v >= status_verbosity::debug)
              merge(response, ps, policy::merge_lists::no);
          },
          [rs, &ps, &field = i.first](caf::error& err) {
            VAST_WARN("{} failed to retrieve status from {} : {}", *rs->self,
                      field.fqn(), fmt::to_string(err));
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

} // namespace vast::system
