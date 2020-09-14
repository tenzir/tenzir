/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/system/partition.hpp"

#include "vast/aliases.hpp"
#include "vast/chunk.hpp"
#include "vast/concept/hashable/xxhash.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/table_slice.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/detail/assert.hpp"
#include "vast/expression.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/fbs/partition.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/fbs/uuid.hpp"
#include "vast/fwd.hpp"
#include "vast/ids.hpp"
#include "vast/load.hpp"
#include "vast/logger.hpp"
#include "vast/qualified_record_field.hpp"
#include "vast/save.hpp"
#include "vast/system/filesystem.hpp"
#include "vast/system/index.hpp"
#include "vast/system/indexer.hpp"
#include "vast/system/shutdown.hpp"
#include "vast/system/terminate.hpp"
#include "vast/table_slice_column.hpp"
#include "vast/time.hpp"
#include "vast/type.hpp"
#include "vast/value_index.hpp"

#include <caf/attach_continuous_stream_stage.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/local_actor.hpp>
#include <caf/make_counted.hpp>
#include <caf/stateful_actor.hpp>

#include <flatbuffers/flatbuffers.h>

#include "caf/actor_system.hpp"
#include "caf/binary_deserializer.hpp"
#include "caf/binary_serializer.hpp"
#include "caf/broadcast_downstream_manager.hpp"
#include "caf/deserializer.hpp"
#include "caf/error.hpp"
#include "caf/exit_reason.hpp"
#include "caf/fwd.hpp"
#include "caf/scheduled_actor.hpp"
#include "caf/sec.hpp"

using namespace std::chrono;
using namespace caf;

namespace vast::system {

namespace {

// The three functions in this namespace take PartitionState as template
// argument because they are shared between the read-only and active
// partitions.
template <typename PartitionState>
caf::actor indexer_at(const PartitionState& state, size_t position) {
  VAST_ASSERT(position < state.indexers.size());
  auto& [_, indexer] = as_vector(state.indexers)[position];
  return indexer;
}

template <typename PartitionState>
caf::actor fetch_indexer(const PartitionState& state, const data_extractor& dx,
                         relational_operator op, const data& x) {
  VAST_TRACE(VAST_ARG(dx), VAST_ARG(op), VAST_ARG(x));
  // Sanity check.
  if (dx.offset.empty())
    return nullptr;
  auto& r = caf::get<record_type>(dx.type);
  auto k = r.resolve(dx.offset);
  VAST_ASSERT(k);
  auto index = r.flat_index_at(dx.offset);
  if (!index) {
    VAST_DEBUG(state.self, "got invalid offset for record type", dx.type);
    return nullptr;
  }
  return indexer_at(state, *index);
}

template <typename PartitionState>
caf::actor
fetch_indexer(const PartitionState& state, const attribute_extractor& ex,
              relational_operator op, const data& x) {
  VAST_TRACE(VAST_ARG(ex), VAST_ARG(op), VAST_ARG(x));
  if (ex.attr == atom::type_v) {
    // We know the answer immediately: all IDs that are part of the table.
    // However, we still have to "lift" this result into an actor for the
    // EVALUATOR.
    ids row_ids;
    for (auto& [name, ids] : state.type_ids)
      if (evaluate(name, op, x))
        row_ids |= ids;
    // TODO: Spawning a one-shot actor is quite expensive. Maybe the
    //       partition could instead maintain this actor lazily.
    return state.self->spawn([row_ids]() -> caf::behavior {
      return [=](const curried_predicate&) { return row_ids; };
    });
  }
  VAST_WARNING(state.self, "got unsupported attribute:", ex.attr);
  return nullptr;
}

template <typename PartitionState>
evaluation_triples
evaluate(const PartitionState& state, const expression& expr) {
  evaluation_triples result;
  // Pretend the partition is a table, and return fitted predicates for the
  // partitions layout.
  auto resolved = resolve(expr, state.combined_layout);
  for (auto& kvp : resolved) {
    // For each fitted predicate, look up the corresponding INDEXER
    // according to the specified type of extractor.
    auto& pred = kvp.second;
    auto get_indexer_handle = [&](const auto& ext, const data& x) {
      return state.fetch_indexer(ext, pred.op, x);
    };
    auto v = detail::overload(
      [&](const attribute_extractor& ex, const data& x) {
        return get_indexer_handle(ex, x);
      },
      [&](const data_extractor& dx, const data& x) {
        return get_indexer_handle(dx, x);
      },
      [](const auto&, const auto&) {
        return caf::actor{}; // clang-format fix
      });
    // Package the predicate, its position in the query and the required
    // INDEXER as a "job description".
    if (auto hdl = caf::visit(v, pred.lhs, pred.rhs))
      result.emplace_back(kvp.first, curried(pred), std::move(hdl));
  }
  // Return the list of jobs, to be used by the EVALUATOR.
  return result;
}

} // namespace

// TODO: Currently these wrappers are a bit useless, but eventually we
// probably want to implement them differently to avoid deserialization
// costs for the passive partitions.
caf::actor active_partition_state::indexer_at(size_t position) const {
  return vast::system::indexer_at(*this, position);
}

caf::actor passive_partition_state::indexer_at(size_t position) const {
  return vast::system::indexer_at(*this, position);
}

caf::actor active_partition_state::fetch_indexer(const attribute_extractor& ex,
                                                 relational_operator op,
                                                 const data& x) const {
  return vast::system::fetch_indexer(*this, ex, op, x);
}

caf::actor passive_partition_state::fetch_indexer(const attribute_extractor& ex,
                                                  relational_operator op,
                                                  const data& x) const {
  return vast::system::fetch_indexer(*this, ex, op, x);
}

caf::actor active_partition_state::fetch_indexer(const data_extractor& ex,
                                                 relational_operator op,
                                                 const data& x) const {
  return vast::system::fetch_indexer(*this, ex, op, x);
}

caf::actor passive_partition_state::fetch_indexer(const data_extractor& ex,
                                                  relational_operator op,
                                                  const data& x) const {
  return vast::system::fetch_indexer(*this, ex, op, x);
}

evaluation_triples
active_partition_state::evaluate(const expression& expr) const {
  return vast::system::evaluate(*this, expr);
}

evaluation_triples
passive_partition_state::evaluate(const expression& expr) const {
  return vast::system::evaluate(*this, expr);
}

bool partition_selector::operator()(const vast::qualified_record_field& filter,
                                    const table_slice_column& x) const {
  auto& layout = x.slice->layout();
  vast::qualified_record_field fqf{layout.name(), layout.fields.at(x.column)};
  return filter == fqf;
}

caf::expected<flatbuffers::Offset<fbs::Partition>>
pack(flatbuffers::FlatBufferBuilder& builder, const active_partition_state& x) {
  auto uuid = pack(builder, x.id);
  if (!uuid)
    return uuid.error();
  std::vector<flatbuffers::Offset<fbs::QualifiedValueIndex>> indices;
  // Note that the deserialization code relies on the order of indexers within
  // the flatbuffers being preserved.
  for (auto& [qf, actor] : x.indexers) {
    auto actor_id = actor.id();
    auto chunk_it = x.chunks.find(actor_id);
    if (chunk_it == x.chunks.end())
      return make_error(ec::logic_error,
                        "No chunk for for actor id " + to_string(actor_id));
    auto& chunk = chunk_it->second;
    auto data = builder.CreateVector(
      reinterpret_cast<const uint8_t*>(chunk->data()), chunk->size());
    auto fqf = builder.CreateString(qf.field_name);
    fbs::ValueIndexBuilder vbuilder(builder);
    vbuilder.add_data(data);
    auto vindex = vbuilder.Finish();
    fbs::QualifiedValueIndexBuilder qbuilder(builder);
    qbuilder.add_qualified_field_name(fqf);
    qbuilder.add_index(vindex);
    auto qindex = qbuilder.Finish();
    indices.push_back(qindex);
  }
  auto indexes = builder.CreateVector(indices);
  // Serialize layout.
  // TODO: Create a generic function for arbitrary type -> flatbuffers byte
  // array serialization.
  std::vector<char> buf;
  caf::binary_serializer bs{nullptr, buf};
  bs(x.combined_layout);
  auto layout_chunk = chunk::make(std::move(buf));
  auto combined_layout = builder.CreateVector(
    reinterpret_cast<const uint8_t*>(layout_chunk->data()),
    layout_chunk->size());
  std::vector<flatbuffers::Offset<fbs::TypeIds>> tids;
  for (const auto& kv : x.type_ids) {
    auto name = builder.CreateString(kv.first);
    buf.clear();
    caf::binary_serializer bs{nullptr, buf};
    bs(kv.second);
    auto ids = builder.CreateVector(
      reinterpret_cast<const uint8_t*>(buf.data()), buf.size());
    fbs::TypeIdsBuilder tids_builder(builder);
    tids_builder.add_name(name);
    tids_builder.add_ids(ids);
    tids.push_back(tids_builder.Finish());
  }
  auto type_ids = builder.CreateVector(tids);
  fbs::PartitionBuilder partition_builder(builder);
  partition_builder.add_version(fbs::Version::v0);
  partition_builder.add_uuid(*uuid);
  partition_builder.add_offset(x.offset);
  partition_builder.add_events(x.events);
  partition_builder.add_indexes(indexes);
  partition_builder.add_combined_layout(combined_layout);
  partition_builder.add_type_ids(type_ids);
  return partition_builder.Finish();
}

caf::error
unpack(const fbs::Partition& partition, passive_partition_state& state) {
  // Check that all fields exist.
  if (partition.version() != fbs::Version::v0)
    return make_error(ec::format_error, "unknown version for partition "
                                        "flatbuffer");
  if (!partition.uuid())
    return make_error(ec::format_error, "missing 'uuid' field in partition "
                                        "flatbuffer");
  auto combined_layout = partition.combined_layout();
  if (!combined_layout)
    return make_error(ec::format_error, "missing 'layouts' field in partition "
                                        "flatbuffer");
  auto indexes = partition.indexes();
  if (!indexes)
    return make_error(ec::format_error, "missing 'indexes' field in partition "
                                        "flatbuffer");
  for (auto qualified_index : *indexes) {
    if (!qualified_index->qualified_field_name())
      return make_error(ec::format_error, "missing field name in qualified "
                                          "index");
    auto index = qualified_index->index();
    if (!index)
      return make_error(ec::format_error, "missing index name in qualified "
                                          "index");
    if (!index->data())
      return make_error(ec::format_error, "missing data in index");
  }
  if (auto error = unpack(*partition.uuid(), state.id))
    return error;
  state.events = partition.events();
  state.offset = partition.offset();
  state.name = "partition-" + to_string(state.id);
  caf::binary_deserializer bds(
    nullptr, reinterpret_cast<const char*>(combined_layout->data()),
    combined_layout->size());
  if (auto error = bds(state.combined_layout))
    return error;
  // This condition should be '!=', but then we cant deserialize in unit tests
  // anymore without creating a bunch of index actors first. :/
  if (state.combined_layout.fields.size() < indexes->size()) {
    VAST_DEBUG(state.self, state.combined_layout.fields.size(), "fields vs",
               indexes->size(), "indexes");
    return make_error(ec::format_error, "incoherent number of indexers");
  }
  // We rely on the indexes being stored in the same order as the layout
  // fields, and need to preserve the same order in `indexer_states`.
  state.indexer_states.resize(indexes->size());
  for (size_t i = 0; i < indexes->size(); ++i) {
    auto qualified_index = indexes->Get(i);
    auto field = state.combined_layout.fields.at(i);
    auto& indexer_state = state.indexer_states.at(i);
    VAST_DEBUG("restoring indexer", i, "with name",
               qualified_index->qualified_field_name()->str(), "and type",
               field);
    // Deserialize the value index.
    indexer_state.first = qualified_record_field{
      qualified_index->qualified_field_name()->str(), field};
    auto index = qualified_index->index();
    auto data = index->data();
    auto& vindex_ptr = indexer_state.second;
    caf::binary_deserializer bds(
      nullptr, reinterpret_cast<const char*>(data->data()), data->size());
    if (auto error = bds(vindex_ptr))
      return error;
  }
  VAST_VERBOSE_ANON("restored", state.indexer_states.size(),
                    "indexers for partition", state.id);
  auto type_ids = partition.type_ids();
  for (size_t i = 0; i < type_ids->size(); ++i) {
    auto type_ids_tuple = type_ids->Get(i);
    auto name = type_ids_tuple->name();
    auto ids_data = type_ids_tuple->ids();
    auto& ids = state.type_ids[name->str()];
    caf::binary_deserializer bds(
      nullptr, reinterpret_cast<const char*>(ids_data->data()),
      ids_data->size());
    if (auto error = bds(ids))
      return error;
  }
  VAST_VERBOSE_ANON("restored", state.type_ids.size(),
                    "type to ids mappings for partition", state.id);
  return caf::none;
}

caf::behavior
active_partition(caf::stateful_actor<active_partition_state>* self, uuid id,
                 filesystem_type fs, caf::settings index_opts) {
  self->state.self = self;
  self->state.name = "partition-" + to_string(id);
  self->state.id = id;
  self->state.offset = vast::invalid_id;
  self->state.events = 0;
  self->state.fs_actor = fs;
  self->state.streaming_initiated = false;
  // The active partition stage is a caf stream stage that takes
  // a stream of `table_slice_ptr` as input and produces several
  // streams of `table_slice_column` as output.
  self->state.stage = caf::attach_continuous_stream_stage(
    self, [=](caf::unit_t&) { VAST_DEBUG(self, "initializes stream manager"); },
    [=](caf::unit_t&, caf::downstream<table_slice_column>& out,
        table_slice_ptr x) {
      VAST_DEBUG(self, "got new table slice", to_string(*x));
      // TODO: putting this line in the handshake message should be enough, but
      // for some reason it is not :( Why?
      self->state.streaming_initiated = true;
      // We rely on `invalid_id` actually being the highest possible id
      // when using `min()` below.
      VAST_ASSERT(vast::invalid_id == std::numeric_limits<vast::id>::max());
      auto first = x->offset();
      auto last = x->offset() + x->rows();
      auto it
        = self->state.type_ids.emplace(x->layout().name(), vast::ids{}).first;
      auto& ids = it->second;
      VAST_ASSERT(first >= ids.size());
      ids.append_bits(false, first - ids.size());
      ids.append_bits(true, last - first);
      self->state.offset = std::min(x->offset(), self->state.offset);
      self->state.events += x->rows();
      size_t col = 0;
      VAST_ASSERT(!x->layout().fields.empty());
      for (auto& field : x->layout().fields) {
        auto qf = qualified_record_field{x->layout().name(), field};
        auto& idx = self->state.indexers[qf];
        if (!idx) {
          self->state.combined_layout.fields.push_back(as_record_field(qf));
          idx = self->spawn(active_indexer, field.type, index_opts);
          auto slot = self->state.stage->add_outbound_path(idx);
          self->state.stage->out().set_filter(slot, qf);
          VAST_DEBUG(self, "spawned new indexer for field", field.name,
                     "at slot", slot);
        }
        out.push(table_slice_column{x, col++});
      }
    },
    [=](caf::unit_t&, const caf::error& err) {
      // We get an 'unreachable' error when the stream becomes unreachable
      // because the actor was destroyed; in this case we can't use `self`
      // anymore.
      if (err
          && caf::exit_reason{err.code()} != caf::exit_reason::unreachable) {
        VAST_ERROR(self, "was aborted with error", self->system().render(err));
        self->send_exit(self, err);
      }
      VAST_DEBUG_ANON("partition", id, "finalized streaming");
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
    caf::policy::arg<broadcast_downstream_manager<
      table_slice_column, vast::qualified_record_field, partition_selector>>{});
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    VAST_DEBUG(self, "received EXIT from", msg.source,
               "with reason:", msg.reason);
    if (self->state.stage->idle()) {
      self->state.stage->out().fan_out_flush();
      self->state.stage->out().force_emit_batches();
      self->state.stage->out().close();
    }
    // Delay shutdown if we're currently in the process of persisting.
    if (self->state.persistence_promise.pending()) {
      std::call_once(self->state.shutdown_once, [self] {
        VAST_VERBOSE(self,
                     "delaying partition shutdown because its still writing "
                     "to disk");
      });
      self->delayed_send(self, std::chrono::milliseconds(100), msg);
      return;
    }
    VAST_VERBOSE(self, "will shut down after persist finished");
    auto indexers = std::vector<caf::actor>{};
    auto indexer_ids = std::vector<caf::actor_id>{};
    for ([[maybe_unused]] auto& [_, idx] : self->state.indexers) {
      indexers.push_back(idx);
      indexer_ids.push_back(idx->id());
    }
    self->state.indexers.clear();
    shutdown<policy::parallel>(self, std::move(indexers));
  });
  return {
    [=](caf::stream<table_slice_ptr> in) {
      return self->state.stage->add_inbound_path(in);
    },
    [=](atom::persist, const path& part_dir) {
      auto& st = self->state;
      // Using `source()` to check if the promise was already initialized.
      if (!st.persistence_promise.source())
        st.persistence_promise = self->make_response_promise();
      st.persist_path = part_dir;
      st.persisted_indexers = 0;
      // Wait for outstanding data to avoid data loss.
      // TODO: Maybe a more elegant design would be to send a, say,
      // `resume` atom when finalizing the stream, but then the case
      // where the stream finishes before persisting starts becomes more
      // complicated.
      if (!self->state.streaming_initiated || !self->state.stage->idle()) {
        VAST_INFO(self, "waiting for stream before persisting");
        self->delayed_send(self, 50ms, atom::persist_v, part_dir);
        return st.persistence_promise;
      }
      self->state.stage->out().fan_out_flush();
      self->state.stage->out().force_emit_batches();
      self->state.stage->out().close();
      if (st.indexers.empty()) {
        st.persistence_promise.deliver(
          make_error(ec::logic_error, "partition has no indexers"));
        return st.persistence_promise;
      }
      VAST_VERBOSE(self, "sending 'snapshot' atom to", st.indexers.size(),
                   "indexers");
      for (auto& kv : st.indexers) {
        self->send(kv.second, atom::snapshot_v);
      }
      return st.persistence_promise;
    },
    // Semantically this is the "response" to the "request" represented by the
    // snapshot atom.
    [=](vast::chunk_ptr chunk) {
      ++self->state.persisted_indexers;
      auto sender = self->current_sender()->id();
      if (!chunk) {
        // TODO: If one indexer reports an error, should we abandon the
        // whole partition or still persist the remaining chunks?
        VAST_ERROR(self, "cant persist indexer", sender);
        return;
      }
      VAST_DEBUG(self, "got chunk from", sender);
      self->state.chunks.emplace(sender, chunk);
      if (self->state.persisted_indexers < self->state.indexers.size()) {
        VAST_DEBUG(self, "waiting for more chunks, got",
                   self->state.persisted_indexers, "need",
                   self->state.indexers.size());
        return;
      }
      flatbuffers::FlatBufferBuilder builder;
      auto partition = pack(builder, self->state);
      if (!partition) {
        VAST_ERROR(self, "error serializing partition", self->state.name);
        self->state.persistence_promise.deliver(partition.error());
        return;
      }
      builder.Finish(*partition, "P000");
      VAST_ASSERT(self->state.persist_path);
      auto fb = builder.Release();
      // TODO: This is duplicating code from one of the `chunk` constructors,
      // but otoh its maybe better to be explicit that we're creating a shared
      // pointer here.
      auto ys = std::make_shared<flatbuffers::DetachedBuffer>(std::move(fb));
      auto deleter = [=]() mutable { ys.reset(); };
      auto fbchunk = chunk::make(ys->size(), ys->data(), deleter);
      VAST_VERBOSE(self, "persisting partition with total size", ys->size(),
                   "bytes");
      self->state.persistence_promise.delegate(self->state.fs_actor,
                                               atom::write_v,
                                               *self->state.persist_path,
                                               fbchunk);
      return;
    },
    [=](const expression& expr) { return self->state.evaluate(expr); },
  };
}

caf::behavior
passive_partition(caf::stateful_actor<passive_partition_state>* self, uuid id,
                  filesystem_type fs, vast::path path) {
  self->state.self = self;
  auto passive_partition_behavior = caf::behavior{
    [=](caf::stream<table_slice_ptr>) {
      VAST_ASSERT(!"read-only partition can not receive new table slices");
    },
    [=](atom::persist, const vast::path&) {
      VAST_ASSERT(!"read-only partition does not need to be persisted");
    },
    [=](const expression& expr) { return self->state.evaluate(expr); },
  };
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    VAST_DEBUG(self, "received EXIT from", msg.source,
               "with reason:", msg.reason);
    auto indexers = std::vector<caf::actor>{};
    for (auto& [_, idx] : self->state.indexers)
      indexers.push_back(idx);
    // Receiving an EXIT message does not need to coincide with the state being
    // destructed, so we explicitly clear the vector to release the references.
    self->state.indexers.clear();
    if (msg.reason != caf::exit_reason::user_shutdown) {
      self->quit(msg.reason);
      return;
    }
    // When the shutdown was requested by the user (as opposed to the partition
    // just dropping out of the LRU cache), pro-active remove the indexers.
    terminate<policy::parallel>(self, std::move(indexers))
      .then(
        [=](atom::done) {
          VAST_DEBUG(self, "done shutting down indexers");
          self->quit();
        },
        [=](caf::error err) {
          VAST_ERROR(self, "error shutting down indexesr", err);
          self->quit(err);
        });
  });
  // We send a "read" to the fs actor and upon receiving the result deserialize
  // the flatbuffer and switch to the "normal" partition behavior for responding
  // to queries. The `skip` default handler is used to buffer all messages
  // arriving until then.
  self->set_default_handler(skip);
  self->send(caf::actor_cast<caf::actor>(fs), atom::read_v, path);
  return {
    [=](vast::chunk_ptr chunk) {
      // Deserialize chunk from the filesystem actor
      auto view
        = span(reinterpret_cast<const byte*>(chunk->data()), chunk->size());
      auto partition
        = fbs::as_versioned_flatbuffer<fbs::Partition>(view, fbs::Version::v0);
      if (!partition) {
        VAST_ERROR(self, "could not parse provided chunk as flatbuffer");
        self->quit(make_error(ec::format_error, "chunk did not contain valid "
                                                "partition flatbuffer"));
      }
      if (auto error = unpack(**partition, self->state)) {
        VAST_ERROR(self, "error unpacking partition", error);
        self->quit(error);
      }
      if (id != self->state.id)
        VAST_WARNING(self, "partition id mismatch: restored", self->state.id,
                     "from disk, expected", id);
      for (auto& kv : self->state.indexer_states) {
        auto field = kv.first;
        // Note that we rely on `self->state.indexers` always keeping elements
        // in insertion order in the underlying vector.
        self->state.indexers[field]
          = self->spawn(passive_indexer, id, std::move(kv.second));
      }
      // Switch to "normal" partition mode
      self->become(passive_partition_behavior);
      self->set_default_handler(caf::print_and_drop);
    },
    [=](caf::error err) {
      VAST_ERROR("Could not load partition", id, err);
      self->quit(err);
    },
  };
}

} // namespace vast::system
