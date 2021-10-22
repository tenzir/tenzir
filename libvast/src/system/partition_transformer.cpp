//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/partition_transformer.hpp"

#include "vast/fbs/utils.hpp"
#include "vast/logger.hpp"
#include "vast/partition_synopsis.hpp"
#include "vast/plugin.hpp"
#include "vast/transform.hpp"
#include "vast/value_index_factory.hpp"

#include <caf/attach_continuous_stream_stage.hpp>
#include <caf/attach_stream_stage.hpp>
#include <flatbuffers/flatbuffers.h>

namespace vast::system {

// Since we don't have to answer queries while this partition is being
// constructed, we don't have to spawn separate indexer actors and
// stream data but can just compute everything inline here.
void partition_transformer_state::add_slice(const table_slice& slice) {
  const auto& layout = slice.layout();
  data.events += slice.rows();
  data.synopsis->add(slice, synopsis_opts);
  VAST_ASSERT(slice.columns() == layout.fields.size());
  // Update type ids
  auto it = data.type_ids.emplace(layout.name(), ids{}).first;
  auto& ids = it->second;
  auto first = slice.offset();
  auto last = slice.offset() + slice.rows();
  VAST_ASSERT(first >= ids.size());
  ids.append_bits(false, first - ids.size());
  ids.append_bits(true, last - first);
  // Push the event data to the indexers.
  for (size_t i = 0; i < slice.columns(); ++i) {
    const auto& field = layout.fields[i];
    auto qf = qualified_record_field{layout.name(), field};
    auto it = indexers.find(qf);
    if (it == indexers.end()) {
      auto skip = vast::has_skip_attribute(field.type);
      auto idx
        = skip ? nullptr : factory<value_index>::make(field.type, index_opts);
      data.combined_layout.fields.push_back(as_record_field(qf));
      it = indexers.emplace(qf, std::move(idx)).first;
    }
    auto& idx = it->second;
    if (idx != nullptr) {
      auto column = table_slice_column{slice, i, qf};
      auto offset = column.slice().offset();
      for (size_t i = 0; i < column.size(); ++i)
        idx->append(column[i], offset + i);
    }
  }
}

void partition_transformer_state::finalize_data() {
  // Serialize the finished indexers.
  for (auto& [qf, idx] : indexers) {
    data.indexer_chunks.emplace_back(qf.field_name, chunkify(idx));
  }
  data.synopsis->shrink();
  // TODO: It would probably make more sense if the partition
  // synopsis keeps track of offset/events internally.
  data.synopsis->offset = data.offset;
  data.synopsis->events = data.events;
}

void partition_transformer_state::fulfill(
  partition_transformer_actor::stateful_pointer<partition_transformer_state>
    self,
  persist_eagerly&& eager_data, persist_lazily&& lazy_data) const {
  if (self->state.stream_error) {
    lazy_data.promise.deliver(self->state.stream_error);
    self->quit();
    return;
  }
  // The meta index data can always be regenerated on restart, so we don't need
  // strict error handling for it.
  self
    ->request(fs, caf::infinite, atom::write_v, lazy_data.synopsis_path,
              eager_data.synopsis_chunk)
    .then([](atom::ok) { /* nop */ },
          [path = lazy_data.synopsis_path](const caf::error& e) {
            VAST_WARN("could not write transformed synopsis to {}: {}", path,
                      e);
          });
  auto promise = lazy_data.promise;
  self
    ->request(fs, caf::infinite, atom::write_v, lazy_data.partition_path,
              eager_data.partition_chunk)
    .then(
      [self, promise](atom::ok) mutable {
        promise.deliver(self->state.data.synopsis);
        self->quit();
      },
      [self, promise](caf::error& e) mutable {
        promise.deliver(std::move(e));
        self->quit();
      });
}

partition_transformer_actor::behavior_type partition_transformer(
  partition_transformer_actor::stateful_pointer<partition_transformer_state>
    self,
  uuid id, std::string store_id, const caf::settings& synopsis_opts,
  const caf::settings& index_opts,
  idspace_distributor_actor idspace_distributor, filesystem_actor fs,
  transform_ptr transform) {
  using persist_lazily = partition_transformer_state::persist_lazily;
  using persist_eagerly = partition_transformer_state::persist_eagerly;
  const auto* store_plugin = plugins::find<vast::store_plugin>(store_id);
  if (!store_plugin) {
    self->quit(caf::make_error(ec::invalid_argument,
                               "could not find a store plugin named {}",
                               store_id));
    return partition_transformer_actor::behavior_type::make_empty_behavior();
  }
  auto builder_and_header = store_plugin->make_store_builder(fs, id);
  if (!builder_and_header) {
    self->quit(caf::make_error(ec::invalid_argument,
                               "could not create store builder for backend {}",
                               store_id));
    return partition_transformer_actor::behavior_type::make_empty_behavior();
  }
  self->state.data.id = id;
  self->state.data.store_id = store_id;
  self->state.data.offset = invalid_id;
  self->state.data.synopsis = std::make_shared<partition_synopsis>();
  self->state.data.store_header = builder_and_header->second;
  self->state.synopsis_opts = synopsis_opts;
  self->state.index_opts = index_opts;
  self->state.idspace_distributor = std::move(idspace_distributor);
  self->state.store_builder = builder_and_header->first;
  VAST_ASSERT(self->state.store_builder);
  self->state.stage = caf::attach_continuous_stream_stage(
    self, [](caf::unit_t&) {},
    [](caf::unit_t&, caf::downstream<vast::table_slice>& out,
       vast::table_slice x) {
      VAST_INFO("pushing slice");
      out.push(x);
    },
    [](caf::unit_t&, const caf::error&) { /* nop */ });
  self->state.stage->add_outbound_path(self->state.store_builder);
  self->state.fs = std::move(fs);
  self->state.transform = std::move(transform);
  return {
    [self](vast::table_slice& slice) {
      auto transformed = self->state.transform->apply(std::move(slice));
      if (!transformed) {
        VAST_ERROR("failed to apply transform");
        return;
      }
      self->state.events += transformed->rows();
      self->state.slices.push_back(std::move(*transformed));
    },
    [self](atom::done) {
      VAST_DEBUG("partition-transformer received all table slices");
      self
        ->request(self->state.idspace_distributor, caf::infinite,
                  atom::reserve_v, self->state.events)
        .then(
          [self](vast::id id) {
            self->send(self, atom::internal_v, atom::resume_v, atom::done_v,
                       id);
          },
          [self](const caf::error& e) {
            self->state.stream_error = e;
          });
    },
    [self](atom::internal, atom::resume, atom::done, vast::id offset) {
      VAST_DEBUG("partition-transformer got new offset {}", offset);
      self->state.data.offset = offset;
      for (auto& slice : self->state.slices) {
        slice.offset(offset);
        offset += slice.rows();
        self->state.add_slice(slice);
        self->state.stage->out().push(slice);
      }
      self->state.finalize_data();
      self->state.stage->out().fan_out_flush();
      self->state.stage->out().close();
      self->state.stage->out().force_emit_batches();
      auto eager_data = partition_transformer_state::persist_eagerly{};
      [&] {
        { // Pack partition
          flatbuffers::FlatBufferBuilder builder;
          auto partition = pack(builder, self->state.data);
          if (!partition) {
            eager_data.error = partition.error();
            return;
          }
          eager_data.partition_chunk = fbs::release(builder);
        }
        { // Pack partition synopsis
          flatbuffers::FlatBufferBuilder builder;
          auto synopsis = pack(builder, *self->state.data.synopsis);
          if (!synopsis) {
            eager_data.error = synopsis.error();
            return;
          }
          fbs::PartitionSynopsisBuilder ps_builder(builder);
          ps_builder.add_partition_synopsis_type(
            fbs::partition_synopsis::PartitionSynopsis::v0);
          ps_builder.add_partition_synopsis(synopsis->Union());
          auto ps_offset = ps_builder.Finish();
          fbs::FinishPartitionSynopsisBuffer(builder, ps_offset);
          eager_data.synopsis_chunk = fbs::release(builder);
        }
      }();
      if (std::holds_alternative<std::monostate>(self->state.persist)) {
        self->state.persist = std::move(eager_data);
      } else {
        auto* lazy_data = std::get_if<persist_lazily>(&self->state.persist);
        VAST_ASSERT(lazy_data != nullptr, "unexpected variant content");
        self->state.fulfill(self, std::move(eager_data), std::move(*lazy_data));
      }
    },
    [self](atom::persist, std::filesystem::path partition_path,
           std::filesystem::path synopsis_path)
      -> caf::result<std::shared_ptr<partition_synopsis>> {
      VAST_DEBUG("partition-transformer will persist itself to {}",
                 partition_path);
      auto lazy_data = partition_transformer_state::persist_lazily{};
      lazy_data.partition_path = std::move(partition_path);
      lazy_data.synopsis_path = std::move(synopsis_path);
      auto promise
        = self->make_response_promise<std::shared_ptr<partition_synopsis>>();
      lazy_data.promise = promise;
      // Immediately fulfill the promise if we are already done
      // with the serialization.
      if (std::holds_alternative<std::monostate>(self->state.persist)) {
        self->state.persist = std::move(lazy_data);
      } else {
        auto* eager_data = std::get_if<persist_eagerly>(&self->state.persist);
        VAST_ASSERT(eager_data != nullptr, "unexpected variant content");
        self->state.fulfill(self, std::move(*eager_data), std::move(lazy_data));
      }
      return promise;
    }};
}

} // namespace vast::system
