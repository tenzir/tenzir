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
    auto& [idx, skip] = indexers[qf];
    if (!idx) {
      skip = vast::has_skip_attribute(field.type);
      idx = factory<value_index>::make(field.type, index_opts);
      data.combined_layout.fields.push_back(as_record_field(qf));
    }
    if (!skip) {
      auto column = table_slice_column{slice, i, qf};
      auto offset = column.slice().offset();
      for (size_t i = 0; i < column.size(); ++i)
        idx->append(column[i], offset + i);
    }
  }
}

void partition_transformer_state::finalize_data() {
  // Serialize the finished indexers.
  for (auto& [qf, value] : indexers) {
    auto& [idx, skip] = value;
    data.indexer_chunks.emplace_back(qf.field_name, chunkify(idx));
  }
  data.synopsis->shrink();
  // TODO: It would probably make more sense if the partition
  // synopsis keeps track of offset/events internally.
  data.synopsis->offset = data.offset;
  data.synopsis->events = data.events;
}

// Pre: `self->state.promise` is valid and both chunks or an error are present
// in the state.
void partition_transformer_state::fulfill(
  partition_transformer_actor::stateful_pointer<partition_transformer_state>
    self) {
  VAST_ASSERT(self->state.promise.pending());
  if (self->state.error) {
    self->state.promise.deliver(self->state.error);
    return;
  }
  // The meta index data can always be regenerated on restart, so we don't need
  // real error handling for it.
  self->request(fs, caf::infinite, atom::write_v, synopsis_path, synopsis_chunk)
    .then([](atom::ok) { /* nop */ },
          [](const caf::error& e) {
            VAST_WARN("bulk_partition could not persist "
                      "partition synopsis: {}",
                      render(e));
          });
  self
    ->request(fs, caf::infinite, atom::write_v, partition_path, partition_chunk)
    .then(
      [self](atom::ok) {
        self->state.promise.deliver(self->state.data.synopsis);
      },
      [self](caf::error& e) {
        self->state.promise.deliver(std::move(e));
      });
}

partition_transformer_actor::behavior_type partition_transformer(
  partition_transformer_actor::stateful_pointer<partition_transformer_state>
    self,
  uuid id, std::string store_id, const caf::settings& synopsis_opts,
  const caf::settings& index_opts, idspace_distributor_actor importer,
  filesystem_actor fs, transform_ptr transform) {
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
  self->state.importer = std::move(importer);
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
        ->request(self->state.importer, caf::infinite, atom::reserve_v,
                  self->state.events)
        .then(
          [self](vast::id id) {
            self->send(self, atom::internal_v, atom::resume_v, atom::done_v,
                       id);
          },
          [self](const caf::error& e) {
            self->state.error = e;
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
      [&] {
        { // Pack partition
          flatbuffers::FlatBufferBuilder builder;
          auto partition = pack(builder, self->state.data);
          if (!partition) {
            self->state.error = partition.error();
            return;
          }
          self->state.partition_chunk = fbs::release(builder);
        }
        { // Pack partition synopsis
          flatbuffers::FlatBufferBuilder builder;
          auto synopsis = pack(builder, *self->state.data.synopsis);
          if (!synopsis) {
            self->state.error = synopsis.error();
            return;
          }
          fbs::PartitionSynopsisBuilder ps_builder(builder);
          ps_builder.add_partition_synopsis_type(
            fbs::partition_synopsis::PartitionSynopsis::v0);
          ps_builder.add_partition_synopsis(synopsis->Union());
          auto ps_offset = ps_builder.Finish();
          fbs::FinishPartitionSynopsisBuffer(builder, ps_offset);
          self->state.synopsis_chunk = fbs::release(builder);
        }
      }();
      if (self->state.promise.pending())
        self->state.fulfill(self);
    },
    [self](atom::persist, std::filesystem::path partition_path,
           std::filesystem::path synopsis_path)
      -> caf::result<std::shared_ptr<partition_synopsis>> {
      VAST_DEBUG("partition-transformer will persist itself to {}",
                 partition_path);
      self->state.promise
        = self->make_response_promise<std::shared_ptr<partition_synopsis>>();
      self->state.partition_path = std::move(partition_path);
      self->state.synopsis_path = std::move(synopsis_path);
      // Immediately fulfill the promise if we are already done
      // with the serialization.
      if (self->state.partition_chunk != nullptr
          || self->state.error != caf::none)
        self->state.fulfill(self);
      return self->state.promise;
    }};
}

} // namespace vast::system
