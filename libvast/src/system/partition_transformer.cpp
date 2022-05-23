//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/partition_transformer.hpp"

#include "vast/detail/shutdown_stream_stage.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/logger.hpp"
#include "vast/partition_synopsis.hpp"
#include "vast/plugin.hpp"
#include "vast/transform.hpp"
#include "vast/value_index_factory.hpp"

#include <caf/attach_continuous_stream_stage.hpp>
#include <caf/attach_stream_stage.hpp>
#include <caf/make_copy_on_write.hpp>
#include <flatbuffers/flatbuffers.h>

namespace vast::system {

namespace {

void store_or_fulfill(
  partition_transformer_actor::stateful_pointer<partition_transformer_state>
    self,
  partition_transformer_state::stream_data&& stream_data) {
  if (std::holds_alternative<std::monostate>(self->state.persist)) {
    self->state.persist = std::move(stream_data);
  } else {
    auto* path_data = std::get_if<partition_transformer_state::path_data>(
      &self->state.persist);
    VAST_ASSERT(path_data != nullptr, "unexpected variant content");
    self->state.fulfill(self, std::move(stream_data), std::move(*path_data));
  }
}

void store_or_fulfill(
  partition_transformer_actor::stateful_pointer<partition_transformer_state>
    self,
  partition_transformer_state::path_data&& path_data) {
  if (std::holds_alternative<std::monostate>(self->state.persist)) {
    self->state.persist = std::move(path_data);
  } else {
    auto* stream_data = std::get_if<partition_transformer_state::stream_data>(
      &self->state.persist);
    VAST_ASSERT(stream_data != nullptr, "unexpected variant content");
    self->state.fulfill(self, std::move(*stream_data), std::move(path_data));
  }
}

} // namespace

// Since we don't have to answer queries while this partition is being
// constructed, we don't have to spawn separate indexer actors and
// stream data but can just compute everything inline here.
void partition_transformer_state::add_slice(const table_slice& slice) {
  const auto& layout = slice.layout();
  data.events += slice.rows();
  data.synopsis.unshared().add(slice, partition_capacity, synopsis_opts);
  // Update type ids
  auto it = data.type_ids.emplace(layout.name(), ids{}).first;
  auto& ids = it->second;
  auto first = slice.offset();
  auto last = slice.offset() + slice.rows();
  VAST_ASSERT(first >= ids.size());
  ids.append_bits(false, first - ids.size());
  ids.append_bits(true, last - first);
  // Push the event data to the indexers.
  VAST_ASSERT(slice.columns() == caf::get<record_type>(layout).num_leaves());
  for (size_t flat_index = 0;
       const auto& [field, offset] : caf::get<record_type>(layout).leaves()) {
    const auto qf = qualified_record_field{layout, offset};
    auto it = indexers.find(qf);
    if (it == indexers.end()) {
      const auto skip = field.type.attribute("skip").has_value();
      auto idx
        = skip ? nullptr : factory<value_index>::make(field.type, index_opts);
      it = indexers.emplace(qf, std::move(idx)).first;
    }
    auto& idx = it->second;
    if (idx != nullptr) {
      auto column = table_slice_column{slice, flat_index};
      auto offset = column.slice().offset();
      for (size_t i = 0; i < column.size(); ++i)
        idx->append(column[i], offset + i);
    }
    ++flat_index;
  }
}

void partition_transformer_state::finalize_data() {
  // Serialize the finished indexers.
  for (auto& [qf, idx] : indexers) {
    data.indexer_chunks.emplace_back(qf.name(), chunkify(idx));
  }
  auto& mutable_synopsis = data.synopsis.unshared();
  mutable_synopsis.shrink();
  // TODO: It would probably make more sense if the partition
  // synopsis keeps track of offset/events internally.
  mutable_synopsis.offset = data.offset;
  mutable_synopsis.events = data.events;
}

void partition_transformer_state::fulfill(
  partition_transformer_actor::stateful_pointer<partition_transformer_state>
    self,
  stream_data&& stream_data, path_data&& path_data) const {
  if (self->state.stream_error) {
    path_data.promise.deliver(self->state.stream_error);
    self->quit();
    return;
  }
  auto promise = path_data.promise;
  if (self->state.events == 0) {
    promise.deliver(augmented_partition_synopsis{
      .uuid = vast::uuid::nil(),
      .stats = std::move(self->state.stats),
      .synopsis = nullptr,
    });
    self->quit();
    return;
  }
  if (!stream_data.partition_chunk) {
    promise.deliver(stream_data.partition_chunk.error());
    self->quit();
    return;
  }
  if (!stream_data.synopsis_chunk) {
    promise.deliver(stream_data.synopsis_chunk.error());
    self->quit();
    return;
  }
  // When we get here we know that there was at least one event and
  // no error during packing, so these chunks can not be null.
  VAST_ASSERT(*stream_data.partition_chunk != nullptr);
  VAST_ASSERT(*stream_data.synopsis_chunk != nullptr);
  // The catalog data can always be regenerated on restart, so we don't need
  // strict error handling for it.
  self
    ->request(fs, caf::infinite, atom::write_v, path_data.synopsis_path,
              *stream_data.synopsis_chunk)
    .then([](atom::ok) { /* nop */ },
          [self, path = path_data.synopsis_path](const caf::error& e) {
            VAST_WARN("{} could not write transformed synopsis to {}: {}",
                      *self, path, e);
          });
  self
    ->request(fs, caf::infinite, atom::write_v, path_data.partition_path,
              *stream_data.partition_chunk)
    .then(
      [self, promise](atom::ok) mutable {
        promise.deliver(augmented_partition_synopsis{
          .uuid = self->state.data.id,
          .stats = std::move(self->state.stats),
          .synopsis = std::move(self->state.data.synopsis),
        });
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
  uuid id, std::string store_id, const index_config& synopsis_opts,
  const caf::settings& index_opts, accountant_actor accountant,
  idspace_distributor_actor idspace_distributor,
  type_registry_actor type_registry, filesystem_actor fs,
  transform_ptr transform) {
  self->state.data.id = id;
  self->state.data.store_id = store_id;
  self->state.data.offset = invalid_id;
  self->state.data.synopsis = caf::make_copy_on_write<partition_synopsis>();
  self->state.synopsis_opts = synopsis_opts;
  self->state.partition_capacity = caf::get_or(
    index_opts, "max-partition-size", defaults::system::max_partition_size);
  self->state.index_opts = index_opts;
  self->state.accountant = std::move(accountant);
  self->state.idspace_distributor = std::move(idspace_distributor);
  self->state.fs = std::move(fs);
  self->state.type_registry = std::move(type_registry);
  // transform can be an aggregate transform here
  self->state.transform = std::move(transform);
  return {
    [self](vast::table_slice& slice) {
      // Store the old import time before applying any transformations to the
      // data, as for now we do not want to assign a new import time range to
      // transformed partitions.
      const auto old_import_time = slice.import_time();
      const auto* slice_identity = as_bytes(slice).data();
      self->state.original_import_times[slice_identity] = old_import_time;
      if (auto err = self->state.transform->add(std::move(slice))) {
        VAST_ERROR("{} failed to add slice: {}", *self, err);
        return;
      }
      // Adjust the import time range iff necessary.
      auto& mutable_synopsis = self->state.data.synopsis.unshared();
      mutable_synopsis.min_import_time
        = std::min(mutable_synopsis.min_import_time, old_import_time);
      mutable_synopsis.max_import_time
        = std::max(mutable_synopsis.max_import_time, old_import_time);
    },
    [self](atom::done) {
      auto transformed = self->state.transform->finish();
      if (!transformed) {
        VAST_ERROR("{} failed to finish transform: {}", *self,
                   transformed.error());
        return;
      }
      for (auto& slice : *transformed) {
        // TODO: Technically we'd only need to send *new* layouts here.
        self->send(self->state.type_registry, atom::put_v, slice.layout());
        // If the transform is a no-op we may get back the original table slice
        // that's still mapped as read-only, but in this case we also don't need
        // to adjust the import time.
        const auto* slice_identity = as_bytes(slice).data();
        if (!self->state.original_import_times.contains(slice_identity))
          slice.import_time(self->state.data.synopsis->max_import_time);
        self->state.original_import_times.clear();
        auto layout_name = slice.layout().name();
        auto& layouts = self->state.stats.layouts;
        auto it = layouts.find(layout_name);
        if (it == layouts.end())
          it = layouts.emplace(std::string{layout_name}, layout_statistics{})
                 .first;
        it.value().count += slice.rows();
        self->state.events += slice.rows();
        self->state.slices.push_back(std::move(slice));
      }
      auto stream_data = partition_transformer_state::stream_data{
        .partition_chunk = nullptr,
        .synopsis_chunk = nullptr,
      };
      // We're already done if the whole partition got deleted
      if (self->state.events == 0) {
        store_or_fulfill(self, std::move(stream_data));
        return;
      }
      // ...otherwise, prepare for writing out the transformed data by creating
      // a new store, sending it the slices and requesting new idspace.
      auto store_id = self->state.data.store_id;
      const auto* store_plugin = plugins::find<vast::store_plugin>(store_id);
      if (!store_plugin) {
        self->state.stream_error
          = caf::make_error(ec::invalid_argument,
                            "could not find a store plugin named {}", store_id);
        store_or_fulfill(self, std::move(stream_data));
        return;
      }
      auto builder_and_header = store_plugin->make_store_builder(
        self->state.accountant, self->state.fs, self->state.data.id);
      if (!builder_and_header) {
        self->state.stream_error
          = caf::make_error(ec::invalid_argument,
                            "could not create store builder for backend {}",
                            store_id);
        store_or_fulfill(self, std::move(stream_data));
        return;
      }
      self->state.data.store_header = builder_and_header->second;
      self->state.store_builder = builder_and_header->first;
      VAST_ASSERT(self->state.store_builder);
      self->state.stage = caf::attach_continuous_stream_stage(
        self, [](caf::unit_t&) {},
        [](caf::unit_t&, caf::downstream<vast::table_slice>& out,
           vast::table_slice x) {
          out.push(x);
        },
        [](caf::unit_t&, const caf::error&) { /* nop */ });
      self->state.stage->add_outbound_path(self->state.store_builder);
      VAST_DEBUG("{} received all table slices", *self);
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
      VAST_DEBUG("{} got new offset {}", *self, offset);
      self->state.data.offset = offset;
      for (auto& slice : self->state.slices) {
        slice.offset(offset);
        offset += slice.rows();
        self->state.add_slice(slice);
        self->state.stage->out().push(slice);
      }
      self->state.finalize_data();
      detail::shutdown_stream_stage(self->state.stage);
      auto stream_data = partition_transformer_state::stream_data{};
      [&] {
        { // Pack partition
          flatbuffers::FlatBufferBuilder builder;
          if (self->state.indexers.empty()) {
            stream_data.partition_chunk
              = caf::make_error(ec::logic_error, "cannot create partition with "
                                                 "empty layout");
            return;
          }
          auto fields = std::vector<struct record_type::field>{};
          fields.reserve(self->state.indexers.size());
          for (const auto& [qf, _] : self->state.indexers)
            fields.push_back({std::string{qf.name()}, qf.type()});
          auto partition = pack(builder, self->state.data, record_type{fields});
          if (!partition) {
            stream_data.partition_chunk = partition.error();
            return;
          }
          stream_data.partition_chunk = fbs::release(builder);
        }
        { // Pack partition synopsis
          flatbuffers::FlatBufferBuilder builder;
          auto synopsis = pack(builder, *self->state.data.synopsis);
          if (!synopsis) {
            stream_data.synopsis_chunk = synopsis.error();
            return;
          }
          fbs::PartitionSynopsisBuilder ps_builder(builder);
          ps_builder.add_partition_synopsis_type(
            fbs::partition_synopsis::PartitionSynopsis::legacy);
          ps_builder.add_partition_synopsis(synopsis->Union());
          auto ps_offset = ps_builder.Finish();
          fbs::FinishPartitionSynopsisBuffer(builder, ps_offset);
          stream_data.synopsis_chunk = fbs::release(builder);
        }
      }();
      store_or_fulfill(self, std::move(stream_data));
    },
    [self](atom::persist, std::filesystem::path partition_path,
           std::filesystem::path synopsis_path)
      -> caf::result<augmented_partition_synopsis> {
      VAST_DEBUG("{} will persist itself to {}", *self, partition_path);
      auto path_data = partition_transformer_state::path_data{};
      path_data.partition_path = std::move(partition_path);
      path_data.synopsis_path = std::move(synopsis_path);
      auto promise
        = self->make_response_promise<augmented_partition_synopsis>();
      path_data.promise = promise;
      store_or_fulfill(self, std::move(path_data));
      return promise;
    }};
}

} // namespace vast::system
