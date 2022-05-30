//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/partition_transformer.hpp"

#include "vast/detail/fanout_counter.hpp"
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

bool partition_transformer_selector::operator()(
  const vast::type& filter, const vast::table_slice& slice) const {
  return slice.layout() == filter;
}

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

// Since we don't have to answer queries while this partition is being
// constructed, we don't have to spawn separate indexer actors and
// stream data but can just compute everything inline here.
void partition_transformer_state::add_slice(const table_slice& slice) {
  const auto& layout = slice.layout();
  // data.events += slice.rows();
  data[layout].synopsis.unshared().add(slice, partition_capacity,
                                       synopsis_opts);
  // Update type ids
  auto it = data[layout].type_ids.emplace(layout.name(), ids{}).first;
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
    auto& typed_indexers = indexers[layout];
    auto it = typed_indexers.find(qf);
    if (it == typed_indexers.end()) {
      const auto skip = field.type.attribute("skip").has_value();
      auto idx
        = skip ? nullptr : factory<value_index>::make(field.type, index_opts);
      it = typed_indexers.emplace(qf, std::move(idx)).first;
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

void partition_transformer_state::fulfill(
  partition_transformer_actor::stateful_pointer<partition_transformer_state>
    self,
  stream_data&& stream_data, path_data&& path_data) const {
  VAST_DEBUG("{} fulfills promise", *self);
  auto promise = path_data.promise;
  if (self->state.stream_error) {
    promise.deliver(self->state.stream_error);
    self->quit();
    return;
  }
  if (self->state.transform_error) {
    promise.deliver(self->state.transform_error);
    self->quit();
    return;
  }
  // Return early if no error occured and no new data was created,
  // ie. the input was erased completely.
  if (self->state.events == 0) {
    promise.deliver(std::vector<augmented_partition_synopsis>{});
    self->quit();
    return;
  }
  if (!stream_data.partition_chunks) {
    promise.deliver(stream_data.partition_chunks.error());
    self->quit();
    return;
  }
  if (!stream_data.synopsis_chunks) {
    promise.deliver(stream_data.synopsis_chunks.error());
    self->quit();
    return;
  }
  // When we get here we know that there was at least one event and
  // no error during packing, so at least one of these chunks must be
  // nonnull.
  for (auto& [id, synopsis_chunk] : *stream_data.synopsis_chunks) {
    if (!synopsis_chunk)
      continue;
    auto filename
      = fmt::format(fmt::runtime(self->state.synopsis_path_template), id);
    auto synopsis_path = std::filesystem::path{filename};
    self
      ->request(fs, caf::infinite, atom::write_v, synopsis_path, synopsis_chunk)
      .then([](atom::ok) { /* nop */ },
            [synopsis_path, self](const caf::error& e) {
              // The catalog data can always be regenerated on restart, so we
              // don't need strict error handling for it.
              VAST_WARN("{} could not write transformed synopsis to {}: {}",
                        *self, synopsis_path, e);
            });
  }
  auto fanout_counter
    = detail::make_fanout_counter<std::vector<augmented_partition_synopsis>>(
      stream_data.partition_chunks->size(),
      [self,
       promise](std::vector<augmented_partition_synopsis>&& result) mutable {
        promise.deliver(std::move(result));
        self->quit();
      },
      [self, promise](std::vector<augmented_partition_synopsis>&&,
                      caf::error&& e) mutable {
        promise.deliver(std::move(e));
        self->quit();
      });
  for (auto& [id, layout, partition_chunk] : *stream_data.partition_chunks) {
    auto synopsis = std::move(self->state.data[layout].synopsis);
    auto aps = augmented_partition_synopsis{
      .uuid = id,
      .type = layout,
      .synopsis = std::move(synopsis),
    };
    auto filename
      = fmt::format(fmt::runtime(self->state.partition_path_template), id);
    auto partition_path = std::filesystem::path{filename};
    self
      ->request(fs, caf::infinite, atom::write_v, partition_path,
                partition_chunk)
      .then(
        [fanout_counter, aps = std::move(aps)](atom::ok) mutable {
          fanout_counter->state().emplace_back(std::move(aps));
          fanout_counter->receive_success();
        },
        [fanout_counter](caf::error& e) mutable {
          fanout_counter->receive_error(std::move(e));
        });
  }
}

partition_transformer_actor::behavior_type partition_transformer(
  partition_transformer_actor::stateful_pointer<partition_transformer_state>
    self,
  std::string store_id, const index_config& synopsis_opts,
  const caf::settings& index_opts, accountant_actor accountant,
  idspace_distributor_actor idspace_distributor,
  type_registry_actor type_registry, filesystem_actor fs,
  transform_ptr transform, std::string partition_path_template,
  std::string synopsis_path_template) {
  self->state.synopsis_opts = synopsis_opts;
  self->state.partition_path_template = std::move(partition_path_template);
  self->state.synopsis_path_template = std::move(synopsis_path_template);
  self->state.partition_capacity = caf::get_or(
    index_opts, "max-partition-size", defaults::system::max_partition_size);
  self->state.index_opts = index_opts;
  self->state.accountant = std::move(accountant);
  self->state.idspace_distributor = std::move(idspace_distributor);
  self->state.fs = std::move(fs);
  self->state.type_registry = std::move(type_registry);
  // transform can be an aggregate transform here
  self->state.transform = std::move(transform);
  self->state.store_id = std::move(store_id);
  return {
    [self](vast::table_slice& slice) {
      // auto const& layout = slice.layout();
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
      self->state.min_import_time
        = std::min(self->state.min_import_time, old_import_time);
      self->state.max_import_time
        = std::max(self->state.max_import_time, old_import_time);
    },
    [self](atom::done) {
      auto transformed = self->state.transform->finish();
      if (!transformed) {
        VAST_ERROR("{} failed to finish transform: {}", *self,
                   transformed.error());
        self->state.transform_error = transformed.error();
        return;
      }
      for (auto& slice : *transformed) {
        auto const& layout = slice.layout();
        // TODO: Technically we'd only need to send *new* layouts here.
        self->send(self->state.type_registry, atom::put_v, layout);
        auto& partition_data = self->state.data[layout];
        if (!partition_data.synopsis) {
          partition_data.id = vast::uuid::random();
          partition_data.store_id = self->state.store_id;
          partition_data.events = 0ull;
          partition_data.offset = invalid_id;
          partition_data.synopsis
            = caf::make_copy_on_write<partition_synopsis>();
        }
        // If the transform is a no-op we may get back the original table slice
        // that's still mapped as read-only, but in this case we also don't need
        // to adjust the import time.
        const auto* slice_identity = as_bytes(slice).data();
        if (!self->state.original_import_times.contains(slice_identity))
          slice.import_time(self->state.max_import_time);
        auto layout_name = layout.name();
        auto& layouts = self->state.stats_out.layouts;
        auto it = layouts.find(layout_name);
        if (it == layouts.end())
          it = layouts.emplace(std::string{layout_name}, layout_statistics{})
                 .first;
        it.value().count += slice.rows();
        partition_data.events += slice.rows();
        self->state.events += slice.rows();
        self->state.slices.push_back(std::move(slice));
      }
      self->state.original_import_times.clear();
      auto stream_data = partition_transformer_state::stream_data{
        .partition_chunks
        = std::vector<std::tuple<vast::uuid, vast::type, chunk_ptr>>{},
        .synopsis_chunks = std::vector<std::tuple<vast::uuid, chunk_ptr>>{},
      };
      // We're already done if the whole partition got deleted
      if (self->state.events == 0) {
        store_or_fulfill(self, std::move(stream_data));
        return;
      }
      // ...otherwise, prepare for writing out the transformed data by creating
      // new stores, sending out the slices and requesting new idspace.
      auto store_id = self->state.store_id;
      const auto* store_plugin = plugins::find<vast::store_plugin>(store_id);
      if (!store_plugin) {
        self->state.stream_error
          = caf::make_error(ec::invalid_argument,
                            "could not find a store plugin named {}", store_id);
        store_or_fulfill(self, std::move(stream_data));
        return;
      }
      self->state.stage = caf::attach_continuous_stream_stage(
        self, [](caf::unit_t&) {},
        [](caf::unit_t&, caf::downstream<vast::table_slice>&,
           vast::table_slice) {
          // We never get input through a source but push directly
          // to `out` from external code below.
          /* nop */
        },
        [](caf::unit_t&, const caf::error&) { /* nop */ },
        // For this stream stage we want a `broadcast_downstream_manager<T>`,
        // where slices only go the store with the correct layout, so:
        //
        //   T:      vast::table_slice
        //   Filter: vast::type
        //   Select: vast::(anon)::partition_transformer_selector
        caf::policy::arg<caf::broadcast_downstream_manager<
          table_slice, vast::type, partition_transformer_selector>>{});
      for (auto& [layout, partition_data] : self->state.data) {
        if (partition_data.events == 0)
          continue;
        auto builder_and_header = store_plugin->make_store_builder(
          self->state.accountant, self->state.fs, partition_data.id);
        if (!builder_and_header) {
          self->state.stream_error
            = caf::make_error(ec::invalid_argument,
                              "could not create store builder for backend {}",
                              store_id);
          store_or_fulfill(self, std::move(stream_data));
          return;
        }
        partition_data.store_header = builder_and_header->second;
        auto [store_builder_it, _] = self->state.store_builders.insert(
          std::make_pair(layout, builder_and_header->first));
        auto& store_builder = store_builder_it->second;
        VAST_ASSERT(store_builder);
        auto slot = self->state.stage->add_outbound_path(store_builder);
        self->state.stage->out().set_filter(slot, layout);
      }
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
      for (auto& [layout, data] : self->state.data) {
        data.offset = offset;
        // Push the slices to the store.
        // TODO: This could be optimized by storing the slices in a map
        // of vectors so we can iterate immediately over the correct range.
        for (auto& slice : self->state.slices) {
          if (slice.layout() != layout)
            continue;
          slice.offset(offset);
          offset += slice.rows();
          self->state.add_slice(slice);
          self->state.stage->out().push(slice);
        }
        auto& mutable_synopsis = data.synopsis.unshared();
        mutable_synopsis.shrink();
        // TODO: It would probably make more sense if the partition
        // synopsis keeps track of offset/events internally.
        mutable_synopsis.offset = data.offset;
        mutable_synopsis.events = data.events;
      }
      for (auto& [layout, typed_indexers] : self->state.indexers)
        for (auto& [qf, idx] : typed_indexers)
          self->state.data[layout].indexer_chunks.emplace_back(qf.name(),
                                                               chunkify(idx));
      detail::shutdown_stream_stage(self->state.stage);
      auto stream_data = partition_transformer_state::stream_data{
        .partition_chunks
        = std::vector<std::tuple<vast::uuid, vast::type, chunk_ptr>>{},
        .synopsis_chunks
        = std::vector<std::tuple<vast::uuid, vast::chunk_ptr>>{},
      };
      // This is an inline lambda so we can use `return` after errors
      // instead of `goto`.
      [&] {
        for (auto& [layout, partition_data] :
             self->state.data) { // Pack partitions
          flatbuffers::FlatBufferBuilder builder;
          auto indexers_it = self->state.indexers.find(layout);
          if (indexers_it == self->state.indexers.end()) {
            stream_data.partition_chunks
              = caf::make_error(ec::logic_error, "cannot create partition with "
                                                 "empty layout");
            return;
          }
          auto& indexers = indexers_it->second;
          auto fields = std::vector<struct record_type::field>{};
          fields.reserve(indexers.size());
          for (const auto& [qf, _] : indexers)
            fields.push_back({std::string{qf.name()}, qf.type()});
          auto partition = pack(builder, partition_data, record_type{fields});
          if (!partition) {
            stream_data.partition_chunks = partition.error();
            return;
          }
          stream_data.partition_chunks->emplace_back(
            std::make_tuple(partition_data.id, layout, fbs::release(builder)));
        }
        for (auto& [layout, partition_data] :
             self->state.data) { // Pack partition synopsis
          flatbuffers::FlatBufferBuilder builder;
          auto synopsis = pack(builder, *partition_data.synopsis);
          if (!synopsis) {
            stream_data.synopsis_chunks = synopsis.error();
            return;
          }
          fbs::PartitionSynopsisBuilder ps_builder(builder);
          ps_builder.add_partition_synopsis_type(
            fbs::partition_synopsis::PartitionSynopsis::legacy);
          ps_builder.add_partition_synopsis(synopsis->Union());
          auto ps_offset = ps_builder.Finish();
          fbs::FinishPartitionSynopsisBuffer(builder, ps_offset);
          stream_data.synopsis_chunks->emplace_back(
            std::make_tuple(partition_data.id, fbs::release(builder)));
        }
      }();
      store_or_fulfill(self, std::move(stream_data));
    },
    [self](
      atom::persist) -> caf::result<std::vector<augmented_partition_synopsis>> {
      VAST_DEBUG("{} received request to persist", *self);
      auto promise
        = self
            ->make_response_promise<std::vector<augmented_partition_synopsis>>();
      auto path_data = partition_transformer_state::path_data{
        .promise = promise,
      };
      store_or_fulfill(self, std::move(path_data));
      return promise;
    }};
}

} // namespace vast::system
