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
#include "vast/pipeline.hpp"
#include "vast/plugin.hpp"
#include "vast/value_index_factory.hpp"

#include <caf/attach_continuous_stream_stage.hpp>
#include <caf/attach_stream_stage.hpp>
#include <caf/make_copy_on_write.hpp>
#include <flatbuffers/flatbuffers.h>

namespace vast::system {

namespace {

void update_statistics(index_statistics& stats, const table_slice& slice) {
  auto layout_name = slice.layout().name();
  auto& layouts = stats.layouts;
  auto it = layouts.find(layout_name);
  if (it == layouts.end())
    it = layouts.emplace(std::string{layout_name}, layout_statistics{}).first;
  it.value().count += slice.rows();
}

// A local reimplementation of `caf::broadcast_downstream_manager::push_to()`,
// because that function was only added late in the 0.17.x cycle and is not
// available on the Debian 10 packaged version of CAF.
template <typename T, typename... Ts>
bool push_to(caf::broadcast_downstream_manager<T>& manager,
             caf::outbound_stream_slot<T> slot, Ts&&... xs) {
  auto i = manager.states().find(slot);
  if (i != manager.states().end()) {
    i->second.buf.emplace_back(std::forward<Ts>(xs)...);
    return true;
  }
  return false;
}

} // namespace

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

active_partition_state::serialization_data&
partition_transformer_state::create_or_get_partition(const table_slice& slice) {
  auto const& layout = slice.layout();
  // x marks the spot
  auto [x, end] = data.equal_range(layout);
  if (x == end) {
    x = data.insert(
      std::make_pair(layout, active_partition_state::serialization_data{}));
  } else {
    x = std::prev(end);
    // Create a new partition if inserting the slice would overflow
    // the old one.
    if (x->second.events + slice.rows() > partition_capacity)
      x = data.insert(
        std::make_pair(layout, active_partition_state::serialization_data{}));
  }
  return x->second;
}

// Since we don't have to answer queries while this partition is being
// constructed, we don't have to spawn separate indexer actors and
// stream data but can just compute everything inline here.
void partition_transformer_state::update_type_ids_and_indexers(
  std::unordered_map<std::string, ids>& type_ids,
  const vast::uuid& partition_id, const table_slice& slice) {
  const auto& layout = slice.layout();
  // Update type ids
  auto it = type_ids.emplace(layout.name(), ids{}).first;
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
    auto& typed_indexers = partition_buildup.at(partition_id).indexers;
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
      = fmt::format(VAST_FMT_RUNTIME(self->state.synopsis_path_template), id);
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
  // Make a write request to the filesystem actor for every partition.
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
    auto rng = self->state.data.equal_range(layout);
    auto it = std::find_if(rng.first, rng.second, [id = id](auto const& kv) {
      return kv.second.id == id;
    });
    VAST_ASSERT(it != rng.second); // The id must exist with this layout.
    auto synopsis = std::move(it->second.synopsis);
    auto aps = augmented_partition_synopsis{
      .uuid = id,
      .type = layout,
      .synopsis = std::move(synopsis),
    };
    auto filename
      = fmt::format(VAST_FMT_RUNTIME(self->state.partition_path_template), id);
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
  type_registry_actor type_registry, filesystem_actor fs,
  pipeline_ptr transform, std::string partition_path_template,
  std::string synopsis_path_template) {
  self->state.synopsis_opts = synopsis_opts;
  self->state.partition_path_template = std::move(partition_path_template);
  self->state.synopsis_path_template = std::move(synopsis_path_template);
  // For historic reasons, the `vast.max-partition-size` is stored as the
  // `cardinality` in the value index options.
  self->state.partition_capacity = caf::get_or(
    index_opts, "cardinality", defaults::system::max_partition_size);
  self->state.index_opts = index_opts;
  self->state.accountant = std::move(accountant);
  self->state.fs = std::move(fs);
  self->state.type_registry = std::move(type_registry);
  // transform can be an aggregate transform here
  self->state.transform = std::move(transform);
  self->state.store_id = std::move(store_id);
  return {
    [self](atom::receive, vast::table_slice& slice) {
      update_statistics(self->state.stats_in, slice);
      const auto old_import_time = slice.import_time();
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
    [self](atom::receive, atom::done) -> caf::result<void> {
      auto transformed = self->state.transform->finish();
      if (!transformed) {
        VAST_ERROR("{} failed to finish transform: {}", *self,
                   transformed.error());
        self->state.transform_error = transformed.error();
        return {};
      }
      for (auto& slice : *transformed) {
        auto const& layout = slice.layout();
        // TODO: Technically we'd only need to send *new* layouts here.
        self->send(self->state.type_registry, atom::put_v, layout);
        auto& partition_data = self->state.create_or_get_partition(slice);
        if (!partition_data.synopsis) {
          partition_data.id = vast::uuid::random();
          partition_data.store_id = self->state.store_id;
          partition_data.events = 0ull;
          partition_data.synopsis
            = caf::make_copy_on_write<partition_synopsis>();
        }
        auto* unshared_synopsis = partition_data.synopsis.unshared_ptr();
        unshared_synopsis->min_import_time
          = std::min(slice.import_time(), unshared_synopsis->min_import_time);
        unshared_synopsis->max_import_time
          = std::max(slice.import_time(), unshared_synopsis->max_import_time);
        update_statistics(self->state.stats_out, slice);
        partition_data.events += slice.rows();
        self->state.events += slice.rows();
        self->state.partition_buildup[partition_data.id].slices.push_back(
          std::move(slice));
      }
      auto stream_data = partition_transformer_state::stream_data{
        .partition_chunks
        = std::vector<std::tuple<vast::uuid, vast::type, chunk_ptr>>{},
        .synopsis_chunks = std::vector<std::tuple<vast::uuid, chunk_ptr>>{},
      };
      // We're already done if the whole partition got deleted
      if (self->state.events == 0) {
        store_or_fulfill(self, std::move(stream_data));
        return {};
      }
      // ...otherwise, prepare for writing out the transformed data by creating
      // new stores, sending out the slices and requesting new idspace.
      auto store_id = self->state.store_id;
      auto const* store_actor_plugin
        = plugins::find<vast::store_actor_plugin>(store_id);
      if (!store_actor_plugin) {
        self->state.stream_error
          = caf::make_error(ec::invalid_argument,
                            "could not find a store plugin named {}", store_id);
        store_or_fulfill(self, std::move(stream_data));
        return {};
      }
      self->state.stage = caf::attach_continuous_stream_stage(
        self, [](caf::unit_t&) {},
        [](caf::unit_t&, caf::downstream<vast::table_slice>&,
           vast::table_slice) {
          // We never get input through a source but push directly
          // to `out` from external code below.
          /* nop */
        },
        [](caf::unit_t&, const caf::error&) { /* nop */ });
      for (auto& [layout, partition_data] : self->state.data) {
        if (partition_data.events == 0)
          continue;
        auto builder_and_header = store_actor_plugin->make_store_builder(
          self->state.accountant, self->state.fs, partition_data.id);
        if (!builder_and_header) {
          self->state.stream_error
            = caf::make_error(ec::invalid_argument,
                              "could not create store builder for backend {}",
                              store_id);
          store_or_fulfill(self, std::move(stream_data));
          return {};
        }
        partition_data.store_header = builder_and_header->header;
        // Empirically adding the outbound path and pushing data to it
        // need to be separated by a continuation, although I'm not
        // completely sure why.
        self->state.partition_buildup.at(partition_data.id).slot
          = self->state.stage->add_outbound_path(
            builder_and_header->store_builder);
      }
      VAST_DEBUG("{} received all table slices", *self);
      return self->delegate(static_cast<partition_transformer_actor>(self),
                            atom::internal_v, atom::resume_v, atom::done_v);
    },
    [self](atom::internal, atom::resume, atom::done) {
      VAST_DEBUG("{} got resume", *self);
      for (auto& [layout, data] : self->state.data) {
        auto& mutable_synopsis = data.synopsis.unshared();
        // Push the slices to the store.
        auto& buildup = self->state.partition_buildup.at(data.id);
        auto slot = buildup.slot;
        auto offset = id{0};
        for (auto& slice : buildup.slices) {
          slice.offset(offset);
          offset += slice.rows();
          push_to(self->state.stage->out(), slot, slice);
          self->state.update_type_ids_and_indexers(data.type_ids, data.id,
                                                   slice);
          mutable_synopsis.add(slice, self->state.partition_capacity,
                               self->state.synopsis_opts);
        }
        // Update the synopsis
        // TODO: It would make more sense if the partition
        // synopsis keeps track of offset/events internally.
        mutable_synopsis.shrink();
        mutable_synopsis.offset = 0;
        mutable_synopsis.events = data.events;
        // Create the value indices.
        for (auto& [qf, idx] :
             self->state.partition_buildup.at(data.id).indexers)
          data.indexer_chunks.emplace_back(qf.name(), chunkify(idx));
      }
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
          auto indexers_it
            = self->state.partition_buildup.find(partition_data.id);
          if (indexers_it == self->state.partition_buildup.end()) {
            stream_data.partition_chunks
              = caf::make_error(ec::logic_error, "missing data for partition");
            return;
          }
          auto& indexers = indexers_it->second.indexers;
          auto fields = std::vector<struct record_type::field>{};
          fields.reserve(indexers.size());
          for (const auto& [qf, _] : indexers)
            fields.emplace_back(std::string{qf.name()}, qf.type());
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
