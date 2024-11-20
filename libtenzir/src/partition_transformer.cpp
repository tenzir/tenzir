//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/partition_transformer.hpp"

#include "tenzir/detail/fanout_counter.hpp"
#include "tenzir/fbs/utils.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/value_index_factory.hpp"

#include <caf/make_copy_on_write.hpp>
#include <flatbuffers/flatbuffers.h>

namespace tenzir {

namespace {

void store_or_fulfill(
  partition_transformer_actor::stateful_pointer<partition_transformer_state>
    self,
  partition_transformer_state::stream_data&& stream_data) {
  if (std::holds_alternative<std::monostate>(self->state.persist)) {
    TENZIR_DEBUG("{} stores stream data in state.persist", *self);
    self->state.persist = std::move(stream_data);
  } else {
    auto* path_data = std::get_if<partition_transformer_state::path_data>(
      &self->state.persist);
    TENZIR_ASSERT(path_data != nullptr, "unexpected variant content");
    self->state.fulfill(self, std::move(stream_data), std::move(*path_data));
  }
}

void store_or_fulfill(
  partition_transformer_actor::stateful_pointer<partition_transformer_state>
    self,
  partition_transformer_state::path_data&& path_data) {
  if (std::holds_alternative<std::monostate>(self->state.persist)) {
    TENZIR_DEBUG("{} stores path data in state.persist", *self);
    self->state.persist = std::move(path_data);
  } else {
    auto* stream_data = std::get_if<partition_transformer_state::stream_data>(
      &self->state.persist);
    TENZIR_ASSERT(stream_data != nullptr, "unexpected variant content");
    self->state.fulfill(self, std::move(*stream_data), std::move(path_data));
  }
}

void quit_or_stall(
  partition_transformer_actor::stateful_pointer<partition_transformer_state>
    self,
  partition_transformer_state::transformer_is_finished&& result) {
  using stores_are_finished = partition_transformer_state::stores_are_finished;
  auto& shutdown_state = self->state.shutdown_state;
  if (std::holds_alternative<std::monostate>(shutdown_state)) {
    shutdown_state = std::move(result);
  } else {
    TENZIR_ASSERT(std::holds_alternative<stores_are_finished>(shutdown_state),
                  "unexpected variant content");
    result.promise.deliver(std::move(result.result));
    self->quit();
  }
}

void quit_or_stall(
  partition_transformer_actor::stateful_pointer<partition_transformer_state>
    self,
  partition_transformer_state::stores_are_finished&& result) {
  using transformer_is_finished
    = partition_transformer_state::transformer_is_finished;
  auto& shutdown_state = self->state.shutdown_state;
  if (std::holds_alternative<std::monostate>(shutdown_state)) {
    shutdown_state = std::move(result);
  } else {
    auto* finished = std::get_if<transformer_is_finished>(&shutdown_state);
    TENZIR_ASSERT(finished != nullptr, "unexpected variant content");
    finished->promise.deliver(std::move(finished->result));
    self->quit();
  }
}

class fixed_source final : public crtp_operator<fixed_source> {
public:
  explicit fixed_source(std::vector<table_slice> slices)
    : slices_{std::move(slices)} {
  }

  auto name() const -> std::string override {
    return "<fixed_source>";
  }

  auto operator()() const -> generator<table_slice> {
    for (const auto& slice : slices_) {
      co_yield slice;
    }
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter, (void)order;
    return do_not_optimize(*this);
  }

private:
  std::vector<table_slice> slices_;
};

class collecting_sink final : public crtp_operator<collecting_sink> {
public:
  explicit collecting_sink(std::shared_ptr<std::vector<table_slice>> result)
    : result_{std::move(result)} {
    TENZIR_ASSERT(result_);
  }

  auto name() const -> std::string override {
    return "<collecting_sink>";
  }

  auto operator()(generator<table_slice> input) const
    -> generator<std::monostate> {
    for (auto&& slice : input) {
      if (slice.rows() > 0)
        result_->push_back(std::move(slice));
      co_yield {};
    }
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter, (void)order;
    return do_not_optimize(*this);
  }

private:
  std::shared_ptr<std::vector<table_slice>> result_;
};

} // namespace

active_partition_state::serialization_data&
partition_transformer_state::create_or_get_partition(const table_slice& slice) {
  auto const& schema = slice.schema();
  // x marks the spot
  auto [x, end] = data.equal_range(schema);
  if (x == end) {
    x = data.insert(
      std::make_pair(schema, active_partition_state::serialization_data{}));
  } else {
    x = std::prev(end);
    // Create a new partition if inserting the slice would overflow
    // the old one.
    if (x->second.events + slice.rows() > partition_capacity)
      x = data.insert(
        std::make_pair(schema, active_partition_state::serialization_data{}));
  }
  return x->second;
}

// Since we don't have to answer queries while this partition is being
// constructed, we don't have to spawn separate indexer actors and
// stream data but can just compute everything inline here.
void partition_transformer_state::update_type_ids_and_indexers(
  std::unordered_map<std::string, ids>& type_ids,
  const tenzir::uuid& partition_id, const table_slice& slice) {
  const auto& schema = slice.schema();
  // Update type ids
  auto it = type_ids.emplace(schema.name(), ids{}).first;
  auto& ids = it->second;
  auto first = slice.offset();
  auto last = slice.offset() + slice.rows();
  TENZIR_ASSERT(first >= ids.size());
  ids.append_bits(false, first - ids.size());
  ids.append_bits(true, last - first);
  // Push the event data to the indexers.
  TENZIR_ASSERT_EXPENSIVE(slice.columns()
                          == as<record_type>(schema).num_leaves());
  for (size_t flat_index = 0;
       const auto& [field, offset] : as<record_type>(schema).leaves()) {
    const auto qf = qualified_record_field{schema, offset};
    auto& typed_indexers = partition_buildup.at(partition_id).indexers;
    auto it = typed_indexers.find(qf);
    if (it == typed_indexers.end()) {
      it = typed_indexers.emplace(qf, nullptr).first;
    }
    auto& idx = it->second;
    if (idx != nullptr)
      slice.append_column_to_index(flat_index, *idx);
    ++flat_index;
  }
}

void partition_transformer_state::fulfill(
  partition_transformer_actor::stateful_pointer<partition_transformer_state>
    self,
  stream_data&& stream_data, path_data&& path_data) const {
  TENZIR_DEBUG("{} fulfills promise", *self);
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
    promise.deliver(std::vector<partition_synopsis_pair>{});
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
      = fmt::format(TENZIR_FMT_RUNTIME(self->state.synopsis_path_template), id);
    auto synopsis_path = std::filesystem::path{filename};
    self
      ->request(fs, caf::infinite, atom::write_v, synopsis_path, synopsis_chunk)
      .then([](atom::ok) { /* nop */ },
            [synopsis_path, self](const caf::error& e) {
              // The catalog data can always be regenerated on restart, so we
              // don't need strict error handling for it.
              TENZIR_WARN("{} could not write transformed synopsis to {}: {}",
                          *self, synopsis_path, e);
            });
  }
  // Make a write request to the filesystem actor for every partition.
  auto fanout_counter
    = detail::make_fanout_counter<std::vector<partition_synopsis_pair>>(
      stream_data.partition_chunks->size(),
      [self, promise](std::vector<partition_synopsis_pair>&& result) mutable {
        // We're done now, but we may still need to wait for the stores.
        quit_or_stall(self,
                      partition_transformer_state::transformer_is_finished{
                        .promise = std::move(promise),
                        .result = std::move(result),
                      });
      },
      [self, promise](std::vector<partition_synopsis_pair>&&,
                      caf::error&& e) mutable {
        promise.deliver(std::move(e));
        self->quit();
      });
  for (auto& [id, schema, partition_chunk] : *stream_data.partition_chunks) {
    auto rng = self->state.data.equal_range(schema);
    auto it = std::find_if(rng.first, rng.second, [id = id](auto const& kv) {
      return kv.second.id == id;
    });
    TENZIR_ASSERT(it != rng.second); // The id must exist with this schema.
    auto synopsis = std::move(it->second.synopsis);
    auto aps = partition_synopsis_pair{
      .uuid = id,
      .synopsis = std::move(synopsis),
    };
    auto filename = fmt::format(
      TENZIR_FMT_RUNTIME(self->state.partition_path_template), id);
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

auto partition_transformer(
  partition_transformer_actor::stateful_pointer<partition_transformer_state>
    self,
  std::string store_id, const index_config& synopsis_opts,
  const caf::settings& index_opts, catalog_actor catalog, filesystem_actor fs,
  pipeline transform, std::string partition_path_template,
  std::string synopsis_path_template)
  -> partition_transformer_actor::behavior_type {
  self->state.synopsis_opts = synopsis_opts;
  self->state.partition_path_template = std::move(partition_path_template);
  self->state.synopsis_path_template = std::move(synopsis_path_template);
  // For historic reasons, the `tenzir.max-partition-size` is stored as the
  // `cardinality` in the value index options.
  self->state.partition_capacity
    = caf::get_or(index_opts, "cardinality", defaults::max_partition_size);
  self->state.index_opts = index_opts;
  self->state.fs = std::move(fs);
  self->state.catalog = std::move(catalog);
  self->state.transform = std::move(transform);
  self->state.store_id = std::move(store_id);
  self->set_down_handler([self](caf::down_msg& msg) {
    // This is currently safe because we do all increases to
    // `launched_stores` within the same continuation, but when
    // that changes we need to take a bit more care here to avoid
    // a race.
    ++self->state.stores_finished;
    TENZIR_DEBUG("{} sees {} finished for a total of {}/{} stores", *self,
                 msg.source, self->state.stores_finished,
                 self->state.stores_launched);
    if (self->state.stores_finished >= self->state.stores_launched)
      quit_or_stall(self, partition_transformer_state::stores_are_finished{});
  });
  return {
    [self](tenzir::table_slice& slice) {
      // Adjust the import time range iff necessary.
      const auto old_import_time = slice.import_time();
      self->state.min_import_time
        = std::min(self->state.min_import_time, old_import_time);
      self->state.max_import_time
        = std::max(self->state.max_import_time, old_import_time);
      self->state.input.push_back(std::move(slice));
    },
    [self](atom::done) -> caf::result<void> {
      // We copy the pipeline because we will modify it.
      auto pipe = self->state.transform;
      auto open = pipe.check_type<table_slice, table_slice>();
      if (!open) {
        return open.error();
      }
      pipe.prepend(
        std::make_unique<fixed_source>(std::move(self->state.input)));
      auto output = std::make_shared<std::vector<table_slice>>();
      pipe.append(std::make_unique<collecting_sink>(output));
      auto closed = pipe.check_type<void, void>();
      if (!closed) {
        return caf::make_error(ec::logic_error, "internal error: {}",
                               closed.error());
      }
      auto executor = make_local_executor(std::move(pipe));
      for (auto&& result : executor) {
        if (!result) {
          TENZIR_ERROR("{} failed pipeline execution: {}", *self,
                       result.error());
          self->state.transform_error = result.error();
          return {};
        }
      }
      for (auto& slice : *output) {
        auto& partition_data = self->state.create_or_get_partition(slice);
        if (!partition_data.synopsis) {
          partition_data.id = tenzir::uuid::random();
          partition_data.store_id = self->state.store_id;
          partition_data.events = 0ull;
          partition_data.synopsis
            = caf::make_copy_on_write<partition_synopsis>();
        }
        auto* unshared_synopsis = partition_data.synopsis.unshared_ptr();
        if (slice.import_time() == time{}) {
          slice.import_time(self->state.min_import_time);
        }
        unshared_synopsis->min_import_time = self->state.min_import_time;
        unshared_synopsis->max_import_time = self->state.max_import_time;
        partition_data.events += slice.rows();
        self->state.events += slice.rows();
        self->state.partition_buildup[partition_data.id].slices.push_back(
          std::move(slice));
      }
      auto stream_data = partition_transformer_state::stream_data{
        .partition_chunks
        = std::vector<std::tuple<tenzir::uuid, tenzir::type, chunk_ptr>>{},
        .synopsis_chunks = std::vector<std::tuple<tenzir::uuid, chunk_ptr>>{},
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
        = plugins::find<tenzir::store_actor_plugin>(store_id);
      if (!store_actor_plugin) {
        self->state.stream_error
          = caf::make_error(ec::invalid_argument,
                            "could not find a store plugin named {}", store_id);
        store_or_fulfill(self, std::move(stream_data));
        return {};
      }
      for (auto& [schema, partition_data] : self->state.data) {
        if (partition_data.events == 0)
          continue;
        auto builder_and_header = store_actor_plugin->make_store_builder(
          self->state.fs, partition_data.id);
        if (!builder_and_header) {
          self->state.stream_error
            = caf::make_error(ec::invalid_argument,
                              "could not create store builder for backend {}",
                              store_id);
          store_or_fulfill(self, std::move(stream_data));
          return {};
        }
        partition_data.builder = builder_and_header->store_builder;
        self->monitor(partition_data.builder);
        ++self->state.stores_launched;
        partition_data.store_header = builder_and_header->header;
      }
      TENZIR_DEBUG("{} received all table slices", *self);
      return self->delegate(static_cast<partition_transformer_actor>(self),
                            atom::internal_v, atom::resume_v, atom::done_v);
    },
    [self](atom::internal, atom::resume, atom::done) {
      TENZIR_DEBUG("{} got resume", *self);
      for (auto& [schema, data] : self->state.data) {
        auto& mutable_synopsis = data.synopsis.unshared();
        // Push the slices to the store.
        auto& buildup = self->state.partition_buildup.at(data.id);
        auto offset = id{0};
        for (auto& slice : buildup.slices) {
          slice.offset(offset);
          offset += slice.rows();
          self->send(data.builder, slice);
          self->state.update_type_ids_and_indexers(data.type_ids, data.id,
                                                   slice);
          mutable_synopsis.add(slice, self->state.partition_capacity,
                               self->state.synopsis_opts);
        }
        // Update the synopsis
        // TODO: It would make more sense if the partition
        // synopsis keeps track of offset/events internally.
        mutable_synopsis.shrink();
        mutable_synopsis.events = data.events;
        for (auto& [qf, idx] :
             self->state.partition_buildup.at(data.id).indexers) {
          auto chunk = chunk_ptr{};
          // Note that `chunkify(nullptr)` return a chunk of size > 0.
          if (idx)
            chunk = chunkify(idx);
          // We defensively treat every empty chunk as non-existing.
          if (chunk && chunk->size() == 0)
            chunk = nullptr;
          data.indexer_chunks.emplace_back(qf.name(), chunk);
        }
      }
      for (auto& [_, partition_data] : self->state.data) {
        self->request(partition_data.builder, caf::infinite, atom::persist_v)
          .then(
            [](resource&) {
              // This is handled via the down handler.
              // TODO: The logic needs to be moved here when updating to CAF
              // 1.0.
            },
            [self](caf::error& err) {
              auto annotated_error = diagnostic::error(err).note("").to_error();
              std::visit(detail::overload{
                           [&](partition_transformer_state::path_data& pd) {
                             pd.promise.deliver(annotated_error);
                           },
                           [&](auto&) {
                             // We should not get here, but let's not abort the
                             // process if we do.
                             TENZIR_ERROR("{}", annotated_error);
                           },
                         },
                         self->state.persist);
              self->quit(annotated_error);
            });
      }
      auto stream_data = partition_transformer_state::stream_data{
        .partition_chunks
        = std::vector<std::tuple<tenzir::uuid, tenzir::type, chunk_ptr>>{},
        .synopsis_chunks
        = std::vector<std::tuple<tenzir::uuid, tenzir::chunk_ptr>>{},
      };
      // This is an inline lambda so we can use `return` after errors
      // instead of `goto`.
      [&] {
        for (auto& [schema, partition_data] :
             self->state.data) { // Pack partitions
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
          auto partition = pack_full(partition_data, record_type{fields});
          if (!partition) {
            stream_data.partition_chunks = partition.error();
            return;
          }
          stream_data.partition_chunks->emplace_back(
            std::make_tuple(partition_data.id, schema, *partition));
        }
        for (auto& [schema, partition_data] :
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
    [self](atom::persist) -> caf::result<std::vector<partition_synopsis_pair>> {
      TENZIR_DEBUG("{} received request to persist", *self);
      auto promise
        = self->make_response_promise<std::vector<partition_synopsis_pair>>();
      auto path_data = partition_transformer_state::path_data{
        .promise = promise,
      };
      store_or_fulfill(self, std::move(path_data));
      return promise;
    }};
}

} // namespace tenzir
