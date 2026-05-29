//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/partition_transformer.hpp"

#include "tenzir/async/executor.hpp"
#include "tenzir/async/future_util.hpp"
#include "tenzir/async/mail.hpp"
#include "tenzir/atomic.hpp"
#include "tenzir/co_match.hpp"
#include "tenzir/compile_ctx.hpp"
#include "tenzir/detail/available_memory.hpp"
#include "tenzir/detail/fanout_counter.hpp"
#include "tenzir/fbs/utils.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/passive_partition.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/query_context.hpp"
#include "tenzir/session.hpp"
#include "tenzir/store.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tql2/exec.hpp"
#include "tenzir/try.hpp"

#include <caf/actor_from_state.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/make_copy_on_write.hpp>
#include <flatbuffers/flatbuffers.h>
#include <folly/Unit.h>
#include <folly/coro/Invoke.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/futures/Future.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>

namespace tenzir {

namespace {

struct memory_budget {
  uint64_t initial_available = 0;
  uint64_t bytes = 0;
  std::string source = {};
};

auto make_memory_budget() -> std::optional<memory_budget> {
  auto available = detail::available_memory();
  if (not available) {
    return std::nullopt;
  }
  return memory_budget{
    .initial_available = available->bytes,
    .bytes = available->bytes / 4,
    .source = std::move(available->source),
  };
}

auto live_budget_used(const memory_budget& budget)
  -> std::optional<std::pair<uint64_t, uint64_t>> {
  auto current = detail::available_memory();
  if (not current) {
    return std::nullopt;
  }
  const auto used = budget.initial_available > current->bytes
                      ? budget.initial_available - current->bytes
                      : uint64_t{0};
  return std::pair{used, current->bytes};
}

auto collect_table_slices(caf::event_based_actor*,
                          std::shared_ptr<std::vector<table_slice>> slices)
  -> caf::behavior {
  return {
    [slices = std::move(slices)](table_slice slice) -> caf::result<void> {
      slices->push_back(std::move(slice));
      return {};
    },
  };
}

void store_or_fulfill(
  partition_transformer_actor::stateful_pointer<partition_transformer_state>
    self,
  partition_transformer_state::stream_data&& stream_data) {
  if (std::holds_alternative<std::monostate>(self->state().persist)) {
    TENZIR_DEBUG("{} stores stream data in state.persist", *self);
    self->state().persist = std::move(stream_data);
  } else {
    auto* path_data = std::get_if<partition_transformer_state::path_data>(
      &self->state().persist);
    TENZIR_ASSERT(path_data != nullptr, "unexpected variant content");
    self->state().fulfill(self, std::move(stream_data), std::move(*path_data));
  }
}

void store_or_fulfill(
  partition_transformer_actor::stateful_pointer<partition_transformer_state>
    self,
  partition_transformer_state::path_data&& path_data) {
  if (std::holds_alternative<std::monostate>(self->state().persist)) {
    TENZIR_DEBUG("{} stores path data in state.persist", *self);
    self->state().persist = std::move(path_data);
  } else {
    auto* stream_data = std::get_if<partition_transformer_state::stream_data>(
      &self->state().persist);
    TENZIR_ASSERT(stream_data != nullptr, "unexpected variant content");
    self->state().fulfill(self, std::move(*stream_data), std::move(path_data));
  }
}

void quit_or_stall(
  partition_transformer_actor::stateful_pointer<partition_transformer_state>
    self,
  partition_transformer_state::transformer_is_finished&& result) {
  using stores_are_finished = partition_transformer_state::stores_are_finished;
  auto& shutdown_state = self->state().shutdown_state;
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
  auto& shutdown_state = self->state().shutdown_state;
  if (std::holds_alternative<std::monostate>(shutdown_state)) {
    shutdown_state = std::move(result);
  } else {
    auto* finished = std::get_if<transformer_is_finished>(&shutdown_state);
    TENZIR_ASSERT(finished != nullptr, "unexpected variant content");
    finished->promise.deliver(std::move(finished->result));
    self->quit();
  }
}

/// A `DiagHandler` for the partition transformer that just logs whatever the
/// pipeline emits. Diagnostics from compaction/rebuild pipelines have nowhere
/// to surface as user-visible output, but we still want them in the server
/// log so operators can be debugged.
class TransformerDiagHandler final : public DiagHandler {
public:
  void emit(diagnostic diag) override {
    if (diag.severity == severity::error) {
      failed_.store(true, std::memory_order_relaxed);
      TENZIR_ERROR("transformer diagnostic: {:?}", diag);
      return;
    }
    TENZIR_WARN("transformer diagnostic: {:?}", diag);
  }

  auto failure() -> failure_or<void> override {
    if (failed_.load(std::memory_order_relaxed)) {
      return failure::promise();
    }
    return {};
  }

private:
  Atomic<bool> failed_ = false;
};

struct partition_source_state {
  caf::error error = {};
  tenzir::time min_import_time = tenzir::time::max();
  tenzir::time max_import_time = tenzir::time::min();
  std::atomic<duration::rep> loaded_min_import_time
    = time::max().time_since_epoch().count();
  std::vector<partition_info> selected_partitions = {};
  std::vector<partition_info> loaded_partitions = {};
  bool input_complete = true;

  auto observe_import_time(time import_time) -> void {
    min_import_time = std::min(min_import_time, import_time);
    max_import_time = std::max(max_import_time, import_time);
    auto import_time_count = import_time.time_since_epoch().count();
    auto current = loaded_min_import_time.load(std::memory_order_relaxed);
    while (import_time_count < current
           and not loaded_min_import_time.compare_exchange_weak(
             current, import_time_count, std::memory_order_relaxed)) {
      // nop
    }
  }

  auto min_loaded_import_time() const -> time {
    return time{}
           + duration{loaded_min_import_time.load(std::memory_order_relaxed)};
  }
};

class partition_loader {
public:
  partition_loader(std::vector<partition_info> partitions,
                   std::string partition_path_template,
                   std::filesystem::path archive_dir,
                   filesystem_actor filesystem,
                   std::shared_ptr<partition_source_state> state)
    : partitions_{std::move(partitions)},
      partition_path_template_{std::move(partition_path_template)},
      archive_dir_{std::move(archive_dir)},
      filesystem_{std::move(filesystem)},
      memory_budget_{make_memory_budget()},
      state_{std::move(state)} {
    TENZIR_ASSERT(state_);
    state_->selected_partitions = partitions_;
  }

  auto feed(Push<OperatorMsg<table_slice>>& push_input) const -> Task<void> {
    for (const auto& partition : partitions_) {
      if (memory_budget_) {
        if (auto used = live_budget_used(*memory_budget_);
            used and used->first >= memory_budget_->bytes) {
          if (state_->loaded_partitions.empty()) {
            fail(caf::make_error(ec::out_of_memory,
                                 "partition transform memory budget is "
                                 "exhausted before loading partition {} "
                                 "({} bytes used, {} bytes budget, {} bytes "
                                 "available, source: {})",
                                 partition.uuid, used->first,
                                 memory_budget_->bytes, used->second,
                                 memory_budget_->source));
            co_await push_input(OperatorMsg<table_slice>{Signal{EndOfData{}}});
            co_return;
          }
          TENZIR_VERBOSE("{} stops loading transform input before partition {} "
                         "after {} partition(s); live memory budget is full "
                         "({} bytes used, {} bytes budget, {} bytes available, "
                         "source: {})",
                         "partition-transformer", partition.uuid,
                         state_->loaded_partitions.size(), used->first,
                         memory_budget_->bytes, used->second,
                         memory_budget_->source);
          state_->input_complete = false;
          break;
        }
      }
      auto maybe_slices = co_await load_partition(partition);
      if (not maybe_slices) {
        fail(std::move(maybe_slices.error()));
        co_await push_input(OperatorMsg<table_slice>{Signal{EndOfData{}}});
        co_return;
      }
      for (auto&& slice : *maybe_slices) {
        state_->observe_import_time(slice.import_time());
        if (slice.rows() == 0) {
          continue;
        }
        co_await push_input(OperatorMsg<table_slice>{std::move(slice)});
      }
      if (state_->error.valid()) {
        co_await push_input(OperatorMsg<table_slice>{Signal{EndOfData{}}});
        co_return;
      }
      state_->loaded_partitions.push_back(partition);
    }
    co_await push_input(OperatorMsg<table_slice>{Signal{EndOfData{}}});
  }

private:
  void fail(caf::error error) const {
    state_->error = std::move(error);
  }

  auto load_partition(const partition_info& partition) const
    -> Task<caf::expected<std::vector<table_slice>>> {
    const auto filename = fmt::format(
      TENZIR_FMT_RUNTIME(partition_path_template_), partition.uuid);
    auto partition_path = std::filesystem::path{filename};
    auto partition_chunk = chunk::mmap(partition_path);
    if (not partition_chunk) {
      co_return diagnostic::error(partition_chunk.error())
        .note("failed to mmap partition {} at {}", partition.uuid,
              partition_path)
        .to_error();
    }
    auto partition_state = passive_partition_state{};
    if (auto err = partition_state.initialize_from_chunk(*partition_chunk);
        err.valid()) {
      co_return diagnostic::error(std::move(err))
        .note("failed to load partition {}", partition.uuid)
        .to_error();
    }
    if (partition_state.id != partition.uuid) {
      co_return caf::make_error(ec::format_error,
                                "unexpected ID for passive partition: "
                                "expected {}, got {}",
                                partition.uuid, partition_state.id);
    }
    if (auto const* plugin
        = plugins::find<store_plugin>(partition_state.store_id)) {
      if (partition_state.store_header.size() != uuid::num_bytes) {
        co_return caf::make_error(ec::format_error,
                                  "unexpected store header size for "
                                  "partition {}: expected {}, got {}",
                                  partition.uuid, uuid::num_bytes,
                                  partition_state.store_header.size());
      }
      auto store = plugin->make_passive_store();
      if (not store) {
        co_return std::move(store.error());
      }
      const auto store_uuid
        = uuid{partition_state.store_header.subspan<0, uuid::num_bytes>()};
      auto store_path
        = archive_dir_
          / fmt::format("{}.{}", store_uuid, partition_state.store_id);
      if (store_path.extension() == ".feather") {
        TENZIR_DEBUG("{} loads feather store for partition {} from {}",
                     "partition-transformer", partition.uuid, store_path);
      }
      auto store_chunk = chunk::mmap(store_path);
      if (not store_chunk) {
        co_return diagnostic::error(store_chunk.error())
          .note("failed to mmap store for partition {} at {}", partition.uuid,
                store_path)
          .to_error();
      }
      if (auto err = (*store)->load(std::move(*store_chunk)); err.valid()) {
        co_return diagnostic::error(std::move(err))
          .note("failed to load store for partition {}", partition.uuid)
          .to_error();
      }
      auto result = std::vector<table_slice>{};
      for (auto&& slice : (*store)->slices()) {
        result.push_back(std::move(slice));
      }
      co_return result;
    }
    auto const* plugin
      = plugins::find<store_actor_plugin>(partition_state.store_id);
    if (not plugin) {
      co_return caf::make_error(ec::format_error,
                                "encountered unhandled store backend "
                                "'{}' for partition {}",
                                partition_state.store_id, partition.uuid);
    }
    auto store = plugin->make_store(filesystem_, partition_state.store_header);
    if (not store) {
      co_return std::move(store.error());
    }
    auto result = std::make_shared<std::vector<table_slice>>();
    auto collector
      = filesystem_->home_system().spawn(collect_table_slices, result);
    auto query = query_context::make_extract(
      "partition-transformer",
      caf::actor_cast<receiver_actor<table_slice>>(collector),
      trivially_true_expression());
    query.id = uuid::random();
    auto num_hits
      = co_await async_mail(atom::query_v, std::move(query)).request(*store);
    caf::anon_send_exit(collector, caf::exit_reason::user_shutdown);
    caf::anon_send_exit(*store, caf::exit_reason::user_shutdown);
    if (not num_hits) {
      co_return std::move(num_hits.error());
    }
    co_return std::move(*result);
  }

  std::vector<partition_info> partitions_;
  std::string partition_path_template_;
  std::filesystem::path archive_dir_;
  filesystem_actor filesystem_;
  std::optional<memory_budget> memory_budget_;
  std::shared_ptr<partition_source_state> state_;
};

/// Compile an AST `table_slice -> table_slice` pipeline to a closed operator
/// chain. Emits diagnostics via `dh` for any failure.
auto compile_table_slice_transform(ast::pipeline ast, diagnostic_handler& dh)
  -> failure_or<OperatorChain<table_slice, table_slice>> {
  auto provider = session_provider::make(dh);
  auto ctx = provider.as_session();
  auto b_ctx = base_ctx{ctx.dh(), ctx.reg()};
  auto c_ctx = compile_ctx::make_root(b_ctx);
  TRY(auto ir, std::move(ast).compile(c_ctx));
  auto sub_ctx = substitute_ctx{c_ctx, nullptr};
  TRY(ir.substitute(sub_ctx, true));
  TRY(auto output, ir.infer_type(tag_v<table_slice>, dh));
  if (not output) {
    diagnostic::error("partition transform: pipeline output type is unknown")
      .emit(dh);
    return failure::promise();
  }
  if (not output->is<table_slice>()) {
    diagnostic::error("partition transform: pipeline must produce events, "
                      "got {}",
                      *output)
      .emit(dh);
    return failure::promise();
  }
  auto spawned = std::move(ir).spawn(tag_v<table_slice>);
  if (ctx.has_failure()) {
    return failure::promise();
  }
  auto chain
    = OperatorChain<table_slice, table_slice>::try_from(std::move(spawned));
  if (not chain) {
    diagnostic::error("partition transform: pipeline could not be wired as "
                      "table_slice -> table_slice")
      .emit(dh);
    return failure::promise();
  }
  return std::move(chain).unwrap();
}

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
    if (x->second.events + slice.rows() > partition_capacity) {
      x = data.insert(
        std::make_pair(schema, active_partition_state::serialization_data{}));
    }
  }
  return x->second;
}

// Since we don't have to answer queries while this partition is being
// constructed, we don't have to spawn separate indexer actors and
// stream data but can just compute everything inline here.
void partition_transformer_state::update_type_ids(
  std::unordered_map<std::string, ids>& type_ids, const tenzir::uuid&,
  const table_slice& slice) {
  const auto& schema = slice.schema();
  // Update type ids
  auto it = type_ids.emplace(schema.name(), ids{}).first;
  auto& ids = it->second;
  auto first = slice.offset();
  auto last = slice.offset() + slice.rows();
  TENZIR_ASSERT(first >= ids.size());
  ids.append_bits(false, first - ids.size());
  ids.append_bits(true, last - first);
}

void partition_transformer_state::fulfill(
  partition_transformer_actor::stateful_pointer<partition_transformer_state>
    self,
  stream_data&& stream_data, path_data&& path_data) const {
  TENZIR_DEBUG("{} fulfills promise", *self);
  auto promise = path_data.promise;
  if (self->state().stream_error.valid()) {
    promise.deliver(self->state().stream_error);
    self->quit();
    return;
  }
  if (self->state().transform_error.valid()) {
    promise.deliver(self->state().transform_error);
    self->quit();
    return;
  }
  // Return early if no error occured and no new data was created,
  // ie. the input was erased completely.
  if (self->state().events == 0) {
    promise.deliver(partition_transformer_result{
      .input_partitions = self->state().transformed_input_partitions,
      .output_partitions = {},
      .input_complete = self->state().input_complete,
    });
    self->quit();
    return;
  }
  if (not stream_data.partition_chunks) {
    promise.deliver(stream_data.partition_chunks.error());
    self->quit();
    return;
  }
  if (not stream_data.synopsis_chunks) {
    promise.deliver(stream_data.synopsis_chunks.error());
    self->quit();
    return;
  }
  // When we get here we know that there was at least one event and
  // no error during packing, so at least one of these chunks must be
  // nonnull.
  for (auto& [id, synopsis_chunk] : *stream_data.synopsis_chunks) {
    if (not synopsis_chunk) {
      continue;
    }
    auto filename = fmt::format(
      TENZIR_FMT_RUNTIME(self->state().synopsis_path_template), id);
    auto synopsis_path = std::filesystem::path{filename};
    self->mail(atom::write_v, synopsis_path, synopsis_chunk)
      .request(fs, caf::infinite)
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
                        .result = partition_transformer_result{
                          .input_partitions
                          = self->state().transformed_input_partitions,
                          .output_partitions = std::move(result),
                          .input_complete = self->state().input_complete,
                        },
                      });
      },
      [self, promise](std::vector<partition_synopsis_pair>&&,
                      caf::error&& e) mutable {
        promise.deliver(std::move(e));
        self->quit();
      });
  for (auto& [id, schema, partition_chunk] : *stream_data.partition_chunks) {
    auto rng = self->state().data.equal_range(schema);
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
      TENZIR_FMT_RUNTIME(self->state().partition_path_template), id);
    auto partition_path = std::filesystem::path{filename};
    self->mail(atom::write_v, partition_path, partition_chunk)
      .request(fs, caf::infinite)
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
  std::vector<partition_info> input_partitions, ast::pipeline transform,
  std::string input_partition_path_template, std::filesystem::path archive_dir,
  std::string partition_path_template, std::string synopsis_path_template,
  std::string origin) -> partition_transformer_actor::behavior_type {
  self->state().synopsis_opts = synopsis_opts;
  self->state().input_partition_path_template
    = std::move(input_partition_path_template);
  self->state().archive_dir = std::move(archive_dir);
  self->state().partition_path_template = std::move(partition_path_template);
  self->state().synopsis_path_template = std::move(synopsis_path_template);
  // For historic reasons, the `tenzir.max-partition-size` is stored as the
  // `cardinality` in the value index options.
  self->state().partition_capacity
    = caf::get_or(index_opts, "cardinality", defaults::max_partition_size);
  self->state().index_opts = index_opts;
  self->state().fs = std::move(fs);
  self->state().catalog = std::move(catalog);
  self->state().input_partitions = std::move(input_partitions);
  self->state().transform = std::move(transform);
  self->state().store_id = std::move(store_id);
  self->state().origin = std::move(origin);
  self->mail(atom::done_v).send(static_cast<partition_transformer_actor>(self));
  return {
    [](tenzir::table_slice&) -> caf::result<void> {
      return caf::make_error(ec::logic_error,
                             "partition transformer no longer accepts "
                             "externally pushed table slices");
    },
    [self](atom::done) -> caf::result<void> {
      auto process_slice = [self](table_slice slice,
                                  time fallback_import_time) {
        auto& partition_data = self->state().create_or_get_partition(slice);
        if (not partition_data.synopsis) {
          partition_data.id = tenzir::uuid::random();
          partition_data.store_id = self->state().store_id;
          partition_data.events = 0ull;
          partition_data.synopsis
            = caf::make_copy_on_write<partition_synopsis>();
        }
        auto* unshared_synopsis = partition_data.synopsis.unshared_ptr();
        if (slice.import_time() == time{}) {
          if (fallback_import_time == time::max()) {
            fallback_import_time = time{};
          }
          slice.import_time(fallback_import_time);
        }
        unshared_synopsis->min_import_time
          = std::min(unshared_synopsis->min_import_time, slice.import_time());
        unshared_synopsis->max_import_time
          = std::max(unshared_synopsis->max_import_time, slice.import_time());
        partition_data.events += slice.rows();
        self->state().events += slice.rows();
        self->state().partition_buildup[partition_data.id].slices.push_back(
          std::move(slice));
      };
      auto finish_transform = [self]() {
        auto stream_data = partition_transformer_state::stream_data{
          .partition_chunks
          = std::vector<std::tuple<tenzir::uuid, tenzir::type, chunk_ptr>>{},
          .synopsis_chunks = std::vector<std::tuple<tenzir::uuid, chunk_ptr>>{},
        };
        // We're already done if the whole partition got deleted
        if (self->state().events == 0) {
          store_or_fulfill(self, std::move(stream_data));
          return;
        }
        // ...otherwise, prepare for writing out the transformed data by creating
        // new stores, sending out the slices and requesting new idspace.
        auto store_id = self->state().store_id;
        auto const* store_actor_plugin
          = plugins::find<tenzir::store_actor_plugin>(store_id);
        if (not store_actor_plugin) {
          self->state().stream_error
            = caf::make_error(ec::invalid_argument,
                              "could not find a store plugin named {}",
                              store_id);
          store_or_fulfill(self, std::move(stream_data));
          return;
        }
        for (auto& [schema, partition_data] : self->state().data) {
          if (partition_data.events == 0) {
            continue;
          }
          auto builder_and_header = store_actor_plugin->make_store_builder(
            self->state().fs, partition_data.id, self->state().origin);
          if (not builder_and_header) {
            self->state().stream_error
              = caf::make_error(ec::invalid_argument,
                                "could not create store builder for backend {}",
                                store_id);
            store_or_fulfill(self, std::move(stream_data));
            return;
          }
          partition_data.builder = builder_and_header->store_builder;
          self->monitor(partition_data.builder, [self](const caf::error& err) {
            // This is currently safe because we do all increases to
            // `launched_stores` within the same continuation, but when
            // that changes we need to take a bit more care here to avoid
            // a race.
            ++self->state().stores_finished;
            TENZIR_DEBUG("{} sees builder finished for a total of {}/{} "
                         "stores: {}",
                         *self, self->state().stores_finished,
                         self->state().stores_launched, err);
            if (self->state().stores_finished
                >= self->state().stores_launched) {
              quit_or_stall(self,
                            partition_transformer_state::stores_are_finished{});
            }
          });
          ++self->state().stores_launched;
          partition_data.store_header = builder_and_header->header;
        }
        TENZIR_DEBUG("{} received all table slices", *self);
        self->mail(atom::internal_v, atom::resume_v, atom::done_v)
          .send(static_cast<partition_transformer_actor>(self));
      };
      // Move input metadata and AST out so the actor's state is empty when the
      // run begins; they live inside the coroutine until it finishes.
      auto input_partitions = std::exchange(self->state().input_partitions, {});
      auto input_partition_path_template
        = std::move(self->state().input_partition_path_template);
      auto archive_dir = std::move(self->state().archive_dir);
      auto ast = std::move(self->state().transform);
      // We deliver `rp` immediately and signal real completion later via the
      // store-builder monitors set up in `finish_transform`.
      auto rp = self->make_response_promise<void>();
      auto weak = caf::weak_actor_ptr{self->ctrl(), caf::add_ref};
      auto& sys = self->system();
      auto source_state = std::make_shared<partition_source_state>();
      folly::coro::co_withExecutor(
        folly::getGlobalCPUExecutor(),
        folly::coro::co_invoke(
          [ast = std::move(ast), input_partitions = std::move(input_partitions),
           input_partition_path_template
           = std::move(input_partition_path_template),
           archive_dir = std::move(archive_dir), &sys, weak, self,
           process_slice,
           source_state]() mutable -> folly::coro::Task<failure_or<void>> {
            // Compaction and rebuild have no user-facing diagnostic sink, so
            // we log to the server log. The handler is owned by this
            // coroutine frame so its address is stable across awaits and it
            // outlives every operator inside `run_transform` that references
            // it.
            auto dh = TransformerDiagHandler{};
            CO_TRY(auto chain,
                   compile_table_slice_transform(std::move(ast), dh));
            auto loader = partition_loader{
              std::move(input_partitions),
              std::move(input_partition_path_template),
              std::move(archive_dir),
              self->state().fs,
              source_state,
            };
            auto feed_input
              = [loader = std::move(loader)](
                  Push<OperatorMsg<table_slice>>& push_input) mutable
              -> Task<void> {
              co_await loader.feed(push_input);
            };
            auto drain_output
              = [self, weak, process_slice, source_state](
                  Pull<OperatorMsg<table_slice>>& pull_output) mutable
              -> Task<void> {
              while (auto msg = co_await pull_output()) {
                co_await co_match(
                  std::move(*msg),
                  [&](table_slice slice) -> Task<void> {
                    if (slice.rows() == 0) {
                      co_return;
                    }
                    auto [promise, future]
                      = folly::makePromiseContract<folly::Unit>();
                    auto promise_ptr
                      = std::make_shared<folly::Promise<folly::Unit>>(
                        std::move(promise));
                    auto strong = weak.lock();
                    if (not strong) {
                      promise_ptr->setValue(folly::unit);
                    } else {
                      auto fallback_import_time
                        = source_state->min_loaded_import_time();
                      self->schedule_fn(
                        [process_slice, slice = std::move(slice),
                         fallback_import_time,
                         promise = std::move(promise_ptr)]() mutable {
                          process_slice(std::move(slice), fallback_import_time);
                          promise->setValue(folly::unit);
                        });
                    }
                    co_await to_task_interrupt_on_cancel(std::move(future));
                  },
                  [&](Signal signal) -> Task<void> {
                    co_await co_match(
                      signal,
                      [&](EndOfData) -> Task<void> {
                        co_return;
                      },
                      [&](Checkpoint) -> Task<void> {
                        co_return;
                      });
                  });
              }
            };
            CO_TRY(co_await run_transform(
              std::move(chain), sys, dh, NoProfiler{}, /*is_hidden=*/true,
              std::move(feed_input), std::move(drain_output)));
            co_return {};
          }))
        .start([self, weak = std::move(weak), source_state,
                finish_transform = std::move(finish_transform)](
                 folly::Try<failure_or<void>>&& result) mutable {
          auto strong = weak.lock();
          if (not strong) {
            return;
          }
          // We're on the folly executor thread here. All mutations of actor
          // state must happen inside the `schedule_fn` body, which runs on
          // the actor's thread.
          auto error = caf::error{};
          if (result.hasException()) {
            error = caf::make_error(ec::logic_error,
                                    fmt::format("partition transform: {}",
                                                result.exception().what()));
          } else if (result.value().is_error()) {
            error = caf::make_error(ec::logic_error,
                                    "partition transform: pipeline failed; "
                                    "see server log for diagnostics");
          }
          self->schedule_fn([self,
                             finish_transform = std::move(finish_transform),
                             source_state, error = std::move(error)]() mutable {
            if (error) {
              TENZIR_ERROR("{} pipeline executor failed: {}", *self, error);
              self->state().transform_error = std::move(error);
              store_or_fulfill(self,
                               partition_transformer_state::stream_data{});
              return;
            } else if (source_state->error.valid()) {
              self->state().stream_error = std::move(source_state->error);
              store_or_fulfill(self,
                               partition_transformer_state::stream_data{});
              return;
            } else {
              TENZIR_DEBUG("{} pipeline executor completed successfully",
                           *self);
            }
            self->state().min_import_time = source_state->min_import_time;
            self->state().max_import_time = source_state->max_import_time;
            self->state().transformed_input_partitions
              = source_state->input_complete
                  ? std::move(source_state->selected_partitions)
                  : std::move(source_state->loaded_partitions);
            self->state().input_complete = source_state->input_complete;
            finish_transform();
          });
        });
      rp.deliver();
      return rp;
    },
    [self](atom::internal, atom::resume, atom::done) {
      TENZIR_DEBUG("{} got resume", *self);
      for (auto& [schema, data] : self->state().data) {
        auto& mutable_synopsis = data.synopsis.unshared();
        // Push the slices to the store.
        auto& buildup = self->state().partition_buildup.at(data.id);
        auto offset = id{0};
        for (auto& slice : buildup.slices) {
          if (slice.import_time() == time{}) {
            slice.import_time(self->state().min_import_time);
          }
          slice.offset(offset);
          offset += slice.rows();
          self->mail(slice).send(data.builder);
          self->state().update_type_ids(data.type_ids, data.id, slice);
          mutable_synopsis.add(slice, self->state().partition_capacity,
                               self->state().synopsis_opts);
        }
        // Update the synopsis
        // TODO: It would make more sense if the partition
        // synopsis keeps track of offset/events internally.
        mutable_synopsis.shrink();
        mutable_synopsis.events = data.events;
      }
      for (auto& [_, partition_data] : self->state().data) {
        self->mail(atom::persist_v)
          .request(partition_data.builder, caf::infinite)
          .then(
            [self, builder = partition_data.builder](resource&) {
              // Unlike active partitions, the transformer no longer needs the
              // store builder after `persist` succeeds, so we explicitly shut
              // it down to preserve the existing completion signal.
              self->send_exit(builder, caf::exit_reason::normal);
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
                         self->state().persist);
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
             self->state().data) { // Pack partitions
          auto indexers_it
            = self->state().partition_buildup.find(partition_data.id);
          if (indexers_it == self->state().partition_buildup.end()) {
            stream_data.partition_chunks
              = caf::make_error(ec::logic_error, "missing data for partition");
            return;
          }
          auto partition = pack_full(partition_data, record_type{});
          if (not partition) {
            stream_data.partition_chunks = partition.error();
            return;
          }
          stream_data.partition_chunks->emplace_back(
            std::make_tuple(partition_data.id, schema, *partition));
        }
        for (auto& [schema, partition_data] :
             self->state().data) { // Pack partition synopsis
          flatbuffers::FlatBufferBuilder builder;
          auto synopsis = pack(builder, *partition_data.synopsis);
          if (not synopsis) {
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
    [self](atom::persist) -> caf::result<partition_transformer_result> {
      TENZIR_DEBUG("{} received request to persist", *self);
      auto promise
        = self->make_response_promise<partition_transformer_result>();
      auto path_data = partition_transformer_state::path_data{
        .promise = promise,
      };
      store_or_fulfill(self, std::move(path_data));
      return promise;
    }};
}

} // namespace tenzir
