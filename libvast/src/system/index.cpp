//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/index.hpp"

#include "vast/fwd.hpp"

#include "vast/chunk.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/table_slice.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/data.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/fill_status_map.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/detail/notifying_stream_manager.hpp"
#include "vast/detail/shutdown_stream_stage.hpp"
#include "vast/detail/tracepoint.hpp"
#include "vast/error.hpp"
#include "vast/fbs/index.hpp"
#include "vast/fbs/partition.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/fbs/uuid.hpp"
#include "vast/ids.hpp"
#include "vast/io/read.hpp"
#include "vast/io/save.hpp"
#include "vast/logger.hpp"
#include "vast/partition_synopsis.hpp"
#include "vast/system/active_partition.hpp"
#include "vast/system/catalog.hpp"
#include "vast/system/partition_transformer.hpp"
#include "vast/system/passive_partition.hpp"
#include "vast/system/report.hpp"
#include "vast/system/shutdown.hpp"
#include "vast/system/status.hpp"
#include "vast/table_slice.hpp"
#include "vast/uuid.hpp"

#include <caf/error.hpp>
#include <caf/make_copy_on_write.hpp>
#include <caf/response_promise.hpp>
#include <caf/send.hpp>
#include <flatbuffers/flatbuffers.h>

#include <algorithm>
#include <chrono>
#include <ctime>
#include <deque>
#include <filesystem>
#include <memory>
#include <numeric>
#include <span>
#include <unistd.h>

// clang-format off
//
// # Import
//
// The index is implemented as a stream stage that hooks into the table slice
// stream coming from the importer, and forwards them to the current active
// partition
//
//              table slice              table slice                      table slice column
//   importer ----------------> index ---------------> active partition ------------------------> indexer
//                                                                      ------------------------> indexer
//                                                                                ...
//
// # Lookup
//
// At the same time, the index is also involved in the lookup path, where it
// receives an expression and loads the partitions that might contain relevant
// results into memory.
//
//    expression                                lookup()
//   ------------>  index                  --------------------> catalog
//                                                                 |
//     query_id,                                                   |
//     scheduled,                                                  |
//     remaining                                    [uuid]         |
//   <-----------  (creates query state)  <------------------------/
//                            |
//                            |  query_id, n_taste
//                            |
//    query_id, n             v                   expression, client
//   ------------> (spawn n partitions) --------------------------------> partition
//                                                                            |
//                                                      ids                   |
//   <------------------------------------------------------------------------/
//                                                      ids                   |
//   <------------------------------------------------------------------------/
//                                                                            |
//
//                                                                          [...]
//
//                                                      atom::done            |
//   <------------------------------------------------------------------------/
//
//
// # Erase
//
// We currently have two distinct erasure code paths: One externally driven by
// the disk monitor, who looks at the file system and identifies those partitions
// that shall be removed. This is done by the `atom::erase` handler.
//
// The other is data-driven and comes from the `eraser`, who sends us a `vast::query`
// whose results shall be deleted from disk.
//
// clang-format on

namespace vast::system {

caf::error extract_partition_synopsis(
  const std::filesystem::path& partition_path,
  const std::filesystem::path& partition_synopsis_path) {
  // Use blocking operations here since this is part of the startup.
  auto chunk = chunk::mmap(partition_path);
  if (!chunk)
    return std::move(chunk.error());
  const auto* partition = fbs::GetPartition(chunk->get()->data());
  if (partition->partition_type() != fbs::partition::Partition::legacy)
    return caf::make_error(ec::format_error, "found unsupported version for "
                                             "partition "
                                               + partition_path.string());
  const auto* partition_legacy = partition->partition_as_legacy();
  VAST_ASSERT(partition_legacy);
  partition_synopsis ps;
  if (auto error = unpack(*partition_legacy, ps))
    return error;
  flatbuffers::FlatBufferBuilder builder;
  auto ps_offset = pack(builder, ps);
  if (!ps_offset)
    return ps_offset.error();
  fbs::PartitionSynopsisBuilder ps_builder(builder);
  ps_builder.add_partition_synopsis_type(
    fbs::partition_synopsis::PartitionSynopsis::legacy);
  ps_builder.add_partition_synopsis(ps_offset->Union());
  auto flatbuffer = ps_builder.Finish();
  fbs::FinishPartitionSynopsisBuffer(builder, flatbuffer);
  auto chunk_out = fbs::release(builder);
  return io::save(partition_synopsis_path,
                  std::span{chunk_out->data(), chunk_out->size()});
}

caf::expected<flatbuffers::Offset<fbs::Index>>
pack(flatbuffers::FlatBufferBuilder& builder, const index_state& state) {
  VAST_DEBUG("index persists {} uuids of definitely persisted and {}"
             "uuids of maybe persisted partitions",
             state.persisted_partitions.size(), state.unpersisted.size());
  std::vector<flatbuffers::Offset<fbs::LegacyUUID>> partition_offsets;
  for (auto uuid : state.persisted_partitions) {
    if (auto uuid_fb = pack(builder, uuid))
      partition_offsets.push_back(*uuid_fb);
    else
      return uuid_fb.error();
  }
  // We don't know if these will make it to disk before the index and the rest
  // of the system is shut down (in case of a hard/dirty shutdown), so we just
  // store everything and throw out the missing partitions when loading the
  // index.
  for (const auto& kv : state.unpersisted) {
    if (auto uuid_fb = pack(builder, kv.first))
      partition_offsets.push_back(*uuid_fb);
    else
      return uuid_fb.error();
  }
  auto partitions = builder.CreateVector(partition_offsets);
  std::vector<flatbuffers::Offset<fbs::layout_statistics::v0>> stats_offsets;
  for (const auto& [name, layout_stats] : state.stats.layouts) {
    auto name_fb = builder.CreateString(name);
    fbs::layout_statistics::v0Builder stats_builder(builder);
    stats_builder.add_name(name_fb);
    stats_builder.add_count(layout_stats.count);
    auto offset = stats_builder.Finish();
    stats_offsets.push_back(offset);
  }
  auto stats = builder.CreateVector(stats_offsets);
  fbs::index::v0Builder v0_builder(builder);
  v0_builder.add_partitions(partitions);
  v0_builder.add_stats(stats);
  auto index_v0 = v0_builder.Finish();
  fbs::IndexBuilder index_builder(builder);
  index_builder.add_index_type(vast::fbs::index::Index::v0);
  index_builder.add_index(index_v0.Union());
  auto index = index_builder.Finish();
  fbs::FinishIndexBuffer(builder, index);
  return index;
}

// -- partition_factory --------------------------------------------------------

partition_factory::partition_factory(index_state& state) : state_{state} {
  // nop
}

filesystem_actor& partition_factory::filesystem() {
  return filesystem_;
}

partition_actor partition_factory::operator()(const uuid& id) const {
  // Load partition from disk.
  if (std::find(state_.persisted_partitions.begin(),
                state_.persisted_partitions.end(), id)
      == state_.persisted_partitions.end())
    VAST_WARN("{} did not find partition {} in it's internal state, but tries "
              "to load it regardless",
              *state_.self, id);
  const auto path = state_.partition_path(id);
  VAST_DEBUG("{} loads partition {} for path {}", *state_.self, id, path);
  materializations_++;
  return state_.self->spawn(passive_partition, id, state_.accountant,
                            static_cast<store_actor>(state_.global_store),
                            filesystem_, path);
}

size_t partition_factory::materializations() const {
  return materializations_;
}

// -- index_state --------------------------------------------------------------

index_state::index_state(index_actor::pointer self)
  : self{self}, inmem_partitions{0, partition_factory{*this}} {
}

// -- persistence --------------------------------------------------------------

std::filesystem::path
index_state::index_filename(const std::filesystem::path& basename) const {
  return basename / dir / "index.bin";
}

std::filesystem::path index_state::partition_path(const uuid& id) const {
  return dir / to_string(id);
}

std::filesystem::path
index_state::partition_synopsis_path(const uuid& id) const {
  return synopsisdir / (to_string(id) + ".mdx");
}

caf::error index_state::load_from_disk() {
  // We dont use the filesystem actor here because this function is only
  // called once during startup, when no other actors exist yet.
  std::error_code err{};
  const auto file_exists = std::filesystem::exists(dir, err);
  if (!file_exists) {
    VAST_VERBOSE("{} found no prior state, starting with a clean slate", *self);
    return caf::none;
  }
  if (auto fname = index_filename(); std::filesystem::exists(fname, err)) {
    VAST_VERBOSE("{} loads state from {}", *self, fname);
    auto buffer = io::read(fname);
    if (!buffer) {
      VAST_ERROR("{} failed to read index file: {}", *self,
                 render(buffer.error()));
      return buffer.error();
    }
    // TODO: Create a `index_ondisk_state` struct and move this part of the
    // code into an `unpack()` function.
    const auto* index = fbs::GetIndex(buffer->data());
    if (index->index_type() != fbs::index::Index::v0)
      return caf::make_error(ec::format_error, "invalid index version");
    const auto* index_v0 = index->index_as_v0();
    const auto* partition_uuids = index_v0->partitions();
    VAST_ASSERT(partition_uuids);
    auto synopses = std::make_shared<std::map<uuid, partition_synopsis_ptr>>();
    const size_t total = partition_uuids->size();
    for (size_t idx = 0; idx < total; ++idx) {
      const auto* uuid_fb = partition_uuids->Get(idx);
      VAST_ASSERT(uuid_fb);
      vast::uuid partition_uuid{};
      if (auto error = unpack(*uuid_fb, partition_uuid))
        return caf::make_error(ec::format_error, fmt::format("could not unpack "
                                                             "uuid from "
                                                             "index state: {}",
                                                             error));
      auto part_path = partition_path(partition_uuid);
      auto synopsis_path = partition_synopsis_path(partition_uuid);
      if (!exists(part_path)) {
        VAST_WARN("{} found partition {}"
                  "in the index state but not on disk; this may have been "
                  "caused by an unclean shutdown",
                  *self, partition_uuid);
        continue;
      }
      VAST_DEBUG("{} unpacks partition {} ({}/{})", *self, partition_uuid, idx,
                 total);
      // Generate external partition synopsis file if it doesn't exist.
      if (!exists(synopsis_path)) {
        if (auto error = extract_partition_synopsis(part_path, synopsis_path))
          return error;
      }
    retry:
      auto chunk = chunk::mmap(synopsis_path);
      if (!chunk) {
        VAST_WARN("{} could not mmap partition at {}", *self, part_path);
        continue;
      }
      const auto* ps_flatbuffer
        = fbs::GetPartitionSynopsis(chunk->get()->data());
      partition_synopsis_ptr ps = caf::make_copy_on_write<partition_synopsis>();
      if (ps_flatbuffer->partition_synopsis_type()
          != fbs::partition_synopsis::PartitionSynopsis::legacy)
        return caf::make_error(ec::format_error, "invalid partition synopsis "
                                                 "version");
      const auto& synopsis_legacy
        = *ps_flatbuffer->partition_synopsis_as_legacy();
      // Re-write old partition synopses that were created before the offset and
      // id were saved.
      if (!synopsis_legacy.id_range()) {
        VAST_VERBOSE("{} rewrites old catalog data for partition {}", *self,
                     partition_uuid);
        if (auto error = extract_partition_synopsis(part_path, synopsis_path))
          return error;
        // TODO: There is probably a good way to rewrite this without the jump,
        // but in the meantime I defer to Knuth:
        //   http://people.cs.pitt.edu/~zhangyt/teaching/cs1621/goto.paper.pdf
        goto retry;
      }
      if (auto error = unpack(synopsis_legacy, ps.unshared()))
        return error;
      persisted_partitions.insert(partition_uuid);
      synopses->emplace(partition_uuid, std::move(ps));
    }
    // We collect all synopses to send them in bulk, since the `await` interface
    // doesn't lend itself to a huge number of awaited messages: Only the tip of
    // the current awaited list is considered, leading to an O(n**2) worst-case
    // behavior if the responses arrive in the same order to how they were sent.
    VAST_DEBUG("{} requesting bulk merge of {} partitions", *self,
               synopses->size());
    this->accept_queries = false;
    self
      ->request(catalog, caf::infinite, atom::merge_v,
                std::exchange(synopses, {}))
      .then(
        [this](atom::ok) {
          VAST_INFO("{} finished initializing and is ready to accept queries",
                    *self);
          this->accept_queries = true;
        },
        [this](caf::error& err) {
          VAST_ERROR("{} could not load catalog state from disk, shutting "
                     "down with error {}",
                     *self, err);
          self->send_exit(self, std::move(err));
        });
    const auto* stats = index_v0->stats();
    if (!stats)
      return caf::make_error(ec::format_error, "no stats in persisted index "
                                               "state");
    for (const auto* const stat : *stats) {
      this->stats.layouts[stat->name()->str()]
        = layout_statistics{stat->count()};
    }
  } else {
    VAST_DEBUG("{} found existing database dir {} without index "
               "statefile, "
               "will start with fresh state",
               *self, dir);
  }
  return caf::none;
}

/// Persists the state to disk.
void index_state::flush_to_disk() {
  auto builder = flatbuffers::FlatBufferBuilder{};
  auto index = pack(builder, *this);
  if (!index) {
    VAST_WARN("{} failed to pack index: {}", *self, index.error());
    return;
  }
  auto chunk = fbs::release(builder);
  self
    ->request(filesystem, caf::infinite, atom::write_v, index_filename(), chunk)
    .then(
      [this](atom::ok) {
        VAST_DEBUG("{} successfully persisted index state", *self);
      },
      [this](const caf::error& err) {
        VAST_WARN("{} failed to persist index state: {}", *self, render(err));
      });
}

// -- flush handling -----------------------------------------------------------

void index_state::add_flush_listener(flush_listener_actor listener) {
  VAST_DEBUG("{} adds a new 'flush' subscriber: {}", *self, listener);
  flush_listeners.emplace_back(std::move(listener));
  // We may need to call `notify_listeners_if_clean` if the subscription
  // happens after the data has already completely passed the index, but
  // we must not to call it before any data at all has arrived it would
  // create a false positive.
  if (!active_partitions.empty())
    detail::notify_listeners_if_clean(*this, *stage);
}

// The whole purpose of the `-b` flag is to somehow block until all imported
// data is available for querying, so we have to layer hack upon hack here to
// achieve this most of the time. This is only used for integration tests.
// Note that there's still a race condition here if the call to
// `notify_flush_listeners()` arrives when there's still data en route to
// the unpersisted partitions.
// TODO(ch19583): Rip out the whole 'notifying_stream_manager' and replace it
// with some kind of ping/pong protocol.
void index_state::notify_flush_listeners() {
  VAST_DEBUG("{} sends 'flush' messages to {} listeners", *self,
             flush_listeners.size());
  for (auto& listener : flush_listeners) {
    bool downstream = false;
    for (const auto& [_, active_partition] : active_partitions) {
      if (active_partition.actor) {
        self->send(active_partition.actor, atom::subscribe_v, atom::flush_v,
                   listener);
        downstream = true;
      }
    }
    if (!downstream)
      self->send(listener, atom::flush_v);
  }
  flush_listeners.clear();
}

// -- partition handling -----------------------------------------------------

bool i_partition_selector::operator()(const type& filter,
                                      const table_slice& slice) const {
  return filter == slice.layout();
}

void index_state::create_active_partition(const type& layout) {
  auto id = uuid::random();
  auto& active_partition = active_partitions[layout];
  // If we're using the global store, the importer already sends the table
  // slices. (In the long run, this should probably be streamlined so that all
  // data moves through the index. However, that requires some refactoring of
  // the archive itself so it can handle multiple input streams.)
  std::string store_name = {};
  chunk_ptr store_header = chunk::make_empty();
  store_name = store_plugin->name();
  auto builder_and_header
    = store_plugin->make_store_builder(accountant, filesystem, id);
  if (!builder_and_header) {
    VAST_ERROR("could not create new active partition: {}",
               render(builder_and_header.error()));
    self->quit(builder_and_header.error());
    return;
  }
  auto& [builder, header] = *builder_and_header;
  store_header = header;
  active_partition.store = builder;
  active_partition.store_slot
    = stage->add_outbound_path(active_partition.store);
  stage->out().set_filter(active_partition.store_slot, layout);
  active_partition.spawn_time = std::chrono::steady_clock::now();
  active_partition.actor
    = self->spawn(::vast::system::active_partition, id, accountant, filesystem,
                  index_opts, synopsis_opts,
                  static_cast<store_actor>(active_partition.store), store_name,
                  store_header);
  active_partition.stream_slot
    = stage->add_outbound_path(active_partition.actor);
  stage->out().set_filter(active_partition.stream_slot, layout);
  active_partition.capacity = partition_capacity;
  active_partition.id = id;
  VAST_DEBUG("{} created new partition {}", *self, id);
}

void index_state::decomission_active_partition(const type& layout) {
  auto active_partition = active_partitions.find(layout);
  VAST_ASSERT(active_partition != active_partitions.end());
  auto id = active_partition->second.id;
  auto actor = std::exchange(active_partition->second.actor, {});
  unpersisted[id] = actor;
  // Send buffered batches and remove active partition from the stream.
  stage->out().fan_out_flush();
  stage->out().close(active_partition->second.stream_slot);
  stage->out().close(active_partition->second.store_slot);
  stage->out().force_emit_batches();
  // Persist active partition asynchronously.
  auto part_dir = partition_path(id);
  auto synopsis_dir = partition_synopsis_path(id);
  VAST_DEBUG("{} persists active partition {} to {}", *self, layout, part_dir);
  self->request(actor, caf::infinite, atom::persist_v, part_dir, synopsis_dir)
    .then(
      [=, this](partition_synopsis_ptr& ps) {
        VAST_DEBUG("{} successfully persisted partition {} {}", *self, layout,
                   id);
        // The catalog expects to own the partition synopsis it receives,
        // so we make a copy for the listeners.
        // TODO: We should skip this continuation if we're currently shutting
        // down.
        self->request(catalog, caf::infinite, atom::merge_v, id, ps)
          .then(
            [=, this](atom::ok) {
              VAST_DEBUG("{} received ok for request to persist partition {} "
                         "{}",
                         *self, layout, id);
              for (auto& listener : partition_creation_listeners)
                self->send(listener, atom::update_v,
                           partition_synopsis_pair{id, ps});
              unpersisted.erase(id);
              persisted_partitions.insert(id);
            },
            [=, this](const caf::error& err) {
              VAST_DEBUG("{} received error for request to persist partition "
                         "{} "
                         "{}: {}",
                         *self, layout, id, err);
            });
      },
      [=, this](caf::error& err) {
        VAST_ERROR("{} failed to persist partition {} {} with error: {}", *self,
                   layout, id, err);
        self->quit(std::move(err));
      });
}

void index_state::add_partition_creation_listener(
  partition_creation_listener_actor listener) {
  partition_creation_listeners.push_back(listener);
}

// -- query handling ---------------------------------------------------------

void index_state::schedule_lookups() {
  if (!pending_queries.has_work())
    return;
  auto t = timer::start(scheduler_measurement);
  auto num_scheduled = size_t{0};
  auto on_return = caf::detail::make_scope_guard([&] {
    t.stop(num_scheduled);
  });
  while (running_partition_lookups < max_concurrent_partition_lookups) {
    // 1. Get the partition with the highest accumulated priority.
    auto next = pending_queries.next();
    if (!next) {
      VAST_DEBUG("{} did not find a partition to query", *self);
      return;
    }
    auto immediate_completion = [&](const query_queue::entry& x) {
      for (auto qid : x.queries)
        if (auto client = pending_queries.handle_completion(qid))
          self->send(*client, atom::done_v);
    };
    if (next->erased) {
      VAST_DEBUG("{} skips erased partition {}", *self, next->partition);
      immediate_completion(*next);
      continue;
    }
    VAST_DEBUG("{} schedules partition {} for {}", *self, next->partition,
               next->queries);
    // 2. Acquire the actor for the selected partition, potentially materializing
    //    it from its persisted state.
    auto acquire = [&](const uuid& partition_id) -> partition_actor {
      // We need to first check whether the ID is the active partition or one
      // of our unpersisted ones. Only then can we dispatch to our LRU cache.
      partition_actor part;
      for (const auto& [_, active_partition] : active_partitions) {
        if (active_partition.actor != nullptr
            && active_partition.id == partition_id) {
          part = active_partition.actor;
          break;
        }
      }
      if (!part) {
        if (auto it = unpersisted.find(partition_id); it != unpersisted.end())
          part = it->second;
        else if (auto it = persisted_partitions.find(partition_id);
                 it != persisted_partitions.end())
          part = inmem_partitions.get_or_load(partition_id);
      }
      if (!part)
        VAST_WARN("{} failed to load partition {} that was part of a query",
                  *self, partition_id);
      return part;
    };
    auto partition_actor = acquire(next->partition);
    if (!partition_actor) {
      // We need to mark failed partitions as completed to avoid clients going
      // out of sync.
      immediate_completion(*next);
      continue;
    }
    counters.partition_scheduled++;
    counters.partition_lookups += next->queries.size();
    // 3. request all relevant queries in a loop
    auto cnt = std::make_shared<size_t>(next->queries.size());
    for (auto qid : next->queries) {
      auto it = pending_queries.queries().find(qid);
      if (it == pending_queries.queries().end()) {
        VAST_WARN("{} tried to access non-existent query {}", *self, qid);
        *cnt -= 1;
        if (*cnt == 0)
          --running_partition_lookups;
        continue;
      }
      auto handle_completion = [cnt, qid, this] {
        if (auto client = pending_queries.handle_completion(qid))
          self->send(*client, atom::done_v);
        // 4. recursively call schedule_lookups in the done handler. ...or
        //    when all done? (5)
        // 5. decrement running_partition_lookups when all queries that
        //    were started are done. Keep track in the closure.
        *cnt -= 1;
        if (*cnt == 0) {
          --running_partition_lookups;
          schedule_lookups();
        }
      };
      self
        ->request(partition_actor, defaults::system::scheduler_timeout,
                  it->second.query)
        .then(
          [this, handle_completion, qid, pid = next->partition](uint64_t n) {
            VAST_DEBUG("{} received {} results for query {} from partition {}",
                       *self, n, qid, pid);
            handle_completion();
          },
          [this, handle_completion, qid,
           pid = next->partition](const caf::error& err) {
            VAST_WARN("{} failed to evaluate query {} for partition {}: {}",
                      *self, qid, pid, err);
            handle_completion();
          });
    }
    running_partition_lookups++;
    num_scheduled++;
  }
}

// -- introspection ----------------------------------------------------------

namespace {

struct query_counters {
  size_t num_custom_prio = 0;
  size_t num_low_prio = 0;
  size_t num_normal_prio = 0;
};

auto get_query_counters(const query_queue& pending_queries) {
  auto result = query_counters{};
  for (const auto& [_, q] : pending_queries.queries()) {
    if (q.query.priority == query::priority::low)
      result.num_low_prio++;
    else if (q.query.priority == query::priority::normal)
      result.num_normal_prio++;
    else
      result.num_custom_prio++;
  }
  return result;
}

} // namespace

void index_state::send_report() {
  auto materializations = inmem_partitions.factory().materializations()
                          - this->counters.previous_materializations;
  auto counters = std::exchange(this->counters, {});
  this->counters.previous_materializations
    = inmem_partitions.factory().materializations();
  auto query_counters = get_query_counters(pending_queries);
  auto msg = report{
    .data = {
      {"scheduler.backlog.custom", query_counters.num_custom_prio},
      {"scheduler.backlog.low", query_counters.num_low_prio},
      {"scheduler.backlog.normal", query_counters.num_normal_prio},
      {"scheduler.partition.pending", pending_queries.num_partitions()},
      {"scheduler.partition.materializations", materializations},
      {"scheduler.partition.lookups", counters.partition_lookups},
      {"scheduler.partition.scheduled", counters.partition_scheduled},
      {"scheduler.partition.remaining-capacity",
       max_concurrent_partition_lookups - running_partition_lookups},
      {"scheduler.partition.current-lookups", running_partition_lookups},
    }};
  self->send(accountant, std::move(msg));
  auto r = performance_report{.data = {{{"scheduler", scheduler_measurement}}}};
  self->send(accountant, std::move(r));
  scheduler_measurement = measurement{};
}

caf::typed_response_promise<record>
index_state::status(status_verbosity v) const {
  struct extra_state {
    size_t memory_usage = 0;
    void
    deliver(caf::typed_response_promise<record>&& promise, record&& content) {
      content["memory-usage"] = count{memory_usage};
      promise.deliver(std::move(content));
    }
  };
  auto rs = make_status_request_state<extra_state>(self);
  auto stats_object = record{};
  auto sum = uint64_t{0};
  for (const auto& [_, layout_stats] : stats.layouts)
    sum += layout_stats.count;
  auto xs = record{};
  xs["total"] = count{sum};
  stats_object["events"] = xs;
  if (v >= status_verbosity::detailed) {
    auto layout_object = record{};
    for (const auto& [name, layout_stats] : stats.layouts) {
      auto xs = record{};
      xs["count"] = count{layout_stats.count};
      layout_object[name] = xs;
    }
    stats_object["layouts"] = std::move(layout_object);
    auto backlog_status = record{};
    auto query_counters = get_query_counters(pending_queries);
    backlog_status["num-custom-priority"] = query_counters.num_custom_prio;
    backlog_status["num-low-priority"] = query_counters.num_low_prio;
    backlog_status["num-normal-priority"] = query_counters.num_normal_prio;
    rs->content["backlog"] = std::move(backlog_status);
    auto worker_status = record{};
    worker_status["count"] = max_concurrent_partition_lookups;
    worker_status["idle"]
      = max_concurrent_partition_lookups - running_partition_lookups;
    worker_status["busy"] = running_partition_lookups;
    rs->content["workers"] = std::move(worker_status);
    auto pending_status = list{};
    for (const auto& [u, qs] : pending_queries.queries()) {
      auto q = record{};
      q["id"] = fmt::to_string(u);
      q["query"] = fmt::to_string(qs);
      pending_status.emplace_back(std::move(q));
    }
    rs->content["pending"] = std::move(pending_status);
    rs->content["num-active-partitions"] = count{active_partitions.size()};
    rs->content["num-cached-partitions"] = count{inmem_partitions.size()};
    rs->content["num-unpersisted-partitions"] = count{unpersisted.size()};
    const auto timeout = defaults::system::initial_request_timeout / 5 * 4;
    auto partitions = record{};
    auto partition_status
      = [&](const uuid& id, const partition_actor& pa, list& xs) {
          collect_status(
            rs, timeout, v, pa,
            [=, &xs](const record& part_status) {
              auto ps = record{};
              ps["id"] = to_string(id);
              auto it = part_status.find("memory-usage");
              if (it != part_status.end()) {
                if (const auto* s = caf::get_if<count>(&it->second))
                  rs->memory_usage += *s;
              }
              if (v >= status_verbosity::debug)
                merge(part_status, ps, policy::merge_lists::no);
              xs.push_back(std::move(ps));
            },
            [=, this, &xs](const caf::error& err) {
              VAST_WARN("{} failed to retrieve status from {} : {}", *self, id,
                        render(err));
              auto ps = record{};
              ps["id"] = to_string(id);
              ps["error"] = render(err);
              xs.push_back(std::move(ps));
            });
        };
    // Resident partitions.
    // We emplace subrecords directly because we need to give the
    // right pointer to the collect_status callback.
    // Otherwise we would assign to a moved from object.
    partitions.reserve(3u);
    auto& active
      = caf::get<list>(partitions.emplace("active", list{}).first->second);
    active.reserve(active_partitions.size());
    for (const auto& [_, active_partition] : active_partitions) {
      if (active_partition.actor != nullptr)
        partition_status(active_partition.id, active_partition.actor, active);
    }
    auto& cached
      = caf::get<list>(partitions.emplace("cached", list{}).first->second);
    cached.reserve(inmem_partitions.size());
    for (const auto& [id, actor] : inmem_partitions)
      partition_status(id, actor, cached);
    auto& unpersisted
      = caf::get<list>(partitions.emplace("unpersisted", list{}).first->second);
    unpersisted.reserve(this->unpersisted.size());
    for (const auto& [id, actor] : this->unpersisted)
      partition_status(id, actor, unpersisted);
    rs->content["partitions"] = std::move(partitions);
    // General state such as open streams.
  }
  rs->content["statistics"] = std::move(stats_object);
  if (v >= status_verbosity::debug)
    detail::fill_status_map(rs->content, self);
  return rs->promise;
}

index_actor::behavior_type
index(index_actor::stateful_pointer<index_state> self,
      accountant_actor accountant, filesystem_actor filesystem,
      archive_actor archive, catalog_actor catalog,
      type_registry_actor type_registry, const std::filesystem::path& dir,
      std::string store_backend, size_t partition_capacity,
      duration active_partition_timeout, size_t max_inmem_partitions,
      size_t taste_partitions, size_t max_concurrent_partition_lookups,
      const std::filesystem::path& catalog_dir, index_config index_config) {
  VAST_TRACE_SCOPE("index {} {} {} {} {} {} {} {} {} {}", VAST_ARG(self->id()),
                   VAST_ARG(filesystem), VAST_ARG(dir),
                   VAST_ARG(partition_capacity),
                   VAST_ARG(active_partition_timeout),
                   VAST_ARG(max_inmem_partitions), VAST_ARG(taste_partitions),
                   VAST_ARG(max_concurrent_partition_lookups),
                   VAST_ARG(catalog_dir), VAST_ARG(index_config));
  VAST_VERBOSE("{} initializes index in {} with a maximum partition "
               "size of {} events and {} resident partitions",
               *self, dir, partition_capacity, max_inmem_partitions);
  self->state.index_opts["cardinality"] = partition_capacity;
  self->state.synopsis_opts = std::move(index_config);
  // The global archive gets hard-coded special treatment for backwards
  // compatibility.
  if (dir != catalog_dir)
    VAST_VERBOSE("{} uses {} for catalog data", *self, catalog_dir);
  // Set members.
  self->state.self = self;
  self->state.global_store = std::move(archive);
  self->state.type_registry = std::move(type_registry);
  self->state.accept_queries = true;
  self->state.max_concurrent_partition_lookups
    = max_concurrent_partition_lookups;
  self->state.store_plugin = plugins::find<store_plugin>(store_backend);
  if (!self->state.store_plugin) {
    auto error = caf::make_error(ec::invalid_configuration,
                                 fmt::format("could not find "
                                             "store plugin '{}'",
                                             store_backend));
    VAST_ERROR("{}", render(error));
    self->quit(error);
    return index_actor::behavior_type::make_empty_behavior();
  }
  self->state.accountant = std::move(accountant);
  self->state.filesystem = std::move(filesystem);
  self->state.catalog = std::move(catalog);
  self->state.dir = dir;
  self->state.synopsisdir = catalog_dir;
  self->state.partition_capacity = partition_capacity;
  self->state.active_partition_timeout = active_partition_timeout;
  self->state.taste_partitions = taste_partitions;
  self->state.inmem_partitions.factory().filesystem() = self->state.filesystem;
  self->state.inmem_partitions.resize(max_inmem_partitions);
  // Read persistent state.
  if (auto err = self->state.load_from_disk()) {
    VAST_ERROR("{} failed to load index state from disk: {}", *self,
               render(err));
    self->quit(err);
    return index_actor::behavior_type::make_empty_behavior();
  }
  // Setup stream manager.
  self->state.stage = detail::attach_notifying_stream_stage(
    self,
    /* continuous = */ true,
    [](caf::unit_t&) {
      // nop
    },
    [self](caf::unit_t&, caf::downstream<table_slice>& out, table_slice x) {
      VAST_ASSERT(x.encoding() != table_slice_encoding::none);
      auto&& layout = x.layout();
      // TODO: Consider switching layouts to a robin map to take advantage of
      // transparent key lookup with string views, avoding the copy of the name
      // here.
      self->state.stats.layouts[std::string{layout.name()}].count += x.rows();
      auto& active = self->state.active_partitions[layout];
      if (!active.actor) {
        self->state.create_active_partition(layout);
      } else if (x.rows() > active.capacity) {
        VAST_DEBUG("{} exceeds active capacity by {} rows", *self,
                   x.rows() - active.capacity);
        VAST_VERBOSE("{} flushes active partition {} with {}/{} events", *self,
                     layout, self->state.partition_capacity - active.capacity,
                     self->state.partition_capacity);
        self->state.decomission_active_partition(layout);
        self->state.flush_to_disk();
        self->state.create_active_partition(layout);
      }
      out.push(x);
      if (active.capacity == self->state.partition_capacity
          && x.rows() > active.capacity) {
        VAST_WARN("{} got table slice with {} rows that exceeds the "
                  "default partition capacity of {} rows",
                  *self, x.rows(), self->state.partition_capacity);
        active.capacity = 0;
      } else {
        VAST_ASSERT(active.capacity >= x.rows());
        active.capacity -= x.rows();
      }
    },
    [self](caf::unit_t&, const caf::error& err) {
      // During "normal" shutdown, the node will send an exit message to
      // the importer which then cuts the stream to the index, and the
      // index exits afterwards.
      // We get an 'unreachable' error when the stream becomes unreachable
      // during actor destruction; in this case we can't use `self->state`
      // anymore since it will already be destroyed.
      VAST_DEBUG("index finalized streaming with error {}", render(err));
      if (err && err != caf::exit_reason::unreachable) {
        if (err != caf::exit_reason::user_shutdown)
          VAST_ERROR("{} got a stream error: {}", *self, render(err));
        else
          VAST_DEBUG("{} got a user shutdown error: {}", *self, render(err));
        // We can shutdown now because we only get a single stream from the
        // importer.
        self->send_exit(self, err);
      }
    },
    caf::policy::arg<caf::broadcast_downstream_manager<
      table_slice, vast::type, i_partition_selector>>{});
  self->set_exit_handler([self](const caf::exit_msg& msg) {
    VAST_DEBUG("{} received EXIT from {} with reason: {}", *self, msg.source,
               msg.reason);
    // Flush buffered batches and end stream.
    detail::shutdown_stream_stage(self->state.stage);
    // Bring down active partition.
    for (auto& [layout, partinfo] : self->state.active_partitions) {
      if (partinfo.actor)
        self->state.decomission_active_partition(layout);
    }
    // Collect partitions for termination.
    // TODO: We must actor_cast to caf::actor here because 'shutdown' operates
    // on 'std::vector<caf::actor>' only. That should probably be generalized
    // in the future.
    std::vector<caf::actor> partitions;
    partitions.reserve(self->state.inmem_partitions.size() + 1);
    for ([[maybe_unused]] auto& [_, part] : self->state.unpersisted)
      partitions.push_back(caf::actor_cast<caf::actor>(part));
    for ([[maybe_unused]] auto& [_, part] : self->state.inmem_partitions)
      partitions.push_back(caf::actor_cast<caf::actor>(part));
    self->state.flush_to_disk();
    // Receiving an EXIT message does not need to coincide with the state
    // being destructed, so we explicitly clear the tables to release the
    // references.
    self->state.unpersisted.clear();
    self->state.inmem_partitions.clear();
    // Terminate partition actors.
    VAST_DEBUG("{} brings down {} partitions", *self, partitions.size());
    shutdown<policy::parallel>(self, std::move(partitions));
  });
  // Set up a down handler for monitored exporter actors.
  self->set_down_handler([=](const caf::down_msg& msg) {
    auto it = self->state.monitored_queries.find(msg.source);
    if (it == self->state.monitored_queries.end()) {
      VAST_WARN("{} received DOWN from unexpected sender", *self);
      return;
    }
    const auto& [_, ids] = *it;
    if (!ids.empty()) {
      // Workaround to {fmt} 7 / gcc 10 combo, which errors with "passing views
      // as lvalues is disallowed" when not formating the join view separately.
      const auto ids_string = fmt::to_string(fmt::join(ids, ", "));
      VAST_DEBUG("{} received DOWN for queries [{}] and drops remaining "
                 "query results",
                 *self, ids_string);
      for (const auto& id : ids) {
        if (auto err = self->state.pending_queries.remove_query(id))
          VAST_DEBUG("{} did not remove {} from the query queue. It was "
                     "presumably already removed upon completion ({})",
                     *self, id, err);
      }
    }
    self->state.monitored_queries.erase(it);
  });
  // Start metrics loop.
  if (self->state.accountant)
    self->send(self->state.accountant, atom::announce_v, self->name());
  if (self->state.accountant
      || self->state.active_partition_timeout.count() > 0)
    self->delayed_send(self, defaults::system::telemetry_rate,
                       atom::telemetry_v);
  return {
    [self](atom::done, uuid partition_id) {
      VAST_DEBUG("{} queried partition {} successfully", *self, partition_id);
    },
    [self](
      caf::stream<table_slice> in) -> caf::inbound_stream_slot<table_slice> {
      VAST_DEBUG("{} got a new stream source", *self);
      return self->state.stage->add_inbound_path(in);
    },
    [self](atom::telemetry) {
      self->delayed_send(self, defaults::system::telemetry_rate,
                         atom::telemetry_v);
      if (self->state.accountant)
        self->state.send_report();
      if (self->state.active_partition_timeout.count() > 0) {
        auto decomissioned = std::vector<type>{};
        for (const auto& [layout, active_partition] :
             self->state.active_partitions) {
          if (active_partition.spawn_time + self->state.active_partition_timeout
              < std::chrono::steady_clock::now()) {
            VAST_VERBOSE("{} flushes active partition {} with {}/{} events "
                         "after {} timeout",
                         *self, layout,
                         self->state.partition_capacity
                           - active_partition.capacity,
                         self->state.partition_capacity,
                         data{self->state.active_partition_timeout});
            self->state.decomission_active_partition(layout);
            decomissioned.push_back(layout);
          }
        }
        if (!decomissioned.empty()) {
          for (const auto& layout : decomissioned)
            self->state.active_partitions.erase(layout);
          self->state.flush_to_disk();
        }
      }
    },
    [self](atom::subscribe, atom::flush, flush_listener_actor listener) {
      VAST_DEBUG("{} adds flush listener", *self);
      self->state.add_flush_listener(std::move(listener));
    },
    [self](atom::subscribe, atom::create,
           const partition_creation_listener_actor& listener,
           send_initial_dbstate should_send) {
      VAST_DEBUG("{} adds partition creation listener", *self);
      self->state.add_partition_creation_listener(listener);
      if (should_send == send_initial_dbstate::no)
        return;
      // When we get here, the initial bulk upgrade and any table slices
      // finished since then have already been sent to the catalog, and
      // since CAF guarantees message order within the same inbound queue
      // they will all be part of the response vector.
      self->request(self->state.catalog, caf::infinite, atom::get_v)
        .then(
          [=](std::vector<partition_synopsis_pair>& v) {
            self->send(listener, atom::update_v, std::move(v));
          },
          [](const caf::error& e) {
            VAST_WARN("index failed to get list of partitions from catalog: {}",
                      e);
          });
    },
    [self](atom::evaluate, vast::query query) -> caf::result<query_cursor> {
      // Query handling
      auto sender = self->current_sender();
      // Sanity check.
      if (!sender) {
        VAST_WARN("{} ignores an anonymous query", *self);
        return caf::sec::invalid_argument;
      }
      // Abort if the index is already shutting down.
      if (!self->state.stage->running()) {
        VAST_WARN("{} ignores query {} because it is shutting down", *self,
                  query);
        return ec::remote_node_down;
      }
      // Allows the client to query further results after initial taste.
      VAST_ASSERT(query.id == uuid::nil());
      query.id = self->state.pending_queries.create_query_id();
      // Monitor the sender so we can cancel the query in case it goes down.
      if (const auto it = self->state.monitored_queries.find(sender->address());
          it == self->state.monitored_queries.end()) {
        self->state.monitored_queries.emplace_hint(
          it, sender->address(), std::unordered_set{query.id});
        self->monitor(sender);
      } else {
        auto& [_, ids] = *it;
        ids.emplace(query.id);
      }
      if (!self->state.accept_queries) {
        VAST_VERBOSE("{} delays query {} because it is still starting up",
                     *self, query);
        return caf::skip;
      }
      auto rp = self->make_response_promise<query_cursor>();
      std::vector<uuid> candidates;
      for (const auto& [_, active_partition] : self->state.active_partitions)
        candidates.push_back(active_partition.id);
      for (const auto& [id, _] : self->state.unpersisted)
        candidates.push_back(id);
      self
        ->request(self->state.catalog, caf::infinite, atom::candidates_v, query)
        .then([=, candidates = std::move(candidates)](
                catalog_result& midx_result) mutable {
          auto& midx_candidates = midx_result.partitions;
          VAST_DEBUG("{} got initial candidates {} and from meta-index {}",
                     *self, candidates, midx_candidates);
          candidates.insert(candidates.end(), midx_candidates.begin(),
                            midx_candidates.end());
          std::sort(candidates.begin(), candidates.end());
          candidates.erase(std::unique(candidates.begin(), candidates.end()),
                           candidates.end());
          // Allows the client to query further results after initial taste.
          auto query_id = query.id;
          auto client = caf::actor_cast<receiver_actor<atom::done>>(sender);
          if (candidates.empty()) {
            VAST_DEBUG("{} returns without result: no partitions qualify",
                       *self);
            rp.deliver(query_cursor{query_id, 0u, 0u});
            self->send(client, atom::done_v);
            return;
          }
          auto num_candidates = detail::narrow<uint32_t>(candidates.size());
          auto scheduled
            = std::min(num_candidates, self->state.taste_partitions);
          if (auto err = self->state.pending_queries.insert(
                query_state{.query = query,
                            .client = client,
                            .candidate_partitions = num_candidates,
                            .requested_partitions = scheduled},
                std::move(candidates)))
            rp.deliver(err);
          rp.deliver(query_cursor{query_id, num_candidates, scheduled});
          self->state.schedule_lookups();
        });
      return rp;
    },
    [self](atom::resolve,
           vast::expression& expr) -> caf::result<catalog_result> {
      auto lookup_id = vast::uuid::random();
      return self->delegate(self->state.catalog, atom::candidates_v, lookup_id,
                            std::move(expr));
    },
    [self](const uuid& query_id, uint32_t num_partitions) {
      if (auto err
          = self->state.pending_queries.activate(query_id, num_partitions))
        VAST_WARN("{} can't activate unknown query: {}", *self, err);
      self->state.schedule_lookups();
    },
    [self](atom::erase, uuid partition_id) -> caf::result<atom::done> {
      VAST_VERBOSE("{} erases partition {}", *self, partition_id);
      auto rp = self->make_response_promise<atom::done>();
      auto path = self->state.partition_path(partition_id);
      auto synopsis_path = self->state.partition_synopsis_path(partition_id);
      bool adjust_stats = true;
      if (self->state.persisted_partitions.count(partition_id) == 0u) {
        std::error_code err{};
        const auto file_exists = std::filesystem::exists(path, err);
        if (!file_exists) {
          rp.deliver(caf::make_error(
            ec::logic_error, fmt::format("unknown partition for path {}: {}",
                                         path, err.message())));
          return rp;
        }
        // As a special case, if the partition exists on disk we just continue
        // normally here, since this indicates a previous erasure did not go
        // through cleanly.
        adjust_stats = false;
      }
      self
        ->request<caf::message_priority::high>(
          self->state.catalog, caf::infinite, atom::erase_v, partition_id)
        .then(
          [self, partition_id, path, synopsis_path, rp,
           adjust_stats](atom::ok) mutable {
            VAST_DEBUG("{} erased partition {} from meta-index", *self,
                       partition_id);
            auto partition_actor
              = self->state.inmem_partitions.eject(partition_id);
            self->state.persisted_partitions.erase(partition_id);
            // We don't remove the partition from the queue directly because the
            // query API requires clients to keep track of the number of
            // candidate partitions. Removing the partition from the queue
            // would require us to update the partition counters in the query
            // states and the client would go out of sync. That would require
            // the index to deal with a few complicated corner cases.
            self->state.pending_queries.mark_partition_erased(partition_id);
            self
              ->request<caf::message_priority::high>(
                self->state.filesystem, caf::infinite, atom::mmap_v, path)
              .then(
                [=](const chunk_ptr& chunk) mutable {
                  VAST_DEBUG("{} erased partition {} from filesystem", *self,
                             partition_id);
                  if (!chunk) {
                    rp.deliver(caf::make_error( //
                      ec::filesystem_error,
                      fmt::format("failed to load the state for partition {}",
                                  path)));
                    return;
                  }
                  // Adjust layout stats by subtracting the events of the
                  // removed partition.
                  const auto* partition = fbs::GetPartition(chunk->data());
                  if (partition->partition_type()
                      != fbs::partition::Partition::legacy) {
                    rp.deliver(caf::make_error(ec::format_error, "unexpected "
                                                                 "format "
                                                                 "version"));
                    return;
                  }
                  vast::ids all_ids;
                  const auto* partition_legacy
                    = partition->partition_as_legacy();
                  for (const auto* partition_stats :
                       *partition_legacy->type_ids()) {
                    const auto* name = partition_stats->name();
                    vast::ids ids;
                    if (auto error
                        = fbs::deserialize_bytes(partition_stats->ids(), ids)) {
                      rp.deliver(caf::make_error(ec::format_error,
                                                 "could not deserialize "
                                                 "ids: "
                                                   + render(error)));
                      return;
                    }
                    all_ids |= ids;
                    if (adjust_stats)
                      self->state.stats.layouts[name->str()].count -= rank(ids);
                  }
                  // Note that mmap's will increase the reference count of a
                  // file, so unlinking should not affect indexers that are
                  // currently loaded and answering a query.
                  self
                    ->request<caf::message_priority::high>(
                      self->state.filesystem, caf::infinite, atom::erase_v,
                      synopsis_path)
                    .then(
                      [self, partition_id](atom::done) {
                        VAST_DEBUG("{} erased partition synopsis {} from "
                                   "filesystem",
                                   *self, partition_id);
                      },
                      [self, partition_id,
                       synopsis_path](const caf::error& err) {
                        VAST_WARN("{} failed to erase partition "
                                  "synopsis {} at {}: {}",
                                  *self, partition_id, synopsis_path, err);
                      });
                  // TODO: We could send `all_ids` as the second argument
                  // here, which doesn't really make sense from an interface
                  // perspective but would save the partition from recomputing
                  // the same bitmap.
                  rp.delegate(partition_actor, atom::erase_v);
                },
                [=](caf::error& err) mutable {
                  VAST_WARN("{} failed to erase partition {} from filesystem: "
                            "{}",
                            *self, partition_id, err);
                  rp.deliver(std::move(err));
                });
          },
          [self, partition_id, rp](caf::error& err) mutable {
            VAST_WARN("{} failed to erase partition {} from meta-index: {}",
                      *self, partition_id, err);
            rp.deliver(std::move(err));
          });
      return rp;
    },
    // We can't pass this as spawn argument since the importer already
    // needs to know the index actor when spawning.
    [self](atom::importer, idspace_distributor_actor idspace_distributor) {
      self->state.importer = std::move(idspace_distributor);
    },
    [self](atom::apply, transform_ptr transform,
           std::vector<vast::uuid> old_partition_ids,
           keep_original_partition keep) -> caf::result<partition_info> {
      VAST_DEBUG("{} applies a transform to partitions {}", *self,
                 old_partition_ids);
      if (!self->state.store_plugin)
        return caf::make_error(ec::invalid_configuration,
                               "partition transforms are not supported for the "
                               "global archive");
      if (old_partition_ids.empty())
        return caf::make_error(ec::invalid_argument, "no partitions given");
      std::erase_if(old_partition_ids, [&](uuid old_partition_id) {
        if (self->state.persisted_partitions.contains(old_partition_id)) {
          return false;
        }
        VAST_WARN("{} skips unknown partition {} for transform {}", *self,
                  old_partition_id, transform->name());
        return true;
      });
      if (old_partition_ids.empty())
        return caf::make_error(ec::invalid_argument, "no known partitions "
                                                     "given");
      auto new_partition_id = vast::uuid::random();
      auto store_id = std::string{self->state.store_plugin->name()};
      partition_transformer_actor sink = self->spawn(
        partition_transformer, new_partition_id, store_id,
        self->state.synopsis_opts, self->state.index_opts,
        self->state.accountant,
        static_cast<idspace_distributor_actor>(self->state.importer),
        self->state.type_registry, self->state.filesystem, transform);
      // match_everything == '"" in #type'
      static const auto match_everything = vast::predicate{
        selector{selector::type}, relational_operator::ni, data{""}};
      auto query = query::make_extract(sink, match_everything);
      query.id = self->state.pending_queries.create_query_id();
      query.priority = 100;
      VAST_DEBUG("{} emplaces {} for transform {}", *self, query,
                 transform->name());
      auto input_size = detail::narrow_cast<uint32_t>(old_partition_ids.size());
      auto err = self->state.pending_queries.insert(
        query_state{.query = query,
                    .client = caf::actor_cast<receiver_actor<atom::done>>(sink),
                    .candidate_partitions = input_size,
                    .requested_partitions = input_size},
        std::vector{old_partition_ids});
      VAST_ASSERT(err == caf::none);
      self->state.schedule_lookups();
      auto rp = self->make_response_promise<partition_info>();
      // TODO: Implement some kind of monadic composition instead of these
      // nested requests.
      self
        ->request(sink, caf::infinite, atom::persist_v,
                  self->state.partition_path(new_partition_id),
                  self->state.partition_synopsis_path(new_partition_id))
        .then(
          [self, rp, old_partition_ids, new_partition_id,
           keep](augmented_partition_synopsis& aps) mutable {
            // If the partition was completely deleted, `synopsis` may be null.
            auto events = aps.synopsis ? aps.synopsis->events : 0ull;
            auto time = aps.synopsis ? aps.synopsis->max_import_time
                                     : vast::time::clock::time_point{};
            auto result = partition_info{
              .uuid = aps.uuid,
              .events = events,
              .max_import_time = time,
              .stats = std::move(aps.stats),
            };
            // Update the index statistics. We only need to add the events of
            // the new partition here, the subtraction of the old events is
            // done in `erase`.
            for (const auto& [name, stats] : result.stats.layouts)
              self->state.stats.layouts[name].count += stats.count;
            if (keep == keep_original_partition::yes) {
              if (aps.synopsis)
                self
                  ->request(self->state.catalog, caf::infinite, atom::merge_v,
                            new_partition_id, aps.synopsis)
                  .then(
                    [self, rp, new_partition_id, result](atom::ok) mutable {
                      self->state.persisted_partitions.insert(new_partition_id);
                      rp.deliver(result);
                    },
                    [rp](const caf::error& e) mutable {
                      rp.deliver(e);
                    });
              else
                rp.deliver(result);
            } else {
              // Pick one partition id at random to be "transformed", all the
              // other ones are "deleted" from the catalog. If the new
              // partition is empty, all partitions are deleted.
              if (aps.synopsis) {
                VAST_ASSERT(!old_partition_ids.empty());
                auto old_partition_id = old_partition_ids.back();
                old_partition_ids.pop_back();
                self
                  ->request(self->state.catalog, caf::infinite, atom::replace_v,
                            old_partition_id, new_partition_id, aps.synopsis)
                  .then(
                    [self, rp, old_partition_id, new_partition_id,
                     result](atom::ok) mutable {
                      self->state.persisted_partitions.insert(new_partition_id);
                      self
                        ->request(static_cast<index_actor>(self), caf::infinite,
                                  atom::erase_v, old_partition_id)
                        .then(
                          [=](atom::done) mutable {
                            rp.deliver(result);
                          },
                          [=](const caf::error& e) mutable {
                            rp.deliver(e);
                          });
                    },
                    [rp](const caf::error& e) mutable {
                      rp.deliver(e);
                    });
              } else {
                rp.deliver(result);
              }
              for (auto partition_id : old_partition_ids) {
                self
                  ->request(static_cast<index_actor>(self), caf::infinite,
                            atom::erase_v, partition_id)
                  .then([](atom::done) { /* nop */ },
                        [](const caf::error& e) {
                          VAST_WARN("index failed to erase {} from catalog", e);
                        });
              }
            }
          },
          [rp](const caf::error& e) mutable {
            rp.deliver(e);
          });
      return rp;
    },
    // -- status_client_actor --------------------------------------------------
    [self](atom::status, status_verbosity v) { //
      return self->state.status(v);
    },
  };
}

} // namespace vast::system
