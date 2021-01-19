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

#include "vast/system/index.hpp"

#include "vast/chunk.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/table_slice.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/cache.hpp"
#include "vast/detail/fill_status_map.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/detail/notifying_stream_manager.hpp"
#include "vast/error.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/fbs/index.hpp"
#include "vast/fbs/partition.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/fbs/uuid.hpp"
#include "vast/fwd.hpp"
#include "vast/ids.hpp"
#include "vast/io/read.hpp"
#include "vast/io/save.hpp"
#include "vast/json.hpp"
#include "vast/logger.hpp"
#include "vast/status.hpp"
#include "vast/system/accountant.hpp"
#include "vast/system/evaluator.hpp"
#include "vast/system/filesystem.hpp"
#include "vast/system/partition.hpp"
#include "vast/system/query_supervisor.hpp"
#include "vast/system/shutdown.hpp"
#include "vast/table_slice.hpp"
#include "vast/value_index.hpp"

#include <caf/make_counted.hpp>
#include <caf/stateful_actor.hpp>

#include <flatbuffers/flatbuffers.h>

#include <algorithm>
#include <chrono>
#include <ctime>
#include <deque>
#include <memory>
#include <unistd.h>
#include <unordered_set>

#include "caf/error.hpp"
#include "caf/response_promise.hpp"
#include "caf/scheduled_actor.hpp"

using namespace std::chrono;

// clang-format off
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
// At the same time, the index is also involved in the lookup path, where it
// receives an expression and loads the partitions that might contain relevant
// results into memory.
//
//                     expression                atom::evaluate
// query_supervisor    ------------>  index     ----------------->   partition
//                                                                      |
//                                                  [indexer]           |
//                                  (spawns     <-----------------------/     
//                                   evaluators) 
//
//                                                  curried_predicate
//                                   evaluator  -------------------------------> indexer
//
//                                                      ids
//                     <--------------------------------------------------------
// clang-format on

namespace vast::system {

vast::path index_state::partition_path(const uuid& id) const {
  return dir / to_string(id);
}

caf::actor partition_factory::operator()(const uuid& id) const {
  // Load partition from disk.
  VAST_ASSERT(std::find(state_.persisted_partitions.begin(),
                        state_.persisted_partitions.end(), id)
              != state_.persisted_partitions.end());
  auto path = state_.partition_path(id);
  VAST_DEBUG(state_.self, "loads partition", id, "for path", path);
  return state_.self->spawn(passive_partition, id, fs_, path);
}

filesystem_type& partition_factory::fs() {
  return fs_;
}

partition_factory::partition_factory(index_state& state) : state_{state} {
  // nop
}

index_state::index_state(caf::stateful_actor<index_state>* self)
  : self{self}, inmem_partitions{0, partition_factory{*this}} {
}

caf::error index_state::load_from_disk() {
  // We dont use the filesystem actor here because this function is only
  // called once during startup, when no other actors exist yet.
  if (!exists(dir)) {
    VAST_VERBOSE(self, "found no prior state, starting with a clean slate");
    return caf::none;
  }
  if (auto fname = index_filename(); exists(fname)) {
    VAST_VERBOSE(self, "loads state from", fname);
    auto buffer = io::read(fname);
    if (!buffer) {
      VAST_ERROR(self, "failed to read index file:", render(buffer.error()));
      return buffer.error();
    }
    // TODO: Create a `index_ondisk_state` struct and move this part of the
    // code into an `unpack()` function.
    auto index = fbs::GetIndex(buffer->data());
    if (index->index_type() != fbs::index::Index::v0)
      return make_error(ec::format_error, "invalid index version");
    auto index_v0 = index->index_as_v0();
    auto partition_uuids = index_v0->partitions();
    VAST_ASSERT(partition_uuids);
    for (auto uuid_fb : *partition_uuids) {
      VAST_ASSERT(uuid_fb);
      vast::uuid partition_uuid;
      unpack(*uuid_fb, partition_uuid);
      auto partition_path = dir / to_string(partition_uuid);
      if (exists(partition_path)) {
        persisted_partitions.insert(partition_uuid);
        // Use blocking operations here since this is part of the startup.
        auto chunk = chunk::mmap(partition_path);
        if (!chunk) {
          VAST_WARNING(self, "could not mmap partition at", partition_path);
          continue;
        }
        auto partition = fbs::GetPartition(chunk->data());
        if (partition->partition_type() != fbs::partition::Partition::v0) {
          VAST_WARNING(self, "found unsupported version for partition",
                       partition_uuid);
          continue;
        }
        auto partition_v0 = partition->partition_as_v0();
        VAST_ASSERT(partition_v0);
        partition_synopsis ps;
        unpack(*partition_v0, ps);
        VAST_DEBUG(self, "merging partition synopsis from", partition_uuid);
        meta_idx.merge(partition_uuid, std::move(ps));
      } else {
        VAST_WARNING(self, "found partition", partition_uuid,
                     "in the index state but not on disk; this may have been "
                     "caused by an unclean shutdown");
      }
    }
    auto stats = index_v0->stats();
    if (!stats)
      return make_error(ec::format_error, "no stats in persisted index state");
    for (const auto stat : *stats) {
      this->stats.layouts[stat->name()->str()]
        = layout_statistics{stat->count()};
    }
  } else {
    VAST_WARNING(self, "found existing database dir", dir,
                 "without index statefile, will start with fresh state");
  }
  return caf::none;
}

bool index_state::worker_available() {
  return !idle_workers.empty();
}

caf::actor index_state::next_worker() {
  VAST_ASSERT(worker_available());
  auto result = std::move(idle_workers.back());
  idle_workers.pop_back();
  // If no more workers are available, revert to the default behavior.
  if (!worker_available()) {
    self->unbecome();
    self->set_default_handler(caf::skip);
    VAST_VERBOSE(self, "waits for query supervisors to become available to "
                       "delegate work; consider increasing 'vast.max-queries'");
  }
  return result;
}

void index_state::add_flush_listener(caf::actor listener) {
  VAST_DEBUG(self, "adds a new 'flush' subscriber:", listener);
  flush_listeners.emplace_back(std::move(listener));
  detail::notify_listeners_if_clean(*this, *stage);
}

void index_state::notify_flush_listeners() {
  VAST_DEBUG(self, "sends 'flush' messages to", flush_listeners.size(),
             "listeners");
  for (auto& listener : flush_listeners)
    self->send(listener, atom::flush_v);
  flush_listeners.clear();
}

caf::dictionary<caf::config_value>
index_state::status(status_verbosity v) const {
  using caf::put;
  using caf::put_dictionary;
  using caf::put_list;
  auto result = caf::settings{};
  auto& index_status = put_dictionary(result, "index");
  if (v >= status_verbosity::info) {
    // nop
  }
  if (v >= status_verbosity::detailed) {
    auto& stats_object = put_dictionary(index_status, "statistics");
    auto& layout_object = put_dictionary(stats_object, "layouts");
    for (auto& [name, layout_stats] : stats.layouts) {
      auto xs = caf::dictionary<caf::config_value>{};
      xs.emplace("count", layout_stats.count);
      // We cannot use put_dictionary(layout_object, name) here, because this
      // function splits the key at '.', which occurs in every layout name.
      // Hence the fallback to low-level primitives.
      layout_object.insert_or_assign(name, std::move(xs));
    }
    put(stats_object, "meta-index-bytes", meta_idx.size_bytes());
  }
  if (v >= status_verbosity::debug) {
    // Resident partitions.
    auto& partitions = put_dictionary(index_status, "partitions");
    if (active_partition.actor != nullptr)
      partitions.emplace("active", to_string(active_partition.id));
    auto& cached = put_list(partitions, "cached");
    for (auto& kv : inmem_partitions)
      cached.emplace_back(to_string(kv.first));
    auto& unpersisted = put_list(partitions, "unpersisted");
    for (auto& kvp : this->unpersisted)
      unpersisted.emplace_back(to_string(kvp.first));
    // General state such as open streams.
    detail::fill_status_map(index_status, self);
  }
  return result;
}

std::vector<std::pair<uuid, caf::actor>>
index_state::collect_query_actors(query_state& lookup,
                                  uint32_t num_partitions) {
  VAST_TRACE(VAST_ARG(lookup), VAST_ARG(num_partitions));
  std::vector<std::pair<uuid, caf::actor>> result;
  if (num_partitions == 0 || lookup.partitions.empty())
    return result;
  // Prefer partitions that are already available in RAM.
  auto partition_is_loaded = [&](const uuid& candidate) {
    return (active_partition.actor != nullptr
            && active_partition.id == candidate)
           || unpersisted.count(candidate)
           || inmem_partitions.contains(candidate);
  };
  std::partition(lookup.partitions.begin(), lookup.partitions.end(),
                 partition_is_loaded);
  // Helper function to spin up EVALUATOR actors for a single partition.
  auto spin_up = [&](const uuid& partition_id) -> caf::actor {
    // We need to first check whether the ID is the active partition or one
    // of our unpersisted ones. Only then can we dispatch to our LRU cache.
    caf::actor part;
    if (active_partition.actor != nullptr
        && active_partition.id == partition_id)
      part = active_partition.actor;
    else if (auto it = unpersisted.find(partition_id); it != unpersisted.end())
      part = it->second;
    else if (auto it = persisted_partitions.find(partition_id);
             it != persisted_partitions.end())
      part = inmem_partitions.get_or_load(partition_id);
    if (!part)
      VAST_ERROR(self, "could not load partition", partition_id,
                 "that was part of a query");
    return part;
  };
  // Loop over the candidate set until we either successfully scheduled
  // num_partitions partitions or run out of candidates.
  auto it = lookup.partitions.begin();
  auto last = lookup.partitions.end();
  while (it != last && result.size() < num_partitions) {
    auto partition_id = *it++;
    if (auto partition_actor = spin_up(partition_id))
      result.push_back(std::make_pair(partition_id, partition_actor));
  }
  lookup.partitions.erase(lookup.partitions.begin(), it);
  VAST_DEBUG(self, "launched", result.size(),
             "await handlers to fill the pending query map");
  return result;
}

/// Sends an `evaluate` atom to all partition actors passed into this function,
/// and collects the resulting
/// @param c Continuation that takes a single argument of type
/// `caf::expected<pending_query_map>`. The continuation will be called in the
/// context of `self`.
//
// TODO: At some point we should add some more template magic on top of
// this and turn it into a generic functions that maps
//
//   (map from U to A, request param pack R, result handler with param X) ->
//   expected<map from U to X>
template <typename Continuation>
void await_evaluation_maps(
  caf::stateful_actor<index_state>* self, const expression& expr,
  const std::vector<std::pair<vast::uuid, caf::actor>>& actors,
  Continuation then) {
  struct counter {
    size_t received;
    pending_query_map pqm;
  };
  auto expected = actors.size();
  auto shared_counter = std::make_shared<counter>();
  for (auto& [id, actor] : actors) {
    auto& partition_id = id; // Can't use structured binding inside lambda.
    self->request(actor, caf::infinite, expr)
      .then(
        [=](evaluation_triples triples) {
          auto received = ++shared_counter->received;
          if (!triples.empty()) {
            shared_counter->pqm.emplace(partition_id, std::move(triples));
          } else {
            VAST_DEBUG(self, "received no evaluation triples from",
                       self->current_sender());
          }
          if (received == expected)
            then(std::move(shared_counter->pqm));
        },
        [=](const caf::error& err) {
          auto received = ++shared_counter->received;
          // TODO: Add a way to signal to the caller that he is only getting
          // partial results because some of the partitions error'ed out.
          VAST_ERROR(self, "failed to get evaluation triples from partition",
                     partition_id, "with error:", render(err));
          if (received == expected)
            then(std::move(shared_counter->pqm));
        });
  }
}

query_map
index_state::launch_evaluators(pending_query_map& pqm, expression expr) {
  query_map result;
  for (auto& [id, eval] : pqm) {
    std::vector<caf::actor> xs{self->spawn(evaluator, expr, std::move(eval))};
    result.emplace(id, std::move(xs));
  }
  pqm.clear();
  return result;
}

path index_state::index_filename(path basename) const {
  return basename / dir / "index.bin";
}

caf::expected<flatbuffers::Offset<fbs::Index>>
pack(flatbuffers::FlatBufferBuilder& builder, const index_state& state) {
  VAST_DEBUG(state.self, "persists", state.persisted_partitions.size(),
             "uuids of definitely persisted and", state.unpersisted.size(),
             "uuids of maybe persisted partitions");
  std::vector<flatbuffers::Offset<fbs::uuid::v0>> partition_offsets;
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
  for (auto& kv : state.unpersisted) {
    if (auto uuid_fb = pack(builder, kv.first))
      partition_offsets.push_back(*uuid_fb);
    else
      return uuid_fb.error();
  }
  auto partitions = builder.CreateVector(partition_offsets);
  std::vector<flatbuffers::Offset<fbs::layout_statistics::v0>> stats_offsets;
  for (auto& [name, layout_stats] : state.stats.layouts) {
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

/// Persists the state to disk.
void index_state::flush_to_disk() {
  auto builder = flatbuffers::FlatBufferBuilder{};
  auto index = pack(builder, *this);
  if (!index) {
    VAST_WARNING(self, "failed to pack index:", render(index.error()));
    return;
  }
  auto chunk = fbs::release(builder);
  self
    ->request(caf::actor_cast<caf::actor>(filesystem), caf::infinite,
              atom::write_v, index_filename(), chunk)
    .then(
      [=](atom::ok) { VAST_DEBUG(self, "successfully persisted index state"); },
      [=](const caf::error& err) {
        VAST_WARNING(self, "failed to persist index state:", render(err));
      });
}

caf::behavior
index(caf::stateful_actor<index_state>* self, filesystem_type fs, path dir,
      size_t partition_capacity, size_t max_inmem_partitions,
      size_t taste_partitions, size_t num_workers) {
  VAST_TRACE(VAST_ARG(fs), VAST_ARG(dir), VAST_ARG(partition_capacity),
             VAST_ARG(max_inmem_partitions), VAST_ARG(taste_partitions),
             VAST_ARG(num_workers));
  VAST_VERBOSE(self, "initializes index in", dir,
               "with a maximum partition size of", partition_capacity,
               "events and", max_inmem_partitions, "resident partitions");
  // Set members.
  self->state.self = self;
  self->state.filesystem = fs;
  self->state.dir = dir;
  self->state.partition_capacity = partition_capacity;
  self->state.taste_partitions = taste_partitions;
  self->state.inmem_partitions.factory().fs() = fs;
  self->state.inmem_partitions.resize(max_inmem_partitions);
  // Read persistent state.
  if (auto err = self->state.load_from_disk()) {
    VAST_ERROR(self, "failed to load index state from disk:", render(err));
    self->quit(err);
    return {};
  }
  // This option must be kept in sync with vast/address_synopsis.hpp.
  put(self->state.meta_idx.factory_options(), "max-partition-size",
      partition_capacity);
  // Creates a new active partition and updates index state.
  auto create_active_partition = [=] {
    auto id = uuid::random();
    caf::settings index_opts;
    index_opts["cardinality"] = partition_capacity;
    auto part = self->spawn(active_partition, id, self->state.filesystem,
                            index_opts, self->state.meta_idx.factory_options());
    auto slot = self->state.stage->add_outbound_path(part);
    self->state.active_partition.actor = part;
    self->state.active_partition.stream_slot = slot;
    self->state.active_partition.capacity = partition_capacity;
    self->state.active_partition.id = id;
    VAST_DEBUG(self, "created new partition", id);
  };
  auto decomission_active_partition = [=] {
    auto& active = self->state.active_partition;
    auto id = active.id;
    caf::actor actor = nullptr;
    std::swap(actor, active.actor);
    self->state.unpersisted[id] = actor;
    // Send buffered batches.
    self->state.stage->out().fan_out_flush();
    self->state.stage->out().force_emit_batches();
    // Remove active partition from the stream.
    self->state.stage->out().close(active.stream_slot);
    // Persist active partition asynchronously.
    auto part_dir = dir / to_string(id);
    VAST_DEBUG(self, "persists active partition to", part_dir);
    self
      ->request(actor, caf::infinite, atom::persist_v, part_dir,
                caf::actor_cast<caf::actor>(self))
      .then(
        [=](atom::ok) {
          VAST_DEBUG(self, "successfully persisted partition", id);
          self->state.unpersisted.erase(id);
          self->state.persisted_partitions.insert(id);
        },
        [=](const caf::error& err) {
          VAST_ERROR(self, "failed to persist partition", id,
                     "with error:", render(err));
          self->quit(err);
        });
  };
  // Setup stream manager.
  self->state.stage = detail::attach_notifying_stream_stage(
    self,
    /* continuous = */ true, [=](caf::unit_t&) {},
    [=](caf::unit_t&, caf::downstream<table_slice>& out, table_slice x) {
      VAST_ASSERT(x.encoding() != table_slice::encoding::none);
      auto&& layout = x.layout();
      self->state.stats.layouts[layout.name()].count += x.rows();
      auto& active = self->state.active_partition;
      if (!active.actor) {
        create_active_partition();
      } else if (x.rows() > active.capacity) {
        VAST_DEBUG(self, "exceeds active capacity by",
                   (x.rows() - active.capacity), "rows");
        decomission_active_partition();
        self->state.flush_to_disk();
        create_active_partition();
      }
      out.push(x);
      self->state.meta_idx.add(active.id, x);
      if (active.capacity == self->state.partition_capacity
          && x.rows() > active.capacity) {
        VAST_WARNING(self, "got table slice with", x.rows(),
                     "rows that exceeds the default partition capacity of",
                     self->state.partition_capacity, "rows");
        active.capacity = 0;
      } else {
        VAST_ASSERT(active.capacity >= x.rows());
        active.capacity -= x.rows();
        VAST_DEBUG(self, "reduces active partition capacity to",
                   (std::to_string(active.capacity) + '/'
                    + std::to_string(self->state.partition_capacity)),
                   "rows");
      }
    },
    [=](caf::unit_t&, const caf::error& err) {
      // We get an 'unreachable' error when the stream becomes unreachable
      // because the actor was destroyed; in this case we can't use `self`
      // anymore.
      if (err && err != caf::exit_reason::unreachable) {
        if (err != caf::exit_reason::user_shutdown)
          VAST_ERROR(self, "got a stream error:", render(err));
        else
          VAST_DEBUG(self, "got a user shutdown error:", render(err));
        // We can shutdown now because we only get a single stream from the
        // importer.
        self->send_exit(self, err);
      }
      VAST_DEBUG_ANON("index finalized streaming");
    });
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    VAST_DEBUG(self, "received EXIT from", msg.source,
               "with reason:", msg.reason);
    // Flush buffered batches and end stream.
    self->state.stage->out().fan_out_flush();
    self->state.stage->out().force_emit_batches();
    self->state.stage->out().close();
    self->state.stage->shutdown();
    // Bring down active partition.
    if (self->state.active_partition.actor)
      decomission_active_partition();
    // Collect partitions for termination.
    std::vector<caf::actor> partitions;
    partitions.reserve(self->state.inmem_partitions.size() + 1);
    for ([[maybe_unused]] auto& [_, part] : self->state.unpersisted)
      partitions.push_back(part);
    for ([[maybe_unused]] auto& [_, part] : self->state.inmem_partitions)
      partitions.push_back(part);
    self->state.flush_to_disk();
    // Receiving an EXIT message does not need to coincide with the state being
    // destructed, so we explicitly clear the tables to release the references.
    self->state.unpersisted.clear();
    self->state.inmem_partitions.clear();
    // Terminate partition actors.
    VAST_DEBUG(self, "brings down", partitions.size(), "partitions");
    shutdown<policy::parallel>(self, std::move(partitions));
  });
  // Launch workers for resolving queries.
  for (size_t i = 0; i < num_workers; ++i)
    self->spawn(query_supervisor, self);
  // We switch between has_worker behavior and the default behavior (which
  // simply waits for a worker).
  self->set_default_handler(caf::skip);
  self->state.has_worker.assign(
    [=](caf::stream<table_slice> in) {
      VAST_DEBUG(self, "got a new table slice stream");
      return self->state.stage->add_inbound_path(in);
    },
    // The partition delegates the actual writing to the filesystem actor,
    // so we dont really get more information than a binary ok/not-ok here.
    [=](caf::result<atom::ok> write_result) {
      if (write_result.err)
        VAST_ERROR(self,
                   "could not persist partition:", render(write_result.err));
      else
        VAST_DEBUG(self, "successfully persisted partition");
    },
    // Query handling
    [=](vast::expression expr) {
      auto& st = self->state;
      auto mid = self->current_message_id();
      auto sender = self->current_sender();
      auto client = caf::actor_cast<caf::actor>(sender);
      // TODO: As far as I can tell, this is used in order to "respond" to
      // the message and to still continue with the function afterwards.
      // At some point this should be changed to a proper solution for that
      // problem.
      auto respond = [=](auto&&... xs) {
        unsafe_response(self, sender, {}, mid.response_id(),
                        std::forward<decltype(xs)>(xs)...);
      };
      // Convenience function for dropping out without producing hits.
      // Makes sure that clients always receive a 'done' message.
      auto no_result = [=] {
        respond(uuid::nil(), uint32_t{0}, uint32_t{0});
        self->send(client, atom::done_v);
      };
      // Sanity check.
      if (!sender) {
        VAST_WARNING(self, "ignores an anonymous query");
        respond(caf::sec::invalid_argument);
        return;
      }
      // Get all potentially matching partitions.
      auto candidates = st.meta_idx.lookup(expr);
      if (candidates.empty()) {
        VAST_DEBUG(self, "returns without result: no partitions qualify");
        no_result();
        return;
      }
      // Allows the client to query further results after initial taste.
      auto query_id = uuid::random();
      // Ensure the query id is unique.
      while (st.pending.find(query_id) != st.pending.end()
             || query_id == uuid::nil())
        query_id = uuid::random();
      auto total = candidates.size();
      auto scheduled = detail::narrow<uint32_t>(
        std::min(candidates.size(), st.taste_partitions));
      auto lookup = query_state{query_id, expr, std::move(candidates)};
      auto result = st.pending.emplace(query_id, std::move(lookup));
      VAST_ASSERT(result.second);
      // NOTE: The previous version of the index used to do much more
      // validation before assigning a query id; in particular it did
      // evaluate the entries of the pending query map and checked that
      // at least one of them actually produced an evaluation triple.
      // However, the query_processor doesnt really care about the id
      // anyways, so hopefully that shouldnt make too big of a difference.
      respond(query_id, detail::narrow<uint32_t>(total), scheduled);
      self->delegate(caf::actor_cast<caf::actor>(self), query_id, scheduled);
      return;
    },
    [=](const uuid& query_id, uint32_t num_partitions) {
      auto& st = self->state;
      auto mid = self->current_message_id();
      auto sender = self->current_sender();
      auto client = caf::actor_cast<caf::actor>(sender);
      auto respond = [=](auto&&... xs) {
        unsafe_response(self, sender, {}, mid.response_id(),
                        std::forward<decltype(xs)>(xs)...);
      };
      // Sanity checks.
      if (!sender) {
        VAST_ERROR(self, "ignores an anonymous query");
        return;
      }
      // A zero as second argument means the client drops further results.
      if (num_partitions == 0) {
        VAST_DEBUG(self, "drops remaining results for query id", query_id);
        st.pending.erase(query_id);
        return;
      }
      auto iter = st.pending.find(query_id);
      if (iter == st.pending.end()) {
        VAST_WARNING(self, "drops query for unknown query id", query_id);
        self->send(client, atom::done_v);
        return;
      }
      // Get partition actors, spawning new ones if needed.
      auto actors = st.collect_query_actors(iter->second, num_partitions);
      // Send an evaluate atom to all the actors and collect the returned
      // evaluation triples in a `pending_query_map`, then run the continuation
      // below in the same actor context.
      auto worker = st.next_worker();
      await_evaluation_maps(
        self, iter->second.expression, actors,
        [=](caf::expected<pending_query_map> maybe_pqm) {
          auto& st = self->state;
          auto iter = st.pending.find(query_id);
          if (iter == st.pending.end()) {
            VAST_WARNING(self, "ignores continuation for unknown query id",
                         query_id);
            self->send(client, atom::done_v);
            return;
          }
          auto& query_state = iter->second;
          if (!maybe_pqm) {
            VAST_ERROR(self, "failed to collect pending query map:",
                       render(maybe_pqm.error()));
            self->send(client, atom::done_v);
            return;
          }
          auto& pqm = *maybe_pqm;
          if (pqm.empty()) {
            VAST_DEBUG(self, "returns without result: no partitions qualify");
            if (query_state.partitions.empty())
              st.pending.erase(iter);
            self->send(client, atom::done_v);
            return;
          }
          auto qm = st.launch_evaluators(pqm, query_state.expression);
          // Delegate to query supervisor (uses up this worker) and report
          // query ID + some stats to the client.
          VAST_DEBUG(self, "schedules", qm.size(),
                     "more partition(s) for query id", query_id, "with",
                     query_state.partitions.size(), "partitions remaining");
          self->send(worker, query_state.expression, std::move(qm), client);
          // Cleanup if we exhausted all candidates.
          if (query_state.partitions.empty())
            st.pending.erase(iter);
        });
    },
    [=](atom::worker, caf::actor& worker) {
      self->state.idle_workers.emplace_back(std::move(worker));
    },
    [=](atom::done, uuid partition_id) {
      // Nothing to do.
      VAST_DEBUG(self, "queried partition", partition_id, "successfully");
    },
    [=](caf::stream<table_slice> in) {
      VAST_DEBUG(self, "got a new source");
      return self->state.stage->add_inbound_path(in);
    },
    [=](accountant_type accountant) {
      self->state.accountant = std::move(accountant);
    },
    [=](atom::status, status_verbosity v) -> caf::config_value::dictionary {
      return self->state.status(v);
    },
    [=](atom::subscribe, atom::flush, const caf::actor& listener) {
      self->state.add_flush_listener(listener);
    },
    // The idea is that its safe to move from a `shared_ptr&` here since
    // the unique owner of the pointer will be the message (which doesnt
    // need it anymore).
    // Semantically we want a unique_ptr here, but caf message types need
    // to be copy constructible.
    [=](atom::replace, uuid partition_id,
        std::shared_ptr<partition_synopsis>& ps) {
      VAST_DEBUG(self, "replaces synopsis for partition", partition_id);
      if (!ps.unique()) {
        VAST_WARNING(self, "ignores partition synopses thats still in use");
        return;
      }
      auto pu = std::make_unique<partition_synopsis>();
      std::swap(*ps, *pu);
      self->state.meta_idx.replace(partition_id, std::move(pu));
    },
    [=](atom::erase, uuid partition_id) {
      VAST_VERBOSE(self, "erases partition", partition_id);
      caf::response_promise rp = self->make_response_promise();
      auto path = self->state.partition_path(partition_id);
      bool adjust_stats = true;
      if (!self->state.persisted_partitions.count(partition_id)) {
        if (!exists(path)) {
          rp.deliver(make_error(ec::logic_error, "unknown partition"));
          return;
        }
        // As a special case, if the partition exists on disk we just continue
        // normally here, since this indicates a previous erasure did not go
        // through cleanly.
        adjust_stats = false;
      }
      self->state.inmem_partitions.drop(partition_id);
      self->state.persisted_partitions.erase(partition_id);
      self->request(self->state.filesystem, caf::infinite, atom::mmap_v, path)
        .then(
          [=](chunk_ptr chunk) mutable {
            // Adjust layout stats by subtracting the events of the removed
            // partition.
            auto partition = fbs::GetPartition(chunk->data());
            if (partition->partition_type() != fbs::partition::Partition::v0) {
              rp.deliver(make_error(ec::format_error, "unexpected format "
                                                      "version"));
              return;
            }
            vast::ids all_ids;
            auto partition_v0 = partition->partition_as_v0();
            for (auto partition_stats : *partition_v0->type_ids()) {
              auto name = partition_stats->name();
              vast::ids ids;
              if (auto error
                  = fbs::deserialize_bytes(partition_stats->ids(), ids)) {
                rp.deliver(make_error(ec::format_error, "could not deserialize "
                                                        "ids: "
                                                          + render(error)));
                return;
              }
              all_ids |= ids;
              if (adjust_stats)
                self->state.stats.layouts[name->str()].count -= rank(ids);
            }
            // Note that mmap's will increase the reference count of a file, so
            // unlinking should not affect indexers that are currently loaded
            // and answering a query.
            if (!rm(path))
              VAST_WARNING(self, "could not unlink partition at", path);
            rp.deliver(std::move(all_ids));
          },
          [=](caf::error e) mutable { rp.deliver(e); });
    });
  return {
    // The default behaviour
    [=](atom::worker, caf::actor& worker) {
      auto& st = self->state;
      st.idle_workers.emplace_back(std::move(worker));
      self->become(caf::keep_behavior, st.has_worker);
      self->set_default_handler(caf::print_and_drop);
      VAST_VERBOSE(self, "delegates work to query supervisors");
    },
    [=](atom::done, uuid partition_id) {
      VAST_DEBUG(self, "queried partition", partition_id, "successfully");
    },
    [=](caf::stream<table_slice> in) {
      VAST_DEBUG(self, "got a new source");
      return self->state.stage->add_inbound_path(in);
    },
    [=](accountant_type accountant) {
      self->state.accountant = std::move(accountant);
    },
    [=](atom::status, status_verbosity v) -> caf::config_value::dictionary {
      return self->state.status(v);
    },
    [=](atom::subscribe, atom::flush, const caf::actor& listener) {
      self->state.add_flush_listener(listener);
    },
  };
}

} // namespace vast::system
