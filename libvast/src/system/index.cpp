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
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/cache.hpp"
#include "vast/detail/fill_status_map.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/detail/notifying_stream_manager.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/fbs/meta_index.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/ids.hpp"
#include "vast/io/read.hpp"
#include "vast/io/save.hpp"
#include "vast/json.hpp"
#include "vast/load.hpp"
#include "vast/logger.hpp"
#include "vast/save.hpp"
#include "vast/system/accountant.hpp"
#include "vast/system/evaluator.hpp"
#include "vast/system/index_common.hpp"
#include "vast/system/partition.hpp"
#include "vast/system/query_supervisor.hpp"
#include "vast/system/spawn_indexer.hpp"
#include "vast/table_slice.hpp"

#include <caf/make_counted.hpp>
#include <caf/stateful_actor.hpp>

#include <chrono>
#include <deque>
#include <unordered_set>

using namespace std::chrono;

namespace vast::system {

namespace {

auto make_index_stage(index_state* st) {
  using impl = detail::notifying_stream_manager<caf::stateful_actor<index_state>, indexer_stage_driver>;
  auto result = caf::make_counted<impl>(st->self, st->self);
  result->continuous(true);
  return result;
}

} // namespace

partition_ptr index_state::partition_factory::operator()(const uuid& id) const {
  // The factory must not get called for the active partition nor for
  // partitions that are currently unpersisted.
  VAST_ASSERT(st_->active == nullptr || id != st_->active->id());
  VAST_ASSERT(std::none_of(st_->unpersisted.begin(), st_->unpersisted.end(),
                           [&](auto& kvp) { return kvp.first->id() == id; }));
  // Load partition from disk.
  VAST_DEBUG(st_->self, "loads partition", id);
  auto result = std::make_unique<partition>(st_, id, st_->max_partition_size);
  if (auto err = result->init())
    VAST_ERROR(st_->self, "unable to load partition state from disk:", id);
  return result;
}

index_state::index_state(caf::stateful_actor<index_state>* self)
  : self(self),
    factory(spawn_indexer),
    lru_partitions(10, partition_lookup{}, partition_factory{this}) {
  // nop
}

index_state::~index_state() {
  VAST_VERBOSE(self, "tearing down");
  if (active != nullptr) {
    [[maybe_unused]] auto unregistered = stage->out().unregister(active.get());
    VAST_ASSERT(unregistered);
  }
  if (flush_on_destruction)
    flush_to_disk();
}

caf::error
index_state::init(const path& dir, size_t max_partition_size,
                  uint32_t in_mem_partitions, uint32_t taste_partitions,
                  bool delay_flush_until_shutdown) {
  VAST_TRACE(VAST_ARG(dir), VAST_ARG(max_partition_size),
             VAST_ARG(in_mem_partitions), VAST_ARG(taste_partitions),
             VAST_ARG(delay_flush_until_shutdown));
  // This option must be kept in sync with vast/address_synopsis.hpp.
  put(meta_idx.factory_options(), "max-partition-size", max_partition_size);
  // Set members.
  this->dir = dir;
  this->max_partition_size = max_partition_size;
  this->lru_partitions.size(in_mem_partitions);
  this->taste_partitions = taste_partitions;
  this->flush_on_destruction = false;
  this->delay_flush_until_shutdown = delay_flush_until_shutdown;
  // Read persistent state.
  if (auto err = load_from_disk())
    return err;
  // Dont try to overwrite existing state on boot failure.
  this->flush_on_destruction = true;
  // Spin up the stream manager.
  stage = make_index_stage(this);
  return caf::none;
}

caf::error index_state::load_from_disk() {
  VAST_TRACE("");
  // Nothing to load is not an error.
  if (!exists(dir)) {
    VAST_DEBUG(self, "found no directory to load from");
    return caf::none;
  }
  if (auto fname = statistics_filename(); exists(fname)) {
    VAST_VERBOSE(self, "loads statistics from", fname);
    if (auto err = load(&self->system(), fname, stats)) {
      VAST_ERROR(self,
                 "failed to load statistics:", self->system().render(err));
      return err;
    }
    VAST_DEBUG(self, "loaded statistics");
  }
  if (auto fname = meta_index_filename(); exists(fname)) {
    VAST_VERBOSE(self, "loads meta index from", fname);
    auto buffer = io::read(fname);
    if (!buffer) {
      VAST_ERROR(self, "failed to read meta index file:",
                 self->system().render(buffer.error()));
      return buffer.error();
    }
    auto bytes = span<const byte>{*buffer};
    if (auto err = fbs::unwrap<fbs::MetaIndex>(bytes, meta_idx))
      return err;
    VAST_DEBUG(self, "loaded meta index");
  }
  return caf::none;
}

caf::error index_state::flush_meta_index() {
  VAST_VERBOSE(self, "writes meta index to", meta_index_filename());
  auto flatbuf = fbs::wrap(meta_idx, fbs::file_identifier);
  if (!flatbuf)
    return flatbuf.error();
  return io::save(meta_index_filename(), as_bytes(*flatbuf));
}

caf::error index_state::flush_statistics() {
  VAST_VERBOSE(self, "writes statistics to", statistics_filename());
  return save(&self->system(), statistics_filename(), stats);
}

caf::error index_state::flush_to_disk() {
  VAST_TRACE("");
  auto flush_all = [this]() -> caf::error {
    // Flush meta index to disk.
    if (auto err = flush_meta_index())
      return err;
    // Flush statistics to disk.
    if (auto err = flush_statistics())
      return err;
    // Flush active partition.
    if (active != nullptr)
      if (auto err = active->flush_to_disk())
        return err;
    // Flush all unpersisted partitions. This only writes the meta state of
    // each partition. For actually writing the contents of each INDEXER we
    // need to rely on messaging.
    for (auto& kvp : unpersisted)
      if (auto err = kvp.first->flush_to_disk())
        return err;
    return caf::none;
  };
  if (auto err = flush_all()) {
    VAST_ERROR(self, "failed to flush state:", self->system().render(err));
    return err;
  }
  return caf::none;
}

path index_state::statistics_filename() const {
  return dir / "statistics";
}

path index_state::meta_index_filename() const {
  return dir / "meta";
}

bool index_state::worker_available() {
  return !idle_workers.empty();
}

caf::actor index_state::next_worker() {
  auto result = std::move(idle_workers.back());
  idle_workers.pop_back();
  return result;
}

caf::dictionary<caf::config_value>
index_state::status(status_verbosity v) const {
  using caf::put;
  using caf::put_dictionary;
  using caf::put_list;
  auto result = caf::settings{};
  auto& index_status = put_dictionary(result, "index");
  // Misc parameters.
  if (v >= status_verbosity::info) {
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
  }
  if (v >= status_verbosity::debug) {
    put(index_status, "meta-index-filename", meta_index_filename().str());
    // Resident partitions.
    auto& partitions = put_dictionary(index_status, "partitions");
    if (active != nullptr)
      partitions.emplace("active", to_string(active->id()));
    auto& cached = put_list(partitions, "cached");
    for (auto& part : lru_partitions.elements())
      cached.emplace_back(to_string(part->id()));
    auto& unpersisted = put_list(partitions, "unpersisted");
    for (auto& kvp : this->unpersisted)
      unpersisted.emplace_back(to_string(kvp.first->id()));
    // General state such as open streams.
    detail::fill_status_map(index_status, self);
  }
  return result;
}

void index_state::reset_active_partition() {
  // Persist meta data and the state of all INDEXER actors when the active
  // partition gets replaced becomes full.
  if (active != nullptr) {
    [[maybe_unused]] auto unregistered = stage->out().unregister(active.get());
    VAST_ASSERT(unregistered);
    if (auto err = active->flush_to_disk())
      VAST_ERROR(self, "failed to persist active partition:", err);
    // Store this partition as unpersisted to make sure we're not attempting
    // to load it from disk until it is safe to do so.
    if (active_partition_indexers > 0)
      unpersisted.emplace_back(std::move(active), active_partition_indexers);
  }
  // Persist the current version of the meta_index and statistics to preserve
  // the state and be partially robust against crashes.
  if (!delay_flush_until_shutdown) {
    if (auto err = flush_meta_index())
      VAST_ERROR(self, "failed to persist the meta index:", err);
    if (auto err = flush_statistics())
      VAST_ERROR(self, "failed to persist the statistics:", err);
  }
  active = make_partition();
  stage->out().register_partition(active.get());
  active_partition_indexers = 0;
}

partition* index_state::get_or_add_partition(const table_slice_ptr& slice) {
  if (!active || active->capacity() < slice->rows())
    reset_active_partition();
  return active.get();
}

partition_ptr index_state::make_partition() {
  return make_partition(uuid::random());
}

partition_ptr index_state::make_partition(uuid id) {
  VAST_DEBUG(self, "starts a new partition:", id);
  return std::make_unique<partition>(this, std::move(id), max_partition_size);
}

caf::actor index_state::make_indexer(path filename, type column_type,
                                     uuid partition_id, std::string fqn) {
  VAST_TRACE(VAST_ARG(dir), VAST_ARG(column_type), VAST_ARG(index),
             VAST_ARG(partition_id));
  caf::settings index_opts;
  index_opts["cardinality"] = max_partition_size;
  return factory(self, self->state.accountant, std::move(filename),
                 std::move(column_type), std::move(index_opts), self,
                 partition_id, std::move(fqn));
}

void index_state::decrement_indexer_count(uuid partition_id) {
  if (partition_id == active->id())
    active_partition_indexers--;
  else {
    auto i
      = std::find_if(unpersisted.begin(), unpersisted.end(), [&](auto& kvp) {
          return kvp.first->id() == partition_id;
        });
    if (i == unpersisted.end())
      VAST_ERROR(self,
                 "received done from unknown indexer:", self->current_sender());
    if (--i->second == 0) {
      VAST_DEBUG(self, "successfully persisted", partition_id);
      unpersisted.erase(i);
    }
  }
}

partition* index_state::find_unpersisted(const uuid& id) {
  auto i = std::find_if(unpersisted.begin(), unpersisted.end(),
                        [&](auto& kvp) { return kvp.first->id() == id; });
  return i != unpersisted.end() ? i->first.get() : nullptr;
}

index_state::pending_query_map
index_state::build_query_map(lookup_state& lookup, uint32_t num_partitions) {
  VAST_TRACE(VAST_ARG(lookup), VAST_ARG(num_partitions));
  if (num_partitions == 0 || lookup.partitions.empty())
    return {};
  // Prefer partitions that are already available in RAM.
  std::partition(lookup.partitions.begin(), lookup.partitions.end(),
                 [&](const uuid& candidate) {
                   return (active != nullptr && active->id() == candidate)
                          || find_unpersisted(candidate) != nullptr
                          || lru_partitions.contains(candidate);
                 });
  // Maps partition IDs to the EVALUATOR actors we are going to spawn.
  pending_query_map result;
  // Helper function to spin up EVALUATOR actors for a single partition.
  auto spin_up = [&](const uuid& partition_id) {
    // We need to first check whether the ID is the active partition or one
    // of our unpersistet ones. Only then can we dispatch to our LRU cache.
    partition* part;
    if (active != nullptr && active->id() == partition_id)
      part = active.get();
    else if (auto ptr = find_unpersisted(partition_id); ptr != nullptr)
      part = ptr;
    else
      part = lru_partitions.get_or_add(partition_id).get();
    auto eval = part->eval(lookup.expr);
    if (eval.empty()) {
      VAST_DEBUG(self, "identified partition", partition_id,
                 "as candidate in the meta index, but it didn't produce an "
                 "evaluation map");
      return;
    }
    result.emplace(partition_id, std::move(eval));
  };
  // Loop over the candidate set until we either successfully scheduled
  // num_partitions partitions or run out of candidates.
  {
    auto i = lookup.partitions.begin();
    auto last = lookup.partitions.end();
    for (; i != last && result.size() < num_partitions; ++i)
      spin_up(*i);
    lookup.partitions.erase(lookup.partitions.begin(), i);
  }
  return result;
}

query_map
index_state::launch_evaluators(pending_query_map pqm, expression expr) {
  query_map result;
  for (auto& [id, eval] : pqm) {
    std::vector<caf::actor> xs{self->spawn(evaluator, expr, std::move(eval))};
    result.emplace(id, std::move(xs));
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

caf::behavior index(caf::stateful_actor<index_state>* self, const path& dir,
                    size_t max_partition_size, size_t in_mem_partitions,
                    size_t taste_partitions, size_t num_workers,
                    bool delay_flush_until_shutdown) {
  VAST_TRACE(VAST_ARG(dir), VAST_ARG(max_partition_size),
             VAST_ARG(in_mem_partitions), VAST_ARG(taste_partitions),
             VAST_ARG(num_workers), VAST_ARG(delay_flush_until_shutdown));
  VAST_ASSERT(max_partition_size > 0);
  VAST_ASSERT(in_mem_partitions > 0);
  VAST_DEBUG(self, "spawned:", VAST_ARG(max_partition_size),
             VAST_ARG(in_mem_partitions), VAST_ARG(taste_partitions));
  if (auto err
      = self->state.init(dir, max_partition_size, in_mem_partitions,
                         taste_partitions, delay_flush_until_shutdown)) {
    self->quit(std::move(err));
    return {};
  }
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    VAST_DEBUG(self, "got EXIT from", msg.source);
    self->quit(msg.reason);
  });
  // Launch workers for resolving queries.
  for (size_t i = 0; i < num_workers; ++i)
    self->spawn(query_supervisor, self);
  // We switch between has_worker behavior and the default behavior (which
  // simply waits for a worker).
  self->set_default_handler(caf::skip);
  self->state.has_worker.assign(
    [=](expression& expr) {
      auto respond = [&](auto&&... xs) {
        auto mid = self->current_message_id();
        unsafe_response(self, self->current_sender(), {}, mid.response_id(),
                        std::forward<decltype(xs)>(xs)...);
      };
      // Sanity check.
      if (self->current_sender() == nullptr) {
        VAST_ERROR(self, "got an anonymous query (ignored)");
        respond(caf::sec::invalid_argument);
        return;
      }
      auto& st = self->state;
      auto client = caf::actor_cast<caf::actor>(self->current_sender());
      // Convenience function for dropping out without producing hits. Makes
      // sure that clients always receive a 'done' message.
      auto no_result = [&] {
        respond(uuid::nil(), uint32_t{0}, uint32_t{0});
        self->send(client, atom::done_v);
      };
      // Get all potentially matching partitions.
      auto candidates = st.meta_idx.lookup(expr);
      // Report no result if no candidates are found.
      if (candidates.empty()) {
        VAST_DEBUG(self, "returns without result: no partitions qualify");
        no_result();
        return;
      }
      // Allows the client to query further results after initial taste.
      auto query_id = uuid::random();
      auto lookup = index_state::lookup_state{expr, std::move(candidates)};
      auto pqm = st.build_query_map(lookup, st.taste_partitions);
      if (pqm.empty()) {
        VAST_ASSERT(lookup.partitions.empty());
        VAST_DEBUG(self, "returns without result: no partitions qualify");
        no_result();
        return;
      }
      auto hits = pqm.size() + lookup.partitions.size();
      auto scheduling = std::min(taste_partitions, hits);
      // Notify the client that we don't have more hits.
      if (scheduling == hits)
        query_id = uuid::nil();
      respond(query_id, detail::narrow<uint32_t>(hits),
              detail::narrow<uint32_t>(scheduling));
      auto qm = st.launch_evaluators(pqm, expr);
      VAST_DEBUG(self, "scheduled", qm.size(), "/", hits,
                 "partitions for query", expr);
      if (!lookup.partitions.empty()) {
        [[maybe_unused]] auto result
          = st.pending.emplace(query_id, std::move(lookup));
        VAST_ASSERT(result.second);
      }
      // Delegate to query supervisor (uses up this worker) and report
      // query ID + some stats to the client.
      self->send(st.next_worker(), std::move(expr), std::move(qm), client);
      if (!st.worker_available())
        self->unbecome();
    },
    [=](const uuid& query_id, uint32_t num_partitions) {
      auto& st = self->state;
      // A zero as second argument means the client drops further results.
      if (num_partitions == 0) {
        VAST_DEBUG(self, "dropped remaining results for query ID", query_id);
        st.pending.erase(query_id);
        return;
      }
      // Sanity checks.
      if (self->current_sender() == nullptr) {
        VAST_ERROR(self, "got an anonymous query (ignored)");
        return;
      }
      auto client = caf::actor_cast<caf::actor>(self->current_sender());
      auto iter = st.pending.find(query_id);
      if (iter == st.pending.end()) {
        VAST_WARNING(self, "got a request for unknown query ID", query_id);
        self->send(client, atom::done_v);
        return;
      }
      auto pqm = st.build_query_map(iter->second, num_partitions);
      if (pqm.empty()) {
        VAST_ASSERT(iter->second.partitions.empty());
        st.pending.erase(iter);
        VAST_DEBUG(self, "returns without result: no partitions qualify");
        self->send(client, atom::done_v);
        return;
      }
      auto qm = st.launch_evaluators(pqm, iter->second.expr);
      // Delegate to query supervisor (uses up this worker) and report
      // query ID + some stats to the client.
      VAST_DEBUG(self, "schedules", qm.size(), "more partition(s) for query",
                 iter->first, "with", iter->second.partitions.size(),
                 "remaining");
      self->send(st.next_worker(), iter->second.expr, std::move(qm), client);
      // Cleanup if we exhausted all candidates.
      if (iter->second.partitions.empty())
        st.pending.erase(iter);
    },
    [=](atom::worker, caf::actor& worker) {
      self->state.idle_workers.emplace_back(std::move(worker));
    },
    [=](atom::done, uuid partition_id) {
      self->state.decrement_indexer_count(partition_id);
    },
    [=](caf::stream<table_slice_ptr> in) {
      VAST_DEBUG(self, "got a new source");
      return self->state.stage->add_inbound_path(in);
    },
    [=](atom::status, status_verbosity v) -> caf::config_value::dictionary {
      return self->state.status(v);
    },
    [=](atom::subscribe, atom::flush, caf::actor& listener) {
      self->state.add_flush_listener(std::move(listener));
    });
  return {[=](atom::worker, caf::actor& worker) {
            auto& st = self->state;
            st.idle_workers.emplace_back(std::move(worker));
            self->become(caf::keep_behavior, st.has_worker);
          },
          [=](atom::done, uuid partition_id) {
            self->state.decrement_indexer_count(partition_id);
          },
          [=](caf::stream<table_slice_ptr> in) {
            VAST_DEBUG(self, "got a new source");
            return self->state.stage->add_inbound_path(in);
          },
          [=](accountant_type accountant) {
            namespace defs = defaults::system;
            self->state.accountant = std::move(accountant);
            self->send(self->state.accountant, atom::announce_v, "index");
            self->delayed_send(self, defs::telemetry_rate, atom::telemetry_v);
          },
          [=](atom::status, status_verbosity v)
            -> caf::config_value::dictionary { return self->state.status(v); },
          [=](atom::subscribe, atom::flush, caf::actor& listener) {
            self->state.add_flush_listener(std::move(listener));
          }};
}

} // namespace vast::system
