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

#include <deque>
#include <unordered_set>

#include <caf/all.hpp>

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/detail/assert.hpp"
#include "vast/event.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/json.hpp"
#include "vast/load.hpp"
#include "vast/logger.hpp"
#include "vast/save.hpp"

#include "vast/system/accountant.hpp"
#include "vast/system/index.hpp"
#include "vast/system/partition.hpp"
#include "vast/system/task.hpp"

#include "vast/detail/cache.hpp"

using namespace std::chrono;
using namespace caf;

namespace vast {
namespace system {

namespace {

// -- scheduling --------------------------------------------------------------

/*
void evict(stateful_actor<index_state>* self) {
  // TODO: pick the LRU partition, not just a random one.
  for (auto& x : self->state.loaded) {
    if (self->state.evicted.count(x.second) == 0) {
      VAST_DEBUG(self, "evicts partition", x.first);
      self->send(x.second, shutdown_atom::value);
      self->state.evicted.emplace(x.second, x.first);
      break;
    }
  }
}

// FIXME: erase lookups that have completed.
void schedule(stateful_actor<index_state>* self, const uuid& part,
              const uuid& lookup) {
  auto& ctx = self->state.lookups[lookup];
  // If we're dealing with the active partition, we dispatch immediately.
  if (part == self->state.active.id) {
    VAST_DEBUG(self, "dispatches to active partition", part);
    send_as(ctx.sink, self->state.active.partition, ctx.expr);
    return;
  }
  // If the partition is loaded, we can also dispatch immediately.
  auto l = self->state.loaded.find(part);
  if (l != self->state.loaded.end()) {
    VAST_DEBUG(self, "dispatches to loaded partition", part);
    send_as(ctx.sink, l->second, ctx.expr);
    return;
  }
  // If we have enough room, we can spin up the next partition.
  if (self->state.loaded.size() < self->state.capacity) {
    VAST_ASSERT(self->state.scheduled.empty());
    VAST_DEBUG(self, "spawns and dispatches partition", part);
    auto part_dir = self->state.dir / to_string(part);
    auto p = self->spawn<monitored>(partition, std::move(part_dir));
    self->state.loaded.emplace(part, p);
    send_as(ctx.sink, p, ctx.expr);
    return;
  }
  // If we're full, we delay dispatching until having evicted a partition.
  VAST_DEBUG(self, "queues partition", part);
  auto i = std::find_if(self->state.scheduled.begin(),
                        self->state.scheduled.end(),
                        [&](auto& x) { return x.id == part; });
  if (i != self->state.scheduled.end()) {
    VAST_ASSERT(!self->state.evicted.empty());
    i->lookups.insert(lookup);
  } else {
    self->state.scheduled.push_back({part, {lookup}});
    evict(self);
  }
}
*/

// FIXME: erase lookups that have completed.
void unschedule(stateful_actor<index_state>*, const actor&) {
  /* TODO: implement me
  // Check if we got an evicted partition.
  auto i = self->state.evicted.find(part);
  if (i != self->state.evicted.end()) {
    VAST_DEBUG(self, "completed eviction of partition", i->second);
    self->state.loaded.erase(i->second);
    self->state.evicted.erase(i);
    // Fill the hole if we have scheduled partition.
    if (!self->state.scheduled.empty()) {
      auto& next = self->state.scheduled.front();
      VAST_DEBUG(self, "spawns next partition", next.id);
      auto part_dir = self->state.dir / to_string(next.id);
      auto p = self->spawn<monitored>(partition, std::move(part_dir));
      self->state.loaded.emplace(next.id, p);
      for (auto& id : next.lookups) {
        VAST_ASSERT(self->state.lookups.count(id) > 0);
        auto& ctx = self->state.lookups[id];
        VAST_DEBUG(self, "dispatches expression", ctx.expr);
        send_as(ctx.sink, p, ctx.expr);
      }
      self->state.scheduled.pop_front();
      // If we have more pending partitions, try to evict more.
      if (self->state.scheduled.size() > self->state.evicted.size())
        evict(self);
    }
  }
  */
}

} // namespace <anonymous>

partition_ptr index_state::partition_factory::operator()(const uuid& id) const {
  return make_partition(st_->self, st_->dir, id);
}

index_state::index_state()
  // Arbitrary default value, overridden in ::init.
  : partition_cache(10, partition_lookup{}, partition_factory{this}) {
  // nop
}

void index_state::init(event_based_actor* self, const path& dir,
                       size_t partition_size, size_t in_mem_partitions,
                       size_t taste_partitions) {
  // Set members.
  this->self = self;
  this->dir = dir;
  this->partition_size = partition_size;
  this->partition_cache.size(in_mem_partitions);
  this->taste_partitions = taste_partitions;
  // Callback for the stream stage for creating a new partition when the
  // current one becomes full.
  auto fac = [this]() -> partition_ptr {
    // Move the active partition into the LRU cache.
    if (active != nullptr)
      partition_cache.add(active);
    // Create a new active partition.
    auto id = uuid::random();
    active = make_partition(this->self, this->dir, id);
    // Register the new active partition at the stream manager.
    return active;
  };
  stage = self->make_continuous_stage<indexer_stage_driver>(part_index, fac,
                                                            partition_size);
}

behavior index(stateful_actor<index_state>* self, const path& dir,
               size_t partition_size, size_t in_mem_partitions, size_t taste_partitions) {
  VAST_ASSERT(partition_size > 0);
  VAST_ASSERT(in_mem_partitions > 0);
  VAST_DEBUG(self, "caps partitions at", partition_size, "events");
  VAST_DEBUG(self, "keeps at most", in_mem_partitions, "partitions in memory");
  self->state.init(self, dir, partition_size, in_mem_partitions,
                   taste_partitions);
  auto accountant = accountant_type{};
  if (auto a = self->system().registry().get(accountant_atom::value))
    accountant = actor_cast<accountant_type>(a);
  // Read persistent state.
  if (exists(self->state.dir / "meta")) {
    auto result = load(self->state.dir / "meta", self->state.part_index);
    if (!result) {
      VAST_ERROR(self, "failed to load partition index:",
                 self->system().render(result.error()));
      self->quit(result.error());
      return {};
    }
  }
  /*
  self->set_exit_handler(
    [=](const exit_msg& msg) {
      auto can_terminate = [=] {
        return !self->state.active.partition && self->state.loaded.empty();
      };
      // Shut down all partitions.
      if (!can_terminate()) {
        if (self->state.active.partition)
          self->send(self->state.active.partition, shutdown_atom::value);
        for (auto& x : self->state.loaded)
          self->send(x.second, shutdown_atom::value);
        self->set_down_handler(
          [=](const down_msg& msg) {
            if (self->state.active.partition == msg.source) {
              self->state.active.partition = {};
            } else {
              auto pred = [&](auto& x) { return x.second == msg.source; };
              auto i = std::find_if(self->state.loaded.begin(),
                                    self->state.loaded.end(), pred);
              if (i != self->state.loaded.end())
                self->state.loaded.erase(i);
            }
            if (can_terminate())
              self->quit(msg.reason);
          }
        );
      }
      // Save our own state only if we have written something.
      if (self->state.active.partition) {
        VAST_DEBUG(self, "persists partition index");
        if (!exists(self->state.dir)) {
          auto result = mkdir(self->state.dir);
          if (!result) {
            VAST_ERROR(self, self->system().render(result.error()));
            self->quit(result.error());
            return;
          }
        }
        auto result = save(self->state.dir / "meta",
                           self->state.part_index);
        if (!result) {
          VAST_ERROR(self, "failed to persist partition index:",
                     self->system().render(result.error()));
          self->quit(result.error());
          return;
        }
      }
      if (can_terminate())
        self->quit(msg.reason);
    }
  );
  self->set_down_handler(
    [=](const down_msg& msg) {
      auto i = std::find_if(
        self->state.lookups.begin(),
        self->state.lookups.end(),
        [&](auto& x) { return x.second.sink == msg.source; });
      if (i != self->state.lookups.end()) {
        // A lookup actor went down. We can remove all queued partitions where
        // this actor was the only lookup.
        auto is_only_lookup = [&](auto& x) {
          return x.lookups.size() == 1 && x.lookups.count(i->first) > 0;
        };
        auto j = std::remove_if(self->state.scheduled.begin(),
                                self->state.scheduled.end(), is_only_lookup);
        auto n = self->state.scheduled.end() - j;
        VAST_IGNORE_UNUSED(n);
        VAST_DEBUG(self, "erases", n, "scheduled lookups");
        self->state.scheduled.erase(j, self->state.scheduled.end());
      } else {
        // A partition went down.
        unschedule(self, actor_cast<actor>(msg.source));
      }
    }
  );
  */
  self->set_default_handler(caf::skip);
  self->state.has_worker.assign(
    [=](const expression&) -> result<uuid, size_t, size_t> {
      auto& st = self->state;
      st.next_worker = nullptr;
    },
    [=](caf::stream<event> in) {
      VAST_DEBUG(self, "got a new source");
      return self->state.stage->add_inbound_path(in);
    }
  );
  return {
    [=](worker_atom, caf::actor worker) {
      auto& st = self->state;
      st.next_worker = worker;
      self->become(st.has_worker);
    },
    [=](const expression&) -> result<uuid, size_t, size_t> {
      return caf::sec::bad_function_call;
      /* TODO: implement me
      auto sender = actor_cast<actor>(self->current_sender());
      VAST_DEBUG(self, "got lookup:", expr);
      // Identify the relevant partitions.
      auto id = uuid::random();
      auto partitions = self->state.part_index.lookup(expr);
      if (partitions.empty()) {
        VAST_DEBUG(self, "returns without result: no partitions qualify");
        return {id, 0, 0};
      }
      // Construct a new lookup context.
      VAST_DEBUG(self, "creates new lookup context", id);
      auto ctx = self->state.lookups.insert({id, {expr, sender, {}}});
      self->monitor(sender);
      VAST_ASSERT(ctx.second);
      // TODO: make initial value configurable and figure out a more meaningful
      // way to select the first N partitions, e.g., based on accumulated
      // summary statics.
      auto num_partitions = partitions.size();
      auto n = std::min(partitions.size(), taste_parts);
      // Start processing to deliver a taste of the result.
      VAST_DEBUG(self, "schedules first", n, "partition(s)");
      for (auto i = partitions.end() - n; i != partitions.end(); ++i)
        schedule(self, *i, id);
      partitions.resize(partitions.size() - n);
      ctx.first->second.partitions = std::move(partitions);
      return {id, num_partitions, n};
      */
    },
    [=](const uuid&, size_t) {
      /* TODO: implement me
      auto& ctx = self->state.lookups[id];
      VAST_DEBUG(self, "processes lookup", id << ':', ctx.expr);
      if (n == 0) {
        VAST_DEBUG(self, "cancels lookup");
        self->state.lookups.erase(id);
        return;
      }
      n = std::min(ctx.partitions.size(), n);
      VAST_DEBUG(self, "schedules", n, "more partitions");
      for (auto i = ctx.partitions.end() - n; i != ctx.partitions.end(); ++i)
        schedule(self, *i, id);
      ctx.partitions.resize(ctx.partitions.size() - n);
      */
    },
    [=](caf::stream<event> in) {
      VAST_DEBUG(self, "got a new source");
      return self->state.stage->add_inbound_path(in);
    },
  };
}

} // namespace system
} // namespace vast
