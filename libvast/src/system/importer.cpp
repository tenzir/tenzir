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

#include <fstream>

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/filesystem.hpp"
#include "vast/logger.hpp"

#include "vast/system/atoms.hpp"
#include "vast/system/importer.hpp"

using namespace std::chrono;
using namespace std::chrono_literals;
using namespace caf;

namespace vast {
namespace system {

namespace {

// Persists importer state.
expected<void> read_state(stateful_actor<importer_state>* self) {
  if (exists(self->state.dir / "available")) {
    std::ifstream available{to_string(self->state.dir / "available")};
    available >> self->state.available;
    VAST_DEBUG(self, "found", self->state.available, "local IDs");
  }
  if (exists(self->state.dir / "next")) {
    std::ifstream next{to_string(self->state.dir / "next")};
    next >> self->state.next;
    VAST_DEBUG(self, "found next ID:", self->state.next);
  }
  return {};
}

// Reads persistent importer state.
expected<void> write_state(stateful_actor<importer_state>* self) {
  if (self->state.available > 0) {
    if (!exists(self->state.dir)) {
      auto result = mkdir(self->state.dir);
      if (!result)
        return result.error();
    }
    std::ofstream available{to_string(self->state.dir / "available")};
    available << self->state.available;
    VAST_DEBUG(self, "saved", self->state.available, "available IDs");
  }
  if (self->state.next > 0) {
    if (!exists(self->state.dir)) {
      auto result = mkdir(self->state.dir);
      if (!result)
        return result.error();
    }
    std::ofstream next{to_string(self->state.dir / "next")};
    next << self->state.next;
    VAST_DEBUG(self, "saved next ID:", self->state.next);
  }
  return {};
}

// Generates the default EXIT handler that saves states and shuts down internal
// components.
auto shutdown(stateful_actor<importer_state>* self) {
  return [=](const exit_msg& msg) {
    write_state(self);
    self->anon_send(self->state.archive, sys_atom::value, delete_atom::value);
    self->anon_send(self->state.index, sys_atom::value, delete_atom::value);
    self->send_exit(self->state.archive, msg.reason);
    self->send_exit(self->state.index, msg.reason);
    self->quit(msg.reason);
  };
}

// Ships a batch of events to archive and index.
void ship(stateful_actor<importer_state>* self, std::vector<event>&& batch) {
  CAF_LOG_TRACE(CAF_ARG(batch));
  VAST_ASSERT(batch.size() <= self->state.available);
  for (auto& e : batch)
    e.id(self->state.next++);
  self->state.available -= batch.size();
  VAST_DEBUG(self, "ships", batch.size(), "events");
  // TODO: How to retain type safety without copying the entire batch?
  auto msg = make_message(std::move(batch));
  self->send(actor_cast<actor>(self->state.archive), msg);
  self->send(self->state.index, msg);
  for (auto& e : self->state.continuous_queries)
    self->send(e, msg);
}

// Tries to ship a batch of events to archive and index.
bool ship_some(stateful_actor<importer_state>* self) {
  CAF_LOG_TRACE("");
  using std::swap;
  auto& st = self->state;
  if (st.remainder.empty())
    return false;
  std::vector<event> tmp;
  if (st.available >= st.remainder.size()) {
    // Ship all events immediately if we have enough IDs.
    swap(tmp, st.remainder);
  } else if (st.available > 0) {
    // Ship a subset of the events if we have any IDs left.
    VAST_ASSERT(st.remainder.size() > st.available);
    auto first = st.remainder.begin();
    auto last = first + st.available;
    tmp.insert(tmp.end(), make_move_iterator(first), make_move_iterator(last));
    st.remainder.erase(first, last);
  }
  if (!tmp.empty()) {
    ship(self, std::move(tmp));
    return true;
  }
  return false;
}

// Asks the metastore for more IDs.
void replenish(stateful_actor<importer_state>* self) {
  CAF_LOG_TRACE("");
  auto& st = self->state;
  if (st.available > (st.batch_size * 0.1)) {
    // Don't acquire more IDs if we can still ship at least one batch.
    VAST_DEBUG(self, "did not fall below the threshold of", st.batch_size * 0.1,
               "for replenishing its IDs, available:", st.available);
    return;
  }
  auto now = steady_clock::now();
  if (now - self->state.last_replenish < 10s) {
    VAST_DEBUG(self, "had to replenish twice within 10 secs");
    VAST_DEBUG(self, "doubles batch size:", self->state.batch_size,
                    "->", self->state.batch_size * 2);
    self->state.batch_size *= 2;
  }
  if (self->state.remainder.size() > self->state.batch_size) {
    VAST_DEBUG(self, "raises batch size to buffered events:",
               self->state.batch_size, "->", self->state.remainder.size());
    self->state.batch_size = self->state.remainder.size();
  }
  self->state.last_replenish = now;
  VAST_DEBUG(self, "replenishes", self->state.batch_size, "IDs");
  VAST_ASSERT(max_id - self->state.next >= self->state.batch_size);
  auto n = self->state.batch_size;
  // If we get an EXIT message while expecting a response from the metastore,
  // we'll give it a bit of time to come back;
  self->set_exit_handler(
    [=](const exit_msg& msg) {
      self->delayed_send(self, 5s, msg);
      self->set_exit_handler(shutdown(self));
    }
  );
  self->send(self->state.meta_store, add_atom::value, "id", data{n});
  self->become(
    keep_behavior,
    [=](const data& old) {
      // Update state.
      auto x = is<none>(old) ? count{0} : get<count>(old);
      VAST_DEBUG(self, "got", n, "new IDs starting at", x);
      auto& st = self->state;
      st.available = n;
      st.next = x;
      // Try to send cached events.
      ship_some(self);
      // Save state.
      auto result = write_state(self);
      if (!result) {
        VAST_ERROR(self, "failed to save state:",
                   self->system().render(result.error()));
        self->quit(result.error());
      }
      // Return to previous behavior.
      self->set_exit_handler(shutdown(self));
      self->unbecome();
    }
  );
}

// Stores additional events in the cache and tries to ship events if possible.
void store(stateful_actor<importer_state>* self, std::vector<event>&& xs) {
  CAF_LOG_TRACE(CAF_ARG(xs));
  using std::make_move_iterator;
  auto& st = self->state;
  VAST_ASSERT(st.meta_store);
  VAST_ASSERT(!xs.empty());
  VAST_DEBUG(self, "got", xs.size(), "events");
  VAST_DEBUG(self, "has", st.available, "IDs available");
  // Fill up `remainder` until it has at least batch_size elements.
  st.remainder.insert(st.remainder.end(), make_move_iterator(xs.begin()),
                      make_move_iterator(xs.end()));
  if (st.remainder.size() < st.batch_size) {
    VAST_DEBUG(self, "waits for", st.batch_size - st.remainder.size(),
               "more elements");
  } else {
    ship_some(self);
    replenish(self);
  }
}

class driver : public caf::stream_sink_driver<event> {
public:
  using pointer = stateful_actor<importer_state>*;

  driver(pointer self) : self_(self) {
    // nop
  }

  void process(std::vector<event>& xs) override {
    CAF_LOG_TRACE(CAF_ARG(xs));
    store(self_, std::move(xs));
    auto& st = self_->state;
    if (st.remainder.size() < st.batch_size) {
      VAST_DEBUG(self_, "waits for", st.batch_size - st.remainder.size(),
                 "more elements");
    } else {
      ship_some(self_);
      replenish(self_);
    }
  }

  void finalize(const error& err) override {
    CAF_LOG_TRACE(CAF_ARG(err));
    CAF_IGNORE_UNUSED(err);
    auto& st = self_->state;
    if (!st.remainder.empty()) {
      ship_some(self_);
      replenish(self_);
    }
    VAST_DEBUG(self_, "is done receiving from a source, cached events:",
               st.remainder.size());
  }

  bool congested() const noexcept override {
    auto& st = self_->state;
    return st.remainder.size() > st.batch_size;
  }

private:
  pointer self_;
};

} // namespace <anonymous>

behavior importer(stateful_actor<importer_state>* self, path dir,
                  size_t batch_size) {
  self->state.dir = dir;
  self->state.batch_size = batch_size;
  self->state.last_replenish = steady_clock::time_point::min();
  auto result = read_state(self);
  if (!result) {
    VAST_ERROR(self, "failed to load state:",
               self->system().render(result.error()));
    self->quit(result.error());
    return {};
  }
  auto eu = self->system().dummy_execution_unit();
  self->state.archive = actor_pool::make(eu, actor_pool::round_robin());
  self->monitor(self->state.archive);
  self->state.index = actor_pool::make(eu, actor_pool::round_robin());
  self->monitor(self->state.index);
  self->set_default_handler(skip);
  self->set_exit_handler(shutdown(self));
  self->set_down_handler(
    [=](const down_msg& msg) {
      if (msg.source == self->state.meta_store) {
        self->state.meta_store = meta_store_type{};
      } else {
        auto& cq = self->state.continuous_queries;
        auto itr = find(cq.begin(), cq.end(), msg.source);
        if (itr != cq.end()) {
          VAST_DEBUG(self, "finished continuous query for ", msg.source);
          cq.erase(itr);
        }
      }
    }
  );
  return {
    [=](const meta_store_type& ms) {
      VAST_DEBUG(self, "registers meta store");
      VAST_ASSERT(ms != self->state.meta_store);
      self->monitor(ms);
      self->state.meta_store = ms;
    },
    [=](const archive_type& archive) {
      VAST_DEBUG(self, "registers archive", archive);
      self->send(self->state.archive, sys_atom::value, put_atom::value,
                 actor_cast<actor>(archive));
    },
    [=](index_atom, const actor& index) {
      VAST_DEBUG(self, "registers index", index);
      self->send(self->state.index, sys_atom::value, put_atom::value, index);
    },
    [=](exporter_atom, const actor& exporter) {
      VAST_DEBUG(self, "registers exporter", exporter);
      self->monitor(exporter);
      self->state.continuous_queries.push_back(exporter);
    },
    [=](stream<event>& in) {
      if (!self->state.meta_store) {
        VAST_ERROR("no meta store configured for importer");
        return;
      }
      self->make_sink<driver>(in, self);
    },
    [=](std::vector<event>& one_shot_batch) {
      store(self, std::move(one_shot_batch));
      ship_some(self);
      replenish(self);
    }
  };
}

} // namespace system
} // namespace vast
