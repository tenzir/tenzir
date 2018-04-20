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

namespace vast::system {

int32_t importer_state::available_ids() const noexcept {
  auto f = [](int32_t x, const id_generator& y) {
    return x + y.remaining();
  };
  return std::accumulate(id_generators.begin(), id_generators.end(),
                         int32_t{0}, f);
}

id importer_state::next_id() {
  VAST_ASSERT(!id_generators.empty());
  auto& g = id_generators.front();
  VAST_ASSERT(!g.at_end());
  auto result = g.next();
  if (g.at_end())
    id_generators.erase(id_generators.begin());
  return result;
}

namespace {

// Dummy value until we drop the manual `ship` and switch over to CAF streams
// for the communication to ARCHIVE and INDEX.
constexpr size_t batch_size = 1024;

// Reads persistent importer state.
expected<void> read_state(stateful_actor<importer_state>* self) {
  auto& st = self->state;
  st.id_generators.clear();
  if (exists(st.dir / "available_ids")) {
    std::ifstream available{to_string(self->state.dir / "available_ids")};
    std::string line;
    while (std::getline(available, line)) {
      id i;
      id last;
      std::istringstream in{line};
      if (in >> i >> last) {
        VAST_DEBUG(self, "found ID range:", i, "to", last);
        st.id_generators.emplace_back(i, last);
      } else {
        VAST_ERROR(self, "invalid file format");
      }
    }
  }
  return {};
}

// Persists importer state.
expected<void> write_state(stateful_actor<importer_state>* self) {
  auto& st = self->state;
  if (st.id_generators.empty() || st.available_ids() == 0)
    return caf::unit;
  if (!exists(self->state.dir)) {
    auto result = mkdir(self->state.dir);
    if (!result)
      return result.error();
  }
  std::ofstream available{to_string(self->state.dir / "available_ids")};
  auto i = st.id_generators.begin();
  available << i->i << ' ' << i->last;
  for (++i; i != st.id_generators.end(); ++i) {
    available << '\n' << i->i << ' ' << i->last;
  }
  VAST_DEBUG(self, "saved", st.available_ids(), "available IDs");
  return {};
}

// Generates the default EXIT handler that saves states and shuts down internal
// components.
auto shutdown(stateful_actor<importer_state>* self) {
  return [=](const exit_msg& msg) {
    write_state(self);
    self->anon_send(self->state.index, sys_atom::value, delete_atom::value);
    self->send_exit(self->state.index, msg.reason);
    self->quit(msg.reason);
  };
}

// Ships a batch of events to archive and index.
void ship(stateful_actor<importer_state>* self, std::vector<event>&& batch) {
  CAF_LOG_TRACE(CAF_ARG(batch));
  auto& st = self->state;
  VAST_DEBUG(self, "ships", batch.size(), "events");
  // TODO: How to retain type safety without copying the entire batch?
  auto msg = make_message(std::move(batch));
  self->send(st.index, msg);
  for (auto& e : st.continuous_queries)
    self->send(e, msg);
}

// Tries to ship a batch of events to ARCHIVE and INDEX.
bool ship_some(stateful_actor<importer_state>* self) {
  CAF_LOG_TRACE("");
  using std::swap;
  auto& st = self->state;
  if (st.remainder.size() < batch_size) {
    VAST_DEBUG(self, "waits for", batch_size - st.remainder.size(),
               "more elements before calling ship()");
    return false;
  }
  // Swap remainder with a temporary to not have a moved-from remainder.
  std::vector<event> tmp;
  tmp.reserve(batch_size);
  swap(tmp, st.remainder);
  ship(self, std::move(tmp));
  return true;
}

// Asks the metastore for more IDs.
void replenish(stateful_actor<importer_state>* self) {
  CAF_LOG_TRACE("");
  auto& st = self->state;
  // Do nothing if we're already waiting for a response of the meta store.
  if (st.awaiting_ids)
    return;
  // Check whether we obtain new IDs too frequently.
  auto now = steady_clock::now();
  if ((now - st.last_replenish) < 10s) {
    VAST_DEBUG(self, "had to replenish twice within 10 secs");
    VAST_DEBUG(self, "increase chunk size:", st.id_chunk_size,
                    "->", st.id_chunk_size + 1024);
    st.id_chunk_size += 1024;
  }
  st.last_replenish = now;
  VAST_DEBUG(self, "replenishes", st.id_chunk_size, "IDs");
  // If we get an EXIT message while expecting a response from the metastore,
  // we'll give it a bit of time to come back;
  self->set_default_handler(skip);
  self->set_exit_handler(
    [=](const exit_msg& msg) {
      self->delayed_send(self, 5s, msg);
      self->set_exit_handler(shutdown(self));
    }
  );
  // Trigger meta store and wait for response.
  auto n = st.id_chunk_size;
  self->send(st.meta_store, add_atom::value, "id", data{n});
  st.awaiting_ids = true;
  self->become(
    keep_behavior,
    [=](const data& old) {
      // Update state.
      auto x = is<none>(old) ? count{0} : get<count>(old);
      VAST_DEBUG(self, "got", n, "new IDs starting at", x);
      auto& st = self->state;
      // Add a new ID generator for the available range.
      VAST_ASSERT(st.awaiting_ids);
      st.id_generators.emplace_back(x, x + n);
      // Save state.
      auto result = write_state(self);
      if (!result) {
        VAST_ERROR(self, "failed to save state:",
                   self->system().render(result.error()));
        self->quit(result.error());
      }
      // Try to emit more credit with out new IDs.
      st.stg->advance();
      // Return to previous behavior.
      st.awaiting_ids = false;
      self->set_default_handler(print_and_drop);
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
  VAST_ASSERT(!xs.empty());
  VAST_ASSERT(xs.size()
              <= static_cast<size_t>(std::numeric_limits<int32_t>::max()));
  st.remainder.insert(st.remainder.end(), make_move_iterator(xs.begin()),
                      make_move_iterator(xs.end()));
  ship_some(self);
}

using downstream_manager = caf::broadcast_downstream_manager<event>;

class driver : public caf::stream_stage_driver<event, downstream_manager> {
public:
  using super = caf::stream_stage_driver<event, downstream_manager>;

  using pointer = stateful_actor<importer_state>*;

  driver(downstream_manager& out, pointer self) : super(out), self_(self) {
    // nop
  }

  void process(caf::downstream<event>& out, std::vector<event>& xs) override {
    CAF_LOG_TRACE(CAF_ARG(xs));
    auto& st = self_->state;
    VAST_DEBUG(self_, "has", st.available_ids(), "IDs available");
    VAST_DEBUG(self_, "got", xs.size(), "events with", st.in_flight_events,
               "in-flight events");
    VAST_ASSERT(xs.size() <= static_cast<size_t>(st.available_ids()));
    VAST_ASSERT(xs.size() <= static_cast<size_t>(st.in_flight_events));
    st.in_flight_events -= static_cast<int32_t>(xs.size());
    if (out_.empty()) {
      for (auto& x : xs)
        x.id(st.next_id());
    } else {
      for (auto& x : xs) {
        x.id(st.next_id());
        out.push(x);
      }
    }
    VAST_DEBUG(self_, "tagged", xs.size(), "events with IDs");
    store(self_, std::move(xs));
  }

  void finalize(const error& err) override {
    CAF_LOG_TRACE(CAF_ARG(err));
    CAF_IGNORE_UNUSED(err);
    auto& st = self_->state;
    if (!st.remainder.empty()) {
      // Swap remainder with temporary to not have a moved-from remainder.
      using std::swap;
      std::vector<event> tmp;
      swap(tmp, st.remainder);
      ship(self_, std::move(tmp));
    }
  }

  bool congested() const noexcept override {
    auto& st = self_->state;
    return st.remainder.size() > batch_size || super::congested();
  }

  int32_t acquire_credit(inbound_path* path, int32_t desired) override {
    CAF_LOG_TRACE(CAF_ARG(path) << CAF_ARG(desired));
    CAF_IGNORE_UNUSED(path);
    // This function makes sure that we never hand out more credit than we have
    // IDs available.
    if (desired == 0) {
      // Easy decision if the path acquires no new credit.
      return 0;
    }
    // Calculate how much more in-flight events we can allow.
    auto& st = self_->state;
    auto max_credit = st.available_ids() - st.in_flight_events;
    VAST_ASSERT(max_credit >= 0);
    if (max_credit <= desired) {
      // Get more IDs if we're running out.
      VAST_DEBUG("had to limit acquired credit to", max_credit);
      replenish(self_);
      st.in_flight_events += max_credit;
      return max_credit;
    }
    st.in_flight_events += desired;
    return desired;
  }

private:
  pointer self_;
};

} // namespace <anonymous>

behavior importer(stateful_actor<importer_state>* self, path dir) {
  self->state.dir = dir;
  self->state.last_replenish = steady_clock::time_point::min();
  auto result = read_state(self);
  if (!result) {
    VAST_ERROR(self, "failed to load state:",
               self->system().render(result.error()));
    self->quit(result.error());
    return {};
  }
  auto eu = self->system().dummy_execution_unit();
  self->state.index = actor_pool::make(eu, actor_pool::round_robin());
  self->monitor(self->state.index);
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
  self->state.stg = self->make_continuous_stage<driver>(self);
  return {
    [=](const meta_store_type& ms) {
      VAST_DEBUG(self, "registers meta store");
      VAST_ASSERT(ms != self->state.meta_store);
      self->monitor(ms);
      self->state.meta_store = ms;
    },
    [=](const archive_type& archive) {
      VAST_DEBUG(self, "registers archive", archive);
      return self->state.stg->add_outbound_path(archive);
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
      auto& st = self->state;
      if (!st.meta_store) {
        VAST_ERROR("no meta store configured for importer");
        return;
      }
      VAST_INFO("add a new source to the importer");
      st.stg->add_inbound_path(in);
    },
    [=](add_atom, const actor& subscriber) {
      auto& st = self->state;
      VAST_INFO("add a new sink to the importer");
      st.stg->add_outbound_path(subscriber);
    }
  };
}

} // namespace vast::system
