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
  return [=](exit_msg const& msg) {
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
  VAST_ASSERT(batch.size() <= self->state.available);
  for (auto& e : batch)
    e.id(self->state.next++);
  self->state.available -= batch.size();
  VAST_DEBUG(self, "ships", batch.size(), "events");
  // TODO: How to retain type safety without copying the entire batch?
  auto msg = make_message(std::move(batch));
  self->send(actor_cast<actor>(self->state.archive), msg);
  self->send(self->state.index, msg);
}

// Asks the metastore for more IDs.
void replenish(stateful_actor<importer_state>* self) {
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
  VAST_ASSERT(max_event_id - self->state.next >= self->state.batch_size);
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
      auto x = is<none>(old) ? count{0} : get<count>(old);
      VAST_DEBUG(self, "got", n, "new IDs starting at", x);
      self->state.available = n;
      self->state.next = x;
      if (!self->state.remainder.empty())
        ship(self, std::move(self->state.remainder));
      auto result = write_state(self);
      if (!result) {
        VAST_ERROR(self, "failed to save state:",
                   self->system().render(result.error()));
        self->quit(result.error());
      }
      self->set_exit_handler(shutdown(self));
      self->unbecome();
    }
  );
}

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
  self->state.index = actor_pool::make(eu, actor_pool::round_robin());
  self->set_default_handler(skip);
  self->set_exit_handler(shutdown(self));
  self->set_down_handler(
    [=](down_msg const& msg) {
      if (msg.source == self->state.meta_store)
        self->state.meta_store = meta_store_type{};
    }
  );
  return {
    [=](meta_store_type const& ms) {
      VAST_DEBUG(self, "registers meta store");
      VAST_ASSERT(ms != self->state.meta_store);
      self->monitor(ms);
      self->state.meta_store = ms;
    },
    [=](archive_type const& archive) {
      VAST_DEBUG(self, "registers archive", archive);
      self->send(self->state.archive, sys_atom::value, put_atom::value,
                 actor_cast<actor>(archive));
    },
    [=](index_atom, actor const& index) {
      VAST_DEBUG(self, "registers index", index);
      self->send(self->state.index, sys_atom::value, put_atom::value, index);
    },
    [=](std::vector<event>& events) {
      VAST_ASSERT(!events.empty());
      VAST_DEBUG(self, "got", events.size(), "events");
      if (!self->state.meta_store) {
        self->quit(make_error(ec::unspecified, "no meta store configured"));
        return;
      }
      if (events.size() <= self->state.available) {
        // Ship the events immediately if we have enough IDs.
        ship(self, std::move(events));
      } else if (self->state.available > 0) {
        // Ship a subset if we have any IDs left.
        auto remainder = std::vector<event>(
          std::make_move_iterator(events.begin() + self->state.available),
          std::make_move_iterator(events.end()));
        ship(self, std::move(events));
        events.resize(self->state.available);
        self->state.remainder = std::move(remainder);
      } else {
        // Buffer events otherwise.
        self->state.remainder = std::move(events);
      }
      auto running_low = self->state.available < self->state.batch_size * 0.1;
      if (running_low || !self->state.remainder.empty())
        replenish(self);
    }
  };
}

} // namespace system
} // namespace vast
