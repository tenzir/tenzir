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

template <class Actor>
expected<void> load_state(Actor* self) {
  if (!exists(self->state.dir))
    return {};
  // Load current batch size.
  std::ifstream available{to_string(self->state.dir / "available")};
  std::ifstream next{to_string(self->state.dir / "next")};
  if (!available || !next)
    return make_error(ec::filesystem_error, std::strerror(errno));
  available >> self->state.available;
  next >> self->state.next;
  VAST_DEBUG(self, "found", self->state.available, "local IDs");
  VAST_DEBUG(self, "found next event ID:", self->state.next);
  return {};
}

template <class Actor>
expected<void> save_state(Actor* self) {
  if (self->state.next == 0 && self->state.available == 0)
    return {};
  if (!exists(self->state.dir)) {
    auto result = mkdir(self->state.dir);
    if (!result)
      return result.error();
  }
  std::ofstream available{to_string(self->state.dir / "available")};
  std::ofstream next{to_string(self->state.dir / "next")};
  if (!available || !next)
    return make_error(ec::filesystem_error, std::strerror(errno));
  available << self->state.available;
  next << self->state.next;
  VAST_DEBUG(self, "saved available IDs:", self->state.available);
  VAST_DEBUG(self, "saved next next ID:", self->state.next);
  return {};
}

template <class Actor>
void ship(Actor* self, std::vector<event>&& batch) {
  VAST_ASSERT(batch.size() <= self->state.available);
  for (auto& e : batch)
    e.id(self->state.next++);
  self->state.available -= batch.size();
  // FIXME: How to retain type safety without copying?
  auto msg = make_message(std::move(batch));
  self->send(actor_cast<actor>(self->state.archive), msg);
  self->send(self->state.index, msg);
}

template <class Actor>
void replenish(Actor* self) {
  auto now = steady_clock::now();
  if (now - self->state.last_replenish < 10s) {
    VAST_DEBUG(self, "had to replenish twice within 10 secs");
    VAST_DEBUG(self, "doubles batch size:", self->state.batch_size,
                    "->", self->state.batch_size * 2);
    self->state.batch_size *= 2;
  }
  if (self->state.remainder.size() > self->state.batch_size) {
    VAST_DEBUG(self, "adjusts batch size to buffered events:",
               self->state.batch_size, "->", self->state.remainder.size());
    self->state.batch_size = self->state.remainder.size();
  }
  self->state.last_replenish = now;
  VAST_DEBUG(self, "replenishes", self->state.batch_size, "IDs");
  VAST_ASSERT(max_event_id - self->state.next >= self->state.batch_size);
  auto n = self->state.batch_size;
  self->send(self->state.metastore, add_atom::value, "id", data{n});
  self->become(
    keep_behavior,
    [=](const data& old) {
      auto x = is<none>(old) ? count{0} : get<count>(old);
      VAST_DEBUG(self, "got", n, "new IDs starting at", x);
      self->state.available = n;
      self->state.next = x;
      if (!self->state.remainder.empty())
        ship(self, std::move(self->state.remainder));
      auto result = save_state(self);
      if (!result) {
        VAST_ERROR(self, "failed to save state:",
                   self->system().render(result.error()));
        self->quit(result.error());
      }
      self->unbecome();
    }
  );
}

behavior importer(stateful_actor<importer_state>* self, path dir,
                  size_t batch_size) {
  self->state.dir = dir;
  self->state.batch_size = batch_size;
  self->state.last_replenish = std::chrono::steady_clock::now();
  auto result = load_state(self);
  if (!result) {
    VAST_ERROR(self, "failed to load state:",
               self->system().render(result.error()));
    self->quit(result.error());
    return {};
  }
  self->set_default_handler(skip);
  self->set_down_handler(
    [=](down_msg const& msg) {
      if (msg.source == self->state.metastore)
        self->state.metastore = meta_store_type{};
      else if (msg.source == self->state.archive)
        self->state.archive = archive_type{};
      else if (msg.source == self->state.index)
        self->state.index = {};
    }
  );
  self->set_exit_handler(
    [=](exit_msg const& msg) {
      save_state(self);
      self->quit(msg.reason);
    }
  );
  return {
    [=](meta_store_type const& ms) {
      VAST_DEBUG(self, "registers metastore");
      self->monitor(ms);
      self->state.metastore = ms;
    },
    [=](archive_type const& a) {
      VAST_DEBUG(self, "registers archive");
      self->monitor(a);
      self->state.archive = a;
    },
    [=](index_atom, actor const& a) {
      VAST_DEBUG(self, "registers index", a);
      self->monitor(a);
      self->state.index = a;
    },
    [=](std::vector<event>& events) {
      VAST_ASSERT(!events.empty());
      VAST_DEBUG(self, "got", events.size(), "events");
      if (!self->state.metastore) {
        self->quit(make_error(ec::unspecified, "no metastore configured"));
        return;
      }
      if (!self->state.archive) {
        self->quit(make_error(ec::unspecified, "no archive configured"));
        return;
      }
      if (!self->state.index) {
        self->quit(make_error(ec::unspecified, "no index configured"));
        return;
      }
      // Attempt to ship the incoming events.
      if (events.size() <= self->state.available) {
        ship(self, std::move(events));
      } else if (self->state.available > 0) {
        auto remainder = std::vector<event>(
          std::make_move_iterator(events.begin() + self->state.available),
          std::make_move_iterator(events.end()));
        ship(self, std::move(events));
        events.resize(self->state.available);
        self->state.remainder = std::move(remainder);
      } else {
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
