#include <cstring>
#include <fstream>

#include <caf/all.hpp>

#include "vast/key.h"
#include "vast/actor/atoms.h"
#include "vast/actor/identifier.h"
#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/vast/error.h"
#include "vast/concept/printable/vast/filesystem.h"

namespace vast {
namespace identifier {

state::state(event_based_actor* self)
  : basic_state{self, "identifier"} {
}

state::~state() {
  if (!flush()) {
    VAST_ERROR_AT(self, "failed to save local ID state");
    VAST_ERROR_AT(self, "has", id, "as current ID,", available, "available");
  }
}

bool state::flush() {
  if (id == 0)
    return true;
  if (!exists(dir) && !mkdir(dir))
    return false;
  std::ofstream avail{to_string(dir / "available")};
  if (!avail)
    return false;
  avail << available << std::endl;
  std::ofstream next{to_string(dir / "next")};
  if (!next)
    return false;
  next << id << std::endl;
  return true;
}

behavior actor(stateful_actor<state>* self, caf::actor store, path dir,
               event_id batch_size) {
  self->state.store = std::move(store);
  self->state.dir = std::move(dir);
  self->state.batch_size = batch_size;
  if (exists(self->state.dir)) {
    // Load current batch size.
    std::ifstream available{to_string(self->state.dir / "available")};
    if (!available) {
      VAST_ERROR_AT(self, "failed to open ID batch file:",
                    self->state.dir / "available",
                    '(' << std::strerror(errno) << ')');
      self->quit(exit::error);
      return {};
    }
    available >> self->state.available;
    VAST_INFO_AT(self, "found", self->state.available, "local IDs");
    // Load next ID.
    std::ifstream next{to_string(self->state.dir / "next")};
    if (!next) {
      VAST_ERROR_AT(self, "failed to open ID file:", self->state.dir / "next",
                    '(' << std::strerror(errno) << ')');
      self->quit(exit::error);
      return {};
    }
    next >> self->state.id;
    VAST_INFO_AT(self, "found next event ID:", self->state.id);
  }
  return {
    [=](id_atom) {
      return self->state.id;
    },
    [=](request_atom, event_id n) {
      auto rp = self->make_response_promise();
      if (n == 0) {
        rp.deliver(make_message(error{"cannot hand out 0 ids"}));
        return;
      }
      // If the requester wants more than we can locally offer, we give
      // everything we have, but double the batch size to avoid future
      // shortage.
      if (n > self->state.available) {
        VAST_VERBOSE_AT(self, "got exhaustive request:", n, '>',
                        self->state.available);
        VAST_VERBOSE_AT(self, "doubles batch size:", self->state.batch_size,
                        "->", self->state.batch_size * 2);
        n = self->state.available;
        self->state.batch_size *= 2;
      }
      VAST_DEBUG_AT(self, "hands out [" << self->state.id << ',' <<
                    self->state.id + n << "),", self->state.available - n,
                    "local IDs remaining");
      rp.deliver(make_message(id_atom::value, self->state.id,
                              self->state.id + n));
      self->state.id += n;
      self->state.available -= n;
      // Replenish if we're running low of IDs (or are already out of 'em).
      if (self->state.available == 0 
          || self->state.available < self->state.batch_size * 0.1) {
        // Avoid too frequent replenishing.
        if (time::snapshot() - self->state.last_replenish < time::seconds(10)) {
          VAST_VERBOSE_AT(self, "had to replenish twice within 10 secs");
          VAST_VERBOSE_AT(self, "doubles batch size:", self->state.batch_size,
                          "->", self->state.batch_size * 2);
          self->state.batch_size *= 2;
        }
        self->state.last_replenish = time::snapshot();
        VAST_DEBUG_AT(self, "replenishes local IDs:", self->state.available,
                      "available,", self->state.batch_size, "requested");
        VAST_ASSERT(max_event_id - self->state.id >= self->state.batch_size);
        self->sync_send(self->state.store, add_atom::value, key::str("id"),
                  self->state.batch_size).then(
          [=](event_id old, event_id now) {
            self->state.id = old;
            self->state.available = now - old;
            VAST_VERBOSE_AT(self, "got", self->state.available,
                            "new IDs starting at", old);
            if (!self->state.flush()) {
              self->quit(exit::error);
              VAST_ERROR_AT(self, "failed to save local ID state");
            }
          },
          [=](error const& e) {
            VAST_ERROR_AT(self, "got error:", e);
            VAST_ERROR_AT(self, "failed to obtain", n, "new IDs:");
            self->quit(exit::error);
          },
          quit_on_others(self)
        );
      }
    },
    quit_on_others(self)
  };
}

} // namespace identifier
} // namespace vast
