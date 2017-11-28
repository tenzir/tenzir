#ifndef VAST_SYSTEM_SINK_HPP
#define VAST_SYSTEM_SINK_HPP

#include <cstdint>
#include <chrono>
#include <vector>

#include "vast/logger.hpp"

#include <caf/stateful_actor.hpp>

#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/event.hpp"

#include "vast/system/atoms.hpp"
#include "vast/system/query_statistics.hpp"

namespace vast {
namespace system {

#if 0
/// The *Writer* concept.
struct Writer {
  Writer();

  expected<void> write(event const&);

  expected<void> flush();

  char const* name() const;
};
#endif

// The base class for SINK actors.
template <class Writer>
struct sink_state {
  std::chrono::steady_clock::duration flush_interval = std::chrono::seconds(1);
  std::chrono::steady_clock::time_point last_flush;
  uint64_t processed = 0;
  uint64_t limit = 0;
  Writer writer;
  const char* name;
};

template <class Writer>
caf::behavior
sink(caf::stateful_actor<sink_state<Writer>>* self, Writer&& writer) {
  using namespace std::chrono;
  self->state.writer = std::move(writer);
  self->state.name = self->state.writer.name();
  self->state.last_flush = steady_clock::now();
  return {
    [=](const std::vector<event>& xs) {
      if (self->state.processed + xs.size() <= self->state.limit) {
        // Process batch as a whole.
        auto r = self->state.writer.write(xs);
        if (!r) {
          VAST_ERROR(self->system().render(r.error()));
          self->quit(r.error());
          return;
        }
      } else {
        for (auto& x : xs) {
          auto r = self->state.writer.write(x);
          if (!r) {
            VAST_ERROR(self->system().render(r.error()));
            self->quit(r.error());
            return;
          }
          if (++self->state.processed == self->state.limit) {
            VAST_INFO(self, "reached limit:", self->state.limit, "events");
            self->quit();
            return;
          }
        }
      }
      auto now = steady_clock::now();
      if (now - self->state.last_flush > self->state.flush_interval) {
        self->state.writer.flush();
        self->state.last_flush = now;
      }
    },
    [=](const uuid& id, const query_statistics&) {
      VAST_DEBUG(self, "got query statistics from", id);
    },
    [=](limit_atom, uint64_t max) {
      VAST_DEBUG(self, "caps event export at", max, "events");
      if (self->state.processed < max)
        self->state.limit = max;
      else
        VAST_WARNING(self, "ignores new limit of", max, "(already processed",
                     self->state.processed, " events)");
    },
  };
}

} // namespace system
} // namespace vast

#endif
