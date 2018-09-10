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

#pragma once

#include <cstdint>
#include <chrono>
#include <vector>

#include "vast/logger.hpp"

#include <caf/behavior.hpp>
#include <caf/stateful_actor.hpp>

#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/event.hpp"

#include "vast/system/atoms.hpp"
#include "vast/system/query_statistics.hpp"

namespace vast::system {

#if 0
/// The *Writer* concept.
struct Writer {
  Writer();

  expected<void> write(const event&);

  expected<void> flush();

  const char* name() const;
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
  const char* name = "writer";
};

template <class Writer>
caf::behavior sink(caf::stateful_actor<sink_state<Writer>>* self,
                   Writer&& writer, uint64_t limit) {
  using namespace std::chrono;
  self->state.writer = std::move(writer);
  self->state.name = self->state.writer.name();
  self->state.last_flush = steady_clock::now();
  if (limit > 0) {
    VAST_DEBUG(self, "caps event export at", limit, "events");
    self->state.limit = limit;
  }
  return {
    [=](const std::vector<event>& xs) {
      for (auto& x : xs) {
        auto r = self->state.writer.write(x);
        if (!r) {
          VAST_ERROR(self, self->system().render(r.error()));
          self->quit(r.error());
          return;
        }
        if (++self->state.processed == self->state.limit) {
          VAST_INFO(self, "reached limit:", self->state.limit, "events");
          self->quit();
          return;
        }
        auto now = steady_clock::now();
        if (now - self->state.last_flush > self->state.flush_interval) {
          self->state.writer.flush();
          self->state.last_flush = now;
        }
      }
    },
    [=](const uuid& id, const query_statistics&) {
      VAST_IGNORE_UNUSED(id);
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

} // namespace vast::system

