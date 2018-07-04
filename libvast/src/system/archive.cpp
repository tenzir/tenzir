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

#include <algorithm>

#include "vast/batch.hpp"
#include "vast/event.hpp"
#include "vast/expected.hpp"
#include "vast/logger.hpp"
#include "vast/segment_store.hpp"
#include "vast/store.hpp"
#include "vast/table_slice.hpp"

#include "vast/concept/printable/stream.hpp"

#include "vast/system/archive.hpp"

#include "vast/detail/assert.hpp"

using std::chrono::steady_clock;
using std::chrono::duration_cast;
using std::chrono::microseconds;
using namespace caf;

namespace vast::system {

archive_type::behavior_type
archive(archive_type::stateful_pointer<archive_state> self,
        path dir, size_t capacity, size_t max_segment_size) {
  // TODO: make the choice of store configurable. For most flexibility, it
  // probably makes sense to pass a unique_ptr<stor> directory to the spawn
  // arguments of the actor. This way, users can provide their own store
  // implementation conveniently.
  self->state.store = std::make_unique<segment_store>(dir, max_segment_size,
                                                      capacity);
  self->set_exit_handler(
    [=](const exit_msg& msg) {
      self->state.store->flush();
      self->quit(msg.reason);
    }
  );
  auto handle_batch = [=](const std::vector<event>& xs) {
    auto first_id = xs.front().id();
    auto last_id  = xs.back().id();
    VAST_DEBUG(self, "got", xs.size(),
               "events [" << first_id << ',' << (last_id + 1) << ')');
    auto result = self->state.store->put(xs);
    if (!result) {
      VAST_ERROR(self, "failed to store events:",
                 self->system().render(result.error()));
      self->quit(result.error());
    }
  };
  return {
    [=](const ids& xs) {
      VAST_ASSERT(rank(xs) > 0);
      VAST_DEBUG(self, "got query for", rank(xs), "events in range ["
                 << select(xs, 1) << ',' << (select(xs, -1) + 1) << ')');
      auto result = self->state.store->get(xs);
      if (result)
        VAST_DEBUG(self, "delivers", result->size(), "events");
      else
        VAST_DEBUG(self, "failed to get events:",
                   self->system().render(result.error()));
      return result;
    },
    [=](stream<const_table_slice_ptr> in) {
      self->make_sink(
        in,
        [](unit_t&) {
          // nop
        },
        [=](unit_t&, std::vector<const_table_slice_ptr>& batch) {
          // TODO: port store to table slice API (#3214)
          for (auto& slice : batch)
            handle_batch(slice->rows_to_events());
        },
        [=](unit_t&, const error& err) {
          if (err) {
            VAST_ERROR(self, "got a stream error:", self->system().render(err));
          }
        }
      );
    },
    [=](const std::vector<event>& xs) {
      handle_batch(xs);
    },
  };
}

} // namespace vast::system
