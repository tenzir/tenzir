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

#include <caf/config_value.hpp>

#include "vast/event.hpp"
#include "vast/expected.hpp"
#include "vast/logger.hpp"
#include "vast/segment_store.hpp"
#include "vast/store.hpp"
#include "vast/table_slice.hpp"
#include "vast/to_events.hpp"

#include "vast/concept/printable/stream.hpp"

#include "vast/system/archive.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/fill_status_map.hpp"

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
  VAST_INFO(self, "spawned:", VAST_ARG(capacity), VAST_ARG(max_segment_size));
  self->state.store = segment_store::make(
    self->system(), dir, max_segment_size, capacity);
  VAST_ASSERT(self->state.store != nullptr);
  self->set_exit_handler(
    [=](const exit_msg& msg) {
      self->state.store->flush();
      self->state.store.reset();
      self->quit(msg.reason);
    }
  );
  self->set_down_handler(
    [=](const down_msg& msg) {
      VAST_DEBUG(self, "received DOWN from", msg.source);
      self->state.active_exporters.erase(msg.source);
    }
  );
  return {
    [=](const ids& xs) -> caf::result<std::vector<event>> {
      VAST_ASSERT(rank(xs) > 0);
      VAST_DEBUG(self, "got query for", rank(xs), "events in range ["
                 << select(xs, 1) << ',' << (select(xs, -1) + 1) << ')');
      if (self->state.active_exporters.count(self->current_sender()->address())
          == 0) {
        VAST_DEBUG(self, "dismisses query for inactive sender");
        return make_error(ec::no_error);
      }
      std::vector<event> result;
      auto slices = self->state.store->get(xs);
      if (!slices)
        VAST_DEBUG(self, "failed to lookup IDs in store:",
                   self->system().render(slices.error()));
      else
        for (auto& slice : *slices)
          to_events(result, *slice, xs);
      return result;
    },
    [=](stream<table_slice_ptr> in) {
      self->make_sink(
        in,
        [](unit_t&) {
          // nop
        },
        [=](unit_t&, std::vector<table_slice_ptr>& batch) {
          VAST_DEBUG(self, "got", batch.size(), "table slices");
          for (auto& slice : batch) {
            if (auto error = self->state.store->put(slice)) {
              VAST_ERROR(self, "failed to add table slice to store",
                         self->system().render(error));
              self->quit(error);
              break;
            }
          }
        },
        [=](unit_t&, const error& err) {
          if (err) {
            VAST_ERROR(self, "got a stream error:", self->system().render(err));
          }
        }
      );
    },
    [=](exporter_atom, const actor& exporter) {
      auto sender_addr = self->current_sender()->address();
      self->state.active_exporters.insert(sender_addr);
      self->monitor<caf::message_priority::high>(exporter);
    },
    [=](status_atom) {
      caf::dictionary<caf::config_value> result;
      detail::fill_status_map(result, self);
      self->state.store->inspect_status(put_dictionary(result, "store"));
      return result;
    }
  };
}

} // namespace vast::system
