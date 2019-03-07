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

#include "vast/system/archive.hpp"

#include <algorithm>

#include <caf/config_value.hpp>
#include <caf/settings.hpp>

#include "vast/defaults.hpp"
#include "vast/event.hpp"
#include "vast/expected.hpp"
#include "vast/logger.hpp"
#include "vast/segment_store.hpp"
#include "vast/store.hpp"
#include "vast/table_slice.hpp"
#include "vast/to_events.hpp"

#include "vast/concept/printable/stream.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/fill_status_map.hpp"

using std::chrono::duration_cast;
using std::chrono::microseconds;
using std::chrono::steady_clock;
using namespace caf;

namespace vast::system {

void archive_state::send_report() {
  if (measurement.events > 0) {
    using namespace std::string_literals;
    performance_report r = {{{"archive"s, measurement}}};
    measurement = vast::system::measurement{};
    self->send(accountant, std::move(r));
  }
}

archive_type::behavior_type
archive(archive_type::stateful_pointer<archive_state> self, path dir,
        size_t capacity, size_t max_segment_size) {
  // TODO: make the choice of store configurable. For most flexibility, it
  // probably makes sense to pass a unique_ptr<stor> directory to the spawn
  // arguments of the actor. This way, users can provide their own store
  // implementation conveniently.
  VAST_INFO(self, "spawned:", VAST_ARG(capacity), VAST_ARG(max_segment_size));
  self->state.self = self;
  self->state.store = segment_store::make(dir, max_segment_size, capacity);
  VAST_ASSERT(self->state.store != nullptr);
  self->set_exit_handler([=](const exit_msg& msg) {
    self->state.send_report();
    self->state.store->flush();
    self->state.store.reset();
    self->quit(msg.reason);
  });
  self->set_down_handler([=](const down_msg& msg) {
    VAST_DEBUG(self, "received DOWN from", msg.source);
    self->state.active_exporters.erase(msg.source);
  });
  if (auto a = self->system().registry().get(accountant_atom::value)) {
    namespace defs = defaults::system;
    self->state.accountant = actor_cast<accountant_type>(a);
    self->send(self->state.accountant, announce_atom::value, self->name());
    self->delayed_send(self, defs::telemetry_rate, telemetry_atom::value);
  }
  return {[=](const ids& xs) -> caf::result<done_atom, caf::error> {
            VAST_ASSERT(rank(xs) > 0);
            VAST_DEBUG(self, "got query for", rank(xs),
                       "events in range [" << select(xs, 1) << ','
                                           << (select(xs, -1) + 1) << ')');
            if (self->state.active_exporters.count(
                  self->current_sender()->address())
                == 0) {
              VAST_DEBUG(self, "dismisses query for inactive sender");
              return make_error(ec::no_error);
            }
            std::vector<event> result;
            auto session = self->state.store->extract(xs);
            while (true) {
              auto slice = session->next();
              if (!slice) {
                if (!slice.error()) // Either we are done ...
                  break;
                // ... or an error occured.
                return {done_atom::value, std::move(slice.error())};
              }
              using receiver_type = caf::typed_actor<
                caf::reacts_to<table_slice_ptr>>;
              self->send(caf::actor_cast<receiver_type>(self->current_sender()),
                         *slice);
            }
            return {done_atom::value, make_error(ec::no_error)};
          },
          [=](stream<table_slice_ptr> in) {
            self->make_sink(
              in,
              [](unit_t&) {
                // nop
              },
              [=](unit_t&, std::vector<table_slice_ptr>& batch) {
                VAST_DEBUG(self, "got", batch.size(), "table slices");
                auto t = timer::start(self->state.measurement);
                uint64_t events = 0;
                for (auto& slice : batch) {
                  if (auto error = self->state.store->put(slice)) {
                    VAST_ERROR(self, "failed to add table slice to store",
                               self->system().render(error));
                    self->quit(error);
                    break;
                  }
                  events += slice->rows();
                }
                t.stop(events);
              },
              [=](unit_t&, const error& err) {
                if (err) {
                  VAST_ERROR(self,
                             "got a stream error:", self->system().render(err));
                }
              });
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
          },
          [=](telemetry_atom) {
            self->state.send_report();
            namespace defs = defaults::system;
            self->delayed_send(self, defs::telemetry_rate,
                               telemetry_atom::value);
          },
          [=](erase_atom, ids erase) {
            VAST_INFO(self, "erases", rank(erase), "events from its store");
            if (auto err = self->state.store->erase(erase))
              VAST_ERROR(self,
                         "failed to erase events:", self->system().render(err));
          }};
}

} // namespace vast::system
