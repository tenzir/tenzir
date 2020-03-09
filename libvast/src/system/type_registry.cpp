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

#include "vast/system/type_registry.hpp"

#include "vast/defaults.hpp"
#include "vast/detail/fill_status_map.hpp"
#include "vast/error.hpp"
#include "vast/load.hpp"
#include "vast/logger.hpp"
#include "vast/save.hpp"
#include "vast/table_slice.hpp"

#include <caf/atom.hpp>
#include <caf/attach_stream_sink.hpp>
#include <caf/expected.hpp>
#include <caf/settings.hpp>

namespace vast::system {

report type_registry_state::telemetry() const {
  // TODO: Generate a status report for the accountant.
  return {};
}

caf::dictionary<caf::config_value> type_registry_state::status() const {
  caf::dictionary<caf::config_value> result;
  // TODO: Add some useful information here for the status command.
  caf::put_dictionary(result, "statistics")
    .emplace("count", self->state.data.size());
  detail::fill_status_map(result, self);
  return result;
}

vast::path type_registry_state::filename() const {
  return dir / "type-registry";
}

caf::error type_registry_state::save_to_disk() const {
  return vast::save(&self->system(), filename(), data);
}

caf::error type_registry_state::load_from_disk() {
  // Nothing to load is not an error.
  if (!exists(dir)) {
    VAST_DEBUG(self, "found no directory to load from");
    return caf::none;
  }
  if (auto fname = filename(); exists(fname)) {
    if (auto err = load(&self->system(), fname, data))
      return err;
    VAST_DEBUG(self, "loaded state from disk");
  }
  return caf::none;
}

void type_registry_state::insert(vast::record_type layout) {
  [[maybe_unused]] auto [hint, success]
    = data[layout.name()].insert(std::move(layout));
  if (success)
    VAST_DEBUG(self, "registered", *hint);
}

std::unordered_set<vast::type>
type_registry_state::lookup(std::string key) const {
  if (auto it = data.find(std::move(key)); it != data.end())
    return it->second;
  return {};
}

type_registry_behavior
type_registry(type_registry_actor self, const path& dir) {
  self->state.self = self;
  self->state.dir = dir;
  if (auto accountant = self->system().registry().get(accountant_atom::value)) {
    VAST_DEBUG(self, "connects to", VAST_ARG(accountant));
    self->state.accountant = caf::actor_cast<accountant_type>(accountant);
    self->send(self->state.accountant, announce_atom::value, self->name());
    self->delayed_send(self, defaults::system::telemetry_rate,
                       telemetry_atom::value);
  }
  // Load from disk if possible.
  if (auto err = self->state.load_from_disk())
    self->quit(std::move(err));
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    VAST_DEBUG(self, "received exit from", msg.source,
               "with reason:", msg.reason);
    if (self->state.accountant)
      self->send(self->state.accountant, self->state.telemetry());
    if (auto err = self->state.save_to_disk())
      VAST_ERROR(
        self, "failed to persist state to disk:", self->system().render(err));
    self->quit(msg.reason);
  });
  return {[=](telemetry_atom) {
            // Send out a heartbeat.
            self->send(self->state.accountant, self->state.telemetry());
            self->delayed_send(self, defaults::system::telemetry_rate,
                               telemetry_atom::value);
          },
          [=](status_atom) {
            // Send out a status report.
            return self->state.status();
          },
          [=](caf::stream<table_slice_ptr> in) {
            // Store layout of every incoming table slice.
            caf::attach_stream_sink(
              self, in,
              [=](caf::unit_t&) { VAST_DEBUG(self, "initialized stream"); },
              [=](caf::unit_t&, table_slice_ptr x) {
                VAST_TRACE(self, "received new table slice");
                self->state.insert(std::move(x->layout()));
              });
          },
          [=](std::string name) {
            // Retrieve a list of known states for a name.
            return self->state.lookup(name);
          }};
}

} // namespace vast::system
