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
#include "vast/event_types.hpp"
#include "vast/load.hpp"
#include "vast/logger.hpp"
#include "vast/save.hpp"
#include "vast/system/report.hpp"
#include "vast/status.hpp"
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

caf::dictionary<caf::config_value>
type_registry_state::status(status_verbosity v) const {
  auto s = vast::status{};
  // Sorted list of all keys.
  auto keys = std::vector<std::string>(data.size());
  std::transform(data.begin(), data.end(), keys.begin(),
                 [](const auto& x) { return x.first; });
  std::sort(keys.begin(), keys.end());
  caf::put(s.debug, "type-registry.types", keys);
  // The usual per-component status.
  detail::fill_status_map(s.debug, self);
  return join(s);
}

vast::path type_registry_state::filename() const {
  return dir / name;
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

void type_registry_state::insert(vast::type layout) {
  auto& cont = data[layout.name()].value;
  if ([[maybe_unused]] auto [hint, success]
      = cont.insert(flatten(std::move(layout)));
      success)
    VAST_VERBOSE(self, "registered", hint->name());
}

type_set type_registry_state::types() const {
  auto result = std::unordered_set<vast::type>{};
  // TODO: Replace merging logic once libc++ implements unordered_set::merge.
  //   result.merge(data);
  for ([[maybe_unused]] auto& [k, v] : data)
    for (auto& x : v.value)
      result.insert(x);
  return {result};
}

type_set type_registry_state::types(std::string key) const {
  if (auto it = data.find(std::move(key)); it != data.end())
    return it->second;
  return {};
}

type_registry_behavior
type_registry(type_registry_actor self, const path& dir) {
  self->state.self = self;
  self->state.dir = dir;
  // Register the exit handler.
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    VAST_DEBUG(self, "got EXIT from", msg.source);
    if (auto telemetry = self->state.telemetry(); !telemetry.empty())
      self->send(self->state.accountant, std::move(telemetry));
    if (auto err = self->state.save_to_disk())
      VAST_ERROR(
        self, "failed to persist state to disk:", self->system().render(err));
    self->quit(msg.reason);
  });
  // Load existing state from disk if possible.
  if (auto err = self->state.load_from_disk())
    self->quit(std::move(err));
  // Load loaded schema types from the singleton.
  auto schema = vast::event_types::get();
  if (schema)
    self->send(self, atom::put_v, *schema);
  // The behavior of the type-registry.
  return {
    [=](atom::telemetry) {
      if (auto telemetry = self->state.telemetry(); !telemetry.empty()) {
        VAST_TRACE(self, "sends out a telemetry report to the",
                   VAST_ARG("accountant", self->state.accountant));
        self->send(self->state.accountant, std::move(telemetry));
      }
      self->delayed_send(self, defaults::system::telemetry_rate,
                         atom::telemetry_v);
    },
    [=](atom::status, status_verbosity v) {
      VAST_TRACE(self, "sends out a status report");
      return self->state.status(v);
    },
    [=](caf::stream<table_slice_ptr> in) {
      VAST_TRACE(self, "attaches to", VAST_ARG("stream", in));
      caf::attach_stream_sink(
        self, in,
        [=](caf::unit_t&) {
          // nop
        },
        [=](caf::unit_t&, table_slice_ptr x) {
          self->state.insert(std::move(x->layout()));
        });
    },
    [=](atom::put, vast::type x) {
      VAST_TRACE(self, "tries to add", VAST_ARG("type", x.name()));
      self->state.insert(std::move(x));
    },
    [=](atom::put, vast::schema x) {
      VAST_TRACE(self, "tries to add", VAST_ARG("schema", x));
      for (auto& type : x)
        self->state.insert(std::move(type));
    },
    [=](atom::get) {
      VAST_TRACE(self, "retrieves a list of all known types");
      return self->state.types();
    },
    [=](atom::get, std::string name) {
      VAST_TRACE(self, "retrieves a list of all known types for",
                 VAST_ARG(name));
      return self->state.types(name);
    },
    [=](accountant_type accountant) {
      VAST_ASSERT(accountant);
      VAST_DEBUG(self, "connects to", VAST_ARG(accountant));
      self->state.accountant = caf::actor_cast<accountant_type>(accountant);
      self->send(self->state.accountant, atom::announce_v, self->name());
      self->delayed_send(self, defaults::system::telemetry_rate,
                         atom::telemetry_v);
    }};
}

} // namespace vast::system
