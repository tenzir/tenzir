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

#include "vast/system/importer.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/filesystem.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/fill_status_map.hpp"
#include "vast/detail/notifying_stream_manager.hpp"
#include "vast/fwd.hpp"
#include "vast/logger.hpp"
#include "vast/system/atoms.hpp"
#include "vast/system/type_registry.hpp"
#include "vast/table_slice.hpp"

#include <caf/atom.hpp>
#include <caf/attach_continuous_stream_stage.hpp>
#include <caf/config_value.hpp>
#include <caf/dictionary.hpp>

#include <fstream>

namespace vast::system {

using caf::subscribe_atom, caf::flush_atom, caf::add_atom;

importer_state::importer_state(caf::event_based_actor* self_ptr)
  : self(self_ptr) {
  // nop
}

importer_state::~importer_state() {
  write_state();
}

caf::error importer_state::read_state() {
  auto file = dir / "next_id";
  if (exists(file)) {
    VAST_VERBOSE(self, "reads persistent state from", file);
    std::ifstream available{to_string(file)};
    available >> next_id;
    if (!available.eof())
      VAST_ERROR(self, "got an invalidly formatted persistence file:", file);
  } else {
    VAST_VERBOSE(self, "did not find a state file at", file);
  }
  return caf::none;
}

caf::error importer_state::write_state() {
  if (!exists(dir)) {
    auto result = mkdir(dir);
    if (!result)
      return std::move(result.error());
  }
  std::ofstream available{to_string(dir / "next_id")};
  available << next_id;
  VAST_VERBOSE(self, "persisted id space caret at", next_id);
  return caf::none;
}

id importer_state::available_ids() const noexcept {
  return max_id - next_id;
}

caf::dictionary<caf::config_value> importer_state::status() const {
  caf::dictionary<caf::config_value> result;
  // Misc parameters.
  result.emplace("available-ids", available_ids());
  result.emplace("next-id", next_id);
  // General state such as open streams.
  detail::fill_status_map(result, self);
  return result;
}

void importer_state::send_report() {
  auto now = stopwatch::now();
  if (measurement_.events > 0) {
    using namespace std::string_literals;
    auto elapsed = std::chrono::duration_cast<duration>(now - last_report);
    auto node_throughput = measurement{elapsed, measurement_.events};
    auto r = performance_report{
      {{"importer"s, measurement_}, {"node_throughput"s, node_throughput}}};
    measurement_ = measurement{};
    self->send(accountant, std::move(r));
  }
  last_report = now;
}

void importer_state::notify_flush_listeners() {
  VAST_DEBUG(self, "forwards 'flush' subscribers to", index_actors.size(),
             "INDEX actors");
  for (auto& listener : flush_listeners)
    for (auto& next : index_actors)
      self->send(next, subscribe_atom::value, flush_atom::value, listener);
  flush_listeners.clear();
}

caf::behavior importer(importer_actor* self, path dir, archive_type archive,
                       caf::actor index, type_registry_type type_registry) {
  VAST_TRACE(VAST_ARG(dir));
  self->state.dir = dir;
  auto err = self->state.read_state();
  if (err) {
    VAST_ERROR(self, "failed to load state:", self->system().render(err));
    self->quit(std::move(err));
    return {};
  }
  namespace defs = defaults::system;
  if (auto a = self->system().registry().get(accountant_atom::value)) {
    self->state.accountant = caf::actor_cast<accountant_type>(a);
    self->send(self->state.accountant, announce_atom::value, self->name());
    self->delayed_send(self, defs::telemetry_rate, telemetry_atom::value);
    self->state.last_report = stopwatch::now();
  }
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    self->state.send_report();
    self->quit(msg.reason);
  });
  self->state.stg = caf::attach_continuous_stream_stage(
    self,
    [](caf::unit_t&) {
      // nop
    },
    [=](caf::unit_t&, caf::downstream<table_slice_ptr>& out,
        table_slice_ptr x) {
      VAST_TRACE(VAST_ARG(x));
      auto& st = self->state;
      auto t = timer::start(st.measurement_);
      VAST_DEBUG(self, "has", st.available_ids(), "IDs available");
      VAST_ASSERT(x->rows() <= static_cast<size_t>(st.available_ids()));
      auto events = x->rows();
      auto advance = st.next_id + events;
      x.unshared().offset(st.next_id);
      st.next_id = advance;
      out.push(std::move(x));
      t.stop(events);
    },
    [=](caf::unit_t&, const error& err) {
      VAST_DEBUG(self, "stopped with message:", err);
    });
  if (type_registry)
    self->state.stg->add_outbound_path(type_registry);
  if (archive)
    self->state.stg->add_outbound_path(archive);
  if (index) {
    self->state.index_actors.emplace_back(index);
    self->state.stg->add_outbound_path(index);
  }
  return {
    [=](const archive_type& archive) {
      VAST_DEBUG(self, "registers archive", archive);
      return self->state.stg->add_outbound_path(archive);
    },
    [=](index_atom, const caf::actor& index) {
      VAST_DEBUG(self, "registers index", index);
      self->state.index_actors.emplace_back(index);
      // TODO: currently, the subscriber expects only a single 'flush' message.
      //       Adding multiple INDEX actors will cause the subscriber to
      //       receive more than one 'flush'  message, but the subscriber only
      //       expects one and will stop waiting after the first one. Once we
      //       support multiple INDEX actors at the IMPORTER, we also need to
      //       revise the signaling of these 'flush' messages.
      if (self->state.index_actors.size() > 1)
        VAST_WARNING(self, "registered more than one INDEX actor",
                     "(currently unsupported!)");
      return self->state.stg->add_outbound_path(index);
    },
    [=](exporter_atom, const caf::actor& exporter) {
      VAST_DEBUG(self, "registers exporter", exporter);
      return self->state.stg->add_outbound_path(exporter);
    },
    [=](caf::stream<importer_state::input_type>& in) {
      auto& st = self->state;
      VAST_DEBUG(self, "adds a new source:", self->current_sender());
      st.stg->add_inbound_path(in);
    },
    [=](add_atom, const caf::actor& subscriber) {
      auto& st = self->state;
      VAST_DEBUG(self, "adds a new sink:", self->current_sender());
      st.stg->add_outbound_path(subscriber);
    },
    [=](subscribe_atom, flush_atom, caf::actor& listener) {
      auto& st = self->state;
      VAST_ASSERT(st.stg != nullptr);
      st.flush_listeners.emplace_back(std::move(listener));
      detail::notify_listeners_if_clean(st, *st.stg);
    },
    [=](status_atom) { return self->state.status(); },
    [=](telemetry_atom) {
      self->state.send_report();
      self->delayed_send(self, defs::telemetry_rate, telemetry_atom::value);
    },
  };
}

} // namespace vast::system
