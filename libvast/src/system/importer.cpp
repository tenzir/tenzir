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
#include "vast/fwd.hpp"
#include "vast/logger.hpp"
#include "vast/si_literals.hpp"
#include "vast/system/type_registry.hpp"
#include "vast/table_slice.hpp"

#include <caf/atom.hpp>
#include <caf/attach_continuous_stream_stage.hpp>
#include <caf/config_value.hpp>
#include <caf/dictionary.hpp>

#include <fstream>

namespace vast::system {

importer_state::importer_state(caf::event_based_actor* self_ptr)
  : self(self_ptr) {
  // nop
}

importer_state::~importer_state() {
  write_state(write_mode::with_next);
}

caf::error importer_state::read_state() {
  auto file = dir / "current_id_block";
  if (exists(file)) {
    VAST_VERBOSE(self, "reads persistent state from", file);
    std::ifstream state_file{to_string(file)};
    state_file >> current.end;
    state_file >> current.next;
    // TODO:
    if (!state_file) {
      VAST_WARNING(self, "did not find next id position from the state file; "
                         "detected an irregular shutdown");
      current.next = current.end;
    }
  } else {
    VAST_VERBOSE(self, "did not find a state file at", file);
    current.end = 0;
    current.next = 0;
  }
  return get_next_block();
}

caf::error importer_state::write_state(write_mode mode) {
  if (!exists(dir)) {
    auto result = mkdir(dir);
    if (!result)
      return std::move(result.error());
  }
  std::ofstream state_file{to_string(dir / "current_id_block")};
  state_file << current.end;
  if (mode == write_mode::with_next) {
    state_file << " " << current.next;
    VAST_VERBOSE(self, "persisted id block [", current.next, ",", current.end,
                 ")");
  } else {
    VAST_VERBOSE(self, "persisted id block boundary at", current.end);
  }
  return caf::none;
}

caf::error importer_state::get_next_block() {
  using namespace si_literals;
  while (current.next >= current.end)
    current.end += 8_Mi;
  return write_state(write_mode::without_next);
}

id importer_state::next_id(uint64_t advance) {
  id pre = current.next;
  current.next += advance;
  if (current.next >= current.end)
    get_next_block();
  return pre;
}

id importer_state::available_ids() const noexcept {
  return max_id - current.next;
}

caf::dictionary<caf::config_value> importer_state::status() const {
  caf::dictionary<caf::config_value> result;
  // Misc parameters.
  result.emplace("available-ids", available_ids());
  result.emplace("next-id", current.next);
  result.emplace("block-boundary", current.end);
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
  if (auto a = self->system().registry().get(atom::accountant_v)) {
    self->state.accountant = caf::actor_cast<accountant_type>(a);
    self->send(self->state.accountant, atom::announce_v, self->name());
    self->delayed_send(self, defs::telemetry_rate, atom::telemetry_v);
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
      VAST_ASSERT(x->rows() <= static_cast<size_t>(st.available_ids()));
      auto events = x->rows();
      x.unshared().offset(st.next_id(events));
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
    [=](atom::index, const caf::actor& index) {
      VAST_DEBUG(self, "registers index", index);
      self->state.index_actors.emplace_back(index);
      // TODO: currently, the subscriber expects only a single 'flush'
      // message.
      //       Adding multiple INDEX actors will cause the subscriber to
      //       receive more than one 'flush'  message, but the subscriber
      //       only expects one and will stop waiting after the first one.
      //       Once we support multiple INDEX actors at the IMPORTER, we
      //       also need to revise the signaling of these 'flush' messages.
      if (self->state.index_actors.size() > 1)
        VAST_WARNING(self, "registered more than one INDEX actor",
                     "(currently unsupported!)");
      return self->state.stg->add_outbound_path(index);
    },
    [=](atom::exporter, const caf::actor& exporter) {
      VAST_DEBUG(self, "registers exporter", exporter);
      return self->state.stg->add_outbound_path(exporter);
    },
    [=](caf::stream<importer_state::input_type>& in) {
      auto& st = self->state;
      VAST_DEBUG(self, "adds a new source:", self->current_sender());
      st.stg->add_inbound_path(in);
    },
    [=](atom::add, const caf::actor& subscriber) {
      auto& st = self->state;
      VAST_DEBUG(self, "adds a new sink:", self->current_sender());
      st.stg->add_outbound_path(subscriber);
    },
    [=](atom::subscribe, atom::flush, caf::actor& listener) {
      auto& st = self->state;
      VAST_ASSERT(st.stg != nullptr);
      for (auto& next : st.index_actors)
        self->send(next, atom::subscribe_v, atom::flush_v, listener);
    },
    [=](atom::status) { return self->state.status(); },
    [=](atom::telemetry) {
      self->state.send_report();
      self->delayed_send(self, defs::telemetry_rate, atom::telemetry_v);
    },
  };
}

} // namespace vast::system
