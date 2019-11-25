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

#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/default_table_slice.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/error.hpp"
#include "vast/event.hpp"
#include "vast/expression.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/logger.hpp"
#include "vast/schema.hpp"
#include "vast/system/accountant.hpp"
#include "vast/system/atoms.hpp"
#include "vast/system/source.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/table_slice_builder_factory.hpp"

#include <caf/actor_system_config.hpp>
#include <caf/broadcast_downstream_manager.hpp>
#include <caf/downstream.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/expected.hpp>
#include <caf/io/broker.hpp>
#include <caf/none.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/stream_source.hpp>
#include <caf/streambuf.hpp>

#include <unordered_map>

namespace vast::system {

template <class Reader>
struct datagram_source_state : source_state<Reader, caf::io::broker> {
  // -- member types -----------------------------------------------------------

  using super = source_state<Reader, caf::io::broker>;

  // -- constructors, destructors, and assignment operators --------------------

  using super::super;

  // -- member variables -------------------------------------------------------

  /// Shuts down the stream manager when `true`.
  bool done = false;

  /// Containes the amount of dropped packets since the last heartbeat.
  size_t dropped_packets = 0;

  /// Timestamp when the source was started.
  caf::timestamp start_time;
};

template <class Reader>
using datagram_source_actor = caf::stateful_actor<datagram_source_state<Reader>,
                                                  caf::io::broker>;

/// An event producer.
/// @tparam Reader The concrete source implementation.
/// @param self The actor handle.
/// @param reader The reader instance.
template <class Reader>
caf::behavior datagram_source(datagram_source_actor<Reader>* self,
                              uint16_t udp_listening_port, Reader reader,
                              size_t table_slice_size,
                              caf::optional<size_t> max_events) {
  using namespace caf;
  using namespace std::chrono;
  namespace defs = defaults::system;
  // Try to open requested UDP port.
  auto udp_res = self->add_udp_datagram_servant(udp_listening_port);
  if (!udp_res) {
    VAST_ERROR(self, "could not open port", udp_listening_port);
    self->quit(std::move(udp_res.error()));
    return {};
  }
  VAST_DEBUG(self, "starts listening at port", udp_res->second);
  // Initialize state.
  self->state.init(std::move(reader), std::move(max_events));
  // Spin up the stream manager for the source.
  self->state.mgr = self->make_continuous_source(
    // init
    [=](caf::unit_t&) { self->state.start_time = system_clock::now(); },
    // get next element
    [](caf::unit_t&, downstream<table_slice_ptr>&, size_t) {
      // nop, new slices are generated in the new_datagram_msg handler
    },
    // done?
    [=](const caf::unit_t&) { return self->state.done; });
  return {
    [=](caf::io::new_datagram_msg& msg) {
      // Check whether we can buffer more slices in the stream.
      VAST_DEBUG(self, "got a new datagram of size", msg.buf.size());
      auto& st = self->state;
      auto t = timer::start(st.measurement_);
      auto capacity = st.mgr->out().capacity();
      if (capacity == 0) {
        st.dropped_packets++;
        return;
      }
      // Extract events until the source has exhausted its input or until
      // we have completed a batch.
      caf::arraybuf<> buf{msg.buf.data(), msg.buf.size()};
      st.reader.reset(std::make_unique<std::istream>(&buf));
      auto push_slice = [&](table_slice_ptr slice) {
        VAST_DEBUG(self, "produced a slice with", slice->rows(), "rows");
        st.mgr->out().push(std::move(slice));
      };
      auto events = detail::opt_min(st.remaining, capacity * table_slice_size);
      auto [err, produced] = st.reader.read(events, table_slice_size,
                                            push_slice);
      t.stop(produced);
      if (st.remaining) {
        VAST_ASSERT(*st.remaining >= produced);
        *st.remaining -= produced;
        if (*st.remaining == 0)
          st.done = true;
      }
      if (err != caf::none && err != ec::end_of_input)
        VAST_WARNING(self,
                     "has not enough capacity left in stream, dropping input!");
      if (produced > 0)
        st.mgr->push();
      if (st.done)
        st.send_report();
    },
    [=](accountant_type accountant) {
      VAST_DEBUG(self, "sets accountant to", accountant);
      auto& st = self->state;
      st.accountant = std::move(accountant);
      self->send(st.accountant, "source.start", st.start_time);
      self->send(st.accountant, announce_atom::value, st.name);
      // Start the heartbeat loop
      self->delayed_send(self, defs::telemetry_rate, telemetry_atom::value);
    },
    [=](sink_atom, const actor& sink) {
      // TODO: Currently, we use a broadcast downstream manager. We need to
      //       implement an anycast downstream manager and use it for the
      //       source, because we mustn't duplicate data.
      VAST_ASSERT(sink != nullptr);
      VAST_DEBUG(self, "registers sink", sink);
      // Start streaming.
      self->state.mgr->add_outbound_path(sink);
    },
    [=](get_atom, schema_atom) -> result<schema> {
      return self->state.reader.schema();
    },
    [=](put_atom, schema& sch) -> result<void> {
      if (auto err = self->state.reader.schema(std::move(sch)))
        return err;
      return caf::unit;
    },
    [=](expression& expr) {
      VAST_DEBUG(self, "sets filter expression to:", expr);
      self->state.filter = std::move(expr);
    },
    [=](telemetry_atom) {
      auto& st = self->state;
      st.send_report();
      if (st.dropped_packets > 0) {
        VAST_WARNING(self, "has no capacity left in stream and dropped",
                     st.dropped_packets, "packets");
        st.dropped_packets = 0;
      }
      if (!st.done)
        self->delayed_send(self, defs::telemetry_rate, telemetry_atom::value);
    }};
}

} // namespace vast::system
