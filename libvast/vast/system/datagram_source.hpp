// SPDX-FileCopyrightText: (c) 2018 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/logger.hpp"
#include "vast/schema.hpp"
#include "vast/system/source.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder_factory.hpp"

#include <caf/downstream.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/expected.hpp>
#include <caf/io/broker.hpp>
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
using datagram_source_actor
  = caf::stateful_actor<datagram_source_state<Reader>, caf::io::broker>;

/// An event producer.
/// @tparam Reader The concrete source implementation.
/// @param self The actor handle.
/// @param udp_listening_port The requested port.
/// @param reader The reader instance.
/// @param table_slice_size The maximum size for a table slice.
/// @param max_events The optional maximum amount of events to import.
/// @param type_registry The actor handle for the type-registry component.
/// @oaram local_schema Additional local schemas to consider.
/// @param type_filter Restriction for considered types.
/// @param accountant_actor The actor handle for the accountant component.
template <class Reader>
caf::behavior
datagram_source(datagram_source_actor<Reader>* self,
                uint16_t udp_listening_port, Reader reader,
                size_t table_slice_size, caf::optional<size_t> max_events,
                type_registry_actor type_registry, vast::schema local_schema,
                std::string type_filter, accountant_actor accountant) {
  // Try to open requested UDP port.
  auto udp_res = self->add_udp_datagram_servant(udp_listening_port);
  if (!udp_res) {
    VAST_ERROR("{} could not open port {}", self, udp_listening_port);
    self->quit(std::move(udp_res.error()));
    return {};
  }
  VAST_DEBUG("{} starts listening at port {}", self, udp_res->second);
  // Initialize state.
  self->state.init(self, std::move(reader), std::move(max_events),
                   std::move(type_registry), std::move(local_schema),
                   std::move(type_filter), std::move(accountant),
                   table_slice_size);
  // Spin up the stream manager for the source.
  self->state.mgr = self->make_continuous_source(
    // init
    [self](caf::unit_t&) {
      self->state.start_time = std::chrono::system_clock::now();
    },
    // get next element
    [](caf::unit_t&, caf::downstream<table_slice>&, size_t) {
      // nop, new slices are generated in the new_datagram_msg handler
    },
    // done?
    [self](const caf::unit_t&) { return self->state.done; });
  return {
    [self](caf::io::new_datagram_msg& msg) {
      // Check whether we can buffer more slices in the stream.
      VAST_DEBUG("{} got a new datagram of size {}", self, msg.buf.size());
      auto t = timer::start(self->state.metrics);
      auto capacity = self->state.mgr->out().capacity();
      if (capacity == 0) {
        self->state.dropped_packets++;
        return;
      }
      // Extract events until the source has exhausted its input or until
      // we have completed a batch.
      caf::arraybuf<> buf{msg.buf.data(), msg.buf.size()};
      self->state.reader.reset(std::make_unique<std::istream>(&buf));
      auto push_slice = [&](table_slice slice) {
        VAST_DEBUG("{} produced a slice with {} rows", self, slice.rows());
        self->state.mgr->out().push(std::move(slice));
      };
      auto events = capacity * self->state.table_slice_size;
      if (self->state.requested)
        events = std::min(events, *self->state.requested - self->state.count);
      auto [err, produced] = self->state.reader.read(
        events, self->state.table_slice_size, push_slice);
      t.stop(produced);
      self->state.count += produced;
      if (self->state.requested && self->state.count >= *self->state.requested)
        self->state.done = true;
      if (err != caf::none && err != ec::end_of_input)
        VAST_WARN("{} has not enough capacity left in stream, dropping "
                  "input!",
                  self);
      if (produced > 0)
        self->state.mgr->push();
      if (self->state.done)
        self->state.send_report();
    },
    [self](accountant_actor accountant) {
      VAST_DEBUG("{} sets accountant to {}", self, accountant);
      self->state.accountant = std::move(accountant);
      self->send(self->state.accountant, "source.start",
                 self->state.start_time);
      self->send(self->state.accountant, atom::announce_v, self->state.name);
      // Start the heartbeat loop
      self->delayed_send(self, defaults::system::telemetry_rate,
                         atom::telemetry_v);
    },
    [self](atom::sink, const caf::actor& sink) {
      // TODO: Currently, we use a broadcast downstream manager. We need to
      //       implement an anycast downstream manager and use it for the
      //       source, because we mustn't duplicate data.
      VAST_ASSERT(sink != nullptr);
      VAST_DEBUG("{} registers sink {}", self, sink);
      // Start streaming.
      self->state.mgr->add_outbound_path(sink);
    },
    [self](atom::get, atom::schema) -> caf::result<schema> {
      return self->state.reader.schema();
    },
    [self](atom::put, schema& sch) -> caf::result<void> {
      if (auto err = self->state.reader.schema(std::move(sch)))
        return err;
      return caf::unit;
    },
    [self]([[maybe_unused]] expression& expr) {
      // FIXME: Allow for filtering import data.
      // self->state.filter = std::move(expr);
      VAST_WARN("{} does not currently implement filter expressions", self);
    },
    [self](atom::status, status_verbosity v) {
      caf::settings result;
      if (v >= status_verbosity::detailed) {
        caf::settings src;
        if (self->state.reader_initialized)
          put(src, "format", self->state.reader.name());
        put(src, "produced", self->state.count);
        auto& xs = put_list(result, "sources");
        xs.emplace_back(std::move(src));
      }
      return result;
    },
    [self](atom::telemetry) {
      self->state.send_report();
      if (self->state.dropped_packets > 0) {
        VAST_WARN("{} has no capacity left in stream and dropped {}"
                  "packets",
                  self, self->state.dropped_packets);
        self->state.dropped_packets = 0;
      }
      if (!self->state.done)
        self->delayed_send(self, defaults::system::telemetry_rate,
                           atom::telemetry_v);
    },
  };
}

} // namespace vast::system
