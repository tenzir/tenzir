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
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/type.hpp"
#include "vast/default_table_slice_builder.hpp"
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
#include "vast/system/instrumentation.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/table_slice_builder_factory.hpp"

#include <caf/actor_system_config.hpp>
#include <caf/broadcast_downstream_manager.hpp>
#include <caf/downstream.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/expected.hpp>
#include <caf/none.hpp>
#include <caf/send.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/stream_source.hpp>

#include <unordered_map>

namespace vast::detail {

template <typename T>
constexpr const T& opt_min(caf::optional<T>& opt, T&& rhs) {
  if (!opt)
    return rhs;
  return std::min(*opt, std::forward<T>(rhs));
}

} // namespace vast::detail

namespace vast::system {

/// The source state.
/// @tparam Reader The reader type, which must model the *Reader* concept.
template <class Reader, class Self = caf::event_based_actor>
struct source_state {
  // -- member types -----------------------------------------------------------

  using downstream_manager
    = caf::broadcast_downstream_manager<table_slice_ptr>;

  // -- constructors, destructors, and assignment operators --------------------

  explicit source_state(Self* selfptr) : self(selfptr), initialized(false) {
    // nop
  }

  ~source_state() {
    self->send(accountant, "source.end", caf::make_timestamp());
    if (initialized)
      reader.~Reader();
  }

  // -- member variables -------------------------------------------------------

  /// Filters events, i.e., causes the source to drop all matching events.
  expression filter;

  /// Maps types to the tailored filter.
  std::unordered_map<type, expression> checkers;

  /// Actor for collecting statistics.
  accountant_type accountant;

  /// Wraps the format-specific parser.
  union {
    Reader reader;
  };

  /// Pretty name for log files.
  const char* name = "source";

  /// Takes care of transmitting batches.
  caf::stream_source_ptr<downstream_manager> mgr;

  /// Points to the owning actor.
  Self* self;

  /// The maximum number of events to ingest.
  caf::optional<size_t> remaining;

  /// Stores whether `reader` is constructed.
  bool initialized;

  // -- utility functions ------------------------------------------------------

  /// Initializes the state.
  void init(Reader rd, caf::optional<size_t> max_events) {
    // Initialize members from given arguments.
    name = reader.name();
    new (&reader) Reader(std::move(rd));
    remaining = std::move(max_events);
    initialized = true;
  }

  measurement measurement_;

  void send_report() {
    if (measurement_.events > 0) {
      auto r = performance_report{{{std::string{name}, measurement_}}};
#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_INFO
      for (const auto& [key, m] : r) {
        if (auto rate = m.rate_per_sec(); std::isfinite(rate))
          VAST_INFO(self, "produced", m.events, "events at a rate of",
                    static_cast<uint64_t>(rate), "events/sec in",
                    to_string(m.duration));
        else
          VAST_INFO(self, "produced", m.events, "events in",
                    to_string(m.duration));
      }
#endif
      measurement_ = measurement{};
      if (accountant)
        self->send(accountant, std::move(r));
    }
  }
};

/// An event producer.
/// @tparam Reader The concrete source implementation.
/// @param self The actor handle.
/// @param reader The reader instance.
template <class Reader>
caf::behavior
source(caf::stateful_actor<source_state<Reader>>* self, Reader reader,
       size_t table_slice_size, caf::optional<size_t> max_events) {
  using namespace caf;
  using namespace std::chrono;
  namespace defs = defaults::system;
  // Initialize state.
  self->state.init(std::move(reader), std::move(max_events));
  // Spin up the stream manager for the source.
  self->state.mgr = self->make_continuous_source(
    // init
    [=](bool& done) {
      done = false;
      timestamp now = system_clock::now();
      self->send(self->state.accountant, "source.start", now);
    },
    // get next element
    [=](bool& done, downstream<table_slice_ptr>& out, size_t num) {
      auto& st = self->state;
      auto t = timer::start(st.measurement_);
      // Extract events until the source has exhausted its input or until
      // we have completed a batch.
      auto push_slice = [&](table_slice_ptr x) { out.push(std::move(x)); };
      // We can produce up to num * table_slice_size events per run.
      auto events = detail::opt_min(st.remaining, num * table_slice_size);
      auto [err, produced] = st.reader.read(events, table_slice_size,
                                            push_slice);
      // TODO: If the source is unable to generate new events (returns 0),
      //       the source will stall and never be polled again. We should
      //       trigger CAF to poll the source after a predefined interval of
      //       time again, e.g., via delayed_send.
      t.stop(produced);
      if (st.remaining) {
        VAST_ASSERT(*st.remaining >= produced);
        *st.remaining -= produced;
      }
      auto finish = [&] {
        done = true;
        st.send_report();
        self->quit();
      };
      if (st.remaining && *st.remaining == 0)
        return finish();
      if (err != caf::none) {
        if (err != vast::ec::end_of_input)
          VAST_INFO(self, "completed with message:", render(err));
        return finish();
      }
    },
    // done?
    [](const bool& done) {
      return done;
    }
  );
  return {
    [=](get_atom, schema_atom) { return self->state.reader.schema(); },
    [=](put_atom, schema sch) -> result<void> {
      VAST_INFO(self, "got schema:", sch);
      if (auto err = self->state.reader.schema(std::move(sch)))
        return err;
      return caf::unit;
    },
    [=](expression& expr) {
      VAST_DEBUG(self, "sets filter expression to:", expr);
      self->state.filter = std::move(expr);
    },
    [=](accountant_type accountant) {
      VAST_DEBUG(self, "sets accountant to", accountant);
      self->state.accountant = std::move(accountant);
      self->send(self->state.accountant, announce_atom::value,
                 self->state.name);
    },
    [=](sink_atom, const actor& sink) {
      // TODO: Currently, we use a broadcast downstream manager. We need to
      //       implement an anycast downstream manager and use it for the
      //       source, because we mustn't duplicate data.
      VAST_ASSERT(sink != nullptr);
      // We currently support only a single sink.
      // Switch to streaming and periodic reporting mode.
      self->become([=](telemetry_atom) {
        auto& st = self->state;
        st.send_report();
        if (!self->state.mgr->done())
          self->delayed_send(self, defs::telemetry_rate, telemetry_atom::value);
      });
      self->delayed_send(self, defs::telemetry_rate, telemetry_atom::value);
      VAST_DEBUG(self, "registers sink", sink);
      // Start streaming.
      self->state.mgr->add_outbound_path(sink);
    },
  };
}

/// An event producer with default table slice settings.
template <class Reader>
caf::behavior default_source(caf::stateful_actor<source_state<Reader>>* self,
                             Reader reader) {
  auto slice_size = get_or(self->system().config(), "system.table-slice-size",
                           defaults::system::table_slice_size);
  return source(self, std::move(reader), slice_size, caf::none);
}

} // namespace vast::system
