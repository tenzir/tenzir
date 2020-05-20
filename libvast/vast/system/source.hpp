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
#include "vast/data.hpp"
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
#include "vast/system/type_registry.hpp"
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

  explicit source_state(Self* selfptr)
    : self(selfptr), reader_initialized(false) {
    // nop
  }

  ~source_state() {
    self->send(accountant, "source.end", caf::make_timestamp());
    if (reader_initialized)
      reader.~Reader();
  }

  // -- member variables -------------------------------------------------------

  /// Filters events, i.e., causes the source to drop all matching events.
  expression filter;

  /// Maps types to the tailored filter.
  std::unordered_map<type, expression> checkers;

  /// Actor for collecting statistics.
  accountant_type accountant;

  /// Actor that receives events.
  caf::actor sink;

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

  /// The import-local schema.
  vast::schema local_schema;

  /// The import type restriction.
  vast::data type_restriction;

  /// Stores whether `reader` is constructed.
  bool reader_initialized;

  /// Current metrics for the accountant.
  measurement metrics;

  // -- utility functions ------------------------------------------------------

  /// Initializes the state.
  void init(Reader rd, caf::optional<size_t> max_events,
            type_registry_type type_registry, vast::schema sch,
            vast::data type_restr, accountant_type acc) {
    // Create the reader.
    name = reader.name();
    new (&reader) Reader(std::move(rd));
    reader_initialized = true;
    remaining = std::move(max_events);
    local_schema = std::move(sch);
    type_restriction = std::move(type_restr);
    // Set accountant.
    accountant = std::move(acc);
    self->send(accountant, announce_atom::value, name);
    // Figure out which schemas we need.
    if (type_registry) {
      self->request(type_registry, caf::infinite, get_atom::value)
        .await([=](std::unordered_set<vast::type> types) {
          auto is_valid = [&](const auto& layout) {
            return caf::holds_alternative<caf::none_t>(type_restriction)
                   || detail::starts_with(
                     layout.name(), caf::get<std::string>(type_restriction));
          };
          // First, merge and de-duplicate the local schema with types from the
          // type-registry.
          types.insert(std::make_move_iterator(local_schema.begin()),
                       std::make_move_iterator(local_schema.end()));
          local_schema.clear();
          // Second, filter valid types from all available record types.
          for (auto& type : types)
            if (auto layout = caf::get_if<vast::record_type>(&type))
              if (is_valid(*layout))
                local_schema.add(std::move(*layout));
        });
    } else {
      // We usually expect to have the type registry at the ready, but if we
      // don't we fall back to only using the schemas from disk.
      VAST_WARNING(self, "failed to retrieve registered types and only "
                         "considers types local to the import command");
    }
    // Third, try to set the new schema.
    if (auto err = reader.schema(std::move(local_schema));
        err && err != caf::no_error)
      VAST_ERROR(self, "failed to set schema", err);
  }

  void send_report() {
    // Send the reader-specific status report to the accountant.
    if (auto status = reader.status(); !status.empty() && accountant)
      self->send(accountant, std::move(status));
    // Send the source-specific performance metrics to the accountant.
    if (metrics.events > 0) {
      auto r = performance_report{{{std::string{name}, metrics}}};
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
      metrics = measurement{};
      if (accountant)
        self->send(accountant, std::move(r));
    }
  }
};

/// An event producer.
/// @tparam Reader The concrete source implementation.
/// @param self The actor handle.
/// @param reader The reader instance.
/// @param table_slice_size The maximum size for a table slice.
/// @param max_events The optional maximum amount of events to import.
/// @param type_registry The actor handle for the type-registry component.
/// @oaram local_schema Additional local schemas to consider.
/// @param type_restriction Restriction for considered types.
/// @param accountant_type The actor handle for the accountant component.
template <class Reader>
caf::behavior
source(caf::stateful_actor<source_state<Reader>>* self, Reader reader,
       size_t table_slice_size, caf::optional<size_t> max_events,
       type_registry_type type_registry, vast::schema local_schema,
       vast::data type_restriction, accountant_type accountant) {
  using namespace std::chrono;
  // Initialize state.
  auto& st = self->state;
  st.init(std::move(reader), std::move(max_events), std::move(type_registry),
          std::move(local_schema), std::move(type_restriction),
          std::move(accountant));
  // Spin up the stream manager for the source.
  st.mgr = self->make_continuous_source(
    // init
    [=](bool& done) {
      done = false;
      caf::timestamp now = system_clock::now();
      self->send(self->state.accountant, "source.start", now);
    },
    // get next element
    [=](bool& done, caf::downstream<table_slice_ptr>& out, size_t num) {
      auto& st = self->state;
      auto t = timer::start(st.metrics);
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
      if (produced == 0)
        VAST_WARNING(self, "received 0 events from the source and may stall");
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
    [](const bool& done) { return done; });
  return {
    [=](get_atom, schema_atom) { return self->state.reader.schema(); },
    [=](put_atom, schema sch) -> caf::result<void> {
      VAST_VERBOSE(self, "received", VAST_ARG("schema", sch));
      auto& st = self->state;
      if (auto err = self->state.reader.schema(std::move(sch));
          err && err != caf::no_error)
        return err;
      return caf::unit;
    },
    [=](expression& expr) {
      VAST_INFO(self, "sets filter expression to", expr);
      auto& st = self->state;
      self->state.filter = std::move(expr);
    },
    [=](sink_atom, const caf::actor& sink) {
      VAST_ASSERT(sink);
      VAST_DEBUG(self, "registers", VAST_ARG(sink));
      // TODO: Currently, we use a broadcast downstream manager. We need to
      //       implement an anycast downstream manager and use it for the
      //       source, because we mustn't duplicate data.
      auto& st = self->state;
      if (st.sink) {
        self->quit(make_error(ec::logic_error, "does not support multiple "
                                               "sinks"));
        return;
      }
      st.sink = sink;
      self->delayed_send(self, defaults::system::telemetry_rate,
                         telemetry_atom::value);
      // Start streaming.
      self->state.mgr->add_outbound_path(st.sink);
    },
    [=](telemetry_atom) {
      auto& st = self->state;
      st.send_report();
      if (!self->state.mgr->done())
        self->delayed_send(self, defaults::system::telemetry_rate,
                           telemetry_atom::value);
    },
  };
}

/// An event producer with default table slice settings.
template <class Reader>
caf::behavior default_source(caf::stateful_actor<source_state<Reader>>* self,
                             Reader reader) {
  auto slice_size = get_or(self->system().config(), "system.table-slice-size",
                           defaults::system::table_slice_size);
  return source(self, std::move(reader), slice_size, caf::none, {}, {}, {}, {});
}

} // namespace vast::system
