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

#include "vast/caf_table_slice_builder.hpp"
#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/type.hpp"
#include "vast/data.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/fwd.hpp"
#include "vast/logger.hpp"
#include "vast/schema.hpp"
#include "vast/system/accountant.hpp"
#include "vast/system/instrumentation.hpp"
#include "vast/system/report.hpp"
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

#include <chrono>
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
    : self{selfptr}, reader_initialized{false} {
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

  /// An accumulator for the amount of produced events.
  size_t count = 0;

  /// The maximum number of events to ingest.
  caf::optional<size_t> requested;

  /// The import-local schema.
  vast::schema local_schema;

  /// Stores whether `reader` is constructed.
  bool reader_initialized;

  /// Current metrics for the accountant.
  measurement metrics;

  /// The amount of time to wait until the next wakeup.
  std::chrono::milliseconds wakeup_delay = std::chrono::milliseconds::zero();

  /// Indicates whether the stream source is waiting for input.
  bool waiting_for_input = false;

  /// Indicates whether the stream source is done.
  bool done;

  // -- utility functions ------------------------------------------------------

  /// Initializes the state.
  template <class T>
  void init(T* selfptr, Reader rd, caf::optional<size_t> max_events,
            type_registry_type type_registry, vast::schema sch,
            std::string type_filter, accountant_type acc) {
    // Create the reader.
    self = selfptr;
    name = reader.name();
    new (&reader) Reader(std::move(rd));
    reader_initialized = true;
    requested = std::move(max_events);
    local_schema = std::move(sch);
    accountant = std::move(acc);
    sink = {};
    done = false;
    // Register with the accountant.
    self->send(accountant, atom::announce_v, name);
    // Figure out which schemas we need.
    if (type_registry) {
      self->request(type_registry, caf::infinite, atom::get_v)
        .await([=](type_set types) {
          auto& st = selfptr->state;
          auto is_valid = [&](const auto& layout) {
            return detail::starts_with(layout.name(), type_filter);
          };
          // First, merge and de-duplicate the local schema with types from the
          // type-registry.
          types.value.insert(std::make_move_iterator(st.local_schema.begin()),
                             std::make_move_iterator(st.local_schema.end()));
          st.local_schema.clear();
          // Second, filter valid types from all available record types.
          for (auto& type : types.value)
            if (auto layout = caf::get_if<vast::record_type>(&type))
              if (is_valid(*layout))
                st.local_schema.add(std::move(*layout));
          // Third, try to set the new schema.
          if (auto err = reader.schema(std::move(st.local_schema));
              err && err != caf::no_error)
            VAST_ERROR(self, "failed to set schema", err);
        });
    } else {
      // We usually expect to have the type registry at the ready, but if we
      // don't we fall back to only using the schemas from disk.
      VAST_WARNING(self, "failed to retrieve registered types and only "
                         "considers types local to the import command");
      if (auto err = reader.schema(std::move(local_schema));
          err && err != caf::no_error)
        VAST_ERROR(self, "failed to set schema", err);
    }
  }

  void send_report() {
    // Send the reader-specific status report to the accountant.
    if (auto status = reader.status(); !status.empty())
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
/// @param type_filter Restriction for considered types.
/// @param accountant_type The actor handle for the accountant component.
template <class Reader>
caf::behavior
source(caf::stateful_actor<source_state<Reader>>* self, Reader reader,
       size_t table_slice_size, caf::optional<size_t> max_events,
       type_registry_type type_registry, vast::schema local_schema,
       std::string type_filter, accountant_type accountant) {
  VAST_TRACE(VAST_ARG(self));
  // Initialize state.
  auto& st = self->state;
  st.init(self, std::move(reader), std::move(max_events),
          std::move(type_registry), std::move(local_schema),
          std::move(type_filter), std::move(accountant));
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    VAST_VERBOSE(self, "received EXIT from", msg.source);
    self->state.done = true;
    self->quit(msg.reason);
  });
  // Spin up the stream manager for the source.
  st.mgr = self->make_continuous_source(
    // init
    [=](caf::unit_t&) {
      caf::timestamp now = std::chrono::system_clock::now();
      self->send(self->state.accountant, "source.start", now);
    },
    // get next element
    [=](caf::unit_t&, caf::downstream<table_slice_ptr>& out, size_t num) {
      VAST_DEBUG(self, "tries to generate", num, "messages");
      auto& st = self->state;
      // Extract events until the source has exhausted its input or until
      // we have completed a batch.
      auto push_slice = [&](table_slice_ptr x) { out.push(std::move(x)); };
      // We can produce up to num * table_slice_size events per run.
      auto events = num * table_slice_size;
      if (st.requested)
        events = std::min(events, *st.requested - st.count);
      auto t = timer::start(st.metrics);
      auto [err, produced] = st.reader.read(events, table_slice_size,
                                            push_slice);
      VAST_DEBUG(self, "read", produced, "events");
      t.stop(produced);
      st.count += produced;
      auto finish = [&] {
        st.done = true;
        st.send_report();
        self->quit();
      };
      if (st.requested && st.count >= *st.requested) {
        VAST_DEBUG(self, "finished with", st.count, "events");
        return finish();
      }
      if (err == ec::stalled) {
        if (!st.waiting_for_input) {
          // This pull handler was invoked while we were waiting for a wakeup
          // message. Sending another one would create a parallel wakeup cycle.
          st.waiting_for_input = true;
          self->delayed_send(self, st.wakeup_delay, atom::wakeup_v);
          // Exponential backoff for the wakeup calls.
          // For each consecutive invocation of this generate handler that does
          // not emit any events, we double the wakeup delay.
          // The sequence is 0, 20, 40, 80, 160, 320, 640, 1280.
          if (st.wakeup_delay == std::chrono::milliseconds::zero())
            st.wakeup_delay = std::chrono::milliseconds{20};
          else if (st.wakeup_delay < st.reader.batch_timeout_ / 2)
            st.wakeup_delay *= 2;
          VAST_DEBUG(self, "increases wakeup delay to", st.wakeup_delay);
        } else {
          VAST_DEBUG(self, "timed out but was not already waiting for input");
        }
        return;
      }
      st.wakeup_delay = std::chrono::milliseconds::zero();
      if (err == ec::timeout) {
        VAST_DEBUG(self, "reached batch timeout and flushes its buffers");
        st.mgr->out().force_emit_batches();
      } else if (err != caf::none) {
        if (err != vast::ec::end_of_input)
          VAST_INFO(self, "completed with message:", render(err));
        else
          VAST_DEBUG(self, "completed at end of input");
        return finish();
      }
      VAST_DEBUG(self, "completed successfully");
    },
    // done?
    [=](const caf::unit_t&) { return self->state.done; });
  return {
    [=](atom::get, atom::schema) { return self->state.reader.schema(); },
    [=](atom::put, schema sch) -> caf::result<void> {
      VAST_DEBUG(self, "received", VAST_ARG("schema", sch));
      auto& st = self->state;
      if (auto err = st.reader.schema(std::move(sch));
          err && err != caf::no_error)
        return err;
      return caf::unit;
    },
    [=]([[maybe_unused]] expression& expr) {
      // FIXME: Allow for filtering import data.
      // self->state.filter = std::move(expr);
      VAST_WARNING(self, "does not currently implement filter expressions");
    },
    [=](atom::sink, const caf::actor& sink) {
      VAST_ASSERT(sink);
      VAST_DEBUG(self, "registers", VAST_ARG(sink));
      // TODO: Currently, we use a broadcast downstream manager. We need to
      //       implement an anycast downstream manager and use it for the
      //       source, because we mustn't duplicate data.
      auto& st = self->state;
      if (st.sink) {
        self->quit(caf::make_error(ec::logic_error,
                                   "source does not support "
                                   "multiple sinks; sender =",
                                   self->current_sender()));
        return;
      }
      st.sink = sink;
      self->delayed_send(self, defaults::system::telemetry_rate,
                         atom::telemetry_v);
      // Start streaming.
      st.mgr->add_outbound_path(st.sink);
    },
    [=](atom::status, status_verbosity v) {
      auto& st = self->state;
      caf::settings result;
      if (v >= status_verbosity::detailed) {
        caf::settings src;
        if (st.reader_initialized)
          put(src, "format", st.reader.name());
        put(src, "produced", st.count);
        auto& xs = put_list(result, "sources");
        xs.emplace_back(std::move(src));
      }
      return result;
    },
    [=](atom::wakeup) {
      VAST_VERBOSE(self, "wakes up to check for new input");
      auto& st = self->state;
      st.waiting_for_input = false;
      // If we are here, the reader returned with ec::stalled the last time it
      // was called. Let's check if we can read something now.
      if (st.mgr->generate_messages())
        st.mgr->push();
    },
    [=](atom::telemetry) {
      VAST_DEBUG(self, "got a telemetry atom");
      auto& st = self->state;
      st.send_report();
      if (!st.mgr->done())
        self->delayed_send(self, defaults::system::telemetry_rate,
                           atom::telemetry_v);
    },
  };
}

} // namespace vast::system
