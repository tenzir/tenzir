//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/source.hpp"

#include "vast/fwd.hpp"

#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/format/reader.hpp"
#include "vast/logger.hpp"
#include "vast/schema.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/instrumentation.hpp"
#include "vast/system/source_common.hpp"
#include "vast/system/status_verbosity.hpp"
#include "vast/table_slice.hpp"

#include <caf/downstream.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/settings.hpp>
#include <caf/stateful_actor.hpp>

#include <chrono>

namespace vast::system {

caf::behavior
source(caf::stateful_actor<source_state>* self, format::reader_ptr reader,
       size_t table_slice_size, caf::optional<size_t> max_events,
       type_registry_actor type_registry, vast::schema local_schema,
       std::string type_filter, accountant_actor accountant) {
  VAST_TRACE_SCOPE("{}", VAST_ARG(self));
  // Initialize state.
  auto& st = self->state;
  st.name = reader->name();
  st.reader = std::move(reader);
  st.requested = std::move(max_events);
  st.local_schema = std::move(local_schema);
  st.accountant = std::move(accountant);
  st.sink = nullptr;
  st.done = false;
  // Register with the accountant.
  self->send(accountant, atom::announce_v, st.name);
  init(self, std::move(type_registry), std::move(type_filter));
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    VAST_VERBOSE("{} received EXIT from {}", self, msg.source);
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
    [=](caf::unit_t&, caf::downstream<table_slice>& out, size_t num) {
      VAST_DEBUG("{} tries to generate {} messages", self, num);
      auto& st = self->state;
      // Extract events until the source has exhausted its input or until
      // we have completed a batch.
      auto push_slice = [&](table_slice slice) { out.push(std::move(slice)); };
      // We can produce up to num * table_slice_size events per run.
      auto events = num * table_slice_size;
      if (st.requested)
        events = std::min(events, *st.requested - st.count);
      auto t = timer::start(st.metrics);
      auto [err, produced]
        = st.reader->read(events, table_slice_size, push_slice);
      VAST_DEBUG("{} read {} events", self, produced);
      t.stop(produced);
      st.count += produced;
      auto finish = [&] {
        st.done = true;
        send_report(self);
        self->quit();
      };
      if (st.requested && st.count >= *st.requested) {
        VAST_DEBUG("{} finished with {} events", self, st.count);
        return finish();
      }
      if (err == ec::stalled) {
        if (!st.waiting_for_input) {
          // This pull handler was invoked while we were waiting for a wakeup
          // message. Sending another one would create a parallel wakeup cycle.
          st.waiting_for_input = true;
          self->delayed_send(self, st.wakeup_delay, atom::wakeup_v);
          VAST_DEBUG("{} scheduled itself to resume after {}", self,
                     st.wakeup_delay);
          // Exponential backoff for the wakeup calls.
          // For each consecutive invocation of this generate handler that does
          // not emit any events, we double the wakeup delay.
          // The sequence is 0, 20, 40, 80, 160, 320, 640, 1280.
          if (st.wakeup_delay == std::chrono::milliseconds::zero())
            st.wakeup_delay = std::chrono::milliseconds{20};
          else if (st.wakeup_delay < st.reader->batch_timeout_ / 2)
            st.wakeup_delay *= 2;
        } else {
          VAST_DEBUG("{} timed out but is already scheduled for wakeup", self);
        }
        return;
      }
      st.wakeup_delay = std::chrono::milliseconds::zero();
      if (err == ec::timeout) {
        VAST_DEBUG("{} reached batch timeout and flushes its buffers", self);
        st.mgr->out().force_emit_batches();
      } else if (err != caf::none) {
        if (err != vast::ec::end_of_input)
          VAST_INFO("{} completed with message: {}", self, render(err));
        else
          VAST_DEBUG("{} completed at end of input", self);
        return finish();
      }
      VAST_DEBUG("{} ended a generation round regularly", self);
    },
    // done?
    [=](const caf::unit_t&) { return self->state.done; });
  return {
    [=](atom::get, atom::schema) { return self->state.reader->schema(); },
    [=](atom::put, schema sch) -> caf::result<void> {
      VAST_DEBUG("{} received {}", self, VAST_ARG("schema", sch));
      auto& st = self->state;
      if (auto err = st.reader->schema(std::move(sch));
          err && err != caf::no_error)
        return err;
      return caf::unit;
    },
    [=]([[maybe_unused]] expression& expr) {
      // FIXME: Allow for filtering import data.
      // self->state.filter = std::move(expr);
      VAST_WARN("{} does not currently implement filter expressions", self);
    },
    [=](stream_sink_actor<table_slice, std::string> sink) {
      VAST_ASSERT(sink);
      VAST_DEBUG("{} registers {}", self, VAST_ARG(sink));
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
      auto name = std::string{st.reader->name()};
      st.mgr->add_outbound_path(st.sink, std::make_tuple(std::move(name)));
    },
    [=](atom::status, status_verbosity v) {
      auto& st = self->state;
      caf::settings result;
      if (v >= status_verbosity::detailed) {
        caf::settings src;
        if (st.reader)
          put(src, "format", st.reader->name());
        put(src, "produced", st.count);
        auto& xs = put_list(result, "sources");
        xs.emplace_back(std::move(src));
      }
      return result;
    },
    [=](atom::wakeup) {
      VAST_VERBOSE("{} wakes up to check for new input", self);
      auto& st = self->state;
      st.waiting_for_input = false;
      // If we are here, the reader returned with ec::stalled the last time it
      // was called. Let's check if we can read something now.
      if (st.mgr->generate_messages())
        st.mgr->push();
    },
    [=](atom::telemetry) {
      VAST_DEBUG("{} got a telemetry atom", self);
      auto& st = self->state;
      send_report(self);
      if (!st.mgr->done())
        self->delayed_send(self, defaults::system::telemetry_rate,
                           atom::telemetry_v);
    },
  };
}

} // namespace vast::system
