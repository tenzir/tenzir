//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/source.hpp"

#include "vast/fwd.hpp"

#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/string.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/format/reader.hpp"
#include "vast/logger.hpp"
#include "vast/schema.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/instrumentation.hpp"
#include "vast/system/status_verbosity.hpp"
#include "vast/system/transformer.hpp"
#include "vast/table_slice.hpp"
#include "vast/type_set.hpp"

#include <caf/downstream.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/settings.hpp>
#include <caf/stateful_actor.hpp>

#include <chrono>
#include <optional>

namespace vast::system {

void source_state::initialize(const type_registry_actor& type_registry,
                              std::string type_filter) {
  // Figure out which schemas we need.
  if (type_registry) {
    auto blocking = caf::scoped_actor{self->system()};
    blocking->request(type_registry, caf::infinite, atom::get_v)
      .receive(
        [=](type_set types) {
          auto is_valid = [&](const auto& layout) {
            return detail::starts_with(layout.name(), type_filter);
          };
          // First, merge and de-duplicate the local schema with types from the
          // type-registry.
          auto merged_schema = schema{};
          for (const auto& type : local_schema)
            if (auto&& layout = caf::get_if<vast::record_type>(&type))
              if (is_valid(*layout))
                merged_schema.add(*layout);
          // Second, filter valid types from all available record types.
          for (auto& type : types)
            if (auto&& layout = caf::get_if<vast::record_type>(&type))
              if (is_valid(*layout))
                merged_schema.add(*layout);
          // Third, try to set the new schema.
          if (auto err = reader->schema(std::move(merged_schema));
              err && err != caf::no_error)
            VAST_ERROR("{} source failed to set schema: {}", reader->name(),
                       err);
        },
        [=](const caf::error& err) {
          VAST_ERROR("{} source failed to receive schema: {}", reader->name(),
                     err);
        });
  } else {
    // We usually expect to have the type registry at the ready, but if we
    // don't we fall back to only using the schemas from disk.
    VAST_WARN("{} source failed to retrieve registered types and only "
              "considers types local to the import command",
              reader->name());
    if (auto err = reader->schema(std::move(local_schema));
        err && err != caf::no_error)
      VAST_ERROR("{} source failed to set schema: {}", reader->name(), err);
  }
}

void source_state::send_report() {
  // Send the reader-specific status report to the accountant.
  if (auto status = reader->status(); !status.empty())
    caf::unsafe_send_as(self, accountant, std::move(status));
  // Send the source-specific performance metrics to the accountant.
  auto r = performance_report{{{std::string{name}, metrics}}};
#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_INFO
  for (const auto& [key, m] : r) {
    if (auto rate = m.rate_per_sec(); std::isfinite(rate))
      VAST_INFO("{} source produced {} events at a rate of {} events/sec in {}",
                reader->name(), m.events, static_cast<uint64_t>(rate),
                to_string(m.duration));
    else
      VAST_INFO("{} source produced {} events in {}", reader->name(), m.events,
                to_string(m.duration));
  }
#endif
  metrics = measurement{};
  caf::unsafe_send_as(self, accountant, std::move(r));
}

caf::behavior
source(caf::stateful_actor<source_state>* self, format::reader_ptr reader,
       size_t table_slice_size, std::optional<size_t> max_events,
       const type_registry_actor& type_registry, vast::schema local_schema,
       std::string type_filter, accountant_actor accountant,
       std::vector<transform>&& transforms) {
  VAST_TRACE_SCOPE("{}", VAST_ARG(self));
  // Initialize state.
  self->state.self = self;
  self->state.name = reader->name();
  self->state.reader = std::move(reader);
  self->state.requested = max_events;
  self->state.local_schema = std::move(local_schema);
  self->state.accountant = std::move(accountant);
  self->state.table_slice_size = table_slice_size;
  self->state.has_sink = false;
  self->state.done = false;
  self->state.transformer
    = self->spawn(transformer, "source-transformer", std::move(transforms));
  if (!self->state.transformer) {
    VAST_ERROR("{} failed to spawn transformer", self);
    self->quit();
    return {};
  }
  // Register with the accountant.
  self->send(self->state.accountant, atom::announce_v, self->state.name);
  self->state.initialize(type_registry, std::move(type_filter));
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    VAST_VERBOSE("{} received EXIT from {}", self, msg.source);
    self->state.done = true;
    self->quit(msg.reason);
  });
  // Spin up the stream manager for the source.
  self->state.mgr = self->make_continuous_source(
    // init
    [self](caf::unit_t&) {
      caf::timestamp now = std::chrono::system_clock::now();
      self->send(self->state.accountant, "source.start", now);
    },
    // get next element
    [self](caf::unit_t&, caf::downstream<stream_controlled<table_slice>>& out,
           size_t num) {
      VAST_DEBUG("{} tries to generate {} messages", self, num);
      // Extract events until the source has exhausted its input or until
      // we have completed a batch.
      auto push_slice = [&](table_slice slice) {
        stream_controlled<table_slice> sc_slice{std::move(slice)};
        if (self->state.flush_listener)
          sc_slice.subscribe(self->state.flush_listener);
        out.push(std::move(sc_slice));
      };
      // We can produce up to num * table_slice_size events per run.
      auto events = num * self->state.table_slice_size;
      if (self->state.requested)
        events = std::min(events, *self->state.requested - self->state.count);
      auto t = timer::start(self->state.metrics);
      auto [err, produced] = self->state.reader->read(
        events, self->state.table_slice_size, push_slice);
      VAST_DEBUG("{} read {} events", self, produced);
      t.stop(produced);
      self->state.count += produced;
      auto finish = [&] {
        self->state.done = true;
        self->state.send_report();
        out.push(end_of_stream_marker);
        self->quit();
      };
      if (self->state.requested
          && self->state.count >= *self->state.requested) {
        VAST_DEBUG("{} finished with {} events", self, self->state.count);
        return finish();
      }
      if (err == ec::stalled) {
        if (!self->state.waiting_for_input) {
          // This pull handler was invoked while we were waiting for a wakeup
          // message. Sending another one would create a parallel wakeup cycle.
          self->state.waiting_for_input = true;
          self->delayed_send(self, self->state.wakeup_delay, atom::wakeup_v);
          VAST_DEBUG("{} scheduled itself to resume after {}", self,
                     self->state.wakeup_delay);
          // Exponential backoff for the wakeup calls.
          // For each consecutive invocation of this generate handler that does
          // not emit any events, we double the wakeup delay.
          // The sequence is 0, 20, 40, 80, 160, 320, 640, 1280.
          if (self->state.wakeup_delay == std::chrono::milliseconds::zero())
            self->state.wakeup_delay = std::chrono::milliseconds{20};
          else if (self->state.wakeup_delay
                   < self->state.reader->batch_timeout_ / 2)
            self->state.wakeup_delay *= 2;
        } else {
          VAST_DEBUG("{} timed out but is already scheduled for wakeup", self);
        }
        return;
      }
      self->state.wakeup_delay = std::chrono::milliseconds::zero();
      if (err == ec::timeout) {
        VAST_DEBUG("{} reached batch timeout and flushes its buffers", self);
        self->state.mgr->out().force_emit_batches();
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
    [self](const caf::unit_t&) {
      return self->state.done;
    });
  auto result = source_actor::behavior_type{
    [self](atom::subscribe, atom::flush, flush_listener_actor& flush_listener) {
      VAST_WARN("{} subscribes flush listener", self);
      VAST_ASSERT(!self->state.flush_listener);
      self->state.flush_listener = std::move(flush_listener);
    },
    [self](atom::get, atom::schema) { //
      return self->state.reader->schema();
    },
    [self](atom::put, schema sch) -> caf::result<void> {
      VAST_DEBUG("{} received {}", self, VAST_ARG("schema", sch));
      if (auto err = self->state.reader->schema(std::move(sch));
          err && err != caf::no_error)
        return err;
      return caf::unit;
    },
    [self]([[maybe_unused]] expression& expr) {
      // FIXME: Allow for filtering import data.
      // self->state.filter = std::move(expr);
      VAST_WARN("{} does not currently implement filter expressions", self);
    },
    [self](
      stream_sink_actor<stream_controlled<table_slice>, std::string> sink) {
      VAST_ASSERT(sink);
      VAST_DEBUG("{} registers sink {}", self, VAST_ARG(sink));
      // TODO: Currently, we use a broadcast downstream manager. We need to
      //       implement an anycast downstream manager and use it for the
      //       source, because we mustn't duplicate data.
      if (self->state.has_sink) {
        self->quit(caf::make_error(ec::logic_error,
                                   "source does not support "
                                   "multiple sinks; sender =",
                                   self->current_sender()));
        return;
      }
      // Start streaming.
      self->state.has_sink = true;
      if (self->state.accountant)
        self->delayed_send(self, defaults::system::telemetry_rate,
                           atom::telemetry_v);
      // Start streaming. Note that we add the outbound path only now,
      // otherwise for small imports the source might already shut down
      // before we receive a sink.
      self->state.mgr->add_outbound_path(self->state.transformer);
      auto name = std::string{self->state.reader->name()};
      self->delegate(self->state.transformer, sink, name);
    },
    [self](atom::status, status_verbosity v) {
      caf::settings result;
      if (v >= status_verbosity::detailed) {
        caf::settings src;
        if (self->state.reader)
          put(src, "format", self->state.reader->name());
        put(src, "produced", self->state.count);
        auto& xs = put_list(result, "sources");
        xs.emplace_back(std::move(src));
      }
      return result;
    },
    [self](atom::wakeup) {
      VAST_VERBOSE("{} wakes up to check for new input", self);
      self->state.waiting_for_input = false;
      // If we are here, the reader returned with ec::stalled the last time it
      // was called. Let's check if we can read something now.
      if (self->state.mgr->generate_messages())
        self->state.mgr->push();
    },
    [self](atom::telemetry) {
      VAST_DEBUG("{} got a telemetry atom", self);
      self->state.send_report();
      if (!self->state.mgr->done())
        self->delayed_send(self, defaults::system::telemetry_rate,
                           atom::telemetry_v);
    },
  };
  // We cannot return the behavior directly and make the SOURCE a typed actor
  // as long as SOURCE and DATAGRAM SOURCE coexist with the same interface,
  // because the DATAGRAM SOURCE is a typed broker.
  return result.unbox();
}

} // namespace vast::system
