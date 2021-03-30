//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/archive.hpp"

#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/fill_status_map.hpp"
#include "vast/logger.hpp"
#include "vast/segment_store.hpp"
#include "vast/system/report.hpp"
#include "vast/system/status_verbosity.hpp"
#include "vast/table_slice.hpp"

#include <caf/config_value.hpp>
#include <caf/expected.hpp>
#include <caf/settings.hpp>
#include <caf/stream_sink.hpp>

#include <algorithm>

namespace vast::system {

void archive_state::next_session() {
  // No requester means no work to do.
  if (requesters.empty()) {
    VAST_TRACE_SCOPE("{} has no requesters", self);
    session = nullptr;
    return;
  }
  // Find the work queue for our current requester.
  const auto& current_requester = requesters.front();
  auto it = unhandled_ids.find(current_requester->address());
  // There is no ids queue for our current requester. Let's dismiss the
  // requester and try again.
  if (it == unhandled_ids.end()) {
    VAST_TRACE_SCOPE("{} could not find an ids queue for the current "
                     "requester",
                     self);
    requesters.pop();
    return next_session();
  }
  // There is a work queue for our current requester, but it is empty. Let's
  // clean house, dismiss the requester and try again.
  if (it->second.empty()) {
    VAST_TRACE_SCOPE("{} found an empty ids queue for the current requester",
                     self);
    unhandled_ids.erase(it);
    requesters.pop();
    return next_session();
  }
  // Start working on the next ids for the next requester.
  auto& next_ids = it->second.front();
  session = store->extract(next_ids);
  self->send(self, atom::internal_v, next_ids, current_requester, ++session_id);
  it->second.pop();
}

void archive_state::send_report() {
  auto r = performance_report{{{std::string{name}, measurement}}};
#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_DEBUG
  for (const auto& [key, m] : r) {
    if (auto rate = m.rate_per_sec(); std::isfinite(rate))
      VAST_DEBUG("{} handled {} events at a rate of {} events/sec in "
                 "{}",
                 self, m.events, static_cast<uint64_t>(rate),
                 to_string(m.duration));
    else
      VAST_DEBUG("{} handled {} events in {}", self, m.events,
                 to_string(m.duration));
  }
#endif
  measurement = vast::system::measurement{};
  self->send(accountant, std::move(r));
}

archive_actor::behavior_type
archive(archive_actor::stateful_pointer<archive_state> self,
        const std::filesystem::path& dir, size_t capacity,
        size_t max_segment_size) {
  // TODO: make the choice of store configurable. For most flexibility, it
  // probably makes sense to pass a unique_ptr<stor> directory to the spawn
  // arguments of the actor. This way, users can provide their own store
  // implementation conveniently.
  VAST_VERBOSE("{} initializes archive in {} with a maximum segment "
               "size of {} and {} segments in memory",
               self, dir, max_segment_size, capacity);
  self->state.self = self;
  self->state.store = segment_store::make(dir, max_segment_size, capacity);
  VAST_ASSERT(self->state.store != nullptr);
  self->set_exit_handler([self](const caf::exit_msg& msg) {
    VAST_DEBUG("{} got EXIT from {}", self, msg.source);
    self->state.send_report();
    if (auto err = self->state.store->flush())
      VAST_ERROR("{} failed to flush archive {}", self, to_string(err));
    self->state.store.reset();
    self->quit(msg.reason);
  });
  self->set_down_handler([self](const caf::down_msg& msg) {
    VAST_DEBUG("{} received DOWN from {}", self, msg.source);
    self->state.active_exporters.erase(msg.source);
  });
  return {
    [self](const ids& xs, archive_client_actor requester) {
      VAST_DEBUG("{} got query for {} events in range [{},  {})", self,
                 rank(xs), select(xs, 1), select(xs, -1) + 1);
      if (self->state.active_exporters.count(requester->address()) == 0) {
        VAST_DEBUG("{} dismisses query for inactive sender", self);
        return;
      }
      self->state.requesters.push(requester);
      self->state.unhandled_ids[requester->address()].push(xs);
      if (!self->state.session)
        self->state.next_session();
    },
    [self](atom::internal, const ids& xs, archive_client_actor requester,
           uint64_t session_id) {
      // If the export has since shut down, we need to invalidate the session.
      if (self->state.active_exporters.count(requester->address()) == 0) {
        VAST_DEBUG("{} invalidates running query session for {}", self,
                   requester);
        self->state.next_session();
        return;
      }
      if (!self->state.session || self->state.session_id != session_id) {
        VAST_DEBUG("{} considers extraction finished for invalidated "
                   "session",
                   self);
        self->send(requester, atom::done_v, caf::make_error(ec::no_error));
        self->state.next_session();
        return;
      }
      // Extract the next slice.
      auto slice = self->state.session->next();
      if (!slice) {
        auto err = slice.error() ? std::move(slice.error())
                                 : caf::make_error(ec::no_error);
        VAST_DEBUG("{} finished extraction from the current session: "
                   "{}",
                   self, err);
        self->send(requester, atom::done_v, std::move(err));
        self->state.next_session();
        return;
      }
      // The slice may contain entries that are not selected by xs.
      for (auto& sub_slice : select(*slice, xs))
        self->send(requester, sub_slice);
      // Continue working on the current session.
      self->send(self, atom::internal_v, xs, requester, session_id);
    },
    [self](
      caf::stream<table_slice> in) -> caf::inbound_stream_slot<table_slice> {
      VAST_DEBUG("{} got a new stream source", self);
      return self
        ->make_sink(
          in,
          [](caf::unit_t&) {
            // nop
          },
          [=](caf::unit_t&, std::vector<table_slice>& batch) {
            VAST_TRACE_SCOPE("{} got {} table slices", self, batch.size());
            auto t = timer::start(self->state.measurement);
            uint64_t events = 0;
            for (auto& slice : batch) {
              if (auto error = self->state.store->put(slice))
                VAST_ERROR("{} failed to add table slice to store {}", self,
                           render(error));
              else
                events += slice.rows();
            }
            t.stop(events);
          },
          [=](caf::unit_t&, const caf::error& err) {
            // We get an 'unreachable' error when the stream becomes unreachable
            // because the actor was destroyed; in this case we can't use `self`
            // anymore.
            if (err && err != caf::exit_reason::unreachable) {
              if (err != caf::exit_reason::user_shutdown)
                VAST_ERROR("{} got a stream error: {}", self, render(err));
              else
                VAST_DEBUG("{} got a user shutdown error: {}", self,
                           render(err));
              // We can shutdown now because we only get a single stream from
              // the importer.
              self->send_exit(self, err);
            }
            VAST_DEBUG("archive finalizes streaming");
          })
        .inbound_slot();
    },
    [self](accountant_actor accountant) {
      namespace defs = defaults::system;
      self->state.accountant = std::move(accountant);
      self->send(self->state.accountant, atom::announce_v, self->name());
      self->delayed_send(self, defs::telemetry_rate, atom::telemetry_v);
    },
    [self](atom::exporter, const caf::actor& exporter) {
      auto sender_addr = self->current_sender()->address();
      self->state.active_exporters.insert(sender_addr);
      self->monitor<caf::message_priority::high>(exporter);
    },
    [self](atom::status, status_verbosity v) {
      auto result = caf::settings{};
      auto& archive_status = put_dictionary(result, "archive");
      if (v >= status_verbosity::debug)
        detail::fill_status_map(archive_status, self);
      self->state.store->inspect_status(archive_status, v);
      return result;
    },
    [self](atom::telemetry) {
      self->state.send_report();
      namespace defs = defaults::system;
      self->delayed_send(self, defs::telemetry_rate, atom::telemetry_v);
    },
    [self](atom::erase, const ids& xs) {
      if (auto err = self->state.store->erase(xs))
        VAST_ERROR("{} failed to erase events: {}", self, render(err));
      return atom::done_v;
    },
  };
}

} // namespace vast::system
