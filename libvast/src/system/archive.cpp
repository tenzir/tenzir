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

#include "vast/system/archive.hpp"

#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/fill_status_map.hpp"
#include "vast/logger.hpp"
#include "vast/segment_store.hpp"
#include "vast/store.hpp"
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
    VAST_TRACE(self, "has no requesters");
    session = nullptr;
    return;
  }
  // Find the work queue for our current requester.
  const auto& current_requester = requesters.front();
  auto it = unhandled_ids.find(current_requester->address());
  // There is no ids queue for our current requester. Let's dismiss the
  // requester and try again.
  if (it == unhandled_ids.end()) {
    VAST_TRACE(self, "could not find an ids queue for the current requester");
    requesters.pop();
    return next_session();
  }
  // There is a work queue for our current requester, but it is empty. Let's
  // clean house, dismiss the requester and try again.
  if (it->second.empty()) {
    VAST_TRACE(self, "found an empty ids queue for the current requester");
    unhandled_ids.erase(it);
    requesters.pop();
    return next_session();
  }
  // Start working on the next ids for the next requester.
  auto& next_ids = it->second.front();
  session = store->extract(next_ids);
  self->send(self, next_ids, current_requester, ++session_id);
  it->second.pop();
}

void archive_state::send_report() {
  if (measurement.events > 0) {
    auto r = performance_report{{{std::string{name}, measurement}}};
#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_DEBUG
    for (const auto& [key, m] : r) {
      if (auto rate = m.rate_per_sec(); std::isfinite(rate))
        VAST_LOG_SPD_DEBUG("{} handled {} events at a rate of {} events/sec in "
                           "{}",
                           detail::id_or_name(self), m.events,
                           static_cast<uint64_t>(rate), to_string(m.duration));
      else
        VAST_LOG_SPD_DEBUG("{} handled {} events in {}",
                           detail::id_or_name(self), m.events,
                           to_string(m.duration));
    }
#endif
    measurement = vast::system::measurement{};
    self->send(accountant, std::move(r));
  }
}

archive_actor::behavior_type
archive(archive_actor::stateful_pointer<archive_state> self, path dir,
        size_t capacity, size_t max_segment_size) {
  // TODO: make the choice of store configurable. For most flexibility, it
  // probably makes sense to pass a unique_ptr<stor> directory to the spawn
  // arguments of the actor. This way, users can provide their own store
  // implementation conveniently.
  VAST_LOG_SPD_VERBOSE("{} initializes archive in {} with a maximum segment "
                       "size of {} and {} segments in memory",
                       detail::id_or_name(self), dir, max_segment_size,
                       capacity);
  self->state.self = self;
  self->state.store = segment_store::make(dir, max_segment_size, capacity);
  VAST_ASSERT(self->state.store != nullptr);
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    VAST_LOG_SPD_DEBUG("{} got EXIT from {}", detail::id_or_name(self),
                       msg.source);
    self->state.send_report();
    if (auto err = self->state.store->flush())
      VAST_ERROR(self, "failed to flush archive", to_string(err));
    self->state.store.reset();
    self->quit(msg.reason);
  });
  self->set_down_handler([=](const caf::down_msg& msg) {
    VAST_LOG_SPD_DEBUG("{} received DOWN from {}", detail::id_or_name(self),
                       msg.source);
    self->state.active_exporters.erase(msg.source);
  });
  return {
    [=](const ids& xs) {
      VAST_ASSERT(rank(xs) > 0);
      VAST_LOG_SPD_DEBUG("{} got query for {} events in range [{},  {})",
                         detail::id_or_name(self), rank(xs), select(xs, 1),
                         select(xs, -1) + 1);
      if (auto requester
          = caf::actor_cast<archive_client_actor>(self->current_sender()))
        self->send(self, xs, requester);
      else
        VAST_ERROR(self, "dismisses query for unconforming sender");
    },
    [=](const ids& xs, archive_client_actor requester) {
      auto& st = self->state;
      if (st.active_exporters.count(requester->address()) == 0) {
        VAST_LOG_SPD_DEBUG("{} dismisses query for inactive sender",
                           detail::id_or_name(self));
        return;
      }
      st.requesters.push(requester);
      st.unhandled_ids[requester->address()].push(xs);
      if (!st.session)
        st.next_session();
    },
    [=](const ids& xs, archive_client_actor requester, uint64_t session_id) {
      auto& st = self->state;
      // If the export has since shut down, we need to invalidate the session.
      if (st.active_exporters.count(requester->address()) == 0) {
        VAST_LOG_SPD_DEBUG("{} invalidates running query session for {}",
                           detail::id_or_name(self), requester);
        st.next_session();
        return;
      }
      if (!st.session || st.session_id != session_id) {
        VAST_LOG_SPD_DEBUG("{} considers extraction finished for invalidated "
                           "session",
                           detail::id_or_name(self));
        self->send(requester, atom::done_v, caf::make_error(ec::no_error));
        st.next_session();
        return;
      }
      // Extract the next slice.
      auto slice = st.session->next();
      if (!slice) {
        auto err = slice.error() ? std::move(slice.error())
                                 : caf::make_error(ec::no_error);
        VAST_LOG_SPD_DEBUG("{} finished extraction from the current session: "
                           "{}",
                           detail::id_or_name(self), err);
        self->send(requester, atom::done_v, std::move(err));
        st.next_session();
        return;
      }
      // The slice may contain entries that are not selected by xs.
      for (auto& sub_slice : select(*slice, xs))
        self->send(requester, sub_slice);
      // Continue working on the current session.
      self->send(self, xs, requester, session_id);
    },
    [=](caf::stream<table_slice> in) -> caf::inbound_stream_slot<table_slice> {
      VAST_LOG_SPD_DEBUG("{} got a new stream source",
                         detail::id_or_name(self));
      return self
        ->make_sink(
          in,
          [](caf::unit_t&) {
            // nop
          },
          [=](caf::unit_t&, std::vector<table_slice>& batch) {
            VAST_TRACE(self, "got", batch.size(), "table slices");
            auto t = timer::start(self->state.measurement);
            uint64_t events = 0;
            for (auto& slice : batch) {
              if (auto error = self->state.store->put(slice))
                VAST_ERROR(self, "failed to add table slice to store",
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
                VAST_ERROR(self, "got a stream error:", render(err));
              else
                VAST_LOG_SPD_DEBUG("{} got a user shutdown error: {}",
                                   detail::id_or_name(self), render(err));
              // We can shutdown now because we only get a single stream from
              // the importer.
              self->send_exit(self, err);
            }
            VAST_LOG_SPD_DEBUG("archive finalizes streaming");
          })
        .inbound_slot();
    },
    [=](accountant_actor accountant) {
      namespace defs = defaults::system;
      self->state.accountant = std::move(accountant);
      self->send(self->state.accountant, atom::announce_v, self->name());
      self->delayed_send(self, defs::telemetry_rate, atom::telemetry_v);
    },
    [=](atom::exporter, const caf::actor& exporter) {
      auto sender_addr = self->current_sender()->address();
      self->state.active_exporters.insert(sender_addr);
      self->monitor<caf::message_priority::high>(exporter);
    },
    [=](atom::status, status_verbosity v) {
      auto result = caf::settings{};
      auto& archive_status = put_dictionary(result, "archive");
      if (v >= status_verbosity::debug)
        detail::fill_status_map(archive_status, self);
      self->state.store->inspect_status(archive_status, v);
      return result;
    },
    [=](atom::telemetry) {
      self->state.send_report();
      namespace defs = defaults::system;
      self->delayed_send(self, defs::telemetry_rate, atom::telemetry_v);
    },
    [=](atom::erase, const ids& xs) {
      if (auto err = self->state.store->erase(xs))
        VAST_ERROR(self, "failed to erase events:", render(err));
      return atom::done_v;
    },
  };
}

} // namespace vast::system
