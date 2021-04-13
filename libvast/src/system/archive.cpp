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

std::unique_ptr<segment_store::lookup> next_session(archive_state& state) {
  while (true) {
    if (state.requests.empty()) {
      VAST_TRACE("archive has no requests");
      return nullptr;
    }
    auto& request = state.requests.front();
    if (request.cancelled || request.ids_queue.empty()) {
      // Not sure whether this is really necessary.
      while (!request.ids_queue.empty()) {
        request.ids_queue.front().second.deliver(atom::done_v);
        request.ids_queue.pop();
      }
      state.requests.pop_front();
      continue;
    }
    // We found an active request.
    auto& [ids, rp] = request.ids_queue.front();
    state.active_promise = std::move(rp);
    state.session_ids = std::move(ids);
    request.ids_queue.pop();
    return state.store->extract(state.session_ids);
  }
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
    auto it = std::find_if(
      self->state.requests.begin(), self->state.requests.end(),
      [&](const auto& request) { return request.sink == msg.source; });
    if (it != self->state.requests.end())
      it->cancelled = true;
  });
  return {
    [self](atom::extract, expression expr, const ids& xs,
           receiver_actor<table_slice> requester,
           bool preserve_ids) -> caf::result<atom::done> {
      VAST_DEBUG("{} got query for {} events in range [{},  {})", self,
                 rank(xs), select(xs, 1), select(xs, -1) + 1);
      auto op = preserve_ids ? archive_state::operation::extract_with_ids
                             : archive_state::operation::extract;
      auto rp = self->make_response_promise<atom::done>();
      auto it
        = std::find_if(self->state.requests.begin(), self->state.requests.end(),
                       [&](const auto& request) {
                         return request.sink == requester->address();
                       });
      if (it != self->state.requests.end()) {
        VAST_ASSERT(expr == it->expr);
        VAST_ASSERT(op == it->op);
        // Down messages are sent with high prio.
        // TODO: What if the request is already cleaned up after a down, but we
        // get new ids from the same requester later because of the normal
        // priority afterwards?
        if (it->cancelled) {
          rp.deliver(atom::done_v);
        } else {
          it->ids_queue.emplace(xs, rp);
        }
      } else {
        self->monitor(requester);
        self->state.requests.emplace_back(
          requester, op,
          std::move(expr), std::make_pair(xs, rp));
      }
      if (!self->state.session) {
        self->state.session = next_session(self->state);
        // We just queued the work.
        VAST_ASSERT(self->state.session);
        self->send(self, atom::internal_v);
      }
      return rp;
    },
    [self](atom::internal) {
      VAST_ASSERT(self->state.session);
      VAST_ASSERT(!self->state.requests.empty());
      auto& request = self->state.requests.front();
      if (request.cancelled) {
        self->state.active_promise.deliver(atom::done_v);
        self->state.session = next_session(self->state);
        if (!self->state.session)
          // Nothing to do at the moment;
          return;
      };
      auto slice = self->state.session->next();
      if (slice) {
        switch (request.op) {
          case archive_state::operation::extract: {
            if (request.expr == expression{}) {
              auto final_slice = filter(*slice, self->state.session_ids);
              if (final_slice)
                self->send(request.sink, *final_slice);
            } else {
              auto checker = tailor(request.expr, slice->layout());
              if (!checker) {
                VAST_ERROR("{} failed to tailor expression: {}", self,
                           checker.error());
              } else {
                // TODO: Remove meta predicates (They don't contain false
                // positives).
                auto final_slice
                  = filter(*slice, *checker, self->state.session_ids);
                if (final_slice)
                  self->send(request.sink, *final_slice);
              }
            }
            break;
          }
          case archive_state::operation::extract_with_ids: {
            for (auto& sub_slice : select(*slice, self->state.session_ids)) {
              self->send(request.sink,
                         sub_slice /*, self->state.partition_offset*/);
            }
            if (request.expr == expression{}) {
              for (auto& sub_slice : select(*slice, self->state.session_ids)) {
                self->send(request.sink, sub_slice/*,
                           self->state.partition_offset*/);
              }
            } else {
              auto checker = tailor(request.expr, slice->layout());
              if (!checker) {
                VAST_ERROR("{} failed to tailor expression: {}", self,
                           checker.error());
              } else {
                // TODO: Remove meta predicates (They don't contain false
                // positives).
                for (auto& sub_slice :
                     select(*slice, self->state.session_ids)) {
                  auto hits = evaluate(*checker, sub_slice);
                  for (auto& final_slice : select(sub_slice, hits))
                    self->send(request.sink, final_slice/*,
                               self->state.partition_offset*/);
                }
              }
            }
            break;
          }
          case archive_state::operation::count:
          case archive_state::operation::erase:
            die("not implemented");
        }
      } else {
        // We didn't get a slice from the segment store.
        if (slice.error() != caf::no_error)
          VAST_ERROR("{} failed to retrieve slice: {}", self, slice.error());
        // This session is done.
        // We're at the end, check for more requests.
        self->state.active_promise.deliver(atom::done_v);
        self->state.session = next_session(self->state);
        if (!self->state.session)
          // Nothing to do at the moment;
          return;
      }
      self->send(self, atom::internal_v);
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
            // We get an 'unreachable' error when the stream becomes
            // unreachable because the actor was destroyed; in this case we
            // can't use `self` anymore.
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
