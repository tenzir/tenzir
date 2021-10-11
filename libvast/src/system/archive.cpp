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
#include "vast/detail/overload.hpp"
#include "vast/logger.hpp"
#include "vast/segment_store.hpp"
#include "vast/system/report.hpp"
#include "vast/system/status.hpp"
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
                 *self, m.events, static_cast<uint64_t>(rate),
                 to_string(m.duration));
    else
      VAST_DEBUG("{} handled {} events in {}", *self, m.events,
                 to_string(m.duration));
  }
#endif
  measurement = vast::system::measurement{};
  self->send(accountant, std::move(r));
}

std::unique_ptr<segment_store::lookup> archive_state::next_session() {
  while (!requests.empty()) {
    auto& request = requests.front();
    if (request.cancelled || request.ids_queue.empty()) {
      // Not sure whether this is really necessary.
      while (!request.ids_queue.empty()) {
        request.ids_queue.front().second.deliver(atom::done_v);
        request.ids_queue.pop();
      }
      requests.pop_front();
      continue;
    }
    // We found an active request.
    auto& [ids, rp] = request.ids_queue.front();
    active_promise = std::move(rp);
    session_ids = std::move(ids);
    request.ids_queue.pop();
    return store->extract(session_ids);
  }
  VAST_TRACE("archive has no requests");
  return nullptr;
}

caf::typed_response_promise<atom::done>
archive_state::file_request(vast::query query) {
  auto rp = self->make_response_promise<atom::done>();
  auto it
    = std::find_if(requests.begin(), requests.end(),
                   [&](const auto& request) { return request.query == query; });
  if (it != requests.end()) {
    VAST_ASSERT(query == it->query);
    // Down messages are sent with high prio so the request might still be
    // in the queue but cancelled. In case the first request has already been
    // cleaned up, we are in the else branch.
    if (it->cancelled) {
      rp.deliver(atom::done_v);
      return rp;
    }
    it->ids_queue.emplace(query.ids, rp);
  } else {
    // Monitor the sink. We can cancel query execution for it when it goes down.
    // In case we already cleaned up a previous request we're doing unnecessary
    // work.
    // TODO: Figure out a way to avoid this.
    caf::visit(detail::overload{
                 [&](query::count& count) {
                   self->monitor(count.sink);
                 },
                 [&](query::extract& extract) {
                   self->monitor(extract.sink);
                 },
                 [](query::erase&) {
                   die("erase requests don't get filed");
                 },
               },
               query.cmd);
    auto xs = query.ids;
    requests.emplace_back(std::move(query), std::make_pair(xs, rp));
  }
  if (!session) {
    session = next_session();
    // We just queued the work.
    VAST_ASSERT(session);
    self->send(self, atom::internal_v, atom::resume_v);
  }
  return rp;
}

archive_actor::behavior_type
archive(archive_actor::stateful_pointer<archive_state> self,
        const std::filesystem::path& dir, size_t capacity,
        size_t max_segment_size) {
  VAST_VERBOSE("{} initializes global segment store in {} with a maximum "
               "segment "
               "size of {} and {} segments in memory",
               *self, dir, max_segment_size, capacity);
  self->state.self = self;
  if (auto store = segment_store::make(dir, max_segment_size, capacity)) {
    self->state.store = std::move(*store);
  } else {
    VAST_ERROR("{} failed to load index state from disk: {}", *self,
               render(store.error()));
    self->quit(store.error());
    return archive_actor::behavior_type::make_empty_behavior();
  }
  VAST_ASSERT(self->state.store != nullptr);
  self->set_exit_handler([self](const caf::exit_msg& msg) {
    VAST_DEBUG("{} got EXIT from {}", *self, msg.source);
    self->state.send_report();
    if (auto err = self->state.store->flush())
      VAST_ERROR("{} failed to flush archive {}", *self, to_string(err));
    self->state.store.reset();
    self->quit(msg.reason);
  });
  self->set_down_handler([self](const caf::down_msg& msg) {
    VAST_DEBUG("{} received DOWN from {}", *self, msg.source);
    auto it
      = std::find_if(self->state.requests.begin(), self->state.requests.end(),
                     [&](const auto& request) {
                       return caf::visit(detail::overload{
                                           [&](const query::count& count) {
                                             return count.sink == msg.source;
                                           },
                                           [&](const query::extract& extract) {
                                             return extract.sink == msg.source;
                                           },
                                           [](const query::erase&) {
                                             // erase request don't have sinks
                                             // to monitor
                                             return false;
                                           },
                                         },
                                         request.query.cmd);
                     });
    if (it != self->state.requests.end())
      it->cancelled = true;
  });
  return {
    [self](vast::query query) -> caf::result<atom::done> {
      const auto& xs = query.ids;
      VAST_DEBUG("{} got a request with the query {} and {} hints [{},  {})",
                 *self, query, rank(xs), select(xs, 1), select(xs, -1) + 1);
      if (caf::holds_alternative<query::erase>(query.cmd)) {
        // We erase eagerly.
        if (auto err = self->state.store->erase(xs))
          VAST_ERROR("{} failed to erase events: {}", *self, render(err));
        return atom::done_v;
      }
      return self->state.file_request(std::move(query));
    },
    [self](atom::internal, atom::resume) {
      VAST_ASSERT(self->state.session);
      VAST_ASSERT(!self->state.requests.empty());
      if (self->state.requests.front().cancelled) {
        self->state.active_promise.deliver(atom::done_v);
        self->state.session = self->state.next_session();
        if (!self->state.session)
          // Nothing to do at the moment;
          return;
      };
      auto& request = self->state.requests.front();
      auto slice = self->state.session->next();
      if (slice) {
        // Add an lru cache for checkers in the archive state.
        auto checker = expression{};
        if (request.query.expr != expression{}) {
          auto c = tailor(request.query.expr, slice->layout());
          if (!c) {
            VAST_ERROR("{} {}", *self, c.error());
            request.cancelled = true;
            self->state.active_promise.deliver(c.error());
            // We deliver the remaining promises in this request regularly in
            // the next run of `next_session()`.
            self->send(self, atom::internal_v, atom::resume_v);
            return;
          }
          checker = prune_meta_predicates(std::move(*c));
        }
        caf::visit(detail::overload{
                     [&](const query::count& count) {
                       if (count.mode == query::count::estimate)
                         die("logic error detected");
                       auto result = count_matching(*slice, checker,
                                                    self->state.session_ids);
                       self->send(count.sink, result);
                     },
                     [&](const query::extract& extract) {
                       if (extract.policy == query::extract::preserve_ids) {
                         for (auto& sub_slice :
                              select(*slice, self->state.session_ids)) {
                           if (request.query.expr == expression{}) {
                             self->send(extract.sink, sub_slice);
                           } else {
                             auto hits = evaluate(checker, sub_slice);
                             for (auto& final_slice : select(sub_slice, hits))
                               self->send(extract.sink, final_slice);
                           }
                         }
                       } else {
                         auto final_slice
                           = filter(*slice, checker, self->state.session_ids);
                         if (final_slice)
                           self->send(extract.sink, *final_slice);
                       }
                     },
                     [&](query::erase) {
                       die("logic error detected");
                     },
                   },
                   request.query.cmd);
      } else {
        // We didn't get a slice from the segment store.
        if (slice.error() != caf::no_error) {
          VAST_ERROR("{} failed to retrieve slice: {}", *self, slice.error());
          self->state.active_promise.deliver(slice.error());
        } else {
          self->state.active_promise.deliver(atom::done_v);
        }
        // This session is done.
        // We're at the end, check for more requests.
        self->state.session = self->state.next_session();
        if (!self->state.session)
          // Nothing to do at the moment;
          return;
      }
      self->send(self, atom::internal_v, atom::resume_v);
    },
    [self](
      caf::stream<table_slice> in) -> caf::inbound_stream_slot<table_slice> {
      VAST_DEBUG("{} got a new stream source", *self);
      return self
        ->make_sink(
          in,
          [](caf::unit_t&) {
            // nop
          },
          [=](caf::unit_t&, std::vector<table_slice>& batch) {
            VAST_TRACE_SCOPE("{} got {} table slices", *self, batch.size());
            auto t = timer::start(self->state.measurement);
            uint64_t events = 0;
            for (auto& slice : batch) {
              if (auto error = self->state.store->put(slice))
                VAST_ERROR("{} failed to add table slice to store {}", *self,
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
                VAST_ERROR("{} got a stream error: {}", *self, render(err));
              else
                VAST_DEBUG("{} got a user shutdown error: {}", *self,
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
      auto result = record{};
      if (v >= status_verbosity::debug)
        detail::fill_status_map(result, self);
      self->state.store->inspect_status(result, v);
      return result;
    },
    [self](atom::telemetry) {
      self->state.send_report();
      namespace defs = defaults::system;
      self->delayed_send(self, defs::telemetry_rate, atom::telemetry_v);
    },
    [self](atom::erase, const ids& xs) {
      if (auto err = self->state.store->erase(xs))
        VAST_ERROR("{} failed to erase events: {}", *self, render(err));
      return atom::done_v;
    },
  };
}

} // namespace vast::system
