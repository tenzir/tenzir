//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/query_supervisor.hpp"

#include "vast/fwd.hpp"

#include "vast/detail/tracepoint.hpp"
#include "vast/logger.hpp"
#include "vast/query.hpp"

#include <caf/after.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <algorithm>

namespace vast::system {

namespace {

[[maybe_unused]] auto get_ids(const query_map& xs) {
  std::vector<uuid> ys;
  ys.reserve(xs.size());
  std::transform(xs.begin(), xs.end(), std::back_inserter(ys), [](auto& kvp) {
    return kvp.first;
  });
  return ys;
}

} // namespace

query_supervisor_state::query_supervisor_state(
  query_supervisor_actor::stateful_pointer<query_supervisor_state>) {
  // nop
}

query_supervisor_actor::behavior_type query_supervisor(
  query_supervisor_actor::stateful_pointer<query_supervisor_state> self,
  query_supervisor_master_actor master) {
  VAST_TRACE_SCOPE("query_supervisor {} {}", VAST_ARG(self->id()),
                   VAST_ARG(master));
  // Ask master for initial work.
  self->state.master = std::move(master);
  self->send(self->state.master, atom::worker_v, self);
  return {
    [self](atom::supervise, const vast::uuid& query_id,
           const vast::query& query,
           const std::vector<std::pair<uuid, partition_actor>>& qm,
           const receiver_actor<atom::done>& client) {
      VAST_DEBUG("{} got a new query for {} partitions: {}", *self, qm.size(),
                 get_ids(qm));
      // TODO: We can save one message here if we handle this case in the
      // index immediately.
      if (qm.empty()) {
        self->send(client, atom::done_v);
        self->request(self->state.master, caf::infinite, atom::worker_v, self)
          .then(
            [self]() {
              VAST_DEBUG("{} returns to query supervisor master", *self);
            },
            [self](const caf::error&) {
              VAST_ERROR("{} failed to return to query supervisor "
                         "master",
                         *self);
            });
        return;
      }
      self->state.in_progress.insert(query_id);
      // This should never happen, but empirically it does and
      // we still want to keep working.
      if (self->state.in_progress.size() > 1)
        VAST_WARN("{} saw more than one active query: {}", *self,
                  fmt::join(self->state.in_progress, ", "));
      auto open_requests = std::make_shared<int64_t>(0);
      auto start = std::chrono::steady_clock::now();
      auto query_trace_id = query_id.as_u64().first;
      for (const auto& [id, partition] : qm) {
        auto partition_trace_id = id.as_u64().first;
        ++*open_requests;
        // TODO: Add a proper configurable timeout.
        self->request(partition, caf::infinite, query)
          .then(
            [=](atom::done) {
              auto delta = std::chrono::steady_clock::now() - start;
              VAST_TRACEPOINT(query_partition_done, query_trace_id,
                              partition_trace_id, delta.count());
              if (--*open_requests == 0) {
                VAST_DEBUG("{} collected all results for the current batch "
                           "of partitions",
                           *self);
                VAST_TRACEPOINT(query_supervisor_done, query_trace_id);
                // Ask master for more work after receiving the last sub
                // result.
                // TODO: We should schedule a new partition as soon as the
                // previous one has finished, otherwise each batch will be as
                // slow as the worst case of the batch.
                self->state.in_progress.erase(query_id);
                self->send(client, atom::done_v);
                self
                  ->request(self->state.master, caf::infinite, atom::worker_v,
                            self)
                  .then(
                    [self]() {
                      VAST_DEBUG("{} returns to query supervisor master",
                                 *self);
                    },
                    [self](const caf::error&) {
                      VAST_ERROR("{} failed to return to query supervisor "
                                 "master",
                                 *self);
                    });
              }
            },
            [=](const caf::error& e) {
              // TODO: Add a proper error handling path to escalate the error to
              // the client.
              VAST_ERROR("{} encountered error while supervising query {}",
                         *self, e);
              auto delta = std::chrono::steady_clock::now() - start;
              VAST_TRACEPOINT(query_partition_error, query_trace_id,
                              partition_trace_id, delta.count());
              if (--*open_requests == 0) {
                self->state.in_progress.erase(query_id);
                self->send(client, atom::done_v);
                self
                  ->request(self->state.master, caf::infinite, atom::worker_v,
                            self)
                  .then(
                    [self]() {
                      VAST_DEBUG("{} returns to query supervisor master",
                                 *self);
                    },
                    [self](const caf::error&) {
                      VAST_ERROR("{} failed to return to query supervisor "
                                 "master",
                                 *self);
                    });
              }
            });
      }
    },
    [self](atom::shutdown, atom::sink) -> caf::result<void> {
      return self->delegate(self->state.master, atom::worker_v, atom::wakeup_v,
                            self);
    },
  };
}

} // namespace vast::system
