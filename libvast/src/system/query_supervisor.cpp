//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/query_supervisor.hpp"

#include "vast/fwd.hpp"

#include "vast/expression.hpp"
#include "vast/logger.hpp"

#include <caf/typed_event_based_actor.hpp>

#include <algorithm>

namespace vast::system {

namespace {

[[maybe_unused]] auto get_ids(const query_map& xs) {
  std::vector<uuid> ys;
  ys.reserve(xs.size());
  std::transform(xs.begin(), xs.end(), std::back_inserter(ys),
                 [](auto& kvp) { return kvp.first; });
  return ys;
}

} // namespace

query_supervisor_state::query_supervisor_state(
  query_supervisor_actor::stateful_pointer<query_supervisor_state> self)
  : open_requests(0), log_identifier(std::to_string(self->id())) {
}

query_supervisor_actor::behavior_type query_supervisor(
  query_supervisor_actor::stateful_pointer<query_supervisor_state> self,
  query_supervisor_master_actor master) {
  // Ask master for initial work.
  self->state.master = std::move(master);
  self->send(self->state.master, atom::worker_v, self);
  return {
    [self](const expression& expr,
           const std::vector<std::pair<uuid, partition_actor>>& qm,
           const index_client_actor& client) {
      VAST_DEBUG("{} {} got a new query for {} partitions: {}", self,
                 self->state.log_identifier, qm.size(), get_ids(qm));
      // TODO: We can save one message here if we handle this case in the
      // partition immediately.
      if (qm.empty()) {
        self->send(client, atom::done_v);
        self->send(self->state.master, atom::worker_v, self);
        return;
      }
      VAST_ASSERT(self->state.open_requests == 0);
      for (auto& [id, partition] : qm) {
        ++self->state.open_requests;
        // TODO: Add a proper configurable timeout.
        self
          ->request(partition, caf::infinite, expr,
                    static_cast<const receiver<table_slice>&>(client))
          .then(
            [=](atom::done) {
              if (--self->state.open_requests == 0) {
                VAST_DEBUG("{} {} collected all results for the current batch "
                           "of partitions",
                           self, self->state.log_identifier);
                // Ask master for more work after receiving the last sub
                // result.
                // TODO: We should schedule a new partition as soon as the
                // prvious one has finished, otherwise each batch will be as
                // slow as the worst case of the batch.
                self->send(client, atom::done_v);
                self->send(self->state.master, atom::worker_v, self);
              }
            },
            [=](const caf::error& e) {
              // TODO: Add a proper error handling path to escalate the error to
              // the client.
              VAST_ERROR("{} {} encountered error while supervising query {}",
                         self, self->state.log_identifier, e);
              if (--self->state.open_requests == 0) {
                self->send(client, atom::done_v);
                self->send(self->state.master, atom::worker_v, self);
              }
            });
      }
    },
  };
}

} // namespace vast::system
