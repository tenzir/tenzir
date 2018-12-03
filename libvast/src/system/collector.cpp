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

#include "vast/system/collector.hpp"

#include <algorithm>

#include "caf/event_based_actor.hpp"
#include "caf/local_actor.hpp"
#include "caf/stateful_actor.hpp"

#include "vast/expression.hpp"
#include "vast/logger.hpp"
#include "vast/system/atoms.hpp"

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

collector_state::collector_state(caf::local_actor* self) : name("collector-") {
  name += std::to_string(self->id());
}

caf::behavior collector(caf::stateful_actor<collector_state>* self,
                        caf::actor master) {
  // Ask master for initial work.
  self->send(master, worker_atom::value, self);
  return {
    [=](const expression& expr, const query_map& qm, const caf::actor& client) {
      VAST_DEBUG(self, "got a new query for", qm.size(), "partitions:",
                 get_ids(qm));
      VAST_ASSERT(self->state.open_requests.empty());
      for (auto& kvp : qm) {
        auto& id = kvp.first;
        auto& indexers = kvp.second;
        VAST_DEBUG(self, "asks", indexers.size(),
                   "INDEXER actor(s) for partition", id);
        self->state.open_requests[id] = std::make_pair(indexers.size(), ids{});
        for (auto& indexer : indexers)
          self->request(indexer, caf::infinite, expr)
            .then([=](const ids& sub_result) {
              auto& [num_indexers, result] = self->state.open_requests[id];
              result |= sub_result;
              if (--num_indexers == 0) {
                VAST_DEBUG(self, "collected all sub results for partition", id);
                self->send(client, std::move(result));
                self->state.open_requests.erase(id);
                // Ask master for more work after receiving the last sub
                // result.
                if (self->state.open_requests.empty()) {
                  VAST_DEBUG(self, "asks INDEX for new work");
                  self->send(master, worker_atom::value, self);
                }
              }
            });
      }
    }};
}

} // namespace vast::system
