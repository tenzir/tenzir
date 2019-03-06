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

#include "vast/system/query_processor.hpp"

#include <caf/event_based_actor.hpp>
#include <caf/skip.hpp>
#include <caf/stateful_actor.hpp>

#include "vast/expression.hpp"
#include "vast/ids.hpp"
#include "vast/logger.hpp"
#include "vast/system/atoms.hpp"

namespace vast::system {

// -- constructors, destructors, and assignment operators ----------------------

query_processor::query_processor(caf::event_based_actor* self)
  : state_(idle), self_(self) {
  // We might receive hits before the query ID arrives. Since we don't want to
  // deal with races manually, we skip messages that arrive out of order until
  // transitioning into the corresponding state.
  self->set_default_handler(caf::skip);
  behaviors_[idle].assign(
    // Our default init state simply waits for a query to execute.
    [=](expression& expr, const caf::actor& index) {
      start(std::move(expr), index);
    });
  behaviors_[await_query_id].assign(
    // Received from the INDEX after sending the query when leaving `idle`.
    [=](const uuid& query_id, uint32_t total, uint32_t scheduled) {
      query_id_ = query_id;
      partitions_.received = 0;
      partitions_.scheduled = scheduled;
      partitions_.total = total;
      transition_to(collect_hits);
    });
  behaviors_[collect_hits].assign(
    // Received from EVALUATOR actors while generating query hits.
    [=](const ids& hits) {
      process_hits(hits);
      // No transtion. We will receive a 'done' message after getting all hits.
    },
    [=](done_atom) {
      partitions_.received += partitions_.scheduled;
      process_end_of_hits();
    });
}

query_processor::~query_processor() {
  // nop
}

// -- convenience functions ----------------------------------------------------

void query_processor::start(expression expr, caf::actor index) {
  index_ = std::move(index);
  self_->send(index_, std::move(expr));
  transition_to(await_query_id);
}

void query_processor::request_more_hits(uint32_t n) {
  VAST_DEBUG(self_, "asks the INDEX for more hits by scheduling", n,
             "additional partitions");
  VAST_ASSERT(n > 0);
  VAST_ASSERT(partitions_.received + n <= partitions_.total);
  partitions_.scheduled = n;
  self_->send(index_, query_id_, n);
}

// -- state management ---------------------------------------------------------

void query_processor::transition_to(state_name x) {
  VAST_DEBUG(self_, "transitions from state", state_, "to state", x);
  self_->become(behaviors_[x]);
  state_ = x;
}

// -- implementation hooks -----------------------------------------------------

void query_processor::process_hits(const ids&) {
  // nop
}

void query_processor::process_end_of_hits() {
  transition_to(idle);
}

// -- related functions --------------------------------------------------------

std::string to_string(query_processor::state_name x) {
  static constexpr const char* tbl[] = {
    "idle",
    "await_query_id",
    "collect_hits",
  };
  return tbl[static_cast<size_t>(x)];
}

} // namespace vast::system
