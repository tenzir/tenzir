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

#include "vast/system/eraser.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/expression.hpp"
#include "vast/fwd.hpp"
#include "vast/logger.hpp"

#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/timespan.hpp>

namespace vast::system {

eraser_state::eraser_state(caf::event_based_actor* self) : super{self} {
  // nop
}

void eraser_state::init(caf::timespan interval, std::string query,
                        caf::actor index, caf::actor archive) {
  VAST_TRACE(VAST_ARG(interval), VAST_ARG(query), VAST_ARG(index),
             VAST_ARG(archive));
  // Set member variables.
  interval_ = std::move(interval);
  query_ = std::move(query);
  index_ = std::move(index);
  archive_ = std::move(archive);
  // Override the behavior for the idle state.
  behaviors_[idle].assign([=](atom::run) {
    if (self_->current_sender() != self_->ctrl())
      promise_ = self_->make_response_promise();
    auto expr = to<expression>(query_);
    if (!expr) {
      VAST_ERROR(self_, "failed to parse query", query_);
      return;
    }
    if (expr = normalize_and_validate(*expr); !expr) {
      VAST_ERROR(self_, "failed to normalize and validate", query_);
      return;
    }
    self_->send(index_, std::move(*expr));
    transition_to(await_query_id);
  });
  // Trigger the delayed send message.
  transition_to(idle);
}

void eraser_state::transition_to(query_processor::state_name x) {
  VAST_TRACE(VAST_ARG("state_name", x));
  if (state_ == idle && x != idle)
    VAST_INFO(self_, "triggers new aging cycle");
  super::transition_to(x);
  if (x == idle) {
    if (promise_.pending())
      promise_.deliver(atom::ok_v);
    else
      self_->delayed_send(self_, interval_, atom::run_v);
  }
}

void eraser_state::process_hits(const ids& hits) {
  VAST_TRACE(VAST_ARG(hits));
  hits_ |= hits;
}

void eraser_state::process_end_of_hits() {
  VAST_TRACE("");
  // Fetch more hits if the INDEX has more partitions to go through.
  if (partitions_.received < partitions_.total) {
    auto n = std::min(partitions_.total - partitions_.received,
                      partitions_.scheduled);
    request_more_hits(n);
    return;
  }
  // Tell the ARCHIVE to erase all hits.
  using std::swap;
  ids all_hits;
  swap(all_hits, hits_);
  self_->send(archive_, atom::erase_v, std::move(all_hits));
  transition_to(idle);
}

caf::behavior
eraser(caf::stateful_actor<eraser_state>* self, caf::timespan interval,
       std::string query, caf::actor index, caf::actor archive) {
  VAST_TRACE(VAST_ARG(self), VAST_ARG(interval), VAST_ARG(query),
             VAST_ARG(index), VAST_ARG(archive));
  auto& st = self->state;
  st.init(interval, std::move(query), std::move(index), std::move(archive));
  return st.behavior();
}

} // namespace vast::system
