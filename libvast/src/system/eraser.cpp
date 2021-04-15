//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/eraser.hpp"

#include "vast/fwd.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/logger.hpp"
#include "vast/query.hpp"

#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/timespan.hpp>

namespace vast::system {

eraser_state::eraser_state(caf::event_based_actor* self) : super{self} {
  // nop
}

void eraser_state::init(caf::timespan interval, std::string query,
                        index_actor index) {
  VAST_TRACE_SCOPE("{} {} {} {}", VAST_ARG(interval), VAST_ARG(query),
                   VAST_ARG(index));
  // Set member variables.
  interval_ = interval;
  query_ = std::move(query);
  index_ = std::move(index);
  // Override the behavior for the idle state.
  behaviors_[idle].assign([=](atom::run) {
    if (self_->current_sender() != self_->ctrl())
      promise_ = self_->make_response_promise();
    auto expr = to<expression>(query_);
    if (!expr) {
      VAST_ERROR("{} failed to parse query {}", self_, query_);
      return;
    }
    if (expr = normalize_and_validate(std::move(*expr)); !expr) {
      VAST_ERROR("{} failed to normalize and validate {}", self_, query_);
      return;
    }
    self_->send(index_, vast::query{query::verb::erase, std::move(*expr)});
    transition_to(await_query_id);
  });
  // Trigger the delayed send message.
  transition_to(idle);
}

void eraser_state::transition_to(query_processor::state_name x) {
  VAST_TRACE_SCOPE("{}", VAST_ARG("state_name", x));
  if (state_ == idle && x != idle)
    VAST_INFO("{} triggers new aging cycle", self_);
  super::transition_to(x);
  if (x == idle) {
    if (promise_.pending())
      promise_.deliver(atom::ok_v);
    else
      self_->delayed_send(self_, interval_, atom::run_v);
  }
}

caf::behavior
eraser(caf::stateful_actor<eraser_state>* self, caf::timespan interval,
       std::string query, index_actor index) {
  VAST_TRACE_SCOPE("{} {} {} {} {}", VAST_ARG(self), VAST_ARG(interval),
                   VAST_ARG(query), VAST_ARG(index));
  auto& st = self->state;
  st.init(interval, std::move(query), std::move(index));
  return st.behavior();
}

} // namespace vast::system
