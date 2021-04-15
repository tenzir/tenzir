//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/counter.hpp"

#include "vast/bitmap_algorithms.hpp"
#include "vast/logger.hpp"
#include "vast/query.hpp"
#include "vast/table_slice.hpp"

#include <caf/event_based_actor.hpp>

namespace vast::system {

counter_state::counter_state(caf::event_based_actor* self) : super(self) {
  // nop
}

void counter_state::init(expression expr, index_actor index,
                         bool skip_candidate_check) {
  auto verb
    = skip_candidate_check ? query::verb::count_estimate : query::verb::count;
  auto q = vast::query{verb, std::move(expr)};
  // Transition from idle state when receiving 'run' and client handle.
  behaviors_[idle].assign([=, q = std::move(q)](atom::run, caf::actor client) {
    client_ = std::move(client);
    start(q, index);
    // Stop immediately when losing the client.
    self_->monitor(client_);
    self_->set_down_handler([this](caf::down_msg& dm) {
      if (dm.source == client_)
        self_->quit(dm.reason);
    });
  });
  caf::message_handler base{
    behaviors_[await_results_until_done].as_behavior_impl()};
  behaviors_[await_results_until_done] = base.or_else(
    // Forward results to the sink.
    [this](uint64_t num_results) { self_->send(client_, num_results); });
}

void counter_state::process_done() {
  if (!request_more_results()) {
    self_->send(client_, atom::done_v);
    self_->quit();
  }
}

caf::behavior counter(caf::stateful_actor<counter_state>* self, expression expr,
                      index_actor index, bool skip_candidate_check) {
  self->state.init(std::move(expr), std::move(index), skip_candidate_check);
  return self->state.behavior();
}

} // namespace vast::system
