//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/query_processor.hpp"

#include "vast/fwd.hpp"

#include "vast/ids.hpp"
#include "vast/logger.hpp"

#include <caf/event_based_actor.hpp>
#include <caf/skip.hpp>
#include <caf/stateful_actor.hpp>

namespace vast::system {

// -- constructors, destructors, and assignment operators ----------------------

query_processor::query_processor(caf::event_based_actor* self)
  : state_(idle), self_(self), block_end_of_hits_(false) {
  behaviors_[idle].assign(
    // Our default init state simply waits for a query to execute.
    [=](vast::query& query, const index_actor& index) {
      start(std::move(query), index);
    });
  behaviors_[await_query_id].assign(
    // Received from the INDEX after sending the query when leaving `idle`.
    [=](const uuid& query_id, uint32_t total, uint32_t scheduled) {
      VAST_ASSERT(scheduled <= total);
      query_id_ = query_id;
      partitions_.received = 0;
      partitions_.scheduled = scheduled;
      partitions_.total = total;
      transition_to(await_results_until_done);
    });
  behaviors_[await_results_until_done].assign(
    [=](atom::done) -> caf::result<void> {
      if (block_end_of_hits_)
        return caf::skip;
      partitions_.received += partitions_.scheduled;
      process_done();
      return caf::unit;
    });
}

query_processor::~query_processor() = default;

// -- convenience functions ----------------------------------------------------

void query_processor::start(vast::query query, index_actor index) {
  index_ = std::move(index);
  self_->send(index_, std::move(query));
  transition_to(await_query_id);
}

bool query_processor::request_more_results() {
  auto n
    = std::min(partitions_.total - partitions_.received, partitions_.scheduled);
  VAST_ASSERT(partitions_.received + n <= partitions_.total);
  if (n == 0)
    return false;
  VAST_DEBUG("{} asks the INDEX for more hits by scheduling {}"
             "additional partitions",
             self_, n);
  partitions_.scheduled = n;
  self_->send(index_, query_id_, n);
  return true;
}

// -- state management ---------------------------------------------------------

void query_processor::transition_to(state_name x) {
  VAST_DEBUG("{} transitions from state {} to state {}", self_, state_, x);
  self_->become(behaviors_[x]);
  state_ = x;
}

// -- implementation hooks -----------------------------------------------------

void query_processor::process_done() {
  if (!request_more_results())
    transition_to(idle);
}

// -- related functions --------------------------------------------------------

std::string to_string(query_processor::state_name x) {
  static constexpr const char* tbl[] = {
    "idle",
    "await_query_id",
    "await_results_until_done",
  };
  return tbl[static_cast<size_t>(x)];
}

} // namespace vast::system
