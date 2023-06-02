//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/query_processor.hpp"

#include "vast/fwd.hpp"

#include "vast/catalog.hpp"
#include "vast/ids.hpp"
#include "vast/logger.hpp"
#include "vast/query_cursor.hpp"

#include <caf/event_based_actor.hpp>
#include <caf/skip.hpp>
#include <caf/stateful_actor.hpp>

namespace vast {

// -- constructors, destructors, and assignment operators ----------------------

query_processor::query_processor(caf::event_based_actor* self)
  : state_(idle), self_(self) {
  auto status_handler = [this](atom::status, status_verbosity v) {
    return status(v);
  };
  behaviors_[idle].assign(
    // Our default init state simply waits for a query to execute.
    [this](vast::query_context& query_context, const index_actor& index) {
      start(std::move(query_context), index);
    },
    status_handler);
  behaviors_[await_query_id].assign(
    // Received from the INDEX after sending the query when leaving `idle`.
    [this](const query_cursor& cursor) {
      VAST_ASSERT(cursor.scheduled_partitions <= cursor.candidate_partitions);
      if (cursor.candidate_partitions == 0u) {
        process_done();
      }
      query_id_ = cursor.id;
      partitions_.received = 0;
      partitions_.scheduled = cursor.scheduled_partitions;
      partitions_.total = cursor.candidate_partitions;
      transition_to(await_results_until_done);
    },
    status_handler);
  behaviors_[await_results_until_done].assign(
    [this](atom::done) -> caf::result<void> {
      partitions_.received += partitions_.scheduled;
      process_done();
      return caf::unit;
    },
    status_handler);
}

// -- convenience functions ----------------------------------------------------

void query_processor::start(vast::query_context query_context,
                            index_actor index) {
  index_ = std::move(index);
  self_->send(index_, atom::evaluate_v, std::move(query_context));
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
             *self_, n);
  partitions_.scheduled = n;
  self_->send(index_, atom::query_v, query_id_, n);
  return true;
}

// -- state management ---------------------------------------------------------

void query_processor::transition_to(state_name x) {
  VAST_DEBUG("{} transitions from state {} to state {}", *self_, state_, x);
  self_->become(behaviors_[x]);
  state_ = x;
}

// -- implementation hooks -----------------------------------------------------

void query_processor::process_done() {
  if (!request_more_results())
    transition_to(idle);
}

record query_processor::status(status_verbosity) {
  record result;
  result["state"] = to_string(state_);
  return result;
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

} // namespace vast
