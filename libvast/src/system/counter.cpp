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
#include "vast/table_slice.hpp"

#include <caf/event_based_actor.hpp>

namespace vast::system {

counter_state::counter_state(caf::event_based_actor* self) : super(self) {
  // nop
}

void counter_state::init(expression expr, index_actor index,
                         archive_actor archive, bool skip_candidate_check) {
  skip_candidate_check_ = skip_candidate_check;
  expr_ = std::move(expr);
  archive_ = std::move(archive);
  // Transition from idle state when receiving 'run' and client handle.
  behaviors_[idle].assign([=](atom::run, caf::actor client) {
    client_ = std::move(client);
    start(expr_, index);
    // Stop immediately when losing the client.
    self_->monitor(client_);
    self_->set_down_handler([this](caf::down_msg& dm) {
      if (dm.source == client_)
        self_->quit(dm.reason);
    });
  });
  // Add additional message handlers if we need to perform candidate checks.
  if (skip_candidate_check_)
    return;
  caf::message_handler base{behaviors_[collect_hits].as_behavior_impl()};
  behaviors_[collect_hits] = base.or_else(
    [this](table_slice slice) {
      // Construct a candidate checker if we don't have one for this type.
      auto it = checkers_.find(slice.layout());
      if (it == checkers_.end()) {
        if (auto x = tailor(expr_, slice.layout())) {
          std::tie(it, std::ignore) = checkers_.emplace(
            vast::record_type{slice.layout()}, std::move(*x));
        } else {
          VAST_ERROR("{} failed to tailor expression: {}", self_,
                     self_->system().render(x.error()));
          return;
        }
      }
      // Perform the candidate check and count results.
      auto num_results = rank(evaluate(it->second, slice));
      if (num_results > 0)
        self_->send(client_, num_results);
    },
    [this](atom::done, const caf::error&) {
      if (self_->current_sender() != archive_) {
        VAST_WARN("{} received ('done', error) from unexpected actor", self_);
        return;
      }
      if (--pending_archive_requests_ == 0)
        block_end_of_hits(false);
    });
}

void counter_state::process_hits(const ids& hits) {
  if (skip_candidate_check_) {
    self_->send(client_, static_cast<uint64_t>(rank(hits)));
  } else {
    hits_ |= hits;
    // TODO: Change caf::actor_cast to static_cast once the COUNTER is a typed
    // actor.
    self_->send(archive_, atom::extract_v, expr_, std::move(hits),
                caf::actor_cast<receiver_actor<table_slice>>(self_), false);
    // Block the FSM from advancing until we got all slices from the ARCHIVE.
    if (++pending_archive_requests_ == 1)
      block_end_of_hits(true);
  }
}

void counter_state::process_end_of_hits() {
  // Fetch more hits if the INDEX has more partitions to go through.
  if (partitions_.received < partitions_.total) {
    auto n = std::min(partitions_.total - partitions_.received,
                      partitions_.scheduled);
    request_more_hits(n);
    return;
  }
  // The COUNTER runs only once. Hence, we call quit() after sending a final
  // 'done' atom to the client for end-of-results signaling.
  self_->send(client_, atom::done_v);
  self_->quit();
}

caf::behavior
counter(caf::stateful_actor<counter_state>* self, expression expr,
        index_actor index, archive_actor archive, bool skip_candidate_check) {
  self->state.init(std::move(expr), std::move(index), std::move(archive),
                   skip_candidate_check);
  return self->state.behavior();
}

} // namespace vast::system
