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
#include "vast/query_context.hpp"
#include "vast/system/catalog.hpp"
#include "vast/system/index.hpp"

#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/timespan.hpp>

namespace vast::system {

record eraser_state::status(status_verbosity) const {
  auto result = record{};
  result["query"] = query_;
  result["interval"] = interval_;
  return result;
}

eraser_actor::behavior_type
eraser(eraser_actor::stateful_pointer<eraser_state> self,
       caf::timespan interval, std::string query, index_actor index) {
  VAST_TRACE_SCOPE("eraser: {} {} {} {}", VAST_ARG(self->id()),
                   VAST_ARG(interval), VAST_ARG(query), VAST_ARG(index));
  // Set member variables.
  self->state.interval_ = interval;
  self->state.query_ = std::move(query);
  self->state.index_ = std::move(index);
  self->delayed_send(self, interval, atom::ping_v);
  return {
    [self](atom::ping) {
      self
        ->request(static_cast<eraser_actor>(self), self->state.interval_,
                  atom::run_v)
        .then(
          [self](atom::ok) {
            VAST_VERBOSE("{} successfully finishes run", *self);
            self->delayed_send(static_cast<eraser_actor>(self),
                               self->state.interval_, atom::ping_v);
          },
          [self](const caf::error& e) {
            VAST_WARN("{} encountered error while erasing: {}", *self, e);
            self->delayed_send(static_cast<eraser_actor>(self),
                               self->state.interval_, atom::ping_v);
          });
    },
    [self](atom::run) -> caf::result<atom::ok> {
      auto const& query = self->state.query_;
      VAST_VERBOSE("{} runs with query {}", *self, query);
      auto expr = to<expression>(query);
      if (!expr)
        return caf::make_error(ec::invalid_query, fmt::format("{} failed to "
                                                              "parse query {}",
                                                              *self, query));
      if (expr = normalize_and_validate(std::move(*expr)); !expr)
        return caf::make_error(
          ec::invalid_query,
          fmt::format("{} failed to normalize and validate {}", *self, query));
      auto transform = pipeline::parse(
        fmt::format("where {}", fmt::to_string(expression{negation{*expr}})));
      if (!transform)
        return transform.error();
      auto rp = self->make_response_promise<atom::ok>();
      self->request(self->state.index_, caf::infinite, atom::resolve_v, *expr)
        .then(
          [self, transform = std::move(*transform),
           rp](catalog_lookup_result& result) mutable {
            for (const auto& [_, partition_infos] : result.candidate_infos) {
              VAST_DEBUG("{} resolved query {} to {} partitions", *self,
                         self->state.query_,
                         partition_infos.partition_infos.size());
              if (partition_infos.partition_infos.empty()) {
                rp.deliver(atom::ok_v);
                continue;
              }
              // TODO: Test if the candidate is a false positive before applying
              // the transform to avoid unnecessary noise.
              self
                ->request(self->state.index_, caf::infinite, atom::apply_v,
                          std::move(transform), partition_infos.partition_infos,
                          keep_original_partition::no)
                .then(
                  [self, rp](const std::vector<vast::partition_info>&) mutable {
                    VAST_DEBUG("{} applied filter transform with query {}",
                               *self, self->state.query_);
                    rp.deliver(atom::ok_v);
                  },
                  [self, rp](const caf::error& e) mutable {
                    VAST_WARN("{} failed to apply filter query {}: {}", *self,
                              self->state.query_, e);
                    rp.deliver(e);
                  });
            }
          },
          [rp](const caf::error& e) mutable {
            VAST_ASSERT(false, caf::deep_to_string(e).c_str());
            rp.deliver(e);
          });
      return rp;
    },
    // -- status_client_actor -------------------------------------------------
    [self](atom::status, status_verbosity v) {
      return self->state.status(v);
    },
  };
}

} // namespace vast::system
