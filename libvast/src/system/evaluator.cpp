//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/evaluator.hpp"

#include "vast/fwd.hpp"

#include "vast/expression_visitors.hpp"
#include "vast/logger.hpp"
#include "vast/table_slice.hpp"

#include <caf/behavior.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>

namespace vast::system {

namespace {

/// Concatenates IDs according to given predicates. In paticular, resolves
/// conjunctions, disjunctions, and negations.
class ids_evaluator {
public:
  ids_evaluator(const evaluator_state::predicate_hits_map& xs) : hits_(xs) {
    push();
  }

  ids operator()(caf::none_t) {
    return {};
  }

  template <class Connective>
  ids operator()(const Connective& xs) {
    VAST_ASSERT(xs.size() > 0);
    push();
    auto result = caf::visit(*this, xs[0]);
    for (size_t index = 1; index < xs.size(); ++index) {
      next();
      if constexpr (std::is_same_v<Connective, conjunction>) {
        result &= caf::visit(*this, xs[index]);
      } else {
        static_assert(std::is_same_v<Connective, disjunction>);
        result |= caf::visit(*this, xs[index]);
      }
    }
    pop();
    return result;
  }

  ids operator()(const negation& n) {
    push();
    auto result = caf::visit(*this, n.expr());
    pop();
    result.flip();
    return result;
  }

  ids operator()(const predicate&) {
    auto i = hits_.find(position_);
    return i != hits_.end() ? i->second.second : ids{};
  }

private:
  void push() {
    position_.emplace_back(0);
  }

  void pop() {
    position_.pop_back();
  }

  void next() {
    VAST_ASSERT(!position_.empty());
    ++position_.back();
  }

  const evaluator_state::predicate_hits_map& hits_;
  offset position_;
};

} // namespace

evaluator_state::evaluator_state(
  evaluator_actor::stateful_pointer<evaluator_state> self)
  : self{self} {
  // nop
}

void evaluator_state::handle_result(const offset& position, const ids& result) {
  VAST_DEBUG("{} got {} new hits for predicate at position {}", self,
             rank(result), position);
  auto ptr = hits_for(position);
  VAST_ASSERT(ptr != nullptr);
  auto& [missing, accumulated_hits] = *ptr;
  accumulated_hits |= result;
  if (--missing == 0) {
    VAST_DEBUG("{} collected all results at position {}", self, position);
    evaluate();
  }
  decrement_pending();
}

void evaluator_state::handle_missing_result(const offset& position,
                                            const caf::error& err) {
  VAST_IGNORE_UNUSED(err);
  VAST_WARN("{} received {} instead of a result for predicate at "
            "position {}",
            self, render(err), position);
  auto ptr = hits_for(position);
  VAST_ASSERT(ptr != nullptr);
  if (--ptr->first == 0) {
    VAST_DEBUG("{} collected all results at position {}", self, position);
    evaluate();
  }
  decrement_pending();
}

void evaluator_state::evaluate() {
  auto expr_hits = caf::visit(ids_evaluator{predicate_hits}, expr);
  VAST_DEBUG("{} got predicate_hits: {} expr_hits: {}", self, predicate_hits,
             expr_hits);
  auto delta = expr_hits - hits;
  if (any<1>(delta))
    hits |= delta;
}

void evaluator_state::decrement_pending() {
  // We're done evaluating if all INDEXER actors have reported their hits.
  if (--pending_responses == 0) {
    // Now we ask the store for the actual data.
    // TODO: handle count estimate requests.
    self->send(store, hits, static_cast<archive_client_actor>(self));
  }
}

evaluator_state::predicate_hits_map::mapped_type*
evaluator_state::hits_for(const offset& position) {
  auto i = predicate_hits.find(position);
  return i != predicate_hits.end() ? &i->second : nullptr;
}

evaluator_actor::behavior_type
evaluator(evaluator_actor::stateful_pointer<evaluator_state> self,
          expression expr, std::vector<evaluation_triple> eval,
          store_actor store) {
  VAST_TRACE_SCOPE("{} {}", VAST_ARG(expr), VAST_ARG(eval));
  VAST_ASSERT(!eval.empty());
  self->state.expr = std::move(expr);
  self->state.eval = std::move(eval);
  self->state.store = std::move(store);
  self->send(self->state.store, atom::exporter_v,
             caf::actor_cast<caf::actor>(self));
  return {
    [self](table_slice slice) {
      self->send(self->state.client, std::move(slice));
    },
    [self](atom::done, caf::error err) {
      if (err)
        VAST_DEBUG("{} completed expression evaluation", self);
      else
        VAST_ERROR("{} completed expression evaluation with {}", self, err);
      self->state.promise.deliver(atom::done_v);
      // TODO: quit.
    },
    [self](partition_client_actor client) {
      self->state.client = client;
      self->state.promise = self->make_response_promise<atom::done>();
      self->state.pending_responses += self->state.eval.size();
      for (auto& triple : self->state.eval) {
        // No strucutured bindings available due to subsequent lambda. :-/
        // TODO: C++20
        auto& pos = std::get<0>(triple);
        auto& curried_pred = std::get<1>(triple);
        auto& indexer = std::get<2>(triple);
        ++self->state.predicate_hits[pos].first;
        self->request(indexer, caf::infinite, curried_pred)
          .then([=](const ids& hits) { self->state.handle_result(pos, hits); },
                [=](const caf::error& err) {
                  self->state.handle_missing_result(pos, err);
                });
      }
      if (self->state.pending_responses == 0) {
        VAST_DEBUG("{} has nothing to evaluate for expression", self);
        self->state.promise.deliver(atom::done_v);
      }
      return self->state.promise;
    },
  };
}

} // namespace vast::system
