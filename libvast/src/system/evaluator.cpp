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

#include "vast/system/evaluator.hpp"

#include "vast/expression_visitors.hpp"
#include "vast/fwd.hpp"
#include "vast/logger.hpp"

#include <caf/actor.hpp>
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

evaluator_state::evaluator_state(caf::event_based_actor* self) : self(self) {
  // nop
}

void evaluator_state::init(caf::actor client, expression expr,
                           caf::response_promise promise) {
  VAST_TRACE(VAST_ARG(client), VAST_ARG(expr), VAST_ARG(promise));
  this->client = std::move(client);
  this->expr = std::move(expr);
  this->promise = std::move(promise);
}

void evaluator_state::handle_result(const offset& position, const ids& result) {
  VAST_DEBUG(self, "got", result.size(), "new hits for predicate at position",
             position);
  auto ptr = hits_for(position);
  VAST_ASSERT(ptr != nullptr);
  auto& [missing, accumulated_hits] = *ptr;
  accumulated_hits |= result;
  if (--missing == 0) {
    VAST_DEBUG(self, "collected all INDEXER results at position", position);
    evaluate();
  }
  decrement_pending();
}

void evaluator_state::handle_missing_result(const offset& position,
                                            const caf::error& err) {
  VAST_IGNORE_UNUSED(err);
  VAST_WARNING(self, "INDEXER returned", self->system().render(err),
               "instead of a result for predicate at position", position);
  auto ptr = hits_for(position);
  VAST_ASSERT(ptr != nullptr);
  if (--ptr->first == 0) {
    VAST_DEBUG(self, "collected all INDEXER results at position", position);
    evaluate();
  }
  decrement_pending();
}

void evaluator_state::evaluate() {
  VAST_DEBUG(self, "got predicate_hits:", predicate_hits,
             "expr_hits:", caf::visit(ids_evaluator{predicate_hits}, expr));
  auto delta = caf::visit(ids_evaluator{predicate_hits}, expr) - hits;
  if (any<1>(delta)) {
    hits |= delta;
    self->send(client, std::move(delta));
  }
}

void evaluator_state::decrement_pending() {
  // We're done evaluating if all INDEXER actors have reported their hits.
  if (--pending_responses == 0) {
    VAST_DEBUG(self, "completed expression evaluation");
    promise.deliver(atom::done_v);
  }
}

evaluator_state::predicate_hits_map::mapped_type*
evaluator_state::hits_for(const offset& position) {
  auto i = predicate_hits.find(position);
  return i != predicate_hits.end() ? &i->second : nullptr;
}

caf::behavior evaluator(caf::stateful_actor<evaluator_state>* self,
                        expression expr, evaluation_triples eval) {
  VAST_TRACE(VAST_ARG(expr), VAST_ARG(eval));
  VAST_ASSERT(!eval.empty());
  using std::get;
  using std::move;
  return {[=, expr{move(expr)}, eval{move(eval)}](caf::actor client) {
    auto& st = self->state;
    st.init(client, move(expr), self->make_response_promise());
    st.pending_responses += eval.size();
    for (auto& triple : eval) {
      // No strucutured bindings available due to subsequent lambda. :-/
      // TODO: C++20
      auto& pos = get<0>(triple);
      auto& curried_pred = get<1>(triple);
      auto& indexer = get<2>(triple);
      ++st.predicate_hits[pos].first;
      self->request(indexer, caf::infinite, curried_pred)
        .then([=](const ids& hits) { self->state.handle_result(pos, hits); },
              [=](const caf::error& err) {
                self->state.handle_missing_result(pos, err);
              });
    }
    if (st.pending_responses == 0) {
      VAST_DEBUG(self, "has nothing to evaluate for expression");
      st.promise.deliver(atom::done_v);
    }
    // We can only deal with exactly one expression/client at the moment.
    self->unbecome();
  }};
}

} // namespace vast::system
