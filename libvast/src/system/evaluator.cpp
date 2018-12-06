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

#include <caf/actor.hpp>
#include <caf/behavior.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>

#include "vast/expression_visitors.hpp"
#include "vast/logger.hpp"
#include "vast/system/atoms.hpp"

namespace vast::system {

namespace {

/// Concatenates IDs according to given predicates. In paticular, resolves
/// conjunctions, disjunctions, and negations.
struct ids_evaluator {
  ids operator()(caf::none_t) const {
    return {};
  }

  ids operator()(const conjunction& c) const {
    auto result = caf::visit(*this, c[0]);
    if (result.empty() || all<0>(result))
      return {};
    for (size_t i = 1; i < c.size(); ++i) {
      result &= caf::visit(*this, c[i]);
      // This short-circuit works if and only if all bitmaps have the same size.
      // if (result.empty() || all<0>(result)) // short-circuit
      //   return {};
    }
    return result;
  }

  ids operator()(const disjunction& d) const {
    ids result;
    for (auto& op : d) {
      result |= caf::visit(*this, op);
      // This short-circuit works if and only if all bitmaps have the same size.
      // if (all<1>(result)) // short-circuit
      //   break;
    }
    return result;
  }

  ids operator()(const negation& n) const {
    auto result = caf::visit(*this, n.expr());
    result.flip();
    return result;
  }

  ids operator()(const predicate& pred) const {
    auto i = xs.find(pred);
    return i != xs.end() ? i->second.second : ids{};
  }

  const evaluator_state::predicate_hits_map& xs;
};

} // namespace

evaluator_state::evaluator_state(caf::event_based_actor* self) : self(self) {
  // nop
}

void evaluator_state::init(caf::actor client, expression expr) {
  this->client = std::move(client);
  this->expr = std::move(expr);
}

void evaluator_state::handle_result(const predicate& pred, const ids& result) {
  VAST_DEBUG(self, "got new hits", result, " for predicate", pred);
  auto ptr = hits_for(pred);
  VAST_ASSERT(ptr != nullptr);
  auto& [missing, accumulated_hits] = *ptr;
  accumulated_hits |= result;
  if (--missing == 0) {
    VAST_DEBUG(self, "collected all INDEXER results for predicate", pred);
    evaluate();
  }
  decrement_pending();
}

void evaluator_state::handle_missing_result(const predicate& pred,
                                            const caf::error& err) {
  VAST_IGNORE_UNUSED(err);
  VAST_DEBUG(self, "INDEXER returned", self->system().render(err),
             "instead of a result for", pred, );
  auto ptr = hits_for(pred);
  VAST_ASSERT(ptr != nullptr);
  if (--ptr->first == 0) {
    VAST_DEBUG(self, "collected all INDEXER results for predicate", pred);
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
    self->send(client, done_atom::value);
  }
}

evaluator_state::predicate_hits_map::mapped_type*
evaluator_state::hits_for(const predicate& pred) {
  auto i = predicate_hits.find(pred);
  return i != predicate_hits.end() ? &i->second : nullptr;
}

caf::behavior evaluator(caf::stateful_actor<evaluator_state>* self,
                        std::vector<caf::actor> indexers) {
  return {[=](const expression& expr, caf::actor client) {
    self->state.init(client, expr);
    auto predicates = caf::visit(predicatizer{}, expr);
    VAST_ASSERT(!predicates.empty());
    self->state.pending_responses = predicates.size() * indexers.size();
    for (auto& pred : predicates) {
      self->state.predicate_hits.emplace(pred,
                                         std::pair{indexers.size(), ids{}});
      for (auto& x : indexers)
        self->request(x, caf::infinite, pred)
          .then([=](const ids& hits) { self->state.handle_result(pred, hits); },
                [=](const caf::error& err) {
                  self->state.handle_missing_result(pred, err);
                });
    }
    // We can only deal with exactly one expression/client at the moment.
    self->unbecome();
  }};
}

} // namespace vast::system
