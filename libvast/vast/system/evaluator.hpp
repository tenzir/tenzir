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

#pragma once

#include <unordered_map>
#include <vector>

#include <caf/actor.hpp>
#include <caf/fwd.hpp>
#include <caf/typed_response_promise.hpp>

#include "vast/expression.hpp"
#include "vast/ids.hpp"

namespace vast::system {

/// @relates evaluator
struct evaluator_state {
  using predicate_hits_map = std::unordered_map<predicate,
                                                std::pair<size_t, ids>>;

  evaluator_state(caf::event_based_actor* self);

  void init(caf::actor client, expression expr);

  /// Updates `predicate_hits` and may trigger re-evaluation of the expression
  /// tree.
  void handle_result(const predicate& pred, const ids& result);

  /// Updates `predicate_hits` and may trigger re-evaluation of the expression
  /// tree.
  void handle_missing_result(const predicate& pred, const caf::error& err);

  /// Evaluates the predicate-tree and may produces new deltas.
  void evaluate();

  /// Decrements the `pending_responses` and sends 'done' to the client when it
  /// reaches 0.
  void decrement_pending();

  /// Returns the `predicate_hits` entry for `pred` or `nullptr`.
  predicate_hits_map::mapped_type* hits_for(const predicate& pred);

  /// Stores the number of requests that did not receive a response yet.
  size_t pending_responses = 0;

  /// Stores hits per predicate in the expression.
  predicate_hits_map predicate_hits;

  /// Stores hits for the expression.
  ids hits;

  /// Points to the parent actor.
  caf::event_based_actor* self;

  /// Stores the actor for sendings results to.
  caf::actor client;

  /// Stores the original query expression.
  expression expr;

  /// Gives this actor a recognizable name in logging output.
  static inline const char* name = "evaluator";
};

/// Wraps a query expression in an actor. Upon receiving hits from INDEXER
/// actors, re-evaluates the expression and relays new hits to its sinks.
caf::behavior evaluator(caf::stateful_actor<evaluator_state>* self,
                        std::vector<caf::actor> indexers);

} // namespace vast::system
