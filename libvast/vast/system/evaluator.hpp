//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/expression.hpp"
#include "vast/ids.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/evaluation_triple.hpp"

#include <caf/typed_event_based_actor.hpp>

#include <utility>
#include <vector>

namespace vast::system {

/// @relates evaluator
struct evaluator_state {
  using predicate_hits_map = std::map<offset, std::pair<size_t, ids>>;

  evaluator_state(evaluator_actor::stateful_pointer<evaluator_state> self);

  /// Updates `predicate_hits` and may trigger re-evaluation of the expression
  /// tree.
  void handle_result(const offset& position, const ids& result);

  /// Updates `predicate_hits` and may trigger re-evaluation of the expression
  /// tree.
  void handle_missing_result(const offset& position, const caf::error& err);

  /// Evaluates the predicate-tree and may produces new deltas.
  void evaluate();

  /// Decrements the `pending_responses` and sends 'done' to the client when it
  /// reaches 0.
  void decrement_pending();

  /// Returns the `predicate_hits` entry for `pred` or `nullptr`.
  predicate_hits_map::mapped_type* hits_for(const offset& position);

  /// Stores the number of requests that did not receive a response yet.
  size_t pending_responses = 0;

  /// Stores hits per predicate in the expression.
  predicate_hits_map predicate_hits;

  /// Stores hits for the expression.
  ids hits;

  /// Points to the parent actor.
  evaluator_actor::pointer self;

  /// Stores the actor that returns table slices for ids.
  store_actor store;

  /// Stores the actor for sendings results to.
  partition_client_actor client;

  /// Stores the original query expression.
  expression expr;

  /// Stores the original evaluation triples.
  std::vector<evaluation_triple> eval;

  /// Allows us to respond to the COLLECTOR after finishing a lookup.
  caf::typed_response_promise<atom::done> promise;

  /// Gives this actor a recognizable name in logging output.
  static inline const char* name = "evaluator";
};

/// Wraps a query expression in an actor. Upon receiving hits from INDEXER
/// actors, re-evaluates the expression and relays new hits to the INDEX CLIENT.
/// @pre `!eval.empty()`
evaluator_actor::behavior_type
evaluator(evaluator_actor::stateful_pointer<evaluator_state> self,
          expression expr, std::vector<evaluation_triple> eval,
          store_actor store);

} // namespace vast::system
