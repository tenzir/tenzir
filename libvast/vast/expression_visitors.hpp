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

#ifndef VAST_EXPRESSION_VISITORS_HPP
#define VAST_EXPRESSION_VISITORS_HPP

#include <vector>

#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/expected.hpp"
#include "vast/none.hpp"
#include "vast/operator.hpp"
#include "vast/time.hpp"

#include "vast/concept/printable/vast/data.hpp"
#include "vast/concept/printable/vast/key.hpp"
#include "vast/concept/printable/vast/type.hpp"

namespace vast {

class event;

/// Hoists the contained expression of a single-element conjunction or
/// disjunction one level in the tree.
struct hoister {
  expression operator()(none) const;
  expression operator()(const conjunction& c) const;
  expression operator()(const disjunction& d) const;
  expression operator()(const negation& n) const;
  expression operator()(const predicate& p) const;
};

/// Ensures that extractors always end up on the LHS of a predicate.
struct aligner {
  expression operator()(none) const;
  expression operator()(const conjunction& c) const;
  expression operator()(const disjunction& d) const;
  expression operator()(const negation& n) const;
  expression operator()(const predicate& p) const;
};

/// Pushes negations down to the predicate level and removes double negations.
struct denegator {
  denegator(bool negate = false);

  expression operator()(none) const;
  expression operator()(const conjunction& c) const;
  expression operator()(const disjunction& d) const;
  expression operator()(const negation& n) const;
  expression operator()(const predicate& p) const;

  bool negate_ = false;
};

/// Removes duplicate predicates from conjunctions and disjunctions.
struct deduplicator {
  expression operator()(none) const;
  expression operator()(const conjunction& c) const;
  expression operator()(const disjunction& d) const;
  expression operator()(const negation& n) const;
  expression operator()(const predicate& p) const;
};

/// Extracts all predicates from an expression.
struct predicatizer {
  std::vector<predicate> operator()(none) const;
  std::vector<predicate> operator()(const conjunction& c) const;
  std::vector<predicate> operator()(const disjunction& d) const;
  std::vector<predicate> operator()(const negation& n) const;
  std::vector<predicate> operator()(const predicate& p) const;
};

/// Ensures that LHS and RHS of a predicate fit together.
struct validator {
  expected<void> operator()(none);
  expected<void> operator()(const conjunction& c);
  expected<void> operator()(const disjunction& d);
  expected<void> operator()(const negation& n);
  expected<void> operator()(const predicate& p);
  expected<void> operator()(const attribute_extractor& ex, const data& d);
  expected<void> operator()(const type_extractor& ex, const data& d);
  expected<void> operator()(const key_extractor& ex, const data& d);

  template <class T, class U>
  expected<void> operator()(const T& lhs, const U& rhs) {
    return make_error(ec::syntax_error, "incompatible predicate operands", lhs,
                      rhs);
  }

  relational_operator op_;
};

/// Checks whether an expression is valid for a given time interval. The
/// visitor returns `false` if a time extractor restricts all predicates to lay
/// outside the given interval, and returns `true` if at least one unrestricted
/// predicate exists in the expression.
///
/// @pre Requires prior expression normalization and validation.
struct time_restrictor {
  time_restrictor(timestamp first, timestamp second);

  bool operator()(none) const;
  bool operator()(const conjunction& con) const;
  bool operator()(const disjunction& dis) const;
  bool operator()(const negation& n) const;
  bool operator()(const predicate& p) const;

  timestamp first_;
  timestamp last_;
};

/// Transforms all ::key_extractor and ::type_extractor predicates into
/// ::data_extractor instances according to a given type.
struct type_resolver {
  type_resolver(const type& t);

  expected<expression> operator()(none);
  expected<expression> operator()(const conjunction& c);
  expected<expression> operator()(const disjunction& d);
  expected<expression> operator()(const negation& n);
  expected<expression> operator()(const predicate& p);
  expected<expression> operator()(const type_extractor& ex, const data& d);
  expected<expression> operator()(const data& d, const type_extractor& ex);
  expected<expression> operator()(const key_extractor& ex, const data& d);
  expected<expression> operator()(const data& d, const key_extractor& e);

  template <class T, class U>
  expected<expression> operator()(const T& lhs, const U& rhs) {
    return {predicate{lhs, op_, rhs}};
  }

  relational_operator op_;
  const type& type_;
};

// Tailors an expression to a specific type by pruning all unecessary branches
// and resolving keys into the corresponding data extractors.
struct type_pruner {
  type_pruner(const type& event_type);

  expression operator()(none);
  expression operator()(const conjunction& c);
  expression operator()(const disjunction& d);
  expression operator()(const negation& n);
  expression operator()(const predicate& p);

  relational_operator op_;
  const type& type_;
};

/// Evaluates an event over a [resolved](@ref type_extractor) expression.
struct event_evaluator {
  event_evaluator(const event& e);

  bool operator()(none);
  bool operator()(const conjunction& c);
  bool operator()(const disjunction& d);
  bool operator()(const negation& n);
  bool operator()(const predicate& p);
  bool operator()(const attribute_extractor& e, const data& d);
  bool operator()(const key_extractor&, const data&);
  bool operator()(const type_extractor&, const data&);
  bool operator()(const data_extractor& e, const data& d);

  template <class T>
  bool operator()(const data& d, const T& x) {
    return (*this)(x, d);
  }

  template <class T, class U>
  bool operator()(const T&, const U&) {
    return false;
  }

  const event& event_;
  relational_operator op_;
};

/// Checks whether a [resolved](@ref type_extractor) expression matches a given
/// type. That is, this visitor tests whether an expression consists of a
/// viable set of predicates for a type. For conjunctions, all operands must
/// match. For disjunctions, at least one operand must match.
struct matcher {
  matcher(const type& t);

  bool operator()(none);
  bool operator()(const conjunction&);
  bool operator()(const disjunction&);
  bool operator()(const negation&);
  bool operator()(const predicate&);
  bool operator()(const attribute_extractor&, const data&);
  bool operator()(const data_extractor&, const data&);

  template <class T>
  bool operator()(const data& d, const T& x) {
    return (*this)(x, d);
  }

  template <class T, class U>
  bool operator()(const T&, const U&) {
    return false;
  }

  const type& type_;
  relational_operator op_;
};

} // namespace vast

#endif
