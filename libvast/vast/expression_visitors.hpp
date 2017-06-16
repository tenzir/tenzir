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
  expression operator()(conjunction const& c) const;
  expression operator()(disjunction const& d) const;
  expression operator()(negation const& n) const;
  expression operator()(predicate const& p) const;
};

/// Ensures that extractors always end up on the LHS of a predicate.
struct aligner {
  expression operator()(none) const;
  expression operator()(conjunction const& c) const;
  expression operator()(disjunction const& d) const;
  expression operator()(negation const& n) const;
  expression operator()(predicate const& p) const;
};

/// Pushes negations down to the predicate level and removes double negations.
struct denegator {
  denegator(bool negate = false);

  expression operator()(none) const;
  expression operator()(conjunction const& c) const;
  expression operator()(disjunction const& d) const;
  expression operator()(negation const& n) const;
  expression operator()(predicate const& p) const;

  bool negate_ = false;
};

/// Extracts all predicates from an expression.
struct predicatizer {
  std::vector<predicate> operator()(none) const;
  std::vector<predicate> operator()(conjunction const& c) const;
  std::vector<predicate> operator()(disjunction const& d) const;
  std::vector<predicate> operator()(negation const& n) const;
  std::vector<predicate> operator()(predicate const& p) const;
};

/// Ensures that LHS and RHS of a predicate fit together.
struct validator {
  expected<void> operator()(none);
  expected<void> operator()(conjunction const& c);
  expected<void> operator()(disjunction const& d);
  expected<void> operator()(negation const& n);
  expected<void> operator()(predicate const& p);
  expected<void> operator()(attribute_extractor const& ex, data const& d);
  expected<void> operator()(type_extractor const& ex, data const& d);
  expected<void> operator()(key_extractor const& ex, data const& d);

  template <typename T, typename U>
  expected<void> operator()(T const& lhs, U const& rhs) {
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
  bool operator()(conjunction const& con) const;
  bool operator()(disjunction const& dis) const;
  bool operator()(negation const& n) const;
  bool operator()(predicate const& p) const;

  timestamp first_;
  timestamp last_;
};

/// Transforms all ::key_extractor and ::type_extractor predicates into
/// ::data_extractor instances according to a given type.
struct type_resolver {
  type_resolver(type const& t);

  expected<expression> operator()(none);
  expected<expression> operator()(conjunction const& c);
  expected<expression> operator()(disjunction const& d);
  expected<expression> operator()(negation const& n);
  expected<expression> operator()(predicate const& p);
  expected<expression> operator()(type_extractor const& ex, data const& d);
  expected<expression> operator()(data const& d, type_extractor const& ex);
  expected<expression> operator()(key_extractor const& ex, data const& d);
  expected<expression> operator()(data const& d, key_extractor const& e);

  template <typename T, typename U>
  expected<expression> operator()(T const& lhs, U const& rhs) {
    return {predicate{lhs, op_, rhs}};
  }

  relational_operator op_;
  type const& type_;
};

// Tailors an expression to a specific type by pruning all unecessary branches
// and resolving keys into the corresponding data extractors.
struct type_pruner {
  type_pruner(type const& event_type);

  expression operator()(none);
  expression operator()(conjunction const& c);
  expression operator()(disjunction const& d);
  expression operator()(negation const& n);
  expression operator()(predicate const& p);

  relational_operator op_;
  type const& type_;
};

/// Evaluates an event over a resolved expression.
struct event_evaluator {
  event_evaluator(event const& e);

  bool operator()(none);
  bool operator()(conjunction const& c);
  bool operator()(disjunction const& d);
  bool operator()(negation const& n);
  bool operator()(predicate const& p);
  bool operator()(attribute_extractor const&, data const& d);
  bool operator()(key_extractor const&, data const&);
  bool operator()(type_extractor const&, data const&);
  bool operator()(data_extractor const& e, data const& d);

  template <typename T>
  bool operator()(data const& d, T const& e) {
    return (*this)(e, d);
  }

  template <typename T, typename U>
  bool operator()(T const&, U const&) {
    return false;
  }

  event const& event_;
  relational_operator op_;
};

} // namespace vast

#endif
