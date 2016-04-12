#ifndef VAST_EXPR_VALIDATOR_HPP
#define VAST_EXPR_VALIDATOR_HPP

#include "vast/maybe.hpp"
#include "vast/none.hpp"

namespace vast {

struct conjunction;
struct disjunction;
struct negation;
struct predicate;

namespace expr {

/// Ensures expression node integrity by checking whether the predicates are
/// semantically correct.
struct validator {
  maybe<void> operator()(none) const;
  maybe<void> operator()(conjunction const& c) const;
  maybe<void> operator()(disjunction const& d) const;
  maybe<void> operator()(negation const& n) const;
  maybe<void> operator()(predicate const& p) const;
};

} // namespace expr
} // namespace vast

#endif
