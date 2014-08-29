#ifndef VAST_EXPR_VALIDATOR_H
#define VAST_EXPR_VALIDATOR_H

#include "vast/trial.h"
#include "vast/none.h"

namespace vast {

struct conjunction;
struct disjunction;
struct negation;
struct predicate;

namespace expr {

/// Ensures expression node integrity by checking whether the predicates are
/// semantically correct.
struct validator
{
  trial<void> operator()(none) const;
  trial<void> operator()(conjunction const& c) const;
  trial<void> operator()(disjunction const& d) const;
  trial<void> operator()(negation const& n) const;
  trial<void> operator()(predicate const& p) const;
};

} // namespace expr
} // namespace vast

#endif
