#ifndef VAST_EXPR_HOISTER_H
#define VAST_EXPR_HOISTER_H

#include "vast/none.h"

namespace vast {

class expression;
struct conjunction;
struct disjunction;
struct negation;
struct predicate;

namespace expr {

/// Hoists the contained expression of a single-element conjunction or
/// disjunction one level in the tree.
struct hoister
{
  expression operator()(none) const;
  expression operator()(conjunction const& c) const;
  expression operator()(disjunction const& d) const;
  expression operator()(negation const& n) const;
  expression operator()(predicate const& p) const;
};

} // namespace expr
} // namespace vast

#endif
