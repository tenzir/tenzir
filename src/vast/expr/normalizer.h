#ifndef VAST_EXPR_NORMALIZER_H
#define VAST_EXPR_NORMALIZER_H

#include "vast/expression.h"

namespace vast {
namespace expr {

/// Normalizes an expression such that extractors end up always on the LHS of a
/// predicate.
struct normalizer
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
