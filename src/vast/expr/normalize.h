#ifndef VAST_EXPR_NORMALIZE_H
#define VAST_EXPR_NORMALIZE_H

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

/// Ensures that extractors always end up on the LHS of a predicate.
struct aligner
{
  expression operator()(none) const;
  expression operator()(conjunction const& c) const;
  expression operator()(disjunction const& d) const;
  expression operator()(negation const& n) const;
  expression operator()(predicate const& p) const;
};

/// Pushes negations down to the predicate level and removes double negations.
struct denegator
{
  denegator(bool negate = false);

  expression operator()(none) const;
  expression operator()(conjunction const& c) const;
  expression operator()(disjunction const& d) const;
  expression operator()(negation const& n) const;
  expression operator()(predicate const& p) const;

  bool negate_ = false;
};

/// Normalizes an expression such that:
///   1. Single-element conjunctions/disjunctions don't exist.
///   2. Extractors end up always on the LHS of a predicate.
///   3. Negations are pushed down to the predicate level.
expression normalize(expression const& expr);

} // namespace expr
} // namespace vast

#endif
