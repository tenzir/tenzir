#ifndef VAST_EXPR_PREDICATIZER_HPP
#define VAST_EXPR_PREDICATIZER_HPP

#include <vector>
#include "vast/none.hpp"

namespace vast {

class expression;
struct conjunction;
struct disjunction;
struct negation;
struct predicate;

namespace expr {

/// Extracts all predicates from an expression.
struct predicatizer {
  std::vector<predicate> operator()(none) const;
  std::vector<predicate> operator()(conjunction const& c) const;
  std::vector<predicate> operator()(disjunction const& d) const;
  std::vector<predicate> operator()(negation const& n) const;
  std::vector<predicate> operator()(predicate const& p) const;
};

} // namespace expr
} // namespace vast

#endif
