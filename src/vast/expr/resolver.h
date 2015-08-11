#ifndef VAST_EXPR_RESOLVER_H
#define VAST_EXPR_RESOLVER_H

#include "vast/expression.h"
#include "vast/schema.h"

namespace vast {
namespace expr {

/// Transforms schema extractors into one or more data extractors.
struct schema_resolver {
  schema_resolver(type const& t);

  trial<expression> operator()(none);
  trial<expression> operator()(conjunction const& c);
  trial<expression> operator()(disjunction const& d);
  trial<expression> operator()(negation const& n);
  trial<expression> operator()(predicate const& p);
  trial<expression> operator()(schema_extractor const& e, data const& d);
  trial<expression> operator()(data const& d, schema_extractor const& e);

  template <typename T, typename U>
  trial<expression> operator()(T const& lhs, U const& rhs) {
    return {predicate{lhs, op_, rhs}};
  }

  relational_operator op_;
  type const& type_;
};

// Resolves type and data extractor predicates. Specifically, it handles the
// following predicates:
// - Type extractor: replaces the predicate with one or more data extractors.
// - Data extractor: removes the predicate if the event type does not match the
//   type given to this visitor.
struct type_resolver {
  type_resolver(type const& event_type);

  expression operator()(none);
  expression operator()(conjunction const& c);
  expression operator()(disjunction const& d);
  expression operator()(negation const& n);
  expression operator()(predicate const& p);

  relational_operator op_;
  type const& type_;
};

} // namespace expr
} // namespace vast

#endif
