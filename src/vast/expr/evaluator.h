#ifndef VAST_EXPR_EVALUATOR_H
#define VAST_EXPR_EVALUATOR_H

#include "vast/expression.h"
#include "vast/event.h"

namespace vast {
namespace expr {

/// Evaluates an event over a resolved expression.
struct evaluator
{
  evaluator(event const& e);

  bool operator()(none);
  bool operator()(conjunction const& c);
  bool operator()(disjunction const& d);
  bool operator()(negation const& n);
  bool operator()(predicate const& p);
  bool operator()(event_extractor const&, data const& d);
  bool operator()(time_extractor const&, data const& d);
  bool operator()(type_extractor const&, data const&);
  bool operator()(schema_extractor const&, data const&);
  bool operator()(data_extractor const& e, data const& d);

  template <typename T>
  bool operator()(data const& d, T const& e)
  {
    return (*this)(e, d);
  }

  template <typename T, typename U>
  bool operator()(T const&, U const&)
  {
    return false;
  }

  event const& event_;
  relational_operator op_;
};

} // namespace expr
} // namespace vast

#endif
