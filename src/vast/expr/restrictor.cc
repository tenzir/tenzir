#include "vast/expr/restrictor.h"
#include "vast/expression.h"
#include "vast/util/assert.h"

namespace vast {
namespace expr {

time_restrictor::time_restrictor(time::point first, time::point last)
  : first_{first}, last_{last} {
}

bool time_restrictor::operator()(none) const {
  VAST_ASSERT(!"should never happen");
  return false;
}

bool time_restrictor::operator()(conjunction const& con) const {
  for (auto& op : con)
    if (!visit(*this, op))
      return false;
  return true;
}

bool time_restrictor::operator()(disjunction const& dis) const {
  for (auto& op : dis)
    if (visit(*this, op))
      return true;
  return false;
}

bool time_restrictor::operator()(negation const& n) const {
  // We can only apply a negation if it sits directly on top of a time
  // extractor, because we can then negate the meaning of the temporal
  // constraint.
  auto r = visit(*this, n.expression());
  if (auto p = get<predicate>(n.expression()))
    if (is<time_extractor>(p->lhs))
      return !r;
  return r;
}

bool time_restrictor::operator()(predicate const& p) const {
  if (!is<time_extractor>(p.lhs))
    return true;
  auto d = get<data>(p.rhs);
  VAST_ASSERT(d && is<time::point>(*d));
  return data::evaluate(first_, p.op, *d) || data::evaluate(last_, p.op, *d);
}

} // namespace expr
} // namespace vast
