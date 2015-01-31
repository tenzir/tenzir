#include "vast/expr/restrictor.h"
#include "vast/expression.h"

namespace vast {
namespace expr {

interval_restrictor::interval_restrictor(time_point first, time_point last)
  : first_{first},
    last_{last}
{
}

bool interval_restrictor::operator()(none) const
{
  assert(! "should never happen");
}

bool interval_restrictor::operator()(conjunction const& con) const
{
  for (auto& op : con)
    if (! visit(*this, op))
      return false;
  return true;
}

bool interval_restrictor::operator()(disjunction const& dis) const
{
  for (auto& op : dis)
    if (visit(*this, op))
      return true;
  return false;
}

bool interval_restrictor::operator()(negation const& n) const
{
  // We can only apply a negation if it sits directly on top of a time
  // extractor, because we can then negate the meaning of the temporal
  // constraint.
  auto r = visit(*this, n[0]);
  if (auto p = get<predicate>(n[0]))
    if (is<time_extractor>(p->lhs))
      return ! r;
  return r;
}

bool interval_restrictor::operator()(predicate const& p) const
{
  if (! is<time_extractor>(p.lhs))
    return true;
  auto d = get<data>(p.rhs);
  assert(d && is<time_point>(*d));
  return data::evaluate(first_, p.op, *d) || data::evaluate(last_, p.op, *d);
}

} // namespace expr
} // namespace vast
