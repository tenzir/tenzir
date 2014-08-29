#include "vast/expr/normalizer.h"

#include "vast/expression.h"

namespace vast {
namespace expr {

expression normalizer::operator()(none) const
{
  return nil;
}

expression normalizer::operator()(conjunction const& c) const
{
  conjunction copy;
  for (auto& op : c)
    copy.push_back(visit(*this, op));

  return copy;
}

expression normalizer::operator()(disjunction const& d) const
{
  conjunction copy;
  for (auto& op : d)
    copy.push_back(visit(*this, op));

  return copy;
}

expression normalizer::operator()(negation const& n) const
{
  return {negation{visit(*this, n[0])}};
}

expression normalizer::operator()(predicate const& p) const
{
  return {is<data>(p.rhs) ? p : predicate{p.rhs, flip(p.op), p.lhs}};
}

} // namespace expr
} // namespace vast
