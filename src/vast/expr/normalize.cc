#include "vast/expr/normalize.h"

#include "vast/expression.h"

namespace vast {
namespace expr {

expression hoister::operator()(none) const
{
  return expression{};
}

expression hoister::operator()(conjunction const& c) const
{
  conjunction hoisted;
  for (auto& op : c)
    if (auto inner = get<conjunction>(op))
      for (auto& inner_op : *inner)
        hoisted.push_back(visit(*this, inner_op));
    else
      hoisted.push_back(visit(*this, op));
  return hoisted.size() == 1 ? hoisted[0] : hoisted;
}

expression hoister::operator()(disjunction const& d) const
{
  disjunction hoisted;
  for (auto& op : d)
    if (auto inner = get<disjunction>(op))
      for (auto& inner_op : *inner)
        hoisted.push_back(visit(*this, inner_op));
    else
      hoisted.push_back(visit(*this, op));
  return hoisted.size() == 1 ? hoisted[0] : hoisted;
}

expression hoister::operator()(negation const& n) const
{
  return {negation{visit(*this, n.expression())}};
}

expression hoister::operator()(predicate const& p) const
{
  return {p};
}


expression aligner::operator()(none) const
{
  return nil;
}

expression aligner::operator()(conjunction const& c) const
{
  conjunction copy;
  for (auto& op : c)
    copy.push_back(visit(*this, op));
  return copy;
}

expression aligner::operator()(disjunction const& d) const
{
  disjunction copy;
  for (auto& op : d)
    copy.push_back(visit(*this, op));
  return copy;
}

expression aligner::operator()(negation const& n) const
{
  return {negation{visit(*this, n.expression())}};
}

expression aligner::operator()(predicate const& p) const
{
  return {is<data>(p.rhs) ? p : predicate{p.rhs, flip(p.op), p.lhs}};
}


expression denegator::operator()(none) const
{
  return nil;
}

expression denegator::operator()(conjunction const& c) const
{
  if (negate_)
  {
    disjunction copy;
    for (auto& op : c)
      copy.push_back(visit(*this, op));
    return copy;
  }
  else
  {
    conjunction copy;
    for (auto& op : c)
      copy.push_back(visit(*this, op));
    return copy;
  }
}

expression denegator::operator()(disjunction const& d) const
{
  if (negate_)
  {
    conjunction copy;
    for (auto& op : d)
      copy.push_back(visit(*this, op));
    return copy;
  }
  else
  {
    disjunction copy;
    for (auto& op : d)
      copy.push_back(visit(*this, op));
    return copy;
  }
}

expression denegator::operator()(negation const& n) const
{
  // Step through double negations.
  if (auto inner = get<negation>(n.expression()))
    return visit(*this, inner->expression());
  // Apply De Morgan from here downward.
  denegator visitor{true};
  visitor.negate_ = true;
  return visit(visitor, n.expression());
}

expression denegator::operator()(predicate const& p) const
{
  return predicate{p.lhs, negate_ ? negate(p.op) : p.op, p.rhs};
}

expression normalize(expression const& expr)
{
  expression r;
  r = visit(hoister{}, expr);
  r = visit(aligner{}, r);
  r = visit(denegator{}, r);
  r = visit(hoister{}, r);
  return r;
};

} // namespace expr
} // namespace vast
