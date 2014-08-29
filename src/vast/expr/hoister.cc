#include "vast/expr/hoister.h"

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
    hoisted.push_back(visit(*this, op));

  if (hoisted.size() == 1)
    return {std::move(hoisted[0])};
  else
    return {std::move(hoisted)};
}

expression hoister::operator()(disjunction const& d) const
{
  disjunction hoisted;
  for (auto& op : d)
    hoisted.push_back(visit(*this, op));

  if (hoisted.size() == 1)
    return {std::move(hoisted[0])};
  else
    return {std::move(hoisted)};
}

expression hoister::operator()(negation const& n) const
{
  return {n};
}

expression hoister::operator()(predicate const& p) const
{
  return {p};
}

} // namespace expr
} // namespace vast
