#include "vast/expression.h"
#include "vast/concept/printable/vast/data.h"
#include "vast/concept/printable/vast/operator.h"
#include "vast/concept/printable/vast/type.h"
#include "vast/expr/validator.h"

namespace vast {
namespace expr {

trial<void> validator::operator()(none) const
{
  return error{"nil expression"};
}

trial<void> validator::operator()(conjunction const& c) const
{
  for (auto& op : c)
  {
    auto t = visit(*this, op);
    if (! t)
     return t;
  }
  return nothing;
}

trial<void> validator::operator()(disjunction const& d) const
{
  for (auto& op : d)
  {
    auto t = visit(*this, op);
    if (! t)
     return t;
  }
  return nothing;
}

trial<void> validator::operator()(negation const& n) const
{
  return visit(*this, n.expression());
}

trial<void> validator::operator()(predicate const& p) const
{
  auto valid = [&](predicate::operand const& lhs,
                  predicate::operand const& rhs) -> trial<void>
  {
    auto rhs_data = get<data>(rhs);
    if (is<event_extractor>(lhs) && rhs_data)
    {
      if (! compatible(type::string{}, p.op, type::derive(*rhs_data)))
        return error{"invalid event extractor: ", *rhs_data, " under ", p.op};
    }
    else if (is<time_extractor>(lhs) && rhs_data)
    {
      if (! compatible(type::time_point{}, p.op, type::derive(*rhs_data)))
        return error{"invalid time extractor: ", *rhs_data, " under ", p.op};
    }
    else
    {
      auto t = get<type_extractor>(lhs);
      if (t && rhs_data)
      {
        if (! compatible(t->type, p.op, type::derive(*rhs_data)))
          return error{"invalid type extractor: ",
                       t->type, ' ', p.op, ' ', *rhs_data};
      }
      else if (is<schema_extractor>(lhs) && rhs_data)
      {
        return nothing;
      }
      else
      {
        return error{"invalid extractor"};
      }
    }
    return nothing;
  };
  auto tl = valid(p.lhs, p.rhs);
  auto tr = valid(p.rhs, p.lhs);
  if (tl || tr)
    return nothing;
  else if (tl)
    return tr;
  else
    return tl;
}

} // namespace expr
} // namespace vast
