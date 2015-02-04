#include "vast/expr/resolver.h"

namespace vast {
namespace expr {

schema_resolver::schema_resolver(type const& t)
  : type_{t}
{
}

trial<expression> schema_resolver::operator()(none)
{
  return expression{};
}

trial<expression> schema_resolver::operator()(conjunction const& c)
{
  conjunction copy;
  for (auto& op : c)
  {
    auto r = visit(*this, op);
    if (! r)
      return r;
    else if (is<none>(*r))
      return expression{};
    else
      copy.push_back(std::move(*r));
  }

  if (copy.empty())
    return expression{};
  if (copy.size() == 1)
    return {std::move(copy[0])};
  else
    return {std::move(copy)};
}

trial<expression> schema_resolver::operator()(disjunction const& d)
{
  disjunction copy;
  for (auto& op : d)
  {
    auto r = visit(*this, op);
    if (! r)
      return r;
    else if (! is<none>(*r))
      copy.push_back(std::move(*r));
  }

  if (copy.empty())
    return expression{};
  if (copy.size() == 1)
    return {std::move(copy[0])};
  else
    return {std::move(copy)};
}

trial<expression> schema_resolver::operator()(negation const& n)
{
  auto r = visit(*this, n.expression());
  if (! r)
    return r;
  else if (! is<none>(*r))
    return {negation{std::move(*r)}};
  else
    return expression{};
}

trial<expression> schema_resolver::operator()(predicate const& p)
{
  op_ = p.op;
  return visit(*this, p.lhs, p.rhs);
}

trial<expression> schema_resolver::operator()(schema_extractor const& e,
                                              data const& d)
{
  disjunction dis;
  auto r = get<type::record>(type_);
  if (! r)
  {
    // If we're not dealing with a record, the only possible match is a
    // single-element key which represents the event name.
    if (e.key.size() == 1 && type_.name() == e.key[0])
      dis.emplace_back(predicate{data_extractor{type_, {}}, op_, d});
  }
  else
  {
    auto trace = r->find_suffix(e.key);
    if (trace.size() > 1)
    {
      // Make sure that all found keys resolve to arguments with the same type.
      auto first_type = r->at(trace.front().first);
      for (auto& p : trace)
        if (! p.first.empty())
          if (! congruent(*r->at(p.first), *first_type))
            return error{"type clash: ", type_, " : ", to_string(type_, false),
                         " <--> ", *r->at(p.first), " : ",
                         to_string(*r->at(p.first), false)};
    }

    // Add all offsets from the trace to the disjunction, which will
    // eventually replace this node.
    for (auto& p : trace)
      dis.emplace_back(
          predicate{data_extractor{type_, std::move(p.first)}, op_, d});
  }

  if (dis.empty())
    return expression{};
  else if (dis.size() == 1)
    return {std::move(dis[0])};
  else
    return {std::move(dis)};
}

trial<expression> schema_resolver::operator()(data const& d,
                                              schema_extractor const& e)
{
  return (*this)(e, d);
}


type_resolver::type_resolver(type const& event_type)
  : type_{event_type}
{
}

expression type_resolver::operator()(none)
{
  return {};
}

expression type_resolver::operator()(conjunction const& c)
{
  conjunction copy;
  for (auto& op : c)
  {
    auto e = visit(*this, op);
    if (is<none>(e))
      // If any operand of the conjunction is not a viable type resolver, the
      // entire conjunction is not viable. For example, if we have an event
      // consisting only of numeric types and the conjunction (int == +42 &&
      // string == "foo"), then the entire conjunction is not viable.
      return {};
    else
      copy.push_back(std::move(e));
  }

  if (copy.empty())
    return {};
  if (copy.size() == 1)
    return {std::move(copy[0])};
  else
    return {std::move(copy)};
}

expression type_resolver::operator()(disjunction const& d)
{
  disjunction copy;
  for (auto& op : d)
  {
    auto e = visit(*this, op);
    if (! is<none>(e))
      copy.push_back(std::move(e));
  }

  if (copy.empty())
    return {};
  if (copy.size() == 1)
    return {std::move(copy[0])};
  else
    return {std::move(copy)};
}

expression type_resolver::operator()(negation const& n)
{
  auto e = visit(*this, n.expression());
  if (is<none>(e))
    return e;
  else
    return {negation{std::move(e)}};
}

expression type_resolver::operator()(predicate const& p)
{
  if (auto lhs = get<type_extractor>(p.lhs))
  {
    auto r = get<type::record>(type_);
    if (! r)
    {
      if (congruent(type_, lhs->type))
        return {predicate{data_extractor{type_, {}}, p.op, p.rhs}};
      else
        return {};
    }

    disjunction dis;
    for (auto& e : type::record::each{*r})
      if (congruent(e.trace.back()->type, lhs->type))
        dis.emplace_back(
          predicate{data_extractor{type_, e.offset}, p.op, p.rhs});

    if (dis.empty())
      return {};
    if (dis.size() == 1)
      return {std::move(dis[0])};
    else
      return {std::move(dis)};
  }
  else if (auto lhs = get<data_extractor>(p.lhs))
  {
    if (lhs->type != type_)
      return {};
  }

  return {p};
}

} // namespace expr
} // namespace vast
