#include "vast/expr/evaluator.h"

namespace vast {
namespace expr {

evaluator::evaluator(event const& e)
  : event_{e}
{
}

bool evaluator::operator()(none)
{
  return false;
}

bool evaluator::operator()(conjunction const& c)
{
  for (auto& op : c)
    if (! visit(*this, op))
      return false;

  return true;
}

bool evaluator::operator()(disjunction const& d)
{
  for (auto& op : d)
    if (visit(*this, op))
      return true;

  return false;
}

bool evaluator::operator()(negation const& n)
{
  return ! visit(*this, n[0]);
}

bool evaluator::operator()(predicate const& p)
{
  op_ = p.op;
  return visit(*this, p.lhs, p.rhs);
}

bool evaluator::operator()(event_extractor const&, data const& d)
{
  return data::evaluate(event_.type().name(), op_, d);
}

bool evaluator::operator()(time_extractor const&, data const& d)
{
  return data::evaluate(event_.timestamp(), op_, d);
}

bool evaluator::operator()(type_extractor const&, data const&)
{
  assert(! "type extractor should have been optimized away");
  return false;
}

bool evaluator::operator()(schema_extractor const&, data const&)
{
  assert(! "schema extract should have been resolved");
  return false;
}

bool evaluator::operator()(data_extractor const& e, data const& d)
{
  if (e.type != event_.type())
    return false;

  if (e.offset.empty())
    return data::evaluate(event_.data(), op_, d);

  if (auto r = get<record>(event_))
    if (auto x = r->at(e.offset))
      return data::evaluate(*x, op_, d);

  return false;
}

} // namespace expr
} // namespace vast
