#include <boost/variant/apply_visitor.hpp>

#include "vast/detail/ast/query.h"
#include "vast/util/assert.h"

namespace vast {
namespace detail {
namespace ast {
namespace query {

struct folder : public boost::static_visitor<data>
{
  static data apply(arithmetic_operator op, data const& /* val */)
  {
    switch (op)
    {
      default:
        VAST_ASSERT(! "unary expression folder not yet implemented");
        return {};
        // TODO: implement VAST operations.
        //case positive:
        //    return val;
        //case negative:
        //    return -val;
        //case bitwise_not:
        //    return ~val;
    }
  }

  static data apply(arithmetic_operator op,
                     data const& /* lhs */,
                     data const& /* rhs */)
  {
    switch (op)
    {
      default:
        VAST_ASSERT(! "binary expression folder not yet implemented");
        return {};
        // TODO: implement VAST operations.
        //case bitwise_or:
        //    return lhs | rhs;
        //case bitwise_xor:
        //    return lhs ^ rhs;
        //case bitwise_and:
        //    return lhs & rhs;
        //case plus:
        //    return lhs + rhs;
        //case minus:
        //    return lhs - rhs;
        //case times:
        //    return lhs * rhs;
        //case divide:
        //    return lhs / rhs;
        //case mod:
        //    return lhs % rhs;
    }
  }

  data operator()(data const& d) const
  {
    return d;
  }

  data operator()(unary_expr const& unary) const
  {
    auto operand = boost::apply_visitor(*this, unary.operand);
    return apply(unary.op, operand);
  }

  data operator()(expr_operand const& operand) const
  {
    return boost::apply_visitor(*this, operand);
  }

  data operator()(data_expr const& expr) const
  {
    auto d = boost::apply_visitor(*this, expr.first);
    if (expr.rest.empty())
      return d;

    for (auto& operation : expr.rest)
    {
      auto operand = boost::apply_visitor(*this, operation.operand);
      d = apply(operation.op, d, operand);
    }

    return d;
  }
};

data fold(data_expr const& expr)
{
  auto d = boost::apply_visitor(folder(), expr.first);
  if (expr.rest.empty())
    return d;

  for (auto& operation : expr.rest)
  {
    auto operand = boost::apply_visitor(folder(), operation.operand);
    d = folder::apply(operation.op, d, operand);
  }

  return d;
}

} // namespace query
} // namespace ast
} // namespace detail
} // namespace vast
