#include "vast/detail/ast/query.h"

#include <boost/variant/apply_visitor.hpp>

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
        assert(! "unary expression folder not yet implemented");
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
        assert(! "binary expression folder not yet implemented");
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

struct validator : public boost::static_visitor<bool>
{
  static bool apply(query const& q)
  {
    if (! boost::apply_visitor(validator(), q.first))
      return false;

    for (auto& operation : q.rest)
      if (! boost::apply_visitor(validator(), operation.operand))
        return false;

    return true;
  }

  bool operator()(query const& q) const
  {
    return apply(q);
  }

  bool operator()(predicate const& operand) const
  {
    return boost::apply_visitor(*this, operand);
  }

  bool operator()(tag_predicate const& pred) const
  {
    auto rhs = fold(pred.rhs);
    auto& lhs = pred.lhs;
    return
      (lhs == "type" && (is<std::string>(rhs) || is<pattern>(rhs)))
      || (lhs == "time" && is<time_point>(rhs))
      || (lhs == "id" && is<count>(rhs));
  }

  bool operator()(type_predicate const& pred) const
  {
    auto rhs = fold(pred.rhs);
    auto& op = pred.op;
    return pred.lhs.check(rhs)
      || (is<type::string>(pred.lhs)
          && (op == match || op == not_match || op == in || op == not_in)
          && is<pattern>(rhs))
      || (is<type::address>(pred.lhs) && op == in && is<subnet>(rhs));
  }

  bool operator()(schema_predicate const& pred) const
  {
    auto rhs = fold(pred.rhs);
    return ! is<none>(rhs) && ! pred.lhs.empty();
  }

  bool operator()(negated_predicate const& pred) const
  {
    return boost::apply_visitor(*this, pred.operand);
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

bool validate(query const& q)
{
  return validator::apply(q);
};

} // namespace query
} // namespace ast
} // namespace detail
} // namespace vast
