#include "vast/detail/ast/query.h"

#include <boost/variant/apply_visitor.hpp>

namespace vast {
namespace detail {
namespace ast {
namespace query {

struct folder : public boost::static_visitor<value>
{
  static value apply(arithmetic_operator op, value const& /* val */)
  {
    switch (op)
    {
      default:
        assert(! "unary expression folder not yet implemented");
        return invalid;
        // TODO: implement VAST operations.
        //case positive:
        //    return val;
        //case negative:
        //    return -val;
        //case bitwise_not:
        //    return ~val;
    }
  }

  static value apply(arithmetic_operator op,
                     value const& /* lhs */,
                     value const& /* rhs */)
  {
    switch (op)
    {
      default:
        assert(! "binary expression folder not yet implemented");
        return invalid;
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

  value operator()(value const& val) const
  {
    return val;
  }

  value operator()(unary_expr const& unary) const
  {
    auto operand = boost::apply_visitor(*this, unary.operand);
    return apply(unary.op, operand);
  }

  value operator()(expr_operand const& operand) const
  {
    return boost::apply_visitor(*this, operand);
  }

  value operator()(value_expr const& expr) const
  {
    auto value = boost::apply_visitor(*this, expr.first);
    if (expr.rest.empty())
      return value;

    for (auto& operation : expr.rest)
    {
      auto operand = boost::apply_visitor(*this, operation.operand);
      value = apply(operation.op, value, operand);
    }

    return value;
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
    auto rhs_type = rhs.which();
    auto& lhs = pred.lhs;
    return
      (lhs == "name" && (rhs_type == string_type || rhs_type == regex_type))
      || (lhs == "time" && rhs_type == time_point_type)
      || (lhs == "id" && rhs_type == uint_type);
  }

  bool operator()(type_predicate const& pred) const
  {
    auto rhs = fold(pred.rhs);
    auto rhs_type = rhs.which();
    auto& lhs_type = pred.lhs;
    auto& op = pred.op;
    return
      lhs_type == rhs_type
      || (lhs_type == string_type
          && (op == match || op == not_match || op == in || op == not_in)
          && rhs_type == regex_type)
      || (lhs_type == address_type && pred.op == in
          && rhs_type == prefix_type);
  }

  bool operator()(offset_predicate const& pred) const
  {
    auto rhs = fold(pred.rhs);
    return ! (rhs == invalid || pred.off.empty());
  }

  bool operator()(event_predicate const& pred) const
  {
    auto rhs = fold(pred.rhs);
    return ! (rhs == invalid || pred.lhs.size() < 2);
  }

  bool operator()(negated_predicate const& pred) const
  {
    return boost::apply_visitor(*this, pred.operand);
  }
};

value fold(value_expr const& expr)
{
  auto value = boost::apply_visitor(folder(), expr.first);
  if (expr.rest.empty())
    return value;

  for (auto& operation : expr.rest)
  {
    auto operand = boost::apply_visitor(folder(), operation.operand);
    value = folder::apply(operation.op, value, operand);
  }

  return value;
}

bool validate(query const& q)
{
  return validator::apply(q);
};

} // namespace query
} // namespace ast
} // namespace detail
} // namespace vast
