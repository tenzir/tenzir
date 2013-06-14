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

  value operator()(expression const& expr) const
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
  bool operator()(clause const& operand) const
  {
    return boost::apply_visitor(*this, operand);
  }

  bool operator()(tag_clause const& clause) const
  {
    auto rhs = fold(clause.rhs);
    auto rhs_type = rhs.which();
    auto& lhs = clause.lhs;
    return
      (lhs == "name" && (rhs_type == string_type
                         || rhs_type == regex_type))
      || (lhs == "time" && rhs_type == time_point_type)
      || (lhs == "id" && rhs_type == uint_type);
  }

  bool operator()(type_clause const& clause) const
  {
    auto rhs = fold(clause.rhs);
    auto rhs_type = rhs.which();
    auto& lhs_type = clause.lhs;
    auto& op = clause.op;
    return
      lhs_type == rhs_type
      || (lhs_type == string_type
          && (op == match || op == not_match || op == in || op == not_in)
          && rhs_type == regex_type)
      || (lhs_type == address_type && clause.op == in
          && rhs_type == prefix_type);
  }

  bool operator()(offset_clause const& clause) const
  {
    auto rhs = fold(clause.rhs);
    return ! (rhs == invalid || clause.offsets.empty());
  }

  bool operator()(event_clause const& clause) const
  {
    auto rhs = fold(clause.rhs);
    return ! (rhs == invalid || clause.lhs.size() < 2);
  }

  bool operator()(negated_clause const& clause) const
  {
    return boost::apply_visitor(*this, clause.operand);
  }
};

value fold(expression const& expr)
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

bool validate(query& q)
{
  if (! boost::apply_visitor(validator(), q.first))
    return false;

  for (auto& operation : q.rest)
    if (! boost::apply_visitor(validator(), operation.operand))
      return false;

  return true;
};

} // namespace query
} // namespace ast
} // namespace detail
} // namespace vast
