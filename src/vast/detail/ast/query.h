#ifndef VAST_DETAIL_AST_QUERY_H
#define VAST_DETAIL_AST_QUERY_H

#include <boost/optional.hpp>
#include <boost/variant/recursive_variant.hpp>
#include <boost/fusion/include/adapt_struct.hpp>
#include "vast/operator.h"
#include "vast/value.h"

namespace vast {
namespace detail {
namespace ast {
namespace query {

struct nil {};

struct identifier
{
  identifier(std::string const& s = "")
    : name(s)
  {
  }

  std::string name;
};

struct unary_expr;
struct value_expr;

using expr_operand = boost::variant<
  value,
  boost::recursive_wrapper<unary_expr>,
  boost::recursive_wrapper<value_expr>
>;

struct unary_expr
{
  arithmetic_operator op;
  expr_operand operand;
};

struct expr_operation
{
  arithmetic_operator op;
  expr_operand operand;
};

struct value_expr
{
  expr_operand first;
  std::vector<expr_operation> rest;
};

struct tag_predicate
{
  std::string lhs;
  relational_operator op;
  value_expr rhs;
};

struct type_predicate
{
  type_tag lhs;
  relational_operator op;
  value_expr rhs;
};

struct schema_predicate
{
  std::vector<std::string> lhs;
  relational_operator op;
  value_expr rhs;
};

struct negated_predicate;

using predicate = boost::variant<
  tag_predicate,
  type_predicate,
  schema_predicate,
  boost::recursive_wrapper<negated_predicate>
>;

struct negated_predicate
{
  predicate operand;
};

struct query;

using group = boost::variant<
  predicate,
  boost::recursive_wrapper<query>
>;

struct query_operation
{
  boolean_operator op;
  group operand;
};

struct query
{
  group first;
  std::vector<query_operation> rest;
};

/// Folds a constant expression into a single value.
/// @param expr The constant expression.
/// @returns The folded value.
value fold(value_expr const& expr);

/// Validates a query with respect to semantic correctness. This means ensuring
/// that LHS and RHS of predicate operators have the correct types.
/// @param q The query to validate.
/// @returns `true` *iff* *q* is semantically correct.
bool validate(query const& q);

} // namespace query
} // namespace ast
} // namespace detail
} // namespace vast

BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::query::unary_expr,
    (vast::arithmetic_operator, op)
    (vast::detail::ast::query::expr_operand, operand))

  BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::query::expr_operation,
    (vast::arithmetic_operator, op)
    (vast::detail::ast::query::expr_operand, operand))

  BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::query::value_expr,
    (vast::detail::ast::query::expr_operand, first)
    (std::vector<vast::detail::ast::query::expr_operation>, rest))

  BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::query::tag_predicate,
    (std::string, lhs)
    (vast::relational_operator, op)
    (vast::detail::ast::query::value_expr, rhs))

  BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::query::type_predicate,
    (vast::type_tag, lhs)
    (vast::relational_operator, op)
    (vast::detail::ast::query::value_expr, rhs))

  BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::query::schema_predicate,
    (std::vector<std::string>, lhs)
    (vast::relational_operator, op)
    (vast::detail::ast::query::value_expr, rhs))

  BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::query::negated_predicate,
    (vast::detail::ast::query::predicate, operand))

  BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::query::query_operation,
    (vast::boolean_operator, op)
    (vast::detail::ast::query::group, operand))

  BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::query::query,
    (vast::detail::ast::query::group, first)
    (std::vector<vast::detail::ast::query::query_operation>, rest))

#endif
