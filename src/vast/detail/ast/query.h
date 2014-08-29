#ifndef VAST_DETAIL_AST_QUERY_H
#define VAST_DETAIL_AST_QUERY_H

#include <boost/optional.hpp>
#include <boost/variant/recursive_variant.hpp>
#include <boost/fusion/include/adapt_struct.hpp>
#include "vast/operator.h"
#include "vast/data.h"

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
struct data_expr;

using expr_operand = boost::variant<
  data,
  boost::recursive_wrapper<unary_expr>,
  boost::recursive_wrapper<data_expr>
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

struct data_expr
{
  expr_operand first;
  std::vector<expr_operation> rest;
};

struct predicate
{
  using lhs_or_rhs = boost::variant<std::string, data_expr>;

  lhs_or_rhs lhs;
  relational_operator op;
  lhs_or_rhs rhs;
};

struct query_expr;
struct negated;

using group = boost::variant<
  predicate,
  boost::recursive_wrapper<query_expr>,
  boost::recursive_wrapper<negated>
>;

struct query_operation
{
  boolean_operator op;
  group operand;
};

struct query_expr
{
  group first;
  std::vector<query_operation> rest;
};

struct negated
{
  query_expr expr;
};

/// Folds a constant expression into a single datum.
/// @param expr The data expression.
/// @returns The folded datum.
data fold(data_expr const& expr);

/// Validates a query with respect to semantic correctness. This means ensuring
/// that LHS and RHS of predicate operators have the correct types.
/// @param q The query to validate.
/// @returns `true` *iff* *q* is semantically correct.
bool validate(query_expr const& q);

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
    vast::detail::ast::query::data_expr,
    (vast::detail::ast::query::expr_operand, first)
    (std::vector<vast::detail::ast::query::expr_operation>, rest))

  BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::query::predicate,
    (vast::detail::ast::query::predicate::lhs_or_rhs, lhs)
    (vast::relational_operator, op)
    (vast::detail::ast::query::predicate::lhs_or_rhs, rhs))

  BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::query::query_operation,
    (vast::boolean_operator, op)
    (vast::detail::ast::query::group, operand))

  BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::query::query_expr,
    (vast::detail::ast::query::group, first)
    (std::vector<vast::detail::ast::query::query_operation>, rest))

  BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::query::negated,
    (vast::detail::ast::query::query_expr, expr))

#endif
