#ifndef VAST_DETAIL_AST_QUERY_H
#define VAST_DETAIL_AST_QUERY_H

#include <boost/optional.hpp>
#include <boost/variant/recursive_variant.hpp>
#include <boost/fusion/include/adapt_struct.hpp>
#include <ze/value.h>

namespace vast {
namespace detail {
namespace ast {
namespace query {

struct nil {};
struct unary_expr;
struct negated_clause;
struct expression;

struct identifier
{
  identifier(std::string const& s = "")
    : name(s)
  {
  }

  std::string name;
};

typedef boost::variant<
ze::value
  , boost::recursive_wrapper<unary_expr>
  , boost::recursive_wrapper<expression>
> expr_operand;

// Expression operators sorted by ascending precedence.
enum expr_operator
{
  bitwise_or,
  bitwise_xor,
  bitwise_and,
  plus,
  minus,
  times,
  divide,
  mod,
  positive,
  negative,
  bitwise_not,
};

// Clause operators sorted by ascending precedence.
enum clause_operator : int
{
  match,
  not_match,
  equal,
  not_equal,
  less,
  less_equal,
  greater,
  greater_equal,
  in,
  not_in
};

// Binary boolean operators.
enum boolean_operator
{
  logical_or,
  logical_and
};

struct unary_expr
{
  expr_operator op;
  expr_operand operand;
};

struct expr_operation
{
  expr_operator op;
  expr_operand operand;
};

struct expression
{
  expr_operand first;
  std::vector<expr_operation> rest;
};

struct tag_clause
{
  std::string lhs;
  clause_operator op;
  expression rhs;
};

struct offset_clause
{
  std::vector<size_t> offsets;
  clause_operator op;
  expression rhs;
};

struct type_clause
{
  ze::value_type lhs;
  clause_operator op;
  expression rhs;
};

struct event_clause
{
  std::vector<std::string> lhs;
  clause_operator op;
  expression rhs;
};

typedef boost::variant<
    tag_clause
  , offset_clause
  , type_clause
  , event_clause
  , boost::recursive_wrapper<negated_clause>
> clause;

struct negated_clause
{
  clause operand;
};

struct clause_operation
{
  boolean_operator op;
  clause operand;
};

struct query
{
  clause first;
  std::vector<clause_operation> rest;
};

/// Negates a clause operand.
/// @param op The operator to negate.
/// @return The negation of @a op.
clause_operator negate(clause_operator op);

/// Folds a constant expression into a single value.
/// @param expr The constant expression.
/// @return The folded value.
ze::value fold(expression const& expr);

/// Validates a query with respect to semantic correctness. This means ensuring
/// that LHS and RHS of clause operators have the correct types.
/// @param q The query to validate.
/// @return `true` *iff* *q* is semantically correct.
bool validate(query& q);

} // namespace query
} // namespace ast
} // namespace detail
} // namespace vast

BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::query::unary_expr,
    (vast::detail::ast::query::expr_operator, op)
    (vast::detail::ast::query::expr_operand, operand))

  BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::query::expr_operation,
    (vast::detail::ast::query::expr_operator, op)
    (vast::detail::ast::query::expr_operand, operand))

  BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::query::expression,
    (vast::detail::ast::query::expr_operand, first)
    (std::vector<vast::detail::ast::query::expr_operation>, rest))

  BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::query::tag_clause,
    (std::string, lhs)
    (vast::detail::ast::query::clause_operator, op)
    (vast::detail::ast::query::expression, rhs))

  BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::query::offset_clause,
    (std::vector<size_t>, offsets)
    (vast::detail::ast::query::clause_operator, op)
    (vast::detail::ast::query::expression, rhs))

  BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::query::type_clause,
    (ze::value_type, lhs)
    (vast::detail::ast::query::clause_operator, op)
    (vast::detail::ast::query::expression, rhs))

  BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::query::event_clause,
    (std::vector<std::string>, lhs)
    (vast::detail::ast::query::clause_operator, op)
    (vast::detail::ast::query::expression, rhs))

  BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::query::negated_clause,
    (vast::detail::ast::query::clause, operand))

  BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::query::clause_operation,
    (vast::detail::ast::query::boolean_operator, op)
    (vast::detail::ast::query::clause, operand))

  BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::query::query,
    (vast::detail::ast::query::clause, first)
    (std::vector<vast::detail::ast::query::clause_operation>, rest))

#endif
