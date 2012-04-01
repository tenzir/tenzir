#ifndef VAST_QUERY_AST_H
#define VAST_QUERY_AST_H

#include <boost/config/warning_disable.hpp>
#include <boost/variant/recursive_variant.hpp>
#include <boost/fusion/include/adapt_struct.hpp>
#include <boost/fusion/include/io.hpp>
#include <boost/optional.hpp>
#include <ze/value.h>
#include <vector>

namespace vast {
namespace query {
namespace ast {

struct nil {};
struct unary_expr;
struct unary_clause;
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
    bitewise_not,
};

// Clause operators sorted by ascending precedence.
enum clause_operator
{
    logical_or,
    logical_and,
    match,
    equal,
    not_equal,
    less,
    less_equal,
    greater,
    greater_equal,
    logical_not
};

enum type
{
    // Basic types
    bool_type,
    int_type,
    uint_type,
    double_type,
    duration_type,
    timepoint_type,
    string_type,

    // Containers
    vector_type,
    set_type,
    table_type,
    record_type,

    // Network types
    address_type,
    prefix_type,
    port_type
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

struct type_clause
{
    type lhs;
    clause_operator op;
    expression rhs;
};

struct event_clause
{
    identifier lhs_event;
    identifier lhs_arg;
    clause_operator op;
    expression rhs;
};

typedef boost::variant<
    type_clause
  , event_clause
  , boost::recursive_wrapper<unary_clause>
> clause_operand;

struct unary_clause
{
    clause_operator op;
    clause_operand operand;
};

struct clause_operation
{
    clause_operator op;
    clause_operand operand;
};

struct query
{
    clause_operand first;
    std::vector<clause_operation> rest;
};

} // namespace ast
} // namespace query
} // namespace vast

BOOST_FUSION_ADAPT_STRUCT(
    vast::query::ast::unary_expr,
    (vast::query::ast::expr_operator, op)
    (vast::query::ast::expr_operand, operand))

BOOST_FUSION_ADAPT_STRUCT(
    vast::query::ast::expr_operation,
    (vast::query::ast::expr_operator, op)
    (vast::query::ast::expr_operand, operand))

BOOST_FUSION_ADAPT_STRUCT(
    vast::query::ast::expression,
    (vast::query::ast::expr_operand, first)
    (std::vector<vast::query::ast::expr_operation>, rest))

BOOST_FUSION_ADAPT_STRUCT(
    vast::query::ast::type_clause,
    (vast::query::ast::type, lhs)
    (vast::query::ast::clause_operator, op)
    (vast::query::ast::expression, rhs))

BOOST_FUSION_ADAPT_STRUCT(
    vast::query::ast::event_clause,
    (vast::query::ast::identifier, lhs_event)
    (vast::query::ast::identifier, lhs_arg)
    (vast::query::ast::clause_operator, op)
    (vast::query::ast::expression, rhs))

BOOST_FUSION_ADAPT_STRUCT(
    vast::query::ast::unary_clause,
    (vast::query::ast::clause_operator, op)
    (vast::query::ast::clause_operand, operand))

BOOST_FUSION_ADAPT_STRUCT(
    vast::query::ast::clause_operation,
    (vast::query::ast::clause_operator, op)
    (vast::query::ast::clause_operand, operand))

BOOST_FUSION_ADAPT_STRUCT(
    vast::query::ast::query,
    (vast::query::ast::clause_operand, first)
    (std::vector<vast::query::ast::clause_operation>, rest))

#endif
