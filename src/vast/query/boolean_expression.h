#ifndef VAST_QUERY_BOOLEAN_EXPRESSION_H
#define VAST_QUERY_BOOLEAN_EXPRESSION_H

#include <ze/value.h>
#include "vast/query/forward.h"

namespace vast {
namespace query {

/// A query clause which can evaluate to true or false.
class clause
{
public:
    typedef std::function<bool(ze::value const&, ze::value const&)>
        binary_operator;

    /// Creates a clause from a value and a binary clause operator.
    /// @param rhs The constant RHS of the operator.
    /// @param op The binary operator functor. 
    clause(ze::value rhs, binary_operator op);

    /// Converts the clause to bool.
    /// @return @c true if the clause is true.
    explicit operator bool() const;

    /// Evaluates the clause with an LHS from an event.
    /// @param lhs The value from an event.
    void eval(ze::value const& lhs);

    /// Retrieves the type of the clause.
    /// @return The type of the clause.
    ze::value_type type() const;

private:
    ze::value rhs_;
    binary_operator op_;
    bool status_;
};

/// A sequence of clauses connected by boolean operators.
class boolean_expression
{
public:
    /// Constructs a boolean expression from a parsed query.
    /// @param query The query AST.
    boolean_expression(ast::query const& query);

    /// Converts the expression to @c bool.
    /// @return @c true if the expression is true.
    explicit operator bool() const;

    /// Adds a value to the boolean expression.
    /// @param value The value to respect by the expression.
    void feed(ze::value const& value);

private:
    std::vector<std::vector<clause>> clauses_;
};

} // namespace query
} // namespace vast

#endif
