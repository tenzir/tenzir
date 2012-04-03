#include "vast/query/boolean_expression.h"

#include <map>
#include <boost/variant/apply_visitor.hpp>
#include <ze/type/regex.h>
#include "vast/query/ast.h"
#include "vast/util/logger.h"

namespace vast {
namespace query {

typedef std::map<ast::clause_operator, clause::binary_operator>
    operator_map;

operator_map create_op_map()
{
    operator_map ops;

    ops[ast::match] =
        [](ze::value const& lhs, ze::value const& rhs) 
        { 
            return rhs.get<ze::regex>().match(lhs.get<ze::string>());
        };

    ops[ast::not_match] =
        [](ze::value const& lhs, ze::value const& rhs) 
        { 
            return ! rhs.get<ze::regex>().match(lhs.get<ze::string>());
        };

    ops[ast::equal] =
        [](ze::value const& lhs, ze::value const& rhs) { return lhs == rhs; };
 
    ops[ast::not_equal] =
        [](ze::value const& lhs, ze::value const& rhs) { return lhs != rhs; };

    ops[ast::less] =
        [](ze::value const& lhs, ze::value const& rhs) { return lhs < rhs; };

    ops[ast::less_equal] =
        [](ze::value const& lhs, ze::value const& rhs) { return lhs <= rhs; };

    ops[ast::greater] =
        [](ze::value const& lhs, ze::value const& rhs) { return lhs > rhs; };

    ops[ast::greater_equal] =
        [](ze::value const& lhs, ze::value const& rhs) { return lhs >= rhs; };

    return ops;
}

operator_map clause_ops = create_op_map();

struct clausifier
{
    typedef void result_type;

    clausifier(std::vector<std::vector<clause>>& clauses)
      : clauses(clauses)
    {
        assert(! clauses.empty());
    }

    void operator()(ast::clause const& operand)
    {
        boost::apply_visitor(*this, operand);
    }

    void operator()(ast::type_clause const& clause)
    {
        auto op = clause.op;
        if (invert)
        {
            op = ast::negate(op);
            invert = false;
        }

        auto rhs = ast::fold(clause.rhs);
        clauses.back().emplace_back(rhs, clause_ops[op]);
    }

    void operator()(ast::event_clause const& clause)
    {
        assert(! "not yet implemented");
    }

    void operator()(ast::negated_clause const& clause)
    {
        invert = true;
        boost::apply_visitor(*this, clause.operand);
    }

    std::vector<std::vector<clause>>& clauses;
    bool invert = false;
};

clause::clause(ze::value rhs, binary_operator op)
  : rhs_(std::move(rhs))
  , op_(op)
  , status_(false)
{
}

clause::operator bool() const
{
    return status_;
}

void clause::eval(ze::value const& lhs)
{
    status_ = op_(lhs, rhs_);
}

ze::value_type clause::type() const
{
    return rhs_.which();
}

boolean_expression::boolean_expression(ast::query const& query)
  : clauses_(1)
{
    clausifier visitor(clauses_);
    boost::apply_visitor(visitor, query.first);
    for (auto& clause : query.rest)
    {
        if (clause.op == ast::logical_or)
            clauses_.emplace_back();

        boost::apply_visitor(visitor, clause.operand);
    }

    for (auto& ands : clauses_)
        for (auto& clause : ands)
            LOG(debug, query) << "clause type " << clause.type();
}

boolean_expression::operator bool() const
{
    return std::any_of(
        clauses_.begin(),
        clauses_.end(),
        [](std::vector<clause> const& ands)
        {
            return std::all_of(
                ands.begin(),
                ands.end(),
                [](clause const& c) { return bool(c); });
        });
}

void boolean_expression::feed(ze::value const& value)
{
    for (auto& ands : clauses_)
        for (auto& clause : ands)
        {
            LOG(debug, query) << "LHS: " << value.which() << " RHS: " << clause.type();
            if (! clause && clause.type() == value.which())
                clause.eval(value);
        }
}

} // namespace query
} // namespace vast
