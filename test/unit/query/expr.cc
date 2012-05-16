#include <boost/test/unit_test.hpp>

#include <ze/event.h>
#include "vast/query/ast.h"
#include "vast/query/exception.h"
#include "vast/query/expression.h"
#include "vast/query/parser/query.h"
#include "vast/util/parser/parse.h"

std::vector<ze::event_ptr> events
{
    new ze::event{"foo", "babba", 1.337, 42u, 100, "bar", -4.8},
    new ze::event{"bar", "yadda", ze::record{false, "baz"}}
};

bool test_expression(std::string const& query, ze::event_ptr const& event)
{
    vast::query::ast::query ast;
    if (! vast::util::parser::parse<vast::query::parser::query>(query, ast))
        throw vast::query::syntax_exception(query);

    if (! vast::query::ast::validate(ast))
        throw vast::query::semantic_exception("semantic error", query);

    vast::query::expression expr;
    expr.assign(ast);
    return expr.eval(event);
}

BOOST_AUTO_TEST_CASE(type_queries)
{
    std::vector<std::string> queries
    {
        ":count == 42",
        ":int != +101",
        ":string ~ /bar/ && :int == +100",
        ":double >= -4.8",
        ":int <= -3 || :int >= +100 && :string !~ /bar/ || :double > 1.0"
    };

    for (auto& q : queries)
        BOOST_CHECK(test_expression(q, events[0]));

    for (auto& q : queries)
        BOOST_CHECK(! test_expression(q, events[1]));
}

BOOST_AUTO_TEST_CASE(event_queries)
{
    std::vector<std::string> true_queries
    {
        "foo:count == 42 || bar:string ~ /yad.*/",
        "f*:count == 42 || :bool == F",
        "f*$not$yet$implemented ~ /vast/ || *$not$there$yet ~ /.*[bd]{2}a/"
    };

    std::vector<std::string> false_queries
    {
        "bar:string ~ /x/ || bar:bool == T"
    };

    for (auto& q : true_queries)
        for (auto& e : events)
            BOOST_CHECK(test_expression(q, e));

    for (auto& q : false_queries)
        for (auto& e : events)
            BOOST_CHECK(! test_expression(q, e));
}
