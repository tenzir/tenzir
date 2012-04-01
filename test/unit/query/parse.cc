#define BOOST_SPIRIT_QI_DEBUG

#include <boost/test/unit_test.hpp>
#include "vast/query/parser/clause.h"

template <template <class> class Grammar, typename Attribute>
bool parse(std::string const& str)
{
    typedef std::string::const_iterator iterator_type;
    auto i = str.begin();
    auto end = str.end();

    vast::util::parser::error_handler<iterator_type> error_handler(i, end);
    Grammar<iterator_type> grammar(error_handler);
    vast::query::parser::skipper<iterator_type> skipper;
    Attribute attr;

    bool success = phrase_parse(i, end, grammar, skipper, attr);
    return success && i == end;
}

BOOST_AUTO_TEST_CASE(expressions)
{
    std::vector<std::string> expressions
    {
        "T",
        "53/udp",
        "192.168.0.1 + 127.0.0.1",
        "(42 - 24) / 2",
        "-(42 - 24) / 2"
    };

    typedef vast::query::ast::expression expr;
    for (auto& e : expressions)
        BOOST_CHECK((parse<vast::query::parser::expression, expr>(e)));
}

BOOST_AUTO_TEST_CASE(clauses)
{
    std::vector<std::string> queries
    {
        "@port < 53/udp",
        "@set != {T, F}",
        "@address == 192.168.0.1 && @port == 80/tcp",
        "@string ~ /evil.*/ && @prefix >= 10.0.0.0/8",
        "@address == 1.2.3.4 ^ 5.6.7.8 || @prefix != 10.0.0.0/8",
        "! @int == +8 / +4 || ! @uint < -(4 * 2)"
    };

    typedef vast::query::ast::query query;
    for (auto& q : queries)
         BOOST_CHECK((parse<vast::query::parser::clause, query>(q)));

    auto fail = "@foo == -42";
    BOOST_CHECK(! (parse<vast::query::parser::clause, query>(fail)));
}
