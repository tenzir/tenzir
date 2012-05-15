#define BOOST_SPIRIT_QI_DEBUG

#include <boost/test/unit_test.hpp>
#include "vast/query/parser/query.h"
#include "vast/util/parser/parse.h"

BOOST_AUTO_TEST_CASE(expressions)
{
    using vast::util::parser::parse;

    std::vector<std::string> expressions
    {
        "T",
        "53/udp",
        "192.168.0.1 + 127.0.0.1",
        "(42 - 24) / 2",
        "-(42 - 24) / 2"
    };

    vast::query::ast::expression expr;
    for (auto& e : expressions)
        BOOST_CHECK((parse<vast::query::parser::expression>(e, expr)));
}

BOOST_AUTO_TEST_CASE(queries)
{
    using vast::util::parser::parse;

    std::vector<std::string> queries
    {
        "@port < 53/udp",
        "@set != {T, F}",
        "@address == 192.168.0.1 && @port == 80/tcp",
        "@string ~ /evil.*/ && @prefix >= 10.0.0.0/8",
        "@address == 1.2.3.4 ^ 5.6.7.8 || @prefix != 10.0.0.0/8",
        "! @int == +8 / +4 || ! @count < -(4 * 2)"
    };

    vast::query::ast::query query;
    for (auto& q : queries)
         BOOST_CHECK((parse<vast::query::parser::query>(q, query)));

    auto fail = "@foo == -42";
    BOOST_CHECK(! (parse<vast::query::parser::query>(fail, query)));
}
