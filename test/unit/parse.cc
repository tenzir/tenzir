#define BOOST_SPIRIT_QI_DEBUG

#include <boost/test/unit_test.hpp>
#include <vast/detail/parser/query.h>
#include <vast/util/parser/parse.h>

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

  vast::detail::ast::expression expr;
  using vast::util::parser::parse;
  for (auto& e : expressions)
    BOOST_CHECK((parse<vast::detail::parser::expression>(e, expr)));
}

BOOST_AUTO_TEST_CASE(queries)
{
  std::vector<std::string> queries
  {
    // Type queries.
    ":port < 53/udp",
    ":set != {T, F}",
    ":addr == 192.168.0.1 && :port == 80/tcp",
    ":string ~ /evil.*/ && :prefix >= 10.0.0.0/8",
    ":addr == 1.2.3.4 ^ 5.6.7.8 || :prefix != 10.0.0.0/8",
    "! :int == +8 / +4 || ! :count < -(4 * 2)",
    // Event tags.
    "&name == \"foo\"",
    "&time < 1 hour ago",
    "&id == 42",
    // Offsets.
    "@5 in {1, 2, 3}",
    "@10,3 < 1 hour ago",
    "@0,3,2 ~ /yikes/",
    // Dereferencing event names.
    "foo$bar == T",
    "foo$c$id_orig == 192.168.1.1",
    "*$c$id_orig == 192.168.1.1"
  };

  vast::detail::ast::query query;
  using vast::util::parser::parse;
  for (auto& q : queries)
    BOOST_CHECK((parse<vast::detail::parser::query>(q, query)));

  auto fail = ":foo == -42";
  BOOST_CHECK(! (parse<vast::detail::parser::query>(fail, query)));
}
