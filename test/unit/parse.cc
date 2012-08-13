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
  vast::detail::ast::query query;
  using vast::util::parser::parse;

  // Type queries.
  auto q = ":port < 53/udp";
  BOOST_CHECK(parse<vast::detail::parser::query>(q, query));
  q = ":set != {T, F}";
  BOOST_CHECK(parse<vast::detail::parser::query>(q, query));
  q = ":addr == 192.168.0.1 && :port == 80/tcp";
  BOOST_CHECK(parse<vast::detail::parser::query>(q, query));
  q = ":string ~ /evil.*/ && :prefix >= 10.0.0.0/8";
  BOOST_CHECK(parse<vast::detail::parser::query>(q, query));
  q = ":addr == 1.2.3.4 ^ 5.6.7.8 || :prefix != 10.0.0.0/8";
  BOOST_CHECK(parse<vast::detail::parser::query>(q, query));
  q = "! :int == +8 / +4 || ! :count < -(4 * 2)";
  BOOST_CHECK(parse<vast::detail::parser::query>(q, query));

  // Event tags.
  q = "&name == \"foo\"";
  BOOST_CHECK(parse<vast::detail::parser::query>(q, query));
  q = "&time < now - 5d10m3s";
  BOOST_CHECK(parse<vast::detail::parser::query>(q, query));
  q = "&id == 42";
  BOOST_CHECK(parse<vast::detail::parser::query>(q, query));

  // Offsets.
  q = "@5 in {1, 2, 3}";
  BOOST_CHECK(parse<vast::detail::parser::query>(q, query));
  q = "@10,3 < now - 5d10m3s";
  BOOST_CHECK(parse<vast::detail::parser::query>(q, query));
  q = "@0,3,2 ~ /yikes/";

  // Dereferencing event names.
  q = "foo$bar == T";
  BOOST_CHECK(parse<vast::detail::parser::query>(q, query));
  q = "foo$c$id_orig == 192.168.1.1";
  BOOST_CHECK(parse<vast::detail::parser::query>(q, query));
  q = "*$c$id_orig == 192.168.1.1";
  BOOST_CHECK(parse<vast::detail::parser::query>(q, query));

  // In
  auto fail = ":foo == -42";
  BOOST_CHECK(! (parse<vast::detail::parser::query>(fail, query)));
}
