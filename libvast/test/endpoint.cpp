#include "vast/concept/parseable/vast/endpoint.hpp"
#include "vast/endpoint.hpp"

#define SUITE endpoint
#include "test.hpp"

using namespace vast;
using namespace std::string_literals;

TEST(parseable) {
  endpoint e;
  CHECK(parsers::endpoint(":42000", e));
  CHECK(e.host == "");
  CHECK(e.port == 42000);
  CHECK(parsers::endpoint("localhost", e));
  CHECK(e.host == "localhost");
  CHECK(e.port == 0);
  CHECK(parsers::endpoint("10.0.0.1:80", e));
  CHECK(e.host == "10.0.0.1");
  CHECK(e.port == 80);
  CHECK(parsers::endpoint("foo-bar_baz.test", e));
  CHECK(e.host == "foo-bar_baz.test");
  CHECK(e.port == 0);
}

