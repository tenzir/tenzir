#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/data.hpp"

#define SUITE parseable
#include "test.hpp"

using namespace vast;
using namespace std::string_literals;

TEST(data) {
  auto p = make_parser<data>();
  data d;

  MESSAGE("bool");
  auto str = "T"s;
  auto f = str.begin();
  auto l = str.end();
  CHECK(p.parse(f, l, d));
  CHECK(f == l);
  CHECK(d == true);

  MESSAGE("numbers");
  str = "+1001"s;
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, d));
  CHECK(f == l);
  CHECK(d == 1001);
  str = "1001"s;
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, d));
  CHECK(f == l);
  CHECK(d == 1001u);
  str = "10.01"s;
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, d));
  CHECK(f == l);
  CHECK(d == 10.01);

  MESSAGE("string");
  str = "\"bar\""s;
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, d));
  CHECK(f == l);
  CHECK(d == "bar");

  MESSAGE("pattern");
  str = "/foo/"s;
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, d));
  CHECK(f == l);
  CHECK(d == pattern{"foo"});

  MESSAGE("address");
  str = "10.0.0.1"s;
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, d));
  CHECK(f == l);
  CHECK(d == *to<address>("10.0.0.1"));

  MESSAGE("port");
  str = "22/tcp"s;
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, d));
  CHECK(f == l);
  CHECK(d == port{22, port::tcp});

  MESSAGE("vector");
  str = "[42,4.2,nil]"s;
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, d));
  CHECK(f == l);
  CHECK(d == vector{42u, 4.2, nil});

  MESSAGE("set");
  str = "{-42,+42,-1}"s;
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, d));
  CHECK(f == l);
  CHECK(d == set{-42, 42, -1});

  MESSAGE("table");
  str = "{T->1,F->0}"s;
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, d));
  CHECK(f == l);
  CHECK(d == table{{true, 1u}, {false, 0u}});
}
