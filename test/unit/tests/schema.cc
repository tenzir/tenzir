#include "framework/unit.h"

#include "vast/file_system.h"
#include "vast/schema.h"
#include "vast/io/serialization.h"
#include "vast/util/convert.h"

SUITE("schema")

using namespace vast;

#define DEFINE_SCHEMA_TEST_CASE(name, input)                        \
  TEST(#name)                                                       \
  {                                                                 \
    auto contents = load(input);                                    \
    REQUIRE(contents);                                              \
    auto lval = contents->begin();                                  \
    auto s0 = parse<schema>(lval, contents->end());                 \
    if (! s0)                                                       \
      std::cout << s0.error() << std::endl;                         \
    REQUIRE(s0);                                                    \
                                                                    \
    auto str = to_string(*s0);                                      \
    lval = str.begin();                                             \
    auto s1 = parse<schema>(lval, str.end());                       \
    if (! s1)                                                       \
      std::cout << s1.error() << std::endl;                         \
    REQUIRE(s1);                                                    \
    CHECK(str == to_string(*s1));                                   \
  }

// Contains the test case defintions for all taxonomy test files.
#include "test/unit/schema_test_cases.h"

TEST("offset finding")
{
  std::string str =
    "type a = int\n"
    "type inner = record{ x: int, y: real }\n"
    "type middle = record{ a: int, b: inner }\n"
    "type outer = record{ a: middle, b: record { y: string }, c: int }\n"
    "type foo = record{ a: int, b: real, c: outer, d: middle }";

  auto lval = str.begin();
  auto sch = parse<schema>(lval, str.end());
  REQUIRE(sch);

  auto foo = sch->find_type("foo");
  REQUIRE(foo);
  auto r = get<type::record>(*foo);

  REQUIRE(r);
  auto t = r->at(offset{0});
  REQUIRE(t);
  CHECK(is<type::integer>(*t));

  t = r->at(offset{2, 0, 1, 1});
  REQUIRE(t);
  CHECK(is<type::real>(*t));

  t = r->at(offset{2, 0, 1});
  REQUIRE(t);
  auto inner = get<type::record>(*t);
  REQUIRE(inner);
  CHECK(inner->name() == "inner");
}

TEST("merging")
{
  std::string str =
    "type a = int\n"
    "type inner = record { x: int, y: real }\n";

  auto lval = str.begin();
  auto s1 = parse<schema>(lval, str.end());
  REQUIRE(s1);

  str =
    "type a = int\n"  // Same type allowed.
    "type b = int\n";

  lval = str.begin();
  auto s2 = parse<schema>(lval, str.end());
  REQUIRE(s2);

  auto merged = schema::merge(*s1, *s2);
  REQUIRE(merged);
  CHECK(merged->find_type("a"));
  CHECK(merged->find_type("b"));
  CHECK(merged->find_type("inner"));
}

TEST("serialization")
{
  schema sch, sch2;
  auto t = type::record{
    {"s1", type::string{}},
    {"d1", type::real{}},
    {"c", type::count{{{type::attribute::skip}, {type::attribute::default_}}}},
    {"i", type::integer{}},
    {"s2", type::string{}},
    {"d2", type::real{}}};
  t.name("foo");
  sch.add(t);

  std::vector<uint8_t> buf;
  CHECK(io::archive(buf, sch));
  CHECK(io::unarchive(buf, sch2));

  auto u = sch2.find_type("foo");
  REQUIRE(u);
  CHECK(t == *u);
}
