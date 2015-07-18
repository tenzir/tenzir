#include "vast/filesystem.h"
#include "vast/schema.h"
#include "vast/concept/serializable/io.h"
#include "vast/concept/serializable/vast/schema.h"
#include "vast/concept/parseable/vast/detail/to_schema.h"
#include "vast/concept/printable/stream.h"
#include "vast/concept/printable/vast/error.h"
#include "vast/concept/printable/vast/schema.h"

#define SUITE schema
#include "test.h"

using namespace vast;

#define DEFINE_SCHEMA_TEST_CASE(name, input)                        \
  TEST(name)                                                        \
  {                                                                 \
    auto contents = load_contents(input);                           \
    REQUIRE(contents);                                              \
    auto s0 = detail::to_schema(*contents);                         \
    if (! s0)                                                       \
      ERROR(s0.error());                                            \
    REQUIRE(s0);                                                    \
                                                                    \
    auto str = to_string(*s0);                                      \
    auto s1 = detail::to_schema(str);                               \
    if (! s1)                                                       \
    {                                                               \
      ERROR(s1.error());                                            \
      ERROR("schema: " << str);                                     \
    }                                                               \
    REQUIRE(s1);                                                    \
    CHECK(str == to_string(*s1));                                   \
  }

// Contains the test case defintions for all taxonomy test files.
#include "tests/schema_test_cases.h"

TEST(offset finding)
{
  std::string str =
    "type a = int\n"
    "type inner = record{ x: int, y: real }\n"
    "type middle = record{ a: int, b: inner }\n"
    "type outer = record{ a: middle, b: record { y: string }, c: int }\n"
    "type foo = record{ a: int, b: real, c: outer, d: middle }";

  auto sch = detail::to_schema(str);
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

TEST(merging)
{
  std::string str =
    "type a = int\n"
    "type inner = record { x: int, y: real }\n";

  auto s1 = detail::to_schema(str);
  REQUIRE(s1);

  str =
    "type a = int\n"  // Same type allowed.
    "type b = int\n";

  auto s2 = detail::to_schema(str);
  REQUIRE(s2);

  auto merged = schema::merge(*s1, *s2);
  REQUIRE(merged);
  CHECK(merged->find_type("a"));
  CHECK(merged->find_type("b"));
  CHECK(merged->find_type("inner"));
}

TEST(serialization)
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
  CHECK(save(buf, sch));
  CHECK(load(buf, sch2));

  auto u = sch2.find_type("foo");
  REQUIRE(u);
  CHECK(t == *u);
}
