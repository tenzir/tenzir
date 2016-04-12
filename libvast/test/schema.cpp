#include "vast/json.hpp"
#include "vast/load.hpp"
#include "vast/save.hpp"
#include "vast/schema.hpp"
#include "vast/concept/convertible/vast/schema.hpp"
#include "vast/concept/serializable/vast/schema.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/schema.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/json.hpp"
#include "vast/concept/printable/vast/schema.hpp"

#define SUITE schema
#include "test.hpp"

using namespace vast;

TEST(offset finding) {
  std::string str = R"__(
    type a = int
    type inner = record{ x: int, y: real }
    type middle = record{ a: int, b: inner }
    type outer = record{ a: middle, b: record { y: string }, c: int }
    type foo = record{ a: int, b: real, c: outer, d: middle }
  )__";
  auto sch = to<schema>(str);
  REQUIRE(sch);
  // Type lookup
  auto foo = sch->find("foo");
  REQUIRE(foo);
  auto r = get<type::record>(*foo);
  // Verify type integrity
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

TEST(merging) {
  std::string str = R"__(
    type a = int
    type inner = record{ x: int, y: real }
  )__";
  auto s1 = to<schema>(str);
  REQUIRE(s1);
  str = "type a = int\n" // Same type allowed.
        "type b = int\n";
  auto s2 = to<schema>(str);
  REQUIRE(s2);
  auto merged = schema::merge(*s1, *s2);
  REQUIRE(merged);
  CHECK(merged->find("a"));
  CHECK(merged->find("b"));
  CHECK(merged->find("inner"));
}

TEST(serialization) {
  schema sch;
  auto t = type::record{
    {"s1", type::string{}},
    {"d1", type::real{}},
    {"c", type::count{{type::attribute::skip}}},
    {"i", type::integer{}},
    {"s2", type::string{}},
    {"d2", type::real{}}};
  t.name("foo");
  sch.add(t);
  // Save & load
  std::vector<char> buf;
  CHECK(save(buf, sch));
  schema sch2;
  CHECK(load(buf, sch2));
  // Check integrity
  auto u = sch2.find("foo");
  REQUIRE(u);
  CHECK(t == *u);
}

TEST(json) {
  schema s;
  auto t0 = type::count{};
  t0.name("foo");
  CHECK(s.add(t0));
  auto t1 = type::string{};
  t1.name("bar");
  CHECK(s.add(t1));
  auto expected = R"__({
  "types": [
    {
      "attributes": [],
      "kind": "count",
      "name": "foo",
      "structure": null
    },
    {
      "attributes": [],
      "kind": "string",
      "name": "bar",
      "structure": null
    }
  ]
})__";
  CHECK(to_string(to_json(s)) == expected);
}
