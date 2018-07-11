/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/json.hpp"
#include "vast/load.hpp"
#include "vast/save.hpp"
#include "vast/schema.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/schema.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/json.hpp"
#include "vast/concept/printable/vast/schema.hpp"

#define SUITE schema
#include "test.hpp"

using namespace vast;

using caf::get_if;
using caf::holds_alternative;

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
  auto r = get_if<record_type>(foo);
  // Verify type integrity
  REQUIRE(r);
  auto t = r->at(offset{0});
  REQUIRE(t);
  CHECK(holds_alternative<integer_type>(*t));
  t = r->at(offset{2, 0, 1, 1});
  REQUIRE(t);
  CHECK(holds_alternative<real_type>(*t));
  t = r->at(offset{2, 0, 1});
  REQUIRE(t);
  auto inner = get_if<record_type>(t);
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
  auto t = record_type{
    {"s1", string_type{}},
    {"d1", real_type{}},
    {"c", count_type{}.attributes({{"skip"}})},
    {"i", integer_type{}},
    {"s2", string_type{}},
    {"d2", real_type{}}
  };
  t = t.name("foo");
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

TEST(parseable - simple sequential) {
  auto str = "type a = int type b = string type c = a"s;
  schema sch;
  CHECK(parsers::schema(str, sch));
  CHECK(sch.find("a"));
  CHECK(sch.find("b"));
  CHECK(sch.find("c"));
}

TEST(schema: bro-style) {
  std::string str = R"__(
    type bro::ssl = record{
      ts: time,
      uid: string,
      id: record {orig_h: addr, orig_p: port, resp_h: addr, resp_p: port},
      version: string,
      cipher: string,
      server_name: string,
      session_id: string,
      subject: string,
      issuer_subject: string,
      not_valid_before: time,
      not_valid_after: time,
      last_alert: string,
      client_subject: string,
      client_issuer_subject: string
    }
  )__";
  schema sch;
  CHECK(parsers::schema(str, sch));
  auto ssl = sch.find("bro::ssl");
  REQUIRE(ssl);
  auto r = get_if<record_type>(ssl);
  REQUIRE(r);
  auto id = r->at("id");
  REQUIRE(id);
  CHECK(holds_alternative<record_type>(*id));
}

TEST(schema: aliases) {
  auto str = R"__(
               type foo = addr
               type bar = foo
               type baz = bar
               type x = baz
             )__";
  schema sch;
  CHECK(parsers::schema(std::string{str}, sch));
  auto foo = sch.find("foo");
  REQUIRE(foo);
  CHECK(holds_alternative<address_type>(*foo));
  CHECK(sch.find("bar"));
  CHECK(sch.find("baz"));
  CHECK(sch.find("x"));
}

TEST(parseable - basic types global) {
  auto str = R"__(
    type t1 = bool
    type t2 = int
    type t3 = count
    type t4 = real
    type t5 = duration
    type t6 = time
    type t7 = string
    type t8 = pattern
    type t9 = addr
    type t10 = subnet
    type t11 = port
    type foo = record{
      a1: t1,
      a2: t2,
      a3: t3,
      a4: t4,
      a5: t5,
      a6: t6,
      a7: t7,
      a8: t8,
      a9: t9,
      a10: t10,
      a11: t11
    }
  )__";
  schema sch;
  CHECK(parsers::schema(std::string{str}, sch));
  CHECK(sch.find("t1"));
  CHECK(sch.find("t11"));
  auto foo = sch.find("foo");
  REQUIRE(foo);
  auto r = get_if<record_type>(foo);
  REQUIRE(r);
  auto t8 = r->at("a8");
  REQUIRE(t8);
  CHECK(holds_alternative<pattern_type>(*t8));
}

TEST(parseable - basic types local) {
  auto str = R"__(
    type foo = record{
      a1: bool,
      a2: int,
      a3: count,
      a4: real,
      a5: duration,
      a6: time,
      a7: string,
      a8: pattern,
      a9: addr,
      a10: subnet,
      a11: port
    }
  )__";
  schema sch;
  CHECK(parsers::schema(std::string{str}, sch));
  auto foo = sch.find("foo");
  REQUIRE(foo);
  auto r = get_if<record_type>(foo);
  REQUIRE(r);
  auto p = r->at("a11");
  REQUIRE(p);
  CHECK(holds_alternative<port_type>(*p));
}

TEST(parseable - complex types global) {
  auto str = R"__(
    type enum_t = enum{x, y, z}
    type vector_t = vector<addr>
    type set_t = set<pattern>
    type map_t = map<port, addr>
    type foo = record{
      e: enum_t,
      v: vector_t,
      s: set_t,
      t: map_t
    }
  )__";
  schema sch;
  CHECK(parsers::schema(std::string{str}, sch));
  auto enum_t = sch.find("enum_t");
  REQUIRE(enum_t);
  CHECK(sch.find("vector_t"));
  CHECK(sch.find("set_t"));
  CHECK(sch.find("map_t"));
  auto foo = sch.find("foo");
  REQUIRE(foo);
  auto r = get_if<record_type>(foo);
  REQUIRE(r);
  auto e = r->at("e");
  REQUIRE(e);
  CHECK(*e == *enum_t);
}

TEST(json) {
  schema s;
  auto t0 = count_type{};
  t0 = t0.name("foo");
  CHECK(s.add(t0));
  auto t1 = string_type{};
  t1 = t1.name("bar");
  CHECK(s.add(t1));
  auto expected = R"__({
  "types": [
    {
      "name": "foo",
      "kind": "count",
      "structure": null,
      "attributes": {}
    },
    {
      "name": "bar",
      "kind": "string",
      "structure": null,
      "attributes": {}
    }
  ]
})__";
  CHECK_EQUAL(to_string(to_json(s)), expected);
}
