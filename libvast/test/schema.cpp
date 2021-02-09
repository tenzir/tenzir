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

#define SUITE schema

#include "vast/schema.hpp"

#include "vast/test/test.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/schema.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/schema.hpp"
#include "vast/detail/deserialize.hpp"
#include "vast/detail/serialize.hpp"

#include <caf/test/dsl.hpp>

#include "type_test.hpp"

using namespace vast;

using caf::get;
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
  auto sch = unbox(to<schema>(str));
  auto foo_type = sch.find("foo");
  REQUIRE_NOT_EQUAL(foo_type, nullptr);
  REQUIRE(holds_alternative<record_type>(*foo_type));
  auto& foo_record = get<record_type>(*foo_type);
  CHECK_EQUAL(foo_record.name(), "foo");
  CHECK_EQUAL(foo_record.fields.size(), 4u);
  CHECK_EQUAL(at(foo_record, 0), integer_type{});
  CHECK_EQUAL(at(foo_record, 1), real_type{});
  CHECK_EQUAL(at(foo_record, 2).name(), "outer");
  CHECK_EQUAL(rec_at(foo_record, 2).fields.size(), 3u);
  CHECK_EQUAL(at(foo_record, 2, 0).name(), "middle");
  CHECK_EQUAL(at(foo_record, 2, 1, 0), string_type{});
  CHECK_EQUAL(at(foo_record, 2, 2), integer_type{});
  CHECK_EQUAL(at(foo_record, 3).name(), "middle");
  CHECK_EQUAL(at(foo_record, 3, 0), integer_type{});
  CHECK_EQUAL(at(foo_record, 3, 1).name(), "inner");
  CHECK_EQUAL(at(foo_record, 3, 1, 0), integer_type{});
  CHECK_EQUAL(at(foo_record, 3, 1, 1), real_type{});
}

TEST(combining) {
  auto x = unbox(to<schema>(R"__(
    type b = real
    type int_custom = int
    type a = int_custom
  )__"));
  auto y = unbox(to<schema>(R"__(
    type c = addr
    type d = pattern
  )__"));
  auto z = schema::combine(x, y);
  CHECK(unbox(z.find("a")) == integer_type{}.name("a"));
  CHECK(unbox(z.find("b")) == real_type{}.name("b"));
  CHECK(unbox(z.find("c")) == address_type{}.name("c"));
  CHECK(unbox(z.find("d")) == pattern_type{}.name("d"));
  CHECK(unbox(z.find("int_custom")) == integer_type{}.name("int_custom"));
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
    {"d2", real_type{}},
  };
  t = t.name("foo");
  sch.add(t);
  // Save & load
  std::vector<char> buf;
  CHECK_EQUAL(detail::serialize(buf, sch), caf::none);
  schema sch2;
  CHECK_EQUAL(detail::deserialize(buf, sch2), caf::none);
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

TEST(parseable - toplevel comments) {
  std::string_view str = R"__(
    // A comment at the beginning.
    type foo = int
    // A comment a the end of the schema.
  )__";
  schema sch;
  CHECK(parsers::schema(str, sch));
  CHECK(sch.find("foo"));
}

TEST(parseable - inline comments) {
  std::string_view str = R"__(
    type foo = record{  // so
      ts: time,         // much
      uid: string       // more
    }                   // detail,
    type bar = int      // jeez!
  )__";
  schema sch;
  CHECK(parsers::schema(str, sch));
  CHECK(sch.find("foo"));
  CHECK(sch.find("bar"));
}

TEST(schema : zeek - style) {
  std::string str = R"__(
    type port = count
    type zeek.ssl = record{
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
  auto ssl = sch.find("zeek.ssl");
  REQUIRE(ssl);
  auto r = get_if<record_type>(ssl);
  REQUIRE(r);
  auto id = r->at("id");
  REQUIRE(id);
  CHECK(holds_alternative<record_type>(id->type));
}

TEST(schema : aliases) {
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
    }
  )__";
  schema sch;
  CHECK(parsers::schema(std::string{str}, sch));
  CHECK(sch.find("t1"));
  CHECK(sch.find("t10"));
  auto foo = sch.find("foo");
  REQUIRE(foo);
  auto r = get_if<record_type>(foo);
  REQUIRE(r);
  auto t8 = r->at("a8");
  REQUIRE(t8);
  CHECK(holds_alternative<pattern_type>(t8->type));
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
    }
  )__";
  schema sch;
  CHECK(parsers::schema(std::string{str}, sch));
  auto foo = sch.find("foo");
  REQUIRE(foo);
  auto r = get_if<record_type>(foo);
  REQUIRE(r);
  auto p = r->at("a10");
  REQUIRE(p);
  CHECK(holds_alternative<subnet_type>(p->type));
}

TEST(parseable - complex types global) {
  auto str = R"__(
    type enum_t = enum{x, y, z}
    type list_t = list<addr>
    type map_t = map<count, addr>
    type foo = record{
      e: enum_t,
      v: list_t,
      t: map_t
    }
  )__";
  schema sch;
  CHECK(parsers::schema(std::string{str}, sch));
  auto enum_t = sch.find("enum_t");
  REQUIRE(enum_t);
  CHECK(sch.find("list_t"));
  CHECK(sch.find("map_t"));
  auto foo = sch.find("foo");
  REQUIRE(foo);
  auto r = get_if<record_type>(foo);
  REQUIRE(r);
  auto e = r->at("e");
  REQUIRE(e);
  CHECK(e->type == *enum_t);
}

TEST(parseable - out of order definitions) {
  using namespace std::string_view_literals;
  auto str = R"__(
    type baz = list<bar>
    type bar = record{
      x: foo
    }
    type foo = int
  )__"sv;
  schema sch;
  CHECK(parsers::schema(str, sch));
  auto baz = unbox(sch.find("baz"));
  // clang-format off
  auto expected = type{
    list_type{
      record_type{
        {"x", integer_type{}.name("foo")}
      }.name("bar")
    }.name("baz")
  };
  // clang-format on
  CHECK_EQUAL(baz, expected);
}

TEST(parseable - with context) {
  using namespace std::string_view_literals;
  MESSAGE("prepare the context");
  auto global = symbol_table{};
  auto local = symbol_table{};
  // schema gs;
  {
    auto p = symbol_table_parser{};
    CHECK(p("type foo = count", local));
  }
  {
    MESSAGE("Use definition from global symbol table");
    auto str = R"__(
      type bar = record{
        x: record{
          y: foo
        }
      }
    )__"sv;
    auto st = unbox(to<symbol_table>(str));
    auto r = symbol_resolver{global, local, st};
    auto sch = unbox(r.resolve());
    auto bar = unbox(sch.find("bar"));
    // clang-format off
    auto expected = type{
      record_type{
        {"x", record_type{{"y", count_type{}.name("foo")}}}
      }.name("bar")
    };
    // clang-format on
    CHECK_EQUAL(bar, expected);
  }
  {
    MESSAGE("Override definition in global symbol table - before use");
    auto str = R"__(
      type foo = int
      type bar = record{
        x: record{
          y: foo
        }
      }
    )__"sv;
    auto local = symbol_table{};
    auto st = unbox(to<symbol_table>(str));
    auto r = symbol_resolver{global, local, st};
    auto sch = unbox(r.resolve());
    auto bar = unbox(sch.find("bar"));
    // clang-format off
    auto expected = type{
      record_type{
        {"x", record_type{{"y", integer_type{}.name("foo")}}}
      }.name("bar")
    };
    // clang-format on
    CHECK_EQUAL(bar, expected);
  }
  {
    MESSAGE("Override definition in global symbol table - after use");
    auto str = R"__(
      type bar = record{
        x: record{
          y: foo
        }
      }
      type foo = int
    )__"sv;
    auto local = symbol_table{};
    auto st = unbox(to<symbol_table>(str));
    auto r = symbol_resolver{global, local, st};
    auto sch = unbox(r.resolve());
    auto bar = unbox(sch.find("bar"));
    // clang-format off
    auto expected = type{
      record_type{
        {"x", record_type{{"y", integer_type{}.name("foo")}}}
      }.name("bar")
    };
    // clang-format on
    CHECK_EQUAL(bar, expected);
  }
  {
    MESSAGE("Duplicate definition error");
    auto str = R"__(
      type foo = real
      type bar = record{
        x: record{
          y: foo
        }
      }
      type foo = int
    )__"sv;
    auto local = symbol_table{};
    auto p = symbol_table_parser{};
    symbol_table sb;
    CHECK(!p(str, sb));
  }
  {
    MESSAGE("Duplicate definition error - reusing local context");
    auto st1 = unbox(to<symbol_table>("type foo = real"));
    auto st2 = unbox(to<symbol_table>("type foo = int"));
    auto local = symbol_table{};
    auto r1 = symbol_resolver{global, local, st1};
    CHECK(r1.resolve());
    auto r2 = symbol_resolver{global, local, st2};
    CHECK(!r2.resolve());
  }
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
  CHECK_EQUAL(to_json(to_data(s)), expected);
}
