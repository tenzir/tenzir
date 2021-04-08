//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE schema

#include "vast/schema.hpp"

#include "vast/test/test.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/schema.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/schema.hpp"
#include "vast/data.hpp"
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
  CHECK_EQUAL(unbox(z.find("a")),
              alias_type{integer_type{}.name("int_custom")}.name("a"));
  CHECK_EQUAL(unbox(z.find("b")), real_type{}.name("b"));
  CHECK_EQUAL(unbox(z.find("c")), address_type{}.name("c"));
  CHECK_EQUAL(unbox(z.find("d")), pattern_type{}.name("d"));
  CHECK_EQUAL(unbox(z.find("int_custom")), integer_type{}.name("int_custom"));
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
  auto global = symbol_map{};
  {
    auto local = symbol_map{};
    auto p = symbol_map_parser{};
    CHECK(p("type foo = count", local));
    global = std::move(local);
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
    auto sm = unbox(to<symbol_map>(str));
    auto r = symbol_resolver{global, sm};
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
    auto sm = unbox(to<symbol_map>(str));
    auto r = symbol_resolver{global, sm};
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
    auto sm = unbox(to<symbol_map>(str));
    auto r = symbol_resolver{global, sm};
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
    auto p = symbol_map_parser{};
    symbol_map sm;
    CHECK(!p(str, sm));
  }
  {
    MESSAGE("Duplicate definition error - re-entry");
    auto p = symbol_map_parser{};
    symbol_map sm;
    CHECK(p("type foo = real", sm));
    CHECK(!p("type foo = int", sm));
  }
  {
    MESSAGE("Arithmetic - basic addition");
    auto str = R"__(
      type foo = record{
        x: int
      }
      type bar = record{
        y: int
      }
      type gob = foo + bar + tar
      type tar = record{
        z: int
      }
    )__"sv;
    auto sm = unbox(to<symbol_map>(str));
    auto r = symbol_resolver{global, sm};
    auto sch = unbox(r.resolve());
    auto gob = unbox(sch.find("gob"));
    // clang-format off
    auto expected = type{
      record_type{
        {"x", integer_type{}},
        {"y", integer_type{}},
        {"z", integer_type{}},
      }.name("gob")
    };
    // clang-format on
    CHECK_EQUAL(gob, expected);
  }
  {
    MESSAGE("Arithmetic - field clash");
    auto str = R"__(
      type foo = record{
        a: int,
        b: int
      }
      type bar = record{
        a: real,
        c: real
      }
      type lplus = foo + bar
    )__"sv;
    auto sm = unbox(to<symbol_map>(str));
    auto r = symbol_resolver{global, sm};
    CHECK(!r.resolve());
  }
  {
    MESSAGE("Arithmetic - priorities");
    auto str = R"__(
      type foo = record{
        a: int,
        b: int
      } #attr_one #attr_two=val
      type bar = record{
        a: real,
        c: real
      } #attr_one=val #attr_two
      type lplus = foo <+ bar
      type rplus = foo +> bar
    )__"sv;
    auto sm = unbox(to<symbol_map>(str));
    auto r = symbol_resolver{global, sm};
    auto sch = unbox(r.resolve());
    // clang-format off
    auto expected_lplus = type{
      record_type{
        {"a", integer_type{}},
        {"b", integer_type{}},
        {"c", real_type{}},
      }.name("lplus").attributes({{"attr_one"}, {"attr_two", "val"}})
    };
    auto expected_rplus = type{
      record_type{
        {"a", real_type{}},
        {"b", integer_type{}},
        {"c", real_type{}},
      }.name("rplus").attributes({{"attr_one", "val"}, {"attr_two"}})
    };
    // clang-format on
    auto lplus = unbox(sch.find("lplus"));
    CHECK_EQUAL(lplus, expected_lplus);
    auto rplus = unbox(sch.find("rplus"));
    CHECK_EQUAL(rplus, expected_rplus);
  }
  {
    MESSAGE("Arithmetic - removing multiple fields");
    auto str = R"__(
      type foo = record{
        a: record{
          x: count,
          y: record {
            z: list<string>
          }
        },
        "b.c": record {
          d: count,
          e: count
        },
        f: record {
          g: count
        }
      }
      type bar = foo - a.y - "b.c".d - f.g
    )__"sv;
    auto sm = unbox(to<symbol_map>(str));
    auto r = symbol_resolver{global, sm};
    auto sch = unbox(r.resolve());
    auto bar = unbox(sch.find("bar"));
    // clang-format off
    auto expected = type{
      record_type{
        {"a", record_type{
          {"x", count_type{}}
        }},
        {"b.c", record_type{
          {"e", count_type{}}
        }}
      }.name("bar")
    };
    // clang-format on
    CHECK_EQUAL(bar, expected);
  }
  {
    MESSAGE("Arithmetic - realistic usage");
    auto str = R"__(
      type base = record{
        a: record{
             x: count,
             y: string
           },
        b: int,
        c: int,
      }
      type derived1 = base - c +> record{
        a: record {
             y: addr
           },
        b: real,
        d: time,
      }
      type derived2 = base +> record{
        a: record {
             y: addr
           },
        b: real,
        d: time,
      } - c
    )__"sv;
    auto sm = unbox(to<symbol_map>(str));
    auto r = symbol_resolver{global, sm};
    auto sch = unbox(r.resolve());
    auto derived1 = unbox(sch.find("derived1"));
    auto derived2 = unbox(sch.find("derived2"));
    // clang-format off
    auto expected = type{
      record_type{
        {"a", record_type{
          {"x", count_type{}},
          {"y", address_type{}}
        }},
        {"b", real_type{}},
        {"d", time_type{}},
      }.name("derived1")
    };
    // clang-format on
    CHECK_EQUAL(derived1, expected);
    expected.name("derived2");
    CHECK_EQUAL(derived2, expected);
  }
}

TEST(parseable - overwriting with self reference) {
  using namespace std::string_view_literals;
  auto global = symbol_map{};
  {
    auto local = symbol_map{};
    auto p = symbol_map_parser{};
    CHECK(p("type foo = record{\"x\": count}", local));
    global = std::move(local);
  }
  {
    auto str = R"__(
      type bar = foo
      type foo = foo + record {
        y: string
      }
    )__"sv;
    auto sm = unbox(to<symbol_map>(str));
    auto r = symbol_resolver{global, sm};
    auto sch = unbox(r.resolve());
    auto foo = unbox(sch.find("foo"));
    // clang-format off
    auto expected = type{
      record_type{
        {"x", count_type{}},
        {"y", string_type{}}
      }.name("foo")
    };
    // clang-format on
    CHECK_EQUAL(foo, expected);
    auto bar = unbox(sch.find("bar"));
    expected = alias_type{expected}.name("bar");
    CHECK_EQUAL(bar, expected);
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
