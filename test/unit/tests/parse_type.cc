#include "vast/concept/parseable/to.h"
#include "vast/concept/parseable/vast/schema.h"
#include "vast/concept/parseable/vast/type.h"
#include "vast/concept/printable/stream.h"
#include "vast/concept/printable/vast/schema.h"
#include "vast/concept/printable/vast/type.h"

#define SUITE parseable
#include "test.h"

using namespace vast;
using namespace std::string_literals;

TEST(type) {
  type t;
  MESSAGE("basic");
  CHECK(parsers::type("bool", t));
  CHECK(t == type::boolean{});
  CHECK(parsers::type("string", t));
  CHECK(t == type::string{});
  CHECK(parsers::type("addr", t));
  CHECK(t == type::address{});
  MESSAGE("enum");
  CHECK(parsers::type("enum{foo, bar, baz}", t));
  CHECK(t == type::enumeration{{"foo", "bar", "baz"}});
  MESSAGE("container");
  CHECK(parsers::type("vector<real>", t));
  CHECK(t == type::vector{type::real{}});
  CHECK(parsers::type("set<port>", t));
  CHECK(t == type::set{type::port{}});
  CHECK(parsers::type("table<count, bool>", t));
  CHECK(t == type::table{type::count{}, type::boolean{}});
  MESSAGE("compound");
  auto str = "record{r: record{a: addr, i: record{b: bool}}}"s;
  CHECK(parsers::type(str, t));
  auto r = type::record{
    {"r", type::record{
      {"a", type::address{}},
      {"i", type::record{{"b", type::boolean{}}}}
    }}
  };
  CHECK(t == r);
  MESSAGE("symbol table");
  auto foo = type::boolean{};
  foo.name("foo");
  auto symbols = type_table{{"foo", foo}};
  auto p = type_parser{std::addressof(symbols)}; // overloaded operator&
  CHECK(p("foo", t));
  CHECK(t == foo);
  CHECK(p("vector<foo>", t));
  CHECK(t == type::vector{foo});
  CHECK(p("set<foo>", t));
  CHECK(t == type::set{foo});
  CHECK(p("table<foo, foo>", t));
  CHECK(t == type::table{foo, foo});
  MESSAGE("record");
  CHECK(p("record{x: int, y: string, z: foo}", t));
  r = type::record{
    {"x", type::integer{}},
    {"y", type::string{}},
    {"z", foo}
  };
  CHECK(t == r);
  MESSAGE("attributes");
  // Single attribute.
  CHECK(p("string &skip", t));
  CHECK(t == type::string{{type::attribute::skip}});
  // Two attributes, even though these ones don't make sense together.
  CHECK(p("real &skip &default=\"x \\\" x\"", t));
  CHECK(t == type::real{{type::attribute::skip,
                         {type::attribute::default_, "x \" x"}}});
  // Attributes in types of record fields.
  CHECK(p("record{x: int &skip, y: string &default=\"Y\", z: foo}", t));
  r = type::record{
    {"x", type::integer{{type::attribute::skip}}},
    {"y", type::string{{{type::attribute::default_, "Y"}}}},
    {"z", foo}
  };
  CHECK(t == r);
}

TEST(schema: simple sequential) {
  auto str = "type a = int type b = string type c = a"s;
  schema sch;
  CHECK(parsers::schema(str, sch));
  CHECK(sch.find_type("a"));
  CHECK(sch.find_type("b"));
  CHECK(sch.find_type("c"));
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
  auto ssl = sch.find_type("bro::ssl");
  REQUIRE(ssl);
  auto r = get<type::record>(*ssl);
  REQUIRE(r);
  auto id = r->at(key{"id"});
  REQUIRE(id);
  CHECK(is<type::record>(*id));
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
  auto foo = sch.find_type("foo");
  REQUIRE(foo);
  CHECK(is<type::address>(*foo));
  CHECK(sch.find_type("bar"));
  CHECK(sch.find_type("baz"));
  CHECK(sch.find_type("x"));
}

TEST(schema: basic types global) {
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
  CHECK(sch.find_type("t1"));
  CHECK(sch.find_type("t11"));
  auto foo = sch.find_type("foo");
  REQUIRE(foo);
  auto r = get<type::record>(*foo);
  REQUIRE(r);
  auto t8 = r->at(key{"a8"});
  REQUIRE(t8);
  CHECK(is<type::pattern>(*t8));
}

TEST(schema: basic types local) {
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
  auto foo = sch.find_type("foo");
  REQUIRE(foo);
  auto r = get<type::record>(*foo);
  REQUIRE(r);
  auto p = r->at(key{"a11"});
  REQUIRE(p);
  CHECK(is<type::port>(*p));
}

TEST(schema: complex types global) {
  auto str = R"__(
    type enum_t = enum{x, y, z}
    type vector_t = vector<addr>
    type set_t = set<pattern>
    type table_t = table<port, addr>
    type foo = record{
      e: enum_t,
      v: vector_t,
      s: set_t,
      t: table_t
    }
  )__";
  schema sch;
  CHECK(parsers::schema(std::string{str}, sch));
  auto enum_t = sch.find_type("enum_t");
  REQUIRE(enum_t);
  CHECK(sch.find_type("vector_t"));
  CHECK(sch.find_type("set_t"));
  CHECK(sch.find_type("table_t"));
  auto foo = sch.find_type("foo");
  REQUIRE(foo);
  auto r = get<type::record>(*foo);
  REQUIRE(r);
  auto e = r->at(key{"e"});
  REQUIRE(e);
  CHECK(*e == *enum_t);
}
