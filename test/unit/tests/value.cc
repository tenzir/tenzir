#include "framework/unit.h"
#include "vast/value.h"
#include "vast/concept/serializable/value.h"
#include "vast/io/serialization.h"
#include "vast/util/json.h"

using namespace vast;

SUITE("value")

// An *invalid* value has neither a type nor data.
// This is the default-constructed state.
TEST("invalid/nil")
{
  value v;
  CHECK(is<none>(v));
  CHECK(is<none>(v.type()));
}

// A *data* value contains only data but lacks a type.
TEST("data value")
{
  value v{42};

  CHECK(v.type().check(nil));
  CHECK(is<integer>(v));
  CHECK(is<none>(v.type()));
}

TEST("typed value (empty)")
{
  type t = type::count{};
  value v{nil, t};

  CHECK(t.check(nil));
  CHECK(v.type() == t);
  CHECK(is<none>(v));
  CHECK(is<type::count>(v.type()));
}

TEST("typed value (data)")
{
  type t = type::real{};
  value v{4.2, t};

  CHECK(t.check(4.2));
  CHECK(v.type() == t);
  CHECK(is<real>(v));
  CHECK(is<type::real>(v.type()));
}

TEST("data and type mismatch")
{
  // This value has a data and type mismatch. For performance reasons, the
  // constructor will *not* perform a type check.
  value v{42, type::real{}};
  CHECK(v.data() == 42);
  CHECK(v.type() == type::real{});

  // If we do require type safety and cannot guarantee that data and type
  // match, we can use the type-safe factory function.
  auto fail = value::make(42, type::real{});
  CHECK(is<none>(fail));
  CHECK(is<none>(fail.type()));
}

TEST("relational operators")
{
  value v1;
  value v2;

  // Comparison of nil values.
  CHECK(v1 == v2);

  type t = type::real{};

  // Typed value with equal data.
  v1 = {4.2, t};
  v2 = {4.2, t};
  CHECK(t.check(4.2));
  CHECK(v1 == v2);
  CHECK(! (v1 != v2));
  CHECK(! (v1 < v2));
  CHECK(v1 <= v2);
  CHECK(v1 >= v2);
  CHECK(! (v1 > v2));

  // Different data, same type.
  v2 = {4.3, t};
  CHECK(v1 != v2);
  CHECK(! (v1 == v2));
  CHECK(v1 < v2);

  // No type, but data comparison still works.
  v2 = 4.2;
  CHECK(v1 == v2);

  // Compares only data.
  v1 = 4.2;
  CHECK(v1 == v2);
  v1 = -4.2;
  CHECK(v1 != v2);
  CHECK(v1 < v2);
}

TEST("serialization")
{
  type t = type::set{type::port{}};

  set s;
  s.emplace(port{80, port::tcp});
  s.emplace(port{53, port::udp});
  s.emplace(port{8, port::icmp});

  CHECK(t.check(s));

  value v{s, t};
  value w;
  std::vector<uint8_t> buf;
  io::archive(buf, v);
  io::unarchive(buf, w);

  CHECK(v == w);
  CHECK(to_string(w) == "{8/icmp, 53/udp, 80/tcp}");
}

TEST("printing")
{
  record r;
  r.emplace_back(4.2);
  r.emplace_back(nil);
  r.emplace_back(1337);
  r.emplace_back("fo\x10");
  r.emplace_back(set{42u, 43u});
  value v{std::move(r)};
  CHECK(to_string(v) == "(4.2000000000, <nil>, +1337, \"fo\\x10\", {42, 43})");
}

TEST("parsing (typed)")
{
  // Port
  auto v = to<value>("80/tcp", type::port{});
  REQUIRE(v);
  auto p = get<port>(*v);
  CHECK(p);
  CHECK(*p == port{80, port::tcp});

  // Set
  auto ts = type::set{type::integer{}};
  v = to<value>("{+1, -1}", ts);
  REQUIRE(v);
  auto s = get<set>(*v);
  CHECK(s);
  CHECK(*s == set{-1, 1});

  // Record
  auto tr = type::record{
        {"foo", type::port{}},
        {"bar", type::integer{}},
        {"baz", type::real{}}
      };

  v = to<value>("(53/udp, -42, 4.2)", tr);
  REQUIRE(v);
  auto r = get<record>(*v);
  REQUIRE(r);
  CHECK(*r == record{port{53, port::udp}, -42, 4.2});
}

TEST("JSON")
{
  auto tr = type::record{
        {"foo", type::port{}},
        {"bar", type::integer{}},
        {"baz", type::real{}}
      };

  auto v = to<value>("(53/udp, -42, 4.2)", tr);
  REQUIRE(v);
  auto j = to<util::json>(*v);
  REQUIRE(j);

  auto str = R"json({
  "data": [
    "53\/udp",
    -42,
    4.2
  ],
  "type": "record {foo: port, bar: int, baz: real}"
})json";

  CHECK(to_string(*j, true) == str);
}
