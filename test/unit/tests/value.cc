#include "vast/json.h"
#include "vast/value.h"
#include "vast/concept/convertible/vast/value.h"
#include "vast/concept/convertible/to.h"
#include "vast/concept/parseable/to.h"
#include "vast/concept/parseable/vast/data.h"
#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/vast/json.h"
#include "vast/concept/printable/vast/value.h"
#include "vast/concept/serializable/io.h"
#include "vast/concept/serializable/vast/value.h"

#define SUITE value
#include "test.h"

using namespace vast;

// An *invalid* value has neither a type nor data.
// This is the default-constructed state.
TEST(invalid / nil) {
  value v;
  CHECK(is<none>(v));
  CHECK(is<none>(v.type()));
}

// A *data* value contains only data but lacks a type.
TEST(data value) {
  value v{42};
  CHECK(v.type().check(nil));
  CHECK(is<integer>(v));
  CHECK(is<none>(v.type()));
}

TEST(typed value(empty)) {
  type t = type::count{};
  value v{nil, t};
  CHECK(t.check(nil));
  CHECK(v.type() == t);
  CHECK(is<none>(v));
  CHECK(is<type::count>(v.type()));
}

TEST(typed value(data)) {
  type t = type::real{};
  value v{4.2, t};
  CHECK(t.check(4.2));
  CHECK(v.type() == t);
  CHECK(is<real>(v));
  CHECK(is<type::real>(v.type()));
}

TEST(data and type mismatch) {
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

TEST(relational operators) {
  value v1;
  value v2;

  MESSAGE("comparison of nil values");
  CHECK(v1 == v2);
  type t = type::real{};

  MESSAGE("typed value with equal data");
  v1 = {4.2, t};
  v2 = {4.2, t};
  CHECK(t.check(4.2));
  CHECK(v1 == v2);
  CHECK(!(v1 != v2));
  CHECK(!(v1 < v2));
  CHECK(v1 <= v2);
  CHECK(v1 >= v2);
  CHECK(!(v1 > v2));

  MESSAGE("different data, same type");
  v2 = {4.3, t};
  CHECK(v1 != v2);
  CHECK(!(v1 == v2));
  CHECK(v1 < v2);

  MESSAGE("no type, but data comparison still works");
  v2 = 4.2;
  CHECK(v1 == v2);

  MESSAGE("compares only data");
  v1 = 4.2;
  CHECK(v1 == v2);
  v1 = -4.2;
  CHECK(v1 != v2);
  CHECK(v1 < v2);
}

TEST(serialization) {
  type t = type::set{type::port{}};
  set s;
  s.emplace(port{80, port::tcp});
  s.emplace(port{53, port::udp});
  s.emplace(port{8, port::icmp});
  CHECK(t.check(s));
  value v{s, t};
  value w;
  std::vector<uint8_t> buf;
  save(buf, v);
  load(buf, w);
  CHECK(v == w);
  CHECK(to_string(w) == "{8/icmp, 53/udp, 80/tcp}");
}

TEST(JSON)
{
  auto tr =
    type::record{
      {"foo", type::port{}},
      {"bar", type::integer{}},
      {"baz", type::real{}}
    };
  auto v = value{*to<data>("(53/udp,-42,4.2)"), tr};
  auto j = to<json>(v);
  REQUIRE(j);
  auto str = R"__({
  "data": {
    "bar": -42,
    "baz": 4.2,
    "foo": "53/udp"
  },
  "type": {
    "attributes": [],
    "kind": "record",
    "name": "",
    "structure": {
      "bar": {
        "attributes": [],
        "kind": "integer",
        "name": "",
        "structure": null
      },
      "baz": {
        "attributes": [],
        "kind": "real",
        "name": "",
        "structure": null
      },
      "foo": {
        "attributes": [],
        "kind": "port",
        "name": "",
        "structure": null
      }
    }
  }
})__";
  CHECK(to_string(*j) == str);
}
