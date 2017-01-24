#include "vast/load.hpp"
#include "vast/json.hpp"
#include "vast/save.hpp"
#include "vast/value.hpp"
#include "vast/concept/convertible/to.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/json.hpp"
#include "vast/concept/printable/vast/value.hpp"

#define SUITE value
#include "test.hpp"

using namespace vast;

// An *invalid* value has neither a type nor data.
// This is the default-constructed state.
TEST(invalid) {
  value v;
  CHECK(is<none>(v));
  CHECK(is<none_type>(v.type()));
}

// A *data* value contains only data but lacks a type.
TEST(data value) {
  value v{42};
  CHECK(type_check(v.type(), nil));
  CHECK(is<integer>(v));
  CHECK(is<none_type>(v.type()));
}

TEST(typed value(empty)) {
  type t = count_type{};
  value v{nil, t};
  CHECK(type_check(t, nil));
  CHECK(v.type() == t);
  CHECK(is<none>(v));
  CHECK(is<count_type>(v.type()));
}

TEST(typed value(data)) {
  type t = real_type{};
  value v{4.2, t};
  CHECK(type_check(t, 4.2));
  CHECK(v.type() == t);
  CHECK(is<real>(v));
  CHECK(is<real_type>(v.type()));
}

TEST(data and type mismatch) {
  // This value has a data and type mismatch. For performance reasons, the
  // constructor will *not* perform a type check.
  value v{42, real_type{}};
  CHECK(v.data() == 42);
  CHECK(v.type() == real_type{});
  // If we do require type safety and cannot guarantee that data and type
  // match, we can use the type-safe factory function.
  auto fail = value::make(42, real_type{});
  CHECK(is<none>(fail));
  CHECK(is<none_type>(fail.type()));
}

TEST(relational operators) {
  value v1;
  value v2;

  MESSAGE("comparison of nil values");
  CHECK(v1 == v2);
  type t = real_type{};

  MESSAGE("typed value with equal data");
  v1 = {4.2, t};
  v2 = {4.2, t};
  CHECK(type_check(t, 4.2));
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
  type t = set_type{port_type{}};
  set s;
  s.emplace(port{80, port::tcp});
  s.emplace(port{53, port::udp});
  s.emplace(port{8, port::icmp});
  CHECK(type_check(t, s));
  value v{s, t};
  value w;
  std::vector<char> buf;
  save(buf, v);
  load(buf, w);
  CHECK(v == w);
  CHECK(to_string(w) == "{8/icmp, 53/udp, 80/tcp}");
}

TEST(json)
{
  auto t =
    record_type{
      {"foo", port_type{}},
      {"bar", integer_type{}},
      {"baz", real_type{}}
    };
  auto d = to<data>("[53/udp,-42,4.2]");
  REQUIRE(d);
  auto v = value{*d, t};
  auto j = to<json>(v);
  REQUIRE(j);
  auto str = R"__({
  "type": {
    "name": "",
    "kind": "record",
    "structure": {
      "foo": {
        "name": "",
        "kind": "port",
        "structure": null,
        "attributes": {}
      },
      "bar": {
        "name": "",
        "kind": "integer",
        "structure": null,
        "attributes": {}
      },
      "baz": {
        "name": "",
        "kind": "real",
        "structure": null,
        "attributes": {}
      }
    },
    "attributes": {}
  },
  "data": {
    "foo": "53/udp",
    "bar": -42,
    "baz": 4.2
  }
})__";
  CHECK_EQUAL(to_string(*j), str);
}
