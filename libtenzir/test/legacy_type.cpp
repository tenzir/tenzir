//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/legacy_type.hpp"

#include "tenzir/concept/parseable/tenzir/legacy_type.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/concept/printable/stream.hpp"
#include "tenzir/concept/printable/tenzir/data.hpp"
#include "tenzir/concept/printable/tenzir/offset.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/data.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/variant_traits.hpp"

#include <string_view>

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace tenzir;

TEST("legacy type default construction") {
  legacy_type t;
  CHECK(! t);
  CHECK(! is<legacy_bool_type>(t));
}

TEST("legacy type construction") {
  auto s = legacy_string_type{};
  auto t = legacy_type{s};
  CHECK(t);
  CHECK(is<legacy_string_type>(t));
  CHECK(try_as<legacy_string_type>(&t) != nullptr);
}

TEST("assignment") {
  auto t = legacy_type{legacy_string_type{}};
  CHECK(t);
  CHECK(is<legacy_string_type>(t));
  t = legacy_real_type{};
  CHECK(t);
  CHECK(is<legacy_real_type>(t));
  t = {};
  CHECK(! t);
  CHECK(! is<legacy_real_type>(t));
  auto u = legacy_type{legacy_none_type{}};
  CHECK(u);
  CHECK(is<legacy_none_type>(u));
}

TEST("copying") {
  auto t = legacy_type{legacy_string_type{}};
  auto u = t;
  CHECK(is<legacy_string_type>(u));
}

TEST("names") {
  legacy_type t;
  t.name("foo");
  CHECK(t.name().empty());
  t = legacy_type{legacy_string_type{}};
  t.name("foo");
  CHECK_EQUAL(t.name(), "foo");
}

TEST("equality comparison") {
  MESSAGE("type-erased comparison");
  CHECK(legacy_type{} == legacy_type{});
  CHECK(legacy_type{legacy_bool_type{}} != legacy_type{});
  CHECK(legacy_type{legacy_bool_type{}} == legacy_type{legacy_bool_type{}});
  CHECK(legacy_type{legacy_bool_type{}} != legacy_type{legacy_real_type{}});
  auto x = legacy_type{legacy_string_type{}};
  auto y = legacy_type{legacy_string_type{}};
  x.name("foo");
  CHECK(x != y);
  y.name("foo");
  CHECK(x == y);
  MESSAGE("concrete type comparison");
  CHECK(legacy_real_type{} == legacy_real_type{});
  CHECK(legacy_real_type{}.name("foo") != legacy_real_type{});
  CHECK(legacy_real_type{}.name("foo") == legacy_real_type{}.name("foo"));
  auto attrs = std::vector<legacy_attribute>{{"key", "value"}};
  CHECK(legacy_real_type{}.attributes(attrs) != legacy_real_type{});
  CHECK(legacy_real_type{}.attributes(attrs)
        == legacy_real_type{}.attributes(attrs));
}

TEST("less - than comparison") {
  CHECK(! (legacy_type{} < legacy_type{}));
  CHECK(! (legacy_real_type{} < legacy_real_type{}));
  CHECK(legacy_string_type{}.name("a") < legacy_string_type{}.name("b"));
  CHECK(legacy_record_type{}.name("a") < legacy_record_type{}.name("b"));
}

TEST("strict weak ordering") {
  std::vector<legacy_type> xs{legacy_string_type{}, legacy_address_type{},
                              legacy_pattern_type{}};
  std::vector<legacy_type> ys{legacy_string_type{}, legacy_pattern_type{},
                              legacy_address_type{}};
  std::sort(xs.begin(), xs.end());
  std::sort(ys.begin(), ys.end());
  CHECK(xs == ys);
}

TEST("legacy type parseable") {
  MESSAGE("basic");
  {
    auto t = legacy_type{};
    CHECK(parsers::legacy_type("bool", t));
    CHECK(t == legacy_bool_type{});
  }
  {
    auto t = legacy_type{};
    CHECK(parsers::legacy_type("string", t));
    CHECK(t == legacy_string_type{});
  }
  {
    auto t = legacy_type{};
    CHECK(parsers::legacy_type("ip", t));
    CHECK(t == legacy_address_type{});
  }
  MESSAGE("alias");
  {
    auto t = legacy_type{};
    CHECK(parsers::legacy_type("timestamp", t));
    CHECK_EQUAL(t, legacy_none_type{}.name("timestamp"));
  }
  MESSAGE("enum");
  {
    auto t = legacy_type{};
    CHECK(parsers::legacy_type("enum{foo, bar, baz}", t));
    // TODO: This test is broken (harmless); see tenzir/issues#971
    // CHECK((t == legacy_enumeration_type{{"foo", "bar", "baz"}}));
  }
  MESSAGE("container");
  {
    auto t = legacy_type{};
    CHECK(parsers::legacy_type("list<double>", t));
    // TODO: This test is broken (harmless); see tenzir/issues#971
    // CHECK(t == legacy_type{legacy_list_type{legacy_real_type{}}});
  }
  MESSAGE("record");
  {
    auto t = legacy_type{};
    auto str = R"__(record{"a b": ip, b: bool})__"sv;
    CHECK(parsers::legacy_type(str, t));
    auto r = legacy_record_type{
      {"a b", legacy_address_type{}},
      {"b", legacy_bool_type{}},
    };
    CHECK_EQUAL(t, r);
  }
  MESSAGE("recursive");
  {
    auto t = legacy_type{};
    auto str = "record{r: record{a: ip, i: record{b: bool}}}"sv;
    CHECK(parsers::legacy_type(str, t));
    auto r = legacy_record_type{
      {"r",
       legacy_record_type{
         {"a", legacy_address_type{}},
         {"i", legacy_record_type{{"b", legacy_bool_type{}}}},
       }},
    };
    CHECK_EQUAL(t, r);
  }
  MESSAGE("record algebra");
  {
    auto r = legacy_record_type{
      {"", legacy_none_type{}.name("foo")},
      {"+", legacy_none_type{}.name("bar")},
    }.attributes({{"$algebra"}});
    CHECK_EQUAL(unbox(to<legacy_type>("foo+bar")), r);
    CHECK_EQUAL(unbox(to<legacy_type>("foo + bar")), r);
    r.fields[1]
      = record_field{"-", legacy_record_type{{"bar", legacy_bool_type{}}}};
    CHECK_EQUAL(unbox(to<legacy_type>("foo-bar")), r);
    CHECK_EQUAL(unbox(to<legacy_type>("foo - bar")), r);
  }
  {
    auto str = "record{a: double} + bar"sv;
    auto r
      = legacy_record_type{{"", legacy_record_type{{"a", legacy_real_type{}}}},
                           {"+", legacy_none_type{}.name("bar")}}
          .attributes({{"$algebra"}});
    CHECK_EQUAL(unbox(to<legacy_type>(str)), r);
  }
  MESSAGE("invalid");
  {
    CHECK_ERROR(parsers::legacy_type(":bool"));
  }
}
