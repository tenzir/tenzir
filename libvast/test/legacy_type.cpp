//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/legacy_type.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/legacy_type.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/concept/printable/vast/offset.hpp"
#include "vast/data.hpp"
#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/test.hpp"

#include <string_view>

using caf::get_if;
using caf::holds_alternative;

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace vast;

TEST(default construction) {
  legacy_type t;
  CHECK(!t);
  CHECK(!holds_alternative<legacy_bool_type>(t));
}

TEST(construction) {
  auto s = legacy_string_type{};
  auto t = legacy_type{s};
  CHECK(t);
  CHECK(holds_alternative<legacy_string_type>(t));
  CHECK(get_if<legacy_string_type>(&t) != nullptr);
}

TEST(assignment) {
  auto t = legacy_type{legacy_string_type{}};
  CHECK(t);
  CHECK(holds_alternative<legacy_string_type>(t));
  t = legacy_real_type{};
  CHECK(t);
  CHECK(holds_alternative<legacy_real_type>(t));
  t = {};
  CHECK(!t);
  CHECK(!holds_alternative<legacy_real_type>(t));
  auto u = legacy_type{legacy_none_type{}};
  CHECK(u);
  CHECK(holds_alternative<legacy_none_type>(u));
}

TEST(copying) {
  auto t = legacy_type{legacy_string_type{}};
  auto u = t;
  CHECK(holds_alternative<legacy_string_type>(u));
}

TEST(names) {
  legacy_type t;
  t.name("foo");
  CHECK(t.name().empty());
  t = legacy_type{legacy_string_type{}};
  t.name("foo");
  CHECK_EQUAL(t.name(), "foo");
}

TEST(equality comparison) {
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

TEST(less - than comparison) {
  CHECK(!(legacy_type{} < legacy_type{}));
  CHECK(!(legacy_real_type{} < legacy_real_type{}));
  CHECK(legacy_string_type{}.name("a") < legacy_string_type{}.name("b"));
  CHECK(legacy_record_type{}.name("a") < legacy_record_type{}.name("b"));
}

TEST(strict weak ordering) {
  std::vector<legacy_type> xs{legacy_string_type{}, legacy_address_type{},
                              legacy_pattern_type{}};
  std::vector<legacy_type> ys{legacy_string_type{}, legacy_pattern_type{},
                              legacy_address_type{}};
  std::sort(xs.begin(), xs.end());
  std::sort(ys.begin(), ys.end());
  CHECK(xs == ys);
}

TEST(parseable) {
  legacy_type t;
  MESSAGE("basic");
  CHECK(parsers::legacy_type("bool", t));
  CHECK(t == legacy_bool_type{});
  CHECK(parsers::legacy_type("string", t));
  CHECK(t == legacy_string_type{});
  CHECK(parsers::legacy_type("addr", t));
  CHECK(t == legacy_address_type{});
  MESSAGE("alias");
  CHECK(parsers::legacy_type("timestamp", t));
  CHECK_EQUAL(t, legacy_none_type{}.name("timestamp"));
  MESSAGE("enum");
  CHECK(parsers::legacy_type("enum{foo, bar, baz}", t));
  CHECK((t == legacy_enumeration_type{{"foo", "bar", "baz"}}));
  MESSAGE("container");
  CHECK(parsers::legacy_type("list<real>", t));
  CHECK(t == legacy_type{legacy_list_type{legacy_real_type{}}});
  MESSAGE("record");
  auto str = R"__(record{"a b": ip, b: bool})__"sv;
  CHECK(parsers::legacy_type(str, t));
  // clang-format off
  auto r = legacy_record_type{
    {"a b", legacy_address_type{}},
    {"b", legacy_bool_type{}}
  };
  // clang-format on
  CHECK_EQUAL(t, r);
  MESSAGE("recursive");
  str = "record{r: record{a: ip, i: record{b: bool}}}"sv;
  CHECK(parsers::legacy_type(str, t));
  // clang-format off
  r = legacy_record_type{
    {"r", legacy_record_type{
      {"a", legacy_address_type{}},
      {"i", legacy_record_type{{"b", legacy_bool_type{}}}}
    }}
  };
  // clang-format on
  CHECK_EQUAL(t, r);
  MESSAGE("record algebra");
  // clang-format off
  r = legacy_record_type{
    {"", legacy_none_type{}.name("foo")},
    {"+", legacy_none_type{}.name("bar")}
  }.attributes({{"$algebra"}});
  // clang-format on
  CHECK_EQUAL(unbox(to<legacy_type>("foo+bar")), r);
  CHECK_EQUAL(unbox(to<legacy_type>("foo + bar")), r);
  r.fields[1]
    = record_field{"-", legacy_record_type{{"bar", legacy_bool_type{}}}};
  CHECK_EQUAL(unbox(to<legacy_type>("foo-bar")), r);
  CHECK_EQUAL(unbox(to<legacy_type>("foo - bar")), r);
  str = "record{a: double} + bar"sv;
  // clang-format off
  r = legacy_record_type{
    {"", legacy_record_type{{"a", legacy_real_type{}}}},
    {"+", legacy_none_type{}.name("bar")}
  }.attributes({{"$algebra"}});
  // clang-format on
  CHECK_EQUAL(unbox(to<legacy_type>(str)), r);
  MESSAGE("invalid");
  CHECK_ERROR(parsers::legacy_type(":bool"));
}

namespace {

struct fixture : public fixtures::deterministic_actor_system {
  fixture() : fixtures::deterministic_actor_system(VAST_PP_STRINGIFY(SUITE)) {
  }
};

} // namespace

FIXTURE_SCOPE(type_tests, fixture)

TEST(serialization) {
  CHECK_ROUNDTRIP(legacy_type{});
  CHECK_ROUNDTRIP(legacy_none_type{});
  CHECK_ROUNDTRIP(legacy_bool_type{});
  CHECK_ROUNDTRIP(legacy_integer_type{});
  CHECK_ROUNDTRIP(legacy_count_type{});
  CHECK_ROUNDTRIP(legacy_real_type{});
  CHECK_ROUNDTRIP(legacy_duration_type{});
  CHECK_ROUNDTRIP(legacy_time_type{});
  CHECK_ROUNDTRIP(legacy_string_type{});
  CHECK_ROUNDTRIP(legacy_pattern_type{});
  CHECK_ROUNDTRIP(legacy_address_type{});
  CHECK_ROUNDTRIP(legacy_subnet_type{});
  CHECK_ROUNDTRIP(legacy_enumeration_type{});
  CHECK_ROUNDTRIP(legacy_list_type{});
  CHECK_ROUNDTRIP(legacy_map_type{});
  CHECK_ROUNDTRIP(legacy_record_type{});
  CHECK_ROUNDTRIP(legacy_alias_type{});
  CHECK_ROUNDTRIP(legacy_type{legacy_none_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_bool_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_integer_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_count_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_real_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_duration_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_time_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_string_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_pattern_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_address_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_subnet_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_enumeration_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_list_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_map_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_record_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_alias_type{}});
  auto r
    = legacy_record_type{{"x", legacy_integer_type{}},
                         {"y", legacy_address_type{}},
                         {"z", legacy_real_type{}.attributes({{"key", "valu"
                                                                      "e"}})}};
  // Make it recursive.
  r = {{"a", legacy_map_type{legacy_string_type{}, legacy_count_type{}}},
       {"b", legacy_list_type{legacy_bool_type{}}.name("foo")},
       {"c", r}};
  r.name("foo");
  CHECK_ROUNDTRIP(r);
}

FIXTURE_SCOPE_END()
