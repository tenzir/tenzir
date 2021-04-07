//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE yaml

#include "vast/concept/parseable/vast/yaml.hpp"

#include "vast/test/test.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/data.hpp"
#include "vast/error.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;
using namespace std::chrono_literals;
using namespace std::string_literals;

namespace {

struct fixture {
  fixture() {
    // clang-format off
    rec = record{
      {"foo", -42},
      {"bar", 3.14},
      {"baz", list{"a", caf::none, true}},
      {"qux", record{
        {"x", false},
        {"y", 1337u},
        {"z", list{
          record{
            {"v", "some value"}
          },
          record{
            {"a", "again here"}
          },
          record{
            {"s", "so be it"}
          },
          record{
            {"t", "to the king"}
          }
        }}
      }}
    };
    // clang-format on
    str = R"yaml(foo: -42
bar: 3.14
baz:
  - a
  - ~
  - true
qux:
  x: false
  y: 1337
  z:
    - v: some value
    - a: again here
    - s: so be it
    - t: to the king)yaml";
  }

  record rec;
  std::string str;
};

} // namespace

TEST(from_yaml - basic) {
  auto yaml = unbox(from_yaml("{a: 4.2, b: [foo, bar]}"));
  CHECK_EQUAL(yaml, (record{{"a", 4.2}, {"b", list{"foo", "bar"}}}));
}

TEST(from_yaml - invalid yaml) {
  auto yaml = from_yaml("@!#$%^&*()_+");
  REQUIRE(!yaml);
  CHECK_EQUAL(yaml.error(), ec::parse_error);
}

TEST(to_yaml - basic) {
  auto yaml = unbox(to_yaml(record{{"a", 4.2}, {"b", list{"foo", "bar"}}}));
  auto str = "a: 4.2\nb:\n  - foo\n  - bar";
  CHECK_EQUAL(yaml, str);
}

TEST(to_yaml - time types) {
  auto t = unbox(to<vast::time>("2021-01-01"));
  auto yaml = unbox(to_yaml(record{{"d", 12ms}, {"t", t}}));
  auto str = "d: 12.0ms\nt: 2021-01-01T00:00:00";
  CHECK_EQUAL(yaml, str);
}

TEST(to_yaml - invalid data) {
  // We tried a lot of weird combinations of invalid data values, but none of
  // them triggered a failure in the emitter logic.
  CHECK(to_yaml(caf::none).engaged());
  CHECK(to_yaml(list{map{{"", ""}}}).engaged());
  CHECK(to_yaml(map{{list{}, caf::none}}).engaged());
  CHECK(to_yaml(record{{"", caf::none}}).engaged());
}

TEST(parseable) {
  data yaml;
  CHECK(parsers::yaml("[1, 2, 3]", yaml));
  CHECK_EQUAL(yaml, (list{1u, 2u, 3u}));
}

FIXTURE_SCOPE(yaml_tests, fixture)

TEST(from_yaml - nested) {
  auto x = from_yaml(str);
  CHECK_EQUAL(x, rec);
}

TEST(to_yaml - nested) {
  auto yaml = unbox(to_yaml(rec));
  CHECK_EQUAL(yaml, str);
}

FIXTURE_SCOPE_END()
