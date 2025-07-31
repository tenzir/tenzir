//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/concept/parseable/tenzir/yaml.hpp"

#include "tenzir/concept/parseable/tenzir/time.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/concept/printable/tenzir/data.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/data.hpp"
#include "tenzir/error.hpp"
#include "tenzir/test/test.hpp"

using namespace tenzir;
using namespace std::chrono_literals;
using namespace std::string_literals;

namespace {

struct fixture {
  fixture() {
    // clang-format off
    rec = record{
      {"foo", int64_t{-42}},
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

TEST("from_yaml - basic") {
  auto yaml = unbox(from_yaml("{a: 4.2, b: [foo, bar]}"));
  CHECK_EQUAL(yaml, (record{{"a", 4.2}, {"b", list{"foo", "bar"}}}));
}

TEST("from_yaml - invalid yaml") {
  auto yaml = from_yaml("@!#$%^&*()_+");
  REQUIRE(! yaml);
  CHECK_EQUAL(yaml.error(), ec::parse_error);
}

TEST("to_yaml - basic") {
  auto yaml = unbox(to_yaml(record{{"a", 4.2}, {"b", list{"foo", "bar"}}}));
  auto str = "a: 4.2\nb:\n  - foo\n  - bar";
  CHECK_EQUAL(yaml, str);
}

TEST("to_yaml - time types") {
  auto t = unbox(to<tenzir::time>("2021-01-01"));
  auto yaml = unbox(to_yaml(record{{"d", 12ms}, {"t", t}}));
  auto str = "d: 12ms\nt: 2021-01-01T00:00:00Z";
  CHECK_EQUAL(yaml, str);
}

TEST("to_yaml - invalid data") {
  // We tried a lot of weird combinations of invalid data values, but none of
  // them triggered a failure in the emitter logic.
  CHECK(to_yaml(caf::none).has_value());
  CHECK(to_yaml(list{map{{"", ""}}}).has_value());
  CHECK(to_yaml(map{{list{}, caf::none}}).has_value());
  CHECK(to_yaml(record{{"", caf::none}}).has_value());
}

TEST("yaml parseable") {
  data yaml;
  CHECK(parsers::yaml("[1, 2, 3]", yaml));
  CHECK_EQUAL(yaml, (list{1u, 2u, 3u}));
}

WITH_FIXTURE(fixture) {
  TEST("from_yaml - nested") {
    auto x = from_yaml(str);
    CHECK_EQUAL(x, rec);
  }

  TEST("to_yaml - nested") {
    auto yaml = unbox(to_yaml(rec));
    CHECK_EQUAL(yaml, str);
  }
}
