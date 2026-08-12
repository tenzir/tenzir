//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
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
  REQUIRE(not yaml);
  CHECK_EQUAL(yaml.error(), ec::parse_error);
}

TEST("from_yaml_documents - single document") {
  auto docs = unbox(from_yaml_documents("{a: 4.2, b: [foo, bar]}"));
  REQUIRE_EQUAL(docs.size(), 1u);
  CHECK_EQUAL(docs[0], (record{{"a", 4.2}, {"b", list{"foo", "bar"}}}));
}

TEST("from_yaml_documents - multiple documents") {
  auto str = "a: 1\n---\nb: 2\n---\n- x\n- y\n";
  auto docs = unbox(from_yaml_documents(str));
  REQUIRE_EQUAL(docs.size(), 3u);
  CHECK_EQUAL(docs[0], (record{{"a", 1u}}));
  CHECK_EQUAL(docs[1], (record{{"b", 2u}}));
  CHECK_EQUAL(docs[2], (list{"x", "y"}));
}

TEST("from_yaml_documents - explicit document markers") {
  auto str = "---\na: 1\n...\n---\nb: 2\n...\n";
  auto docs = unbox(from_yaml_documents(str));
  REQUIRE_EQUAL(docs.size(), 2u);
  CHECK_EQUAL(docs[0], (record{{"a", 1u}}));
  CHECK_EQUAL(docs[1], (record{{"b", 2u}}));
}

TEST("from_yaml_documents - empty stream") {
  auto docs = unbox(from_yaml_documents(""));
  CHECK_EQUAL(docs.size(), 0u);
}

TEST("from_yaml_documents - invalid stream") {
  auto docs = from_yaml_documents("a: 1\n---\n@!#$%^&*()_+\n---\nb: 2\n");
  REQUIRE(not docs);
  CHECK_EQUAL(docs.error(), ec::parse_error);
}

TEST("from_yaml_documents - first document matches from_yaml") {
  auto str = "a: 1\n---\nb: 2\n";
  auto docs = unbox(from_yaml_documents(str));
  REQUIRE_EQUAL(docs.size(), 2u);
  CHECK_EQUAL(docs[0], unbox(from_yaml(str)));
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
