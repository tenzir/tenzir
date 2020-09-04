/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#define SUITE yaml

#include "vast/concept/parseable/vast/yaml.hpp"

#include "vast/test/test.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/data.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;
using namespace std::string_literals;

namespace {

struct fixture {
  fixture() {
    // clang-format off
    rec = record{
      {"foo", -42},
      {"bar", 3.14},
      {"baz", list{"a", "b", true}},
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
bar: 3.1400000000000001
baz:
  - a
  - b
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

TEST(to_yaml - basic) {
  auto yaml = to_yaml(record{{"a", 4.2}, {"b", list{"foo", "bar"}}});
  auto str = "a: 4.2000000000000002\nb:\n  - foo\n  - bar";
  CHECK_EQUAL(yaml, str);
}

TEST(parseable) {
  data yaml;
  CHECK(parsers::yaml("[1, 2, 3]", yaml));
  CHECK_EQUAL(yaml, (list{1u, 2u, 3u}));
}

FIXTURE_SCOPE(yaml_tests, fixture)

TEST(from_yaml - nested) {
  auto x = from_yaml(str);
  std::cout << to_string(x) << std::endl;
  CHECK_EQUAL(x, rec);
}

TEST(to_yaml - nested) {
  auto yaml = to_yaml(rec);
  CHECK_EQUAL(yaml, str);
}

FIXTURE_SCOPE_END()
