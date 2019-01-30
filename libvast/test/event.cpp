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

#define SUITE event

#include "vast/test/test.hpp"

#include "vast/event.hpp"
#include "vast/json.hpp"
#include "vast/load.hpp"
#include "vast/save.hpp"
#include "vast/concept/convertible/to.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/event.hpp"
#include "vast/concept/printable/vast/json.hpp"

using namespace vast;

namespace {

struct fixture {
  fixture() {
    // Type
    t = record_type{
      {"x", boolean_type{}},
      {"y", count_type{}},
      {"z", integer_type{}}};
    t.name("foo");
    // Data
    r.emplace_back(true);
    r.emplace_back(42u);
    r.emplace_back(-234987);
    // Type-safe creation through factory.
    e = event::make(r, t);
    e.id(123456789);
  }

  type t;
  vector r;
  event e;
};

} // namespace <anonymous>

FIXTURE_SCOPE(event_tests, fixture)

TEST(basics) {
  CHECK_EQUAL(e.type().name(), "foo");
  REQUIRE(caf::holds_alternative<vector>(e.data()));
  REQUIRE(caf::holds_alternative<record_type>(e.type()));
  MESSAGE("meta data");
  CHECK_EQUAL(e.id(), 123456789ull);
  auto now = timestamp::clock::now();
  e.timestamp(now);
  CHECK_EQUAL(e.timestamp(), now);
  e.timestamp(timestamp{});
}

TEST(flattening) {
  auto flat = flatten(e);
  CHECK_EQUAL(flat, e); // no recursive records
}

TEST(printable) {
  auto str = "<T, 42, -234987>";
  CHECK_EQUAL(to_string(e), str);
}

TEST(serialization) {
  std::vector<char> buf;
  CHECK_EQUAL(save(nullptr, buf, e), caf::none);
  event e2;
  CHECK_EQUAL(load(nullptr, buf, e2), caf::none);
  CHECK_EQUAL(e, e2);
}

TEST(json) {
  auto expected = R"__({
  "id": 123456789,
  "timestamp": 0,
  "value": {
    "x": true,
    "y": 42,
    "z": -234987
  }
})__";
  CHECK_EQUAL(to_string(to_json(e)), expected);
}

FIXTURE_SCOPE_END()
