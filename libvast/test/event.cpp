#include "vast/event.hpp"
#include "vast/json.hpp"
#include "vast/load.hpp"
#include "vast/save.hpp"
#include "vast/concept/convertible/to.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/event.hpp"
#include "vast/concept/printable/vast/json.hpp"

#define SUITE event
#include "test.hpp"

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
  REQUIRE(is<vector>(e.data()));
  REQUIRE(is<record_type>(e.type()));
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
  // TODO: use a saner output format for events
  auto str = "foo [123456789|1970-01-01+00:00:00.0] [T, 42, -234987]";
  CHECK_EQUAL(to_string(e), str);
}

TEST(serialization) {
  std::vector<char> buf;
  save(buf, e);
  event e2;
  load(buf, e2);
  CHECK_EQUAL(e, e2);
}

TEST(json) {
  auto expected = R"json({
  "id": 123456789,
  "timestamp": 0,
  "value": {
    "type": {
      "name": "foo",
      "kind": "record",
      "structure": {
        "x": {
          "name": "",
          "kind": "bool",
          "structure": null,
          "attributes": {}
        },
        "y": {
          "name": "",
          "kind": "count",
          "structure": null,
          "attributes": {}
        },
        "z": {
          "name": "",
          "kind": "integer",
          "structure": null,
          "attributes": {}
        }
      },
      "attributes": {}
    },
    "data": {
      "x": true,
      "y": 42,
      "z": -234987
    }
  }
})json";
  CHECK_EQUAL(to_string(to_json(e)), expected);
}

FIXTURE_SCOPE_END()
