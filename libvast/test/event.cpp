#include "vast/event.hpp"
#include "vast/json.hpp"
#include "vast/load.hpp"
#include "vast/save.hpp"
#include "vast/concept/convertible/vast/event.hpp"
#include "vast/concept/convertible/to.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/event.hpp"
#include "vast/concept/printable/vast/json.hpp"

#include "test.hpp"

using namespace vast;

TEST(event) {
  MESSAGE("construction");
  // Type
  auto tr = type::record{
    {"x", type::boolean{}},
    {"y", type::count{}},
    {"z", type::integer{}}};
  REQUIRE(tr.name("foo"));
  // Data
  record r;
  r.emplace_back(true);
  r.emplace_back(42u);
  r.emplace_back(-234987);
  event e;
  CHECK(e.type().name() == "");
  CHECK(e.timestamp() == time::point{});
  // Type-safe creation through factory.
  e = event::make(r, tr);
  REQUIRE(is<record>(e));
  REQUIRE(is<type::record>(e.type()));
  MESSAGE("meta data");
  e.id(123456789);
  CHECK(e.id() == 123456789);
  auto now = time::now();
  e.timestamp(now);
  CHECK(e.timestamp() == now);
  e.timestamp(time::point{});
  MESSAGE("flattening");
  auto flat = flatten(e);
  CHECK(flat == e); // no recursive records
  MESSAGE("string representation");
  CHECK(to_string(e) == "foo [123456789|1970-01-01+00:00:00] (T, 42, -234987)");
  MESSAGE("seralization");
  std::vector<char> buf;
  save(buf, e);
  decltype(e) e2;
  load(buf, e2);
  CHECK(e == e2);
  MESSAGE("json");
  auto expected = R"json({
  "id": 123456789,
  "timestamp": 0,
  "value": {
    "data": {
      "x": true,
      "y": 42,
      "z": -234987
    },
    "type": {
      "attributes": [],
      "kind": "record",
      "name": "foo",
      "structure": {
        "x": {
          "attributes": [],
          "kind": "boolean",
          "name": "",
          "structure": null
        },
        "y": {
          "attributes": [],
          "kind": "count",
          "name": "",
          "structure": null
        },
        "z": {
          "attributes": [],
          "kind": "integer",
          "name": "",
          "structure": null
        }
      }
    }
  }
})json";
  CHECK(to_string(to_json(e)) == expected);
}
