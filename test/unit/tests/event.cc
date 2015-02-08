#include "framework/unit.h"

#include "vast/event.h"
#include "vast/util/json.h"

using namespace vast;

SUITE("core")

TEST("event")
{
  auto tr = type::record{
    {"x", type::boolean{}},
    {"y", type::count{}},
    {"z", type::integer{}}};

  REQUIRE(tr.name("foo"));

  record r;
  r.emplace_back(true);
  r.emplace_back(42u);
  r.emplace_back(-234987);

  event e;
  CHECK(e.type().name() == "");
  CHECK(e.timestamp() == time::point{});

  e = event::make(r, tr);
  REQUIRE(is<record>(e));
  REQUIRE(is<type::record>(e.type()));

  e.id(123456789);
  CHECK(e.id() == 123456789);

  auto now = time::now();
  e.timestamp(now);
  CHECK(e.timestamp() == now);

  e.timestamp(time::point{});
  CHECK(to_string(e) == "foo [123456789|1970-01-01+00:00:00] (T, 42, -234987)");

  auto t = to<util::json>(e);
  REQUIRE(t);

  auto tree = R"json({
  "id": 123456789,
  "timestamp": 0,
  "value": {
    "data": [
      true,
      42,
      -234987
    ],
    "type": "foo"
  }
})json";

  CHECK(to_string(*t, true) == tree);
}
