#include "vast/event.h"
#include "vast/concept/serializable/value.h"
#include "vast/concept/state/event.h"
#include "vast/concept/serializable/io.h"
#include "vast/util/json.h"

#include "test.h"

using namespace vast;

TEST(event)
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

  std::vector<uint8_t> buf;
  save(buf, e);
  decltype(e) e2;
  load(buf, e2);
  CHECK(e == e2);

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
