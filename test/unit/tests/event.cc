#include "framework/unit.h"

#include "vast/event.h"
#include "vast/util/json.h"

using namespace vast;

SUITE("event")

TEST("construction")
{
  event e;
  CHECK(e.name() == "");
  CHECK(e.timestamp() == time_point());
  CHECK(e.empty());

  std::vector<argument> args;
  args.emplace_back("", type::make<bool_type>());
  args.emplace_back("", type::make<uint_type>());
  args.emplace_back("", type::make<int_type>());

  // TODO: validate event data against type.
  e.type(type::make<record_type>("foo", std::move(args)));

  auto jetzt = now();
  e.id(123456789);
  e.timestamp(jetzt);
  e.emplace_back(true);
  e.emplace_back(42u);
  e.emplace_back(-234987);

  CHECK(e.id() == 123456789);
  CHECK(e.timestamp() == jetzt);
  CHECK(e.size() == 3);
  CHECK(e[0] == true);
  CHECK(e[1] == 42u);
  CHECK(e[2] == -234987);

  e.timestamp(time_point{});
  CHECK(to_string(e) == "foo [123456789|1970-01-01+00:00:00] T, 42, -234987");

  auto t = to<util::json>(e);
  REQUIRE(t);

  auto tree = R"json({
  "data": [
    {
      "type": "bool",
      "value": true
    },
    {
      "type": "uint",
      "value": 42
    },
    {
      "type": "int",
      "value": -234987
    }
  ],
  "id": 123456789,
  "timestamp": 0,
  "type": "foo"
})json";

  CHECK(to_string(*t, true) == tree);

  // The initializer_list ctor forwards the arguments to the base record.
  CHECK(event{42}[0].which() == int_value);

  event e1{
    invalid,
    true,
    -1,
    9u,
    123.456789,
    "bar",
    "12345678901234567890",
    table{{22, "ssh"}, {25, "smtp"}, {80, "http"}},
    vector{"foo", "bar", "baz"},
    regex{"[0-9][a-z]?\\w+$"},
    record{invalid, true, -42, 4711u},
    *address::from_v4("192.168.0.1"),
    *address::from_v6("2001:db8:0000:0000:0202:b3ff:fe1e:8329"),
    prefix{*address::from_v4("10.1.33.22"), 8},
    port{139, port::tcp}
  };

  event e2{
    false,
    1000000,
    123456789u,
    -123.456789,
    "baz\"qux",
    {"baz\0", 4},
    "Das ist also des Pudels Kern.",
    invalid,
    987.654321,
    -12081983,
    regex{"[0-9][a-z]?\\w+$"},
    time_point{now()},
    time_range{now().since_epoch()},
    *address::from_v6("ff01::1"),
    *address::from_v6("2001:db8:0000:0000:0202:b3ff:fe1e:8329"),
    prefix{*address::from_v6("ff00::"), 16},
    port{53, port::udp}
  };
}

TEST("quantifiers")
{
  event e{
    true,
    record
    {
      record{true, record{}},
      record{false, 43u},
      table{{-1.2, "foo"}, {-2.4, "bar"}}
    }
  };

  CHECK(e.any([](value const& v) { return v.which() == bool_value; }));
  CHECK(e.all([](value const& v) { return v.which() != record_value; }));

  CHECK(e.any([](value const& v) { return v.which() == record_value; }, false));

  CHECK(
      e.any(
          [](value const& v)
          {
            return v.which() == uint_value && v && v.get<uint64_t>() == 43;
          }));

  // We currently don't recurse into tables. If such a semantic turns out to be
  // desirable, we'll revisit this decision.
  CHECK(
      ! e.any([](value const& v)
          {
            return v.which() == double_value && v && v.get<double>() == -2.4;
          }));
}
