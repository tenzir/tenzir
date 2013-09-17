#include "test.h"
#include "vast/event.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(event_construction)
{
  event e;
  BOOST_CHECK_EQUAL(e.name(), "");
  BOOST_CHECK_EQUAL(e.timestamp(), time_point());
  BOOST_CHECK(e.empty());

  auto jetzt = now();
  e.id(123456789);
  e.timestamp(jetzt);
  e.name("foo");
  e.emplace_back(true);
  e.emplace_back(42u);
  e.emplace_back(-234987);

  BOOST_CHECK_EQUAL(e.id(), 123456789);
  BOOST_CHECK_EQUAL(e.timestamp(), jetzt);
  BOOST_CHECK_EQUAL(e.name(), "foo");
  BOOST_CHECK_EQUAL(e.size(), 3);
  BOOST_CHECK_EQUAL(e[0], true);
  BOOST_CHECK_EQUAL(e[1], 42u);
  BOOST_CHECK_EQUAL(e[2], -234987);

  e.timestamp(time_point{});
  BOOST_CHECK_EQUAL(to_string(e),
                    "foo [123456789|1970-01-01+00:00:00] T, 42, -234987");

  // The initializer list ctor forwards the arguments to the base record.
  BOOST_CHECK_EQUAL(event{42}[0].which(), int_type);
}

BOOST_AUTO_TEST_CASE(quantifiers)
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

  BOOST_CHECK(e.any([](value const& v) { return v.which() == bool_type; }));
  BOOST_CHECK(e.all([](value const& v) { return v.which() != record_type; }));

  BOOST_CHECK(
      e.any([](value const& v) { return v.which() == record_type; }, false));

  BOOST_CHECK(
      e.any([](value const& v)
            {
              return v.which() == uint_type && v && v.get<uint64_t>() == 43;
            }));

  // We currently don't recurse into tables. If such a semantic turns out to be
  // desirable, we'll revisit this decision.
  BOOST_CHECK(
      ! e.any([](value const& v)
          {
            return v.which() == double_type && v && v.get<double>() == -2.4;
          }));
}
