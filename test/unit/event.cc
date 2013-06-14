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
  e.id(123456789ull);
  e.timestamp(jetzt);
  e.name("foo");
  e.emplace_back(true);
  e.emplace_back(42u);
  e.emplace_back(-234987);

  BOOST_CHECK_EQUAL(e.id(), 123456789ull);
  BOOST_CHECK_EQUAL(e.timestamp(), jetzt);
  BOOST_CHECK_EQUAL(e.name(), "foo");
  BOOST_CHECK_EQUAL(e.size(), 3);
  BOOST_CHECK_EQUAL(e[0], true);
  BOOST_CHECK_EQUAL(e[1], 42u);
  BOOST_CHECK_EQUAL(e[2], -234987);
}

BOOST_AUTO_TEST_CASE(quantifiers)
{
  event e{
    true,
    record
    {
      record{true, record()},
      record{false, 43u},
      set{1, 2, 3},
      table{-1.2, "foo", -2.4, "bar"}
    }
  };

  BOOST_CHECK(e.any(
          [](value const& v)
          {
            return v.which() == uint_type;
          }));

  BOOST_CHECK(e.all(
          [](value const& v)
          {
            return v.which() != record_type;
          }));

  BOOST_CHECK(e.any(
          [](value const& v)
          {
            return v.which() == int_type && v.get<int64_t>() == 3;
          }));

  BOOST_CHECK(e.any(
          [](value const& v)
          {
            return v.which() == double_type && v.get<double>() == -2.4;
          }));
}
