#include "test.h"
#include "vast/util/factory.h"

using namespace vast;

struct base
{
  virtual int f() = 0;
};

template <int N>
struct derived : public base
{
  virtual int f() final
  {
    return N;
  }
};

BOOST_AUTO_TEST_CASE(value_factories)
{
  util::factory<int, util::value_construction> int_factory;
  BOOST_CHECK_EQUAL(int_factory(42), 42);
}

BOOST_AUTO_TEST_CASE(pointer_factories)
{
  util::factory<double, util::pointer_construction> double_factory;
  BOOST_CHECK_EQUAL(*double_factory(4.2), 4.2);
}

BOOST_AUTO_TEST_CASE(polymorphic_factories)
{
  util::polymorphic_factory<base, std::string> poly_factory;
  poly_factory.announce<derived<42>>("foo");
  poly_factory.announce<derived<1337>>("bar");

  auto foo = poly_factory.construct("foo");
  auto bar = poly_factory.construct("bar");
  BOOST_REQUIRE(foo);
  BOOST_REQUIRE(bar);
  BOOST_CHECK_EQUAL(foo->f(), 42);
  BOOST_CHECK_EQUAL(bar->f(), 1337);
}
