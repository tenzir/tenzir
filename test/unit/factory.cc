#include "framework/unit.h"

#include "vast/util/factory.h"

using namespace vast;

SUITE("util")

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

TEST("value factories")
{
  util::factory<int, util::value_construction> int_factory;
  CHECK(int_factory(42) == 42);
}

TEST("pointer factories")
{
  util::factory<std::string, util::bare_pointer_construction> string_factory;
  std::string foo{"foo"};
  CHECK(*string_factory("foo") == foo);

  util::factory<double, util::unique_pointer_construction> double_factory;
  CHECK(*double_factory(4.2) == 4.2);
}

TEST("polymorphic factories")
{
  util::polymorphic_factory<base, std::string> poly_factory;
  poly_factory.announce<derived<42>>("foo");
  poly_factory.announce<derived<1337>>("bar");

  auto foo = poly_factory.construct("foo");
  auto bar = poly_factory.construct("bar");
  REQUIRE(!!foo);
  REQUIRE(!!bar);
  CHECK(foo->f() == 42);
  CHECK(bar->f() == 1337);
}
