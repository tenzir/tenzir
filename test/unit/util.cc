#include "framework/unit.h"

#include "vast/util/trial.h"
#include "vast/util/result.h"

SUITE("util")

using namespace vast;

TEST("error")
{
  error e;
  error shoot{"holy cow"};
  CHECK(shoot.msg() == "holy cow");
}

TEST("trial")
{
  trial<int> t = 42;
  REQUIRE(t);
  CHECK(*t == 42);

  trial<int> u = std::move(t);
  REQUIRE(u);
  CHECK(*u == 42);

  t = error{"whoops"};
  CHECK(! t);
}

TEST("result")
{
  result<int> t;
  REQUIRE(t.empty());
  REQUIRE(! t.engaged());
  REQUIRE(! t.failed());

  t = 42;
  REQUIRE(! t.empty());
  REQUIRE(t.engaged());
  REQUIRE(! t.failed());
  REQUIRE(*t == 42);

  t = error{"whoops"};
  REQUIRE(! t.empty());
  REQUIRE(! t.engaged());
  REQUIRE(t.failed());

  CHECK(t.error().msg() == "whoops");
}
