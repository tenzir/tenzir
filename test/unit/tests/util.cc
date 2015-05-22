#include "vast/error.h"
#include "vast/trial.h"
#include "vast/result.h"

#define SUITE util
#include "test.h"

using namespace vast;

TEST(error)
{
  error e;
  error shoot{"holy cow"};
  CHECK(shoot.msg() == "holy cow");
}

TEST(trial)
{
  trial<int> t = 42;
  REQUIRE(t);
  CHECK(*t == 42);

  trial<int> u = std::move(t);
  REQUIRE(u);
  CHECK(*u == 42);

  t = error{"whoops"};
  CHECK(! t);

  t = std::move(u);
  CHECK(t);

  trial<void> x;
  CHECK(x);
  x = error{"bad"};
  CHECK(! x);
  x = nothing;
  CHECK(x);
}

TEST(result)
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
