#include "framework/unit.h"
#include "vast/util/lru_cache.h"

using namespace vast;

SUITE("util")

TEST("lru_cache")
{
  using lru_cache = util::lru_cache<std::string, int>;
  lru_cache c{2};
  CHECK(c.insert("x", 1).second);
  CHECK(c.insert("fu", 2).second);
  CHECK(c.insert("foo", 3).second);
  CHECK(c.insert("quux", 4).second);
  CHECK(c.insert("corge", 5).second);
  c.on_evict([&](std::string const&, int v) { CHECK(v == 4); });
  CHECK(c.insert("foo", 6).second);
  CHECK(! c.insert("foo", 7).second);
  CHECK(! c.lookup("x"));
  CHECK(c.lookup("corge"));
  auto i = c.lookup("foo");
  REQUIRE(i);
  CHECK(*i == 6);
}
