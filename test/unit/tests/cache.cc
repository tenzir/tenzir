#include <numeric>

#include <vast/util/cache.h>
#include <vast/concept/serializable/std/string.h>
#include <vast/concept/serializable/util/cache.h>
#include <vast/concept/serializable/io.h>

#include "framework/unit.h"

using namespace vast;

SUITE("util")

TEST("cache")
{
  util::cache<std::string, int> c{2};
  c["x"] = 1;
  auto x = c.lookup("x");
  REQUIRE(x);
  CHECK(*x == 1);
  CHECK(c.insert("fu", 2).second);
  CHECK(c.insert("foo", 3).second);
  CHECK(c.insert("quux", 4).second);
  CHECK(c.insert("corge", 5).second);
  // Evict one element.
  c.on_evict([&](std::string const&, int v) { CHECK(v == 4); });
  CHECK(c.insert("foo", 6).second);
  // Duplicate keys cannot be re-inserted.
  CHECK(! c.insert("foo", 7).second);
  CHECK(! c.lookup("x"));
  CHECK(c.lookup("corge"));
  // Ensure key has the right value.
  x = c.lookup("foo");
  REQUIRE(x);
  CHECK(*x == 6);
  // Check iteration.
  auto i = c.begin();
  REQUIRE(i != c.end());
  CHECK(i->first == "corge");
  ++i;
  CHECK(i->first == "foo");
  ++i;
  CHECK(i == c.end());
  // Serialization
  std::vector<uint8_t> buf;
  save(buf, c);
  decltype(c) d{42};
  load(buf, d);
  CHECK(c.size() == d.size());
  CHECK(*c.begin() == *d.begin());
}
