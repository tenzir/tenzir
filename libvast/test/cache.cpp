#include <numeric>

#include <vast/load.hpp>
#include <vast/save.hpp>
#include <vast/util/cache.hpp>

#define SUITE util
#include "test.hpp"

using namespace vast;

TEST(LRU cache) {
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
  CHECK(!c.insert("foo", 7).second);
  CHECK(!c.lookup("x"));
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
  std::vector<char> buf;
  save(buf, c);
  decltype(c) d{42};
  load(buf, d);
  CHECK(c.size() == d.size());
  CHECK(*c.begin() == *d.begin());
  // Erasure (does not call evict function)
  CHECK(!c.contains("x"));
  CHECK(c.contains("foo"));
  CHECK(c.erase("foo") == 1);
}

TEST(MRU cache) {
  util::cache<std::string, int, util::mru> c{2};
  c.on_evict([&](std::string const&, int v) { CHECK(v == 3); });
  CHECK(c.insert("fu", 2).second);
  CHECK(c.insert("foo", 3).second);
  CHECK(c.insert("quux", 4).second);
  CHECK(c.contains("quux"));
  CHECK(!c.contains("foo"));
  CHECK(c.contains("fu"));
}
