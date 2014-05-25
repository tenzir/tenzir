#include "framework/unit.h"
#include "vast/util/lru_cache.h"

SUITE("util")

TEST("lru_cache")
{
  using lru_cache = vast::util::lru_cache<std::string, size_t>;
  lru_cache c{2, [](std::string const& str) { return str.length(); }};

  // Perform some accesses.
  c.retrieve("x");
  c.retrieve("fu");
  c.retrieve("foo");
  c.retrieve("quux");
  c.retrieve("corge");
  c.retrieve("foo");

  CHECK(c.retrieve_latest() == 3);

  std::vector<std::string> v;
  std::transform(
      c.begin(),
      c.end(),
      std::back_inserter(v),
      [](lru_cache::cache::value_type const& pair) { return pair.first; });

  std::sort(v.begin(), v.end());
  decltype(v) expected{"corge", "foo"};
  CHECK(v == expected);
}
