#include "framework/unit.h"
#include "vast/util/search.h"

using namespace vast;

SUITE("util")

TEST("boyer-moore")
{
  std::string needle = "foo";
  std::string haystack = "hello foo world";

  // Function-object API.
  auto bm = util::make_boyer_moore(needle.begin(), needle.end());
  auto i = bm(haystack.begin(), haystack.end());
  REQUIRE(i != haystack.end());

  //// Free-function API
  auto j = util::search_boyer_moore(needle.begin(), needle.end(),
                                    haystack.begin(), haystack.end());
  REQUIRE(j != haystack.end());
  REQUIRE(i == j);
  CHECK(needle == std::string(i, i + needle.size()));

  haystack = "Da steh ich nun, ich armer Tor! Und bin so klug als wie zuvor";
  needle = "ich";
  bm = util::make_boyer_moore(needle.begin(), needle.end());
  for (size_t i = 0; i < 9; ++i)
    CHECK((bm(haystack.begin() + i, haystack.end()) - haystack.begin()) == 8);
  for (size_t i = 9; i < 18; ++i)
    CHECK((bm(haystack.begin() + i, haystack.end()) - haystack.begin()) == 17);
  for (size_t i = 18; i < haystack.size() - needle.size(); ++i)
    CHECK(bm(haystack.begin() + i, haystack.end()) == haystack.end());
}

TEST("knuth-morris-pratt")
{
  std::string needle = "foo";
  std::string haystack = "hello foo world";

  // Function-object API.
  auto kmp = util::make_knuth_morris_pratt(needle.begin(), needle.end());
  auto i = kmp(haystack.begin(), haystack.end());
  REQUIRE(i != haystack.end());

  //// Free-function API
  auto j = util::search_knuth_morris_pratt(needle.begin(), needle.end(),
                                           haystack.begin(), haystack.end());
  REQUIRE(j != haystack.end());
  REQUIRE(i == j);
  CHECK(needle == std::string(i, i + needle.size()));

  haystack = "Da steh ich nun, ich armer Tor! Und bin so klug als wie zuvor";
  needle = "ich";
  kmp = util::make_knuth_morris_pratt(needle.begin(), needle.end());
  for (size_t i = 0; i < 9; ++i)
    CHECK((kmp(haystack.begin() + i, haystack.end()) - haystack.begin()) == 8);
  for (size_t i = 9; i < 18; ++i)
    CHECK((kmp(haystack.begin() + i, haystack.end()) - haystack.begin()) == 17);
  for (size_t i = 18; i < haystack.size() - needle.size(); ++i)
    CHECK(kmp(haystack.begin() + i, haystack.end()) == haystack.end());
}
