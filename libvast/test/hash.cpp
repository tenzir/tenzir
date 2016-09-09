#include "vast/concept/hashable/uhash.hpp"
#include "vast/detail/hash/crc.hpp"
#include "vast/detail/hash/xxhash.hpp"

#define SUITE hash
#include "test.hpp"

using namespace vast;
using namespace vast::detail;

TEST(crc32) {
  // one-shot
  CHECK(uhash<crc32>{}('f') == 1993550816);
  CHECK(uhash<crc32>{}('o') == 252678980);
  // incremental
  crc32 crc;
  crc("foo", 3);
  CHECK(static_cast<crc32::result_type>(crc) == 2356372769);
  crc32 foo;
  hash_append(foo, 'f');
  CHECK(static_cast<crc32::result_type>(foo) == 1993550816);
  hash_append(foo, 'o');
  CHECK(static_cast<crc32::result_type>(foo) == 2943590935);
  hash_append(foo, 'o');
  CHECK(static_cast<crc32::result_type>(foo) == 2356372769);
}

TEST(xxhash32) {
  // one-shot
  CHECK(uhash<xxhash32>{}(42) == 1161967057);
  // incremental
  xxhash32 xxh32;
  hash_append(xxh32, 0);
  hash_append(xxh32, 1);
  hash_append(xxh32, 2);
  CHECK(static_cast<size_t>(xxh32) == 964478135);
}

TEST(xxhash64) {
  // one-shot
  CHECK(uhash<xxhash64>{42}("42") == 7873697032674743835); // includes NUL
  // incremental
  xxhash64 xxh64;
  xxh64("foo", 3);
  CHECK(static_cast<size_t>(xxh64) == 3728699739546630719ul);
  xxh64("bar", 3);
  CHECK(static_cast<size_t>(xxh64) == 11721187498075204345ul);
  xxh64("baz", 3);
  CHECK(static_cast<size_t>(xxh64) == 6505385152087097371ul);
}

TEST(xxhash zero bytes) {
  // Should not segfault or trigger assertions.
  xxhash32 xxh32;
  xxh32(nullptr, 0);
  xxhash64 xxh64;
  xxh64(nullptr, 0);
}
