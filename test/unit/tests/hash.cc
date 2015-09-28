#include "vast/util/hash/crc.h"
#include "vast/util/hash/murmur.h"
#include "vast/util/hash/xxhash.h"

#define SUITE util
#include "test.h"

using namespace vast;
using namespace vast::util;

TEST(murmur hash) {
  CHECK(murmur3<32>::digest(42) == 3160117731);
}

TEST(xxhash) {
  CHECK(xxhash64::digest("42", 42) == 7873697032674743835); // includes NUL
  xxhash64 xxh64;
  xxh64.add("foo", 3);
  CHECK(xxh64.get() == 3728699739546630719ul);
  xxh64.add("bar", 3);
  CHECK(xxh64.get() == 11721187498075204345ul);
  xxh64.add("baz", 3);
  CHECK(xxh64.get() == 6505385152087097371ul);

  CHECK(xxhash32::digest(42) == 1161967057);
  xxhash32 xxh32;
  xxh32.add(0);
  xxh32.add(1);
  xxh32.add(2);
  CHECK(xxh32.get() == 964478135);
}

TEST(crc32) {
  CHECK(crc32::digest('f') == 1993550816);
  CHECK(crc32::digest('o') == 252678980);
  CHECK(crc32::digest_bytes("foo", 3) == 2356372769);

  crc32 crc;
  crc.add('f');
  CHECK(crc.get() == 1993550816);
  crc.add('o');
  CHECK(crc.get() == 2943590935);
  crc.add('o');
  CHECK(crc.get() == 2356372769);
}
