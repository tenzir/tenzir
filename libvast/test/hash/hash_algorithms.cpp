//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/as_bytes.hpp"
#include "vast/detail/coding.hpp"
#include "vast/hash/crc.hpp"
#include "vast/hash/hash.hpp"
#include "vast/hash/sha1.hpp"
#include "vast/hash/uhash.hpp"
#include "vast/hash/xxhash.hpp"

#define SUITE hashable
#include "vast/test/test.hpp"

using namespace vast;
using vast::detail::hexify;

namespace {

template <size_t N>
auto chop(const char (&xs)[N]) {
  return as_bytes(xs).template first<(N - 1)>();
}

} // namespace

TEST(crc32 oneshot) {
  CHECK_EQUAL(hash<crc32>('f'), 1993550816u);
  CHECK_EQUAL(hash<crc32>('o'), 252678980u);
}

TEST(crc32 incremental) {
  crc32 crc;
  crc.add(chop("foo"));
  CHECK_EQUAL(crc.finish(), 2356372769u);
}

TEST(crc32 hash_append) {
  crc32 foo;
  hash_append(foo, 'f');
  CHECK_EQUAL(foo.finish(), 1993550816u);
  hash_append(foo, 'o');
  CHECK_EQUAL(foo.finish(), 2943590935u);
  hash_append(foo, 'o');
  CHECK_EQUAL(foo.finish(), 2356372769u);
}

TEST(xxh64 oneshot with seed) {
  char forty_two[3] = "42"; // incl. NUL byte
  CHECK_EQUAL(xxh64::make(as_bytes(forty_two), 42), 7873697032674743835ul);
}

TEST(xxh64 incremental) {
  xxh64 h;
  h.add(chop("foo"));
  CHECK_EQUAL(h.finish(), 3728699739546630719ul);
  h.add(chop("bar"));
  CHECK_EQUAL(h.finish(), 11721187498075204345ul);
  h.add(chop("baz"));
  CHECK_EQUAL(h.finish(), 6505385152087097371ul);
}

TEST(xxh64 zero bytes) {
  // Should not segfault or trigger assertions.
  auto bytes = std::span<const std::byte>{nullptr, size_t{0u}};
  xxh64 h;
  h.add(bytes);
}

TEST(sha1 validity) {
  std::array<char, 2> forty_two = {'4', '2'};
  auto digest = hash<sha1>(forty_two);
  auto bytes = as_bytes(digest);
  CHECK_EQUAL(hexify(bytes), "92cfceb39d57d914ed8b14d0e37643de0797ae56");
}

TEST(sha1 incremental) {
  sha1 sha;
  sha.add(chop("foo"));
  sha.add(chop("bar"));
  sha.add(chop("baz"));
  sha.add(chop("42"));
  auto digest = sha.finish();
  auto bytes = as_bytes(digest);
  CHECK_EQUAL(hexify(bytes), "4cbfb91f23be76f0836c3007c1b3c8d8c2eacdd1");
}
