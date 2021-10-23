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

TEST(crc32 oneshot) {
  CHECK_EQUAL(hash<crc32>('f'), 1993550816u);
  CHECK_EQUAL(hash<crc32>('o'), 252678980u);
}

TEST(crc32 incremental) {
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

TEST(xxh64 oneshot with seed) {
  auto forty_two = "42"; // incl. NUL byte
  CHECK_EQUAL(xxh64::make(forty_two, 3, 42), 7873697032674743835ul);
}

TEST(xxh64 incremental) {
  xxh64 h;
  h("foo", 3);
  CHECK_EQUAL(static_cast<uint64_t>(h), 3728699739546630719ul);
  h("bar", 3);
  CHECK_EQUAL(static_cast<uint64_t>(h), 11721187498075204345ul);
  h("baz", 3);
  CHECK_EQUAL(static_cast<uint64_t>(h), 6505385152087097371ul);
}

TEST(xxh64 zero bytes) {
  // Should not segfault or trigger assertions.
  xxh64 h;
  h(nullptr, 0);
}

TEST(sha1 validity) {
  std::array<char, 2> forty_two = {'4', '2'};
  auto digest = hash<sha1>(forty_two);
  auto bytes = as_bytes(digest);
  CHECK_EQUAL(hexify(bytes), "92cfceb39d57d914ed8b14d0e37643de0797ae56");
}

TEST(sha1 incremental) {
  sha1 sha;
  sha("foo", 3);
  sha("bar", 3);
  sha("baz", 3);
  sha("42", 2);
  auto digest = static_cast<sha1::result_type>(sha);
  auto bytes = as_bytes(digest);
  CHECK_EQUAL(hexify(bytes), "4cbfb91f23be76f0836c3007c1b3c8d8c2eacdd1");
}
