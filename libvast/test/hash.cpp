//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/concept/hashable/crc.hpp"
#include "vast/concept/hashable/sha1.hpp"
#include "vast/concept/hashable/uhash.hpp"
#include "vast/concept/hashable/xxhash.hpp"
#include "vast/detail/coding.hpp"

#include <iomanip>

#define SUITE hash
#include "vast/test/test.hpp"

using namespace vast;
using vast::detail::hexify;

namespace {

struct foo {
  int a = 42;
  int b = 1337;
};

template <class Inspector>
auto inspect(Inspector& f, foo& x) {
  return f(x.a, x.b);
}

} // namespace

TEST(hashing an inspectable type) {
  using hasher = xxhash32;
  // Manual hashing two values...
  auto a = 42;
  auto b = 1337;
  hasher h;
  hash_append(h, a);
  hash_append(h, b);
  auto manual_digest = static_cast<hasher::result_type>(h);
  // ...and hashing them through the inspection API...
  auto digest = uhash<hasher>{}(foo{});
  // ...must yield the same value.
  CHECK_EQUAL(manual_digest, digest);
}

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

TEST(sha1) {
  // one-shot
  std::array<char, 2> fortytwo = {'4', '2'};
  auto digest = uhash<sha1>{}(fortytwo);
  auto bytes = as_bytes(span{digest.data(), digest.size()});
  CHECK_EQUAL(hexify(bytes), "92cfceb39d57d914ed8b14d0e37643de0797ae56");
  // incremental
  sha1 sha;
  sha("foo", 3);
  sha("bar", 3);
  sha("baz", 3);
  sha("42", 2);
  digest = static_cast<sha1::result_type>(sha);
  CHECK_EQUAL(hexify(bytes), "4cbfb91f23be76f0836c3007c1b3c8d8c2eacdd1");
}
