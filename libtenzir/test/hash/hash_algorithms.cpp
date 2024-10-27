//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/as_bytes.hpp"
#include "tenzir/detail/coding.hpp"
#include "tenzir/hash/crc.hpp"
#include "tenzir/hash/fnv.hpp"
#include "tenzir/hash/hash.hpp"
#include "tenzir/hash/sha1.hpp"
#include "tenzir/hash/sha2.hpp"
#include "tenzir/hash/uhash.hpp"
#include "tenzir/hash/xxhash.hpp"
#include "tenzir/test/test.hpp"

using namespace tenzir;
using tenzir::detail::hexify;

namespace {

template <size_t N>
auto chop(const char (&xs)[N]) {
  return as_bytes(xs).template first<(N - 1)>();
}

// A version of hash_append that doesn't add the size of the input.
template <class Hasher>
auto byte_hash(std::string_view bytes) {
  Hasher h;
  h(bytes.data(), bytes.size());
  return static_cast<typename Hasher::result_type>(h);
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

// FNV test values taken from the canonical reference over at
// http://www.isthe.com/chongo/src/fnv/test_fnv.c.

TEST(fnv1 - 32bit) {
  using hasher_type = fnv1<32>;
  auto h = byte_hash<hasher_type>;
  CHECK_EQUAL(h(""), hasher_type::offset_basis());
  CHECK_EQUAL(h(""), 0x811c9dc5UL);
  CHECK_EQUAL(h("foo"), 0x408f5e13UL);
  CHECK_EQUAL(h("foobar"), 0x31f0b262UL);
}

TEST(fnv1a - 32bit) {
  using hasher_type = fnv1a<32>;
  auto h = byte_hash<hasher_type>;
  CHECK_EQUAL(h(""), hasher_type::offset_basis());
  CHECK_EQUAL(h(""), 0x811c9dc5UL);
  CHECK_EQUAL(h("foo"), 0xa9f37ed7UL);
  CHECK_EQUAL(h("foobar"), 0xbf9cf968UL);
}

TEST(fnv1 - 64bit) {
  using hasher_type = fnv1<64>;
  auto h = byte_hash<hasher_type>;
  CHECK_EQUAL(h(""), hasher_type::offset_basis());
  CHECK_EQUAL(h(""), 0xcbf29ce484222325ULL);
  CHECK_EQUAL(h("foo"), 0xd8cbc7186ba13533ULL);
  CHECK_EQUAL(h("foobar"), 0x340d8765a4dda9c2ULL);
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
  const std::byte* ptr = nullptr;
  auto bytes = std::span<const std::byte, 0>{ptr, ptr};
  xxh64 h;
  // Should not segfault or trigger assertions.
  h.add(bytes);
}

TEST(sha1 validity) {
  std::array<char, 2> forty_two = {'4', '2'};
  auto digest = hexify(hash<sha1>(forty_two));
  CHECK_EQUAL(digest, "92cfceb39d57d914ed8b14d0e37643de0797ae56");
}

TEST(sha1 incremental) {
  sha1 sha;
  sha.add(chop("foo"));
  sha.add(chop("bar"));
  sha.add(chop("baz"));
  sha.add(chop("42"));
  auto digest = hexify(sha.finish());
  CHECK_EQUAL(digest, "4cbfb91f23be76f0836c3007c1b3c8d8c2eacdd1");
}

TEST(sha224 validity) {
  std::array<char, 3> foo = {'f', 'o', 'o'};
  auto digest = hexify(hash<sha224>(foo));
  CHECK_EQUAL(digest,
              "0808f64e60d58979fcb676c96ec938270dea42445aeefcd3a4e6f8db");
}

TEST(sha256 validity) {
  std::array<char, 3> foo = {'f', 'o', 'o'};
  auto digest = hexify(hash<sha256>(foo));
  CHECK_EQUAL(digest, "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e88"
                      "6266e7ae");
}

TEST(sha384 validity) {
  std::array<char, 3> foo = {'f', 'o', 'o'};
  auto digest = hexify(hash<sha384>(foo));
  CHECK_EQUAL(digest, "98c11ffdfdd540676b1a137cb1a22b2a70350c9a44171d6b1180c6be"
                      "5cbb2ee3f79d532c8a1dd9ef2e8e08e752a3babb");
}

TEST(sha512 validity) {
  std::array<char, 3> foo = {'f', 'o', 'o'};
  auto digest = hexify(hash<sha512>(foo));
  CHECK_EQUAL(digest, "f7fbba6e0636f890e56fbbf3283e524c6fa3204ae298382d624741d0"
                      "dc6638326e282c41be5e4254d8820772c5518a2c5a8c0c7f7eda1959"
                      "4a7eb539453e1ed7");
}
