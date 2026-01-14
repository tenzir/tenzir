//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/chunk.hpp"

#include "tenzir/detail/legacy_deserialize.hpp"
#include "tenzir/detail/serialize.hpp"
#include "tenzir/test/fixtures/filesystem.hpp"
#include "tenzir/test/test.hpp"

#include <cstddef>
#include <span>
#include <string_view>

using namespace tenzir;

TEST("deleter") {
  char buf[100] = {};
  auto i = 42;
  MESSAGE("owning chunk");
  auto deleter = [&]() noexcept {
    i = 0;
  };
  auto x = chunk::make(buf, sizeof(buf), std::move(deleter));
  CHECK_EQUAL(i, 42);
  x = nullptr;
  CHECK_EQUAL(i, 0);
  i = 42;
}

TEST("deletion_step") {
  char buf[100] = {};
  auto i = 0;
  MESSAGE("owning chunk");
  auto x = chunk::copy(buf);
  x->add_deletion_step([&]() noexcept {
    i = 42;
  });
  auto y = x->slice(1);
  auto z = y->slice(2);
  CHECK_EQUAL(i, 0);
  x = nullptr;
  CHECK_EQUAL(i, 0);
  y = nullptr;
  CHECK_EQUAL(i, 0);
  z = nullptr;
  CHECK_EQUAL(i, 42);
}

TEST("chunk access") {
  auto xs = std::vector<char>{'f', 'o', 'o'};
  auto chk = chunk::make(std::move(xs));
  REQUIRE_NOT_EQUAL(chk, nullptr);
  CHECK_EQUAL(chk->size(), 3u);
  CHECK_EQUAL(*chk->begin(), static_cast<std::byte>('f'));
}

TEST("slicing") {
  std::array<char, 100> buf = {};
  auto x = chunk::copy(buf);
  auto y = x->slice(50);
  auto z = y->slice(40, 5);
  CHECK_EQUAL(y->size(), 50u);
  CHECK_EQUAL(z->size(), 5u);
}

TEST("compression") {
  // We assemble a large test string with many repetitions for compression
  // tests.
  const auto piece = std::string_view{"foobarbaz"};
  auto str = std::string{};
  str.reserve(piece.size() * 1000);
  for (auto i = 0; i < 1000; ++i) {
    str += piece;
  }
  const auto original = chunk::make(std::move(str));
  const auto compressed = unbox(chunk::compress(as_bytes(original)));
  CHECK_LESS(compressed->size(), original->size());
  const auto decompressed
    = unbox(chunk::decompress(as_bytes(compressed), original->size()));
  CHECK_EQUAL(as_bytes(original), as_bytes(decompressed));
  const auto decompressed_oversized
    = chunk::decompress(as_bytes(compressed), original->size() + 1);
  CHECK_ERROR(decompressed_oversized);
  const auto decompressed_undersized
    = chunk::decompress(as_bytes(compressed), original->size() - 1);
  CHECK_ERROR(decompressed_undersized);
}

TEST("as_bytes") {
  std::string str = "foobarbaz";
  auto copy = str;
  auto bytes
    = std::span{reinterpret_cast<const std::byte*>(copy.data()), copy.size()};
  auto x = chunk::make(std::move(str));
  CHECK_EQUAL(bytes, as_bytes(x));
}

namespace {

struct fixture : public fixtures::filesystem {
  fixture() : fixtures::filesystem(TENZIR_PP_STRINGIFY(CAF_TEST_SUITE_NAME)) {
  }
};

} // namespace

WITH_FIXTURE(fixture) {
  TEST("read / write") {
    std::string str = "foobarbaz";
    auto x = chunk::make(std::move(str));
    const auto filename = directory / "chunk";
    auto err = write(filename, x);
    CHECK_EQUAL(err, caf::none);
    chunk_ptr y;
    err = read(filename, y);
    CHECK_EQUAL(err, caf::none);
    CHECK_EQUAL(as_bytes(x), as_bytes(y));
  }
}
