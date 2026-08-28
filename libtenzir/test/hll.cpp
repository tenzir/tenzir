//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/fbs/aggregation.hpp"
#include "tenzir/flatbuffer.hpp"
#include "tenzir/hash/hash.hpp"
#include "tenzir/hash/xxhash.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/view3.hpp"

#include <algorithm>
#include <bit>
#include <cstdint>
#include <string_view>
#include <utility>
#include <vector>

using namespace tenzir;

namespace {

auto contract_hash(data value) -> uint64_t {
  auto builder = series_builder{};
  builder.data(std::move(value));
  const auto result = builder.finish_assert_one_array();
  return hash<xxh3_64>(view_at(*result.array, 0));
}

auto index_and_rank(uint64_t digest, uint8_t precision)
  -> std::pair<uint64_t, uint8_t> {
  const auto index = digest >> (64 - precision);
  const auto rank = static_cast<uint8_t>(
    std::min(std::countl_zero(digest << precision) + 1, 65 - precision));
  return {index, rank};
}

TEST("HLL v1 hash contract vectors") {
  // The v1 contract is seed-zero xxHash3-64 over data_view3's type-tagged
  // encoding. In particular, numerically equal values of different types are
  // intentionally distinct.
  CHECK_EQUAL(contract_hash(int64_t{42}), 0xa21e1a867f970becULL);
  CHECK_EQUAL(contract_hash(uint64_t{42}), 0x1624339df29c1006ULL);
  CHECK_EQUAL(contract_hash(42.0), 0x2b5f288f6ee06c37ULL);
  CHECK_EQUAL(contract_hash(std::string{"hello"}), 0x66d5f1ec7c7adf1fULL);
  CHECK_EQUAL(
    contract_hash(list{int64_t{1}, std::string{"x"}, list{true, uint64_t{2}}}),
    0x75f369355a7f48e5ULL);
  CHECK_EQUAL(contract_hash(record{{"a", int64_t{1}},
                                   {"b", list{std::string{"x"}, false}}}),
              0x3803197e97d9aba5ULL);
  // Record field order participates in the structural encoding.
  CHECK_EQUAL(contract_hash(record{{"b", list{std::string{"x"}, false}},
                                   {"a", int64_t{1}}}),
              0xd907570ea8eb54faULL);
}

TEST("HLL v1 register mapping vectors") {
  CHECK_EQUAL(index_and_rank(0xa21e1a867f970becULL, 4),
              std::pair(uint64_t{10}, uint8_t{3}));
  CHECK_EQUAL(index_and_rank(0x1624339df29c1006ULL, 4),
              std::pair(uint64_t{1}, uint8_t{2}));
  CHECK_EQUAL(index_and_rank(0x75f369355a7f48e5ULL, 4),
              std::pair(uint64_t{7}, uint8_t{2}));
  // The all-zero suffix is capped at the maximum representable rank for the
  // precision rather than counting index bits.
  CHECK_EQUAL(index_and_rank(0, 4), std::pair(uint64_t{0}, uint8_t{61}));
  CHECK_EQUAL(index_and_rank(0, 18), std::pair(uint64_t{0}, uint8_t{47}));
}

TEST("HLL aggregation persistence schema round-trip") {
  auto input = std::vector<uint8_t>(16, 0);
  input[3] = 4;
  input[10] = 2;
  auto fbb = flatbuffers::FlatBufferBuilder{};
  const auto registers = fbb.CreateVector(input);
  const auto hash_name = fbb.CreateString("tenzir.data.xxh3_64.v1");
  const auto state = fbs::aggregation::CreateHll(
    fbb, registers, uint8_t{4}, hash_name, uint64_t{7}, uint64_t{2});
  fbb.Finish(state);
  const auto fb
    = flatbuffer<fbs::aggregation::Hll>::make(chunk::make(fbb.Release()));
  REQUIRE(fb);
  REQUIRE((*fb)->registers());
  REQUIRE((*fb)->hash());
  CHECK_EQUAL((*fb)->precision(), uint8_t{4});
  CHECK_EQUAL((*fb)->count(), uint64_t{7});
  CHECK_EQUAL((*fb)->null_count(), uint64_t{2});
  CHECK_EQUAL((*fb)->hash()->string_view(),
              std::string_view{"tenzir.data.xxh3_64.v1"});
  CHECK(std::ranges::equal(*(*fb)->registers(), input));
}

} // namespace
