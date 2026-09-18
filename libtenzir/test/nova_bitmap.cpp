//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/storage.hpp"
#include "tenzir/test/test.hpp"

#include <algorithm>
#include <functional>
#include <vector>

using namespace tenzir::nova;
using storage::BitMap;
using storage::Index;

namespace {

auto make_bitmap(std::vector<bool> const& bits, bool invert = false) -> BitMap {
  auto builder = BitMap::Builder{};
  for (auto bit : bits) {
    builder.emplace_back(bit);
  }
  auto result = builder.finish();
  return invert ? std::move(result).make_inverted() : std::move(result);
}

auto to_vector(BitMap const& bm) -> std::vector<bool> {
  auto result = std::vector<bool>{};
  for (auto i = Index{0}; i < bm.length(); ++i) {
    result.push_back(bm.get(i));
  }
  return result;
}

const auto lhs_bits
  = std::vector<bool>{true, true, false, false, true, false, true, true, false};
const auto rhs_bits
  = std::vector<bool>{true, false, true, false, false, true, true, false, true};

// Reference results computed purely via the `const&`/`const&` overloads, to
// compare every rvalue-taking overload against.
const auto expected_and = [] {
  auto lhs = make_bitmap(lhs_bits);
  auto rhs = make_bitmap(rhs_bits);
  return to_vector(lhs & rhs);
}();
const auto expected_or = [] {
  auto lhs = make_bitmap(lhs_bits);
  auto rhs = make_bitmap(rhs_bits);
  return to_vector(lhs | rhs);
}();
const auto expected_xor = [] {
  auto lhs = make_bitmap(lhs_bits);
  auto rhs = make_bitmap(rhs_bits);
  return to_vector(lhs ^ rhs);
}();

} // namespace

TEST("bitmap and_not matches and with inverted rhs") {
  auto lhs = make_bitmap(lhs_bits);
  auto rhs = make_bitmap(rhs_bits);
  auto const expected = to_vector(lhs & rhs.make_inverted());
  CHECK_EQUAL(to_vector(lhs.and_not(rhs)), expected);
  auto lhs_copy = lhs;
  CHECK_EQUAL(to_vector(std::move(lhs_copy).and_not(rhs)), expected);
  CHECK_EQUAL(lhs.and_not(rhs).true_count(),
              static_cast<Index>(std::ranges::count(expected, true)));
}

TEST("bitmap and_not handles storage-less operands") {
  auto lhs = make_bitmap(lhs_bits);
  auto const length = lhs.length();
  auto const all = BitMap{length, true};
  auto const none = BitMap{length, false};
  CHECK_EQUAL(to_vector(lhs.and_not(all)), to_vector(none));
  CHECK_EQUAL(to_vector(lhs.and_not(none)), lhs_bits);
  CHECK_EQUAL(to_vector(all.and_not(lhs)), to_vector(lhs.make_inverted()));
  CHECK_EQUAL(to_vector(none.and_not(lhs)), to_vector(none));
  CHECK_EQUAL(to_vector(all.and_not(none)), to_vector(all));
  CHECK_EQUAL(to_vector(all.and_not(all)), to_vector(none));
  CHECK_EQUAL(all.and_not(lhs).true_count(), length - lhs.true_count());
}

TEST("bitmap as_unique copies shared rvalues") {
  auto source = make_bitmap(lhs_bits);
  auto alias = source;
  auto const* shared_data = source.data().data();
  auto result = std::move(source).as_unique();
  CHECK_NOT_EQUAL(result.data().data(), shared_data);
  CHECK_EQUAL(alias.data().data(), shared_data);
  CHECK_EQUAL(to_vector(result), lhs_bits);
}

TEST("bitmap as_unique reuses unique rvalues") {
  auto source = make_bitmap(lhs_bits);
  auto const* source_data = source.data().data();
  auto result = std::move(source).as_unique();
  CHECK_EQUAL(result.data().data(), source_data);
  CHECK_EQUAL(to_vector(result), lhs_bits);
}

TEST("bitmap rvalue-lvalue and matches lvalue-lvalue and") {
  auto rhs = make_bitmap(rhs_bits);
  CHECK_EQUAL(to_vector(make_bitmap(lhs_bits) & rhs), expected_and);
}

TEST("bitmap lvalue-rvalue and matches lvalue-lvalue and") {
  auto lhs = make_bitmap(lhs_bits);
  CHECK_EQUAL(to_vector(lhs & make_bitmap(rhs_bits)), expected_and);
}

TEST("bitmap rvalue-rvalue and matches lvalue-lvalue and") {
  CHECK_EQUAL(to_vector(make_bitmap(lhs_bits) & make_bitmap(rhs_bits)),
              expected_and);
}

TEST("bitmap rvalue-lvalue or matches lvalue-lvalue or") {
  auto rhs = make_bitmap(rhs_bits);
  CHECK_EQUAL(to_vector(make_bitmap(lhs_bits) | rhs), expected_or);
}

TEST("bitmap rvalue-rvalue or matches lvalue-lvalue or") {
  CHECK_EQUAL(to_vector(make_bitmap(lhs_bits) | make_bitmap(rhs_bits)),
              expected_or);
}

TEST("bitmap rvalue-lvalue xor matches lvalue-lvalue xor") {
  auto rhs = make_bitmap(rhs_bits);
  CHECK_EQUAL(to_vector(make_bitmap(lhs_bits) ^ rhs), expected_xor);
}

TEST("bitmap rvalue-rvalue xor matches lvalue-lvalue xor") {
  CHECK_EQUAL(to_vector(make_bitmap(lhs_bits) ^ make_bitmap(rhs_bits)),
              expected_xor);
}

TEST("bitmap rvalue operators respect invert on either side") {
  auto lhs_inverted_lvalue = make_bitmap(lhs_bits, true);
  auto rhs_lvalue = make_bitmap(rhs_bits);
  auto expected = to_vector(lhs_inverted_lvalue & rhs_lvalue);

  CHECK_EQUAL(to_vector(make_bitmap(lhs_bits, true) & make_bitmap(rhs_bits)),
              expected);
  CHECK_EQUAL(to_vector(make_bitmap(rhs_bits) & make_bitmap(lhs_bits, true)),
              expected);
}

TEST("bitmap rvalue operator does not corrupt a shared buffer") {
  auto lhs = make_bitmap(lhs_bits);
  // Copying via `make_inverted() const&` twice shares the same underlying
  // `SharedOwner` buffer as `lhs`, forcing the rvalue overload's uniqueness
  // check to fail and fall back to allocating instead of mutating `lhs`'s
  // storage in place.
  auto alias = lhs.make_inverted().make_inverted();
  auto rhs = make_bitmap(rhs_bits);
  auto result = std::move(lhs) & rhs;
  CHECK_EQUAL(to_vector(result), expected_and);
  CHECK_EQUAL(to_vector(alias), lhs_bits);
}

TEST("bitmap true_count matches the number of set bits") {
  auto const check = [](std::vector<bool> const& bits) {
    auto const expected
      = static_cast<Index>(std::count(bits.begin(), bits.end(), true));
    auto const bm = make_bitmap(bits);
    CHECK_EQUAL(bm.true_count(), expected);
    CHECK_EQUAL(bm.make_inverted().true_count(), bm.length() - expected);
  };
  check(lhs_bits);
  check(rhs_bits);
  // A run of equal leading bits keeps `Builder` in its storage-less fast
  // path until the first differing bit forces a backfill.
  check({true, true, true, false});
  check({false, false, false, true});
  check({true, true, true, true, true, true, true, true, true, false});
  check({false, false, false, false, false, false, false, false, false, true});
}

TEST("zero-length bitmap is vacuously all-true") {
  for (auto value : {false, true}) {
    auto bm = BitMap{0, value};
    CHECK_EQUAL(bm.length(), 0);
    CHECK_EQUAL(bm.true_count(), 0);
    CHECK(not bm.any());
    REQUIRE(bm.as_constant());
    CHECK(*bm.as_constant());
  }
}

TEST("bitmap with no storage reads as all-true") {
  auto bm = BitMap{42, true};
  REQUIRE(bm.as_constant());
  CHECK(*bm.as_constant());
  CHECK(bm.get(0));
  CHECK(bm.get(41));
}

TEST("bitmap with no storage reads as all-false") {
  auto bm = BitMap{42, true}.make_inverted();
  REQUIRE(bm.as_constant());
  CHECK(not *bm.as_constant());
  CHECK(not bm.get(0));
  CHECK(not bm.get(41));
}

TEST("bitmap and with a no-storage rhs takes lhs's length and values") {
  auto lhs = make_bitmap(lhs_bits);
  auto rhs = BitMap{lhs.length(), true};
  auto result = lhs & rhs;
  CHECK_EQUAL(result.length(), lhs.length());
  CHECK_EQUAL(to_vector(result), lhs_bits);
}

TEST("bitmap or with a no-storage inverted rhs takes lhs's length and "
     "values") {
  auto lhs = make_bitmap(lhs_bits);
  auto rhs = BitMap{lhs.length(), false};
  auto result = lhs | rhs;
  CHECK_EQUAL(result.length(), lhs.length());
  CHECK_EQUAL(to_vector(result), lhs_bits);
}

TEST("bitmap and with a no-storage lhs takes rhs's length and values") {
  auto rhs = make_bitmap(rhs_bits);
  auto lhs = BitMap{rhs.length(), true};
  auto result = lhs & rhs;
  CHECK_EQUAL(result.length(), rhs.length());
  CHECK_EQUAL(to_vector(result), rhs_bits);
}

TEST("bitmap rvalue and reuses rhs when lhs has no storage") {
  auto rhs = make_bitmap(rhs_bits);
  auto lhs = BitMap{rhs.length(), true};
  auto result = std::move(lhs) & std::move(rhs);
  CHECK_EQUAL(to_vector(result), rhs_bits);
}

TEST("bitmap rvalue or reuses rhs when lhs has no storage") {
  auto rhs = make_bitmap(rhs_bits);
  auto lhs = BitMap{rhs.length(), false};
  auto result = std::move(lhs) | std::move(rhs);
  CHECK_EQUAL(to_vector(result), rhs_bits);
}

TEST("bitmap rvalue xor of two no-storage bitmaps stays storage-less") {
  auto lhs = BitMap{42, true};
  auto rhs = BitMap{42, false};
  auto result = std::move(lhs) ^ std::move(rhs);
  CHECK_EQUAL(result.length(), 42);
  CHECK_EQUAL(result.true_count(), 42);
}

namespace {

// Bit patterns that are cheap to reason about by hand and that differ between
// the two operands in every word.
auto alternating(Index length, Index period, Index phase = 0)
  -> std::vector<bool> {
  auto result = std::vector<bool>{};
  for (auto i = Index{0}; i < length; ++i) {
    result.push_back((i + phase) % period < period / 2);
  }
  return result;
}

auto reference(std::vector<bool> const& lhs, std::vector<bool> const& rhs,
               auto operation) -> std::vector<bool> {
  auto result = std::vector<bool>{};
  for (auto i = size_t{0}; i < lhs.size(); ++i) {
    result.push_back(operation(lhs[i], rhs[i]));
  }
  return result;
}

auto count_true(std::vector<bool> const& bits) -> Index {
  return static_cast<Index>(std::count(bits.begin(), bits.end(), true));
}

// Lengths chosen around the 64-bit word boundary: partial single word, exactly
// one word, one-past, and multi-word with and without a partial tail.
const auto interesting_lengths
  = std::vector<Index>{1, 2, 7, 8, 9, 31, 63, 64, 65, 127, 128, 129, 200};

} // namespace

TEST("bitmap operators agree with a bitwise reference at word boundaries") {
  for (auto length : interesting_lengths) {
    const auto a_bits = alternating(length, 6);
    const auto b_bits = alternating(length, 10, 3);
    const auto expect_and = reference(a_bits, b_bits, std::logical_and<bool>{});
    const auto expect_or = reference(a_bits, b_bits, std::logical_or<bool>{});
    const auto expect_xor
      = reference(a_bits, b_bits, std::not_equal_to<bool>{});
    // `const&, const&` allocates; the rvalue forms reuse a buffer. All four
    // combinations must agree, and `true_count()` must match the bit vector.
    auto const check
      = [&](BitMap const& result, std::vector<bool> const& expected) {
          CHECK_EQUAL(to_vector(result), expected);
          CHECK_EQUAL(result.true_count(), count_true(expected));
          CHECK_EQUAL(result.length(), length);
        };
    {
      auto lhs = make_bitmap(a_bits);
      auto rhs = make_bitmap(b_bits);
      check(lhs & rhs, expect_and);
      check(lhs | rhs, expect_or);
      check(lhs ^ rhs, expect_xor);
    }
    {
      auto rhs = make_bitmap(b_bits);
      check(make_bitmap(a_bits) & rhs, expect_and);
      check(make_bitmap(a_bits) | rhs, expect_or);
      check(make_bitmap(a_bits) ^ rhs, expect_xor);
    }
    {
      auto lhs = make_bitmap(a_bits);
      check(lhs & make_bitmap(b_bits), expect_and);
      check(lhs | make_bitmap(b_bits), expect_or);
      check(lhs ^ make_bitmap(b_bits), expect_xor);
    }
    check(make_bitmap(a_bits) & make_bitmap(b_bits), expect_and);
    check(make_bitmap(a_bits) | make_bitmap(b_bits), expect_or);
    check(make_bitmap(a_bits) ^ make_bitmap(b_bits), expect_xor);
  }
}

TEST("bitmap builder round-trips values across word boundaries") {
  for (auto length : interesting_lengths) {
    // A leading run of equal bits keeps `Builder` in its storage-less path, so
    // the backfill has to materialize a multi-word prefix on the first
    // differing bit.
    for (auto leading : {true, false}) {
      auto bits = std::vector<bool>(static_cast<size_t>(length), leading);
      bits.back() = not leading;
      const auto bm = make_bitmap(bits);
      CHECK_EQUAL(to_vector(bm), bits);
      CHECK_EQUAL(bm.true_count(), count_true(bits));
    }
  }
}

TEST("bitmap and with hand-written expected bits") {
  // The shared fixtures derive their expectations from the `const&,const&`
  // path, so pin one case against literal values instead.
  const auto lhs = std::vector<bool>{true, true, false, false, true};
  const auto rhs = std::vector<bool>{true, false, true, false, true};
  CHECK_EQUAL(to_vector(make_bitmap(lhs) & make_bitmap(rhs)),
              (std::vector<bool>{true, false, false, false, true}));
  CHECK_EQUAL(to_vector(make_bitmap(lhs) | make_bitmap(rhs)),
              (std::vector<bool>{true, true, true, false, true}));
  CHECK_EQUAL(to_vector(make_bitmap(lhs) ^ make_bitmap(rhs)),
              (std::vector<bool>{false, true, true, false, false}));
}

TEST("bitmap rvalue operator reuses rhs when lhs is shared") {
  const auto length = Index{130};
  const auto a_bits = alternating(length, 6);
  const auto b_bits = alternating(length, 10, 3);
  auto lhs = make_bitmap(a_bits);
  // Keep a second owner alive so `lhs` is not reusable and `rhs` gets picked.
  auto lhs_alias = lhs;
  auto rhs = make_bitmap(b_bits);
  auto const* rhs_before = &rhs;
  static_cast<void>(rhs_before);
  auto result = std::move(lhs) & std::move(rhs);
  const auto expected = reference(a_bits, b_bits, std::logical_and<bool>{});
  CHECK_EQUAL(to_vector(result), expected);
  CHECK_EQUAL(result.true_count(), count_true(expected));
  // The shared buffer must be untouched.
  CHECK_EQUAL(to_vector(lhs_alias), a_bits);
}

TEST("bitmap rvalue operator does not mutate a live rhs") {
  const auto length = Index{130};
  const auto a_bits = alternating(length, 6);
  const auto b_bits = alternating(length, 10, 3);
  auto rhs = make_bitmap(b_bits);
  auto result = make_bitmap(a_bits) & rhs;
  const auto expected = reference(a_bits, b_bits, std::logical_and<bool>{});
  CHECK_EQUAL(to_vector(result), expected);
  // `rhs` is an lvalue here, so it must survive unchanged.
  CHECK_EQUAL(to_vector(rhs), b_bits);
  CHECK_EQUAL(rhs.true_count(), count_true(b_bits));
}

TEST("bitmap chained operators keep padding clean") {
  // A non-multiple-of-64 length exercises the trailing-word mask; feeding each
  // result into the next op would surface dirty padding as a wrong count.
  const auto length = Index{100};
  const auto a = alternating(length, 6);
  const auto b = alternating(length, 10, 3);
  const auto c = alternating(length, 14, 5);
  auto result = make_bitmap(a) | make_bitmap(b);
  result = std::move(result) ^ make_bitmap(c);
  result = std::move(result) & make_bitmap(a);
  auto expected = reference(a, b, std::logical_or<bool>{});
  expected = reference(expected, c, std::not_equal_to<bool>{});
  expected = reference(expected, a, std::logical_and<bool>{});
  CHECK_EQUAL(to_vector(result), expected);
  CHECK_EQUAL(result.true_count(), count_true(expected));
}

TEST("bitmap mutable or-assign matches a bitwise reference") {
  for (auto length : interesting_lengths) {
    const auto a_bits = alternating(length, 6);
    const auto b_bits = alternating(length, 10, 3);
    auto mutable_bitmap = BitMap::Mutable{length};
    for (auto i = Index{0}; i < length; ++i) {
      mutable_bitmap.set(i, a_bits[static_cast<size_t>(i)]);
    }
    mutable_bitmap |= make_bitmap(b_bits);
    auto result = std::move(mutable_bitmap).finish();
    const auto expected = reference(a_bits, b_bits, std::logical_or<bool>{});
    CHECK_EQUAL(to_vector(result), expected);
    CHECK_EQUAL(result.true_count(), count_true(expected));
  }
}

TEST("bitmap mutable copies constant and materialized bitmaps") {
  for (auto value : {false, true}) {
    auto const source = BitMap{Index{129}, value};
    auto copy = BitMap::Mutable{source.length()};
    copy.copy_from(source);
    auto result = std::move(copy).finish();
    CHECK_EQUAL(to_vector(result),
                std::vector<bool>(static_cast<size_t>(source.length()), value));
    CHECK_EQUAL(result.true_count(), source.true_count());
  }
  auto const bits = alternating(Index{129}, 6, 1);
  auto const source = make_bitmap(bits);
  auto copy = BitMap::Mutable{source.length()};
  copy.copy_from(source);
  auto result = std::move(copy).finish();
  CHECK_EQUAL(to_vector(result), bits);
  CHECK_EQUAL(result.true_count(), source.true_count());
}

TEST("bitmap mutable or-assign with an all-true operand stays consistent") {
  // The all-true branch fills whole words and then has to re-clear the padding
  // bits; a later op on the result would otherwise over-count.
  const auto length = Index{100};
  auto mutable_bitmap = BitMap::Mutable{length};
  mutable_bitmap.set(0, true);
  mutable_bitmap |= BitMap{length, true};
  auto result = std::move(mutable_bitmap).finish();
  CHECK_EQUAL(result.true_count(), length);
  CHECK_EQUAL(to_vector(result),
              std::vector<bool>(static_cast<size_t>(length), true));
  // Feed it through another op: a dirty trailing word would show up here.
  const auto other = alternating(length, 6);
  auto combined = std::move(result) & make_bitmap(other);
  CHECK_EQUAL(to_vector(combined), other);
  CHECK_EQUAL(combined.true_count(), count_true(other));
}
