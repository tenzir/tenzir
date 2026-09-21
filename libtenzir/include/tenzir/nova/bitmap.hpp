//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/assert.hpp"
#include "tenzir/nova/shared_owner.hpp"
#include "tenzir/nova/storage_fwd.hpp"
#include "tenzir/option.hpp"

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <span>
#include <utility>

namespace tenzir::nova::storage {

class BitMap {
public:
  using ViewType = bool;
  /// The unit of storage. Bits are packed least-significant-first, and bits
  /// beyond `length()` in the final word are always zero.
  using Word = __uint128_t;
  static constexpr auto word_bits = Index{std::numeric_limits<Word>::digits};
  static_assert(std::has_single_bit(static_cast<unsigned>(word_bits)));
  /// `i >> word_shift` is the word index and `i & bit_mask` the bit index of
  /// bit `i`. Spelled out as shifts because `std::div` on `Index` is an
  /// out-of-line libc call, which dominated row loops in profiles.
  static constexpr auto word_shift
    = Index{std::countr_zero(static_cast<unsigned>(word_bits))};
  static constexpr auto bit_mask = word_bits - Index{1};

  inline BitMap() = delete;
  inline BitMap(Index length, DataOwner<Word[]> data,
                Option<Index> true_count = None{});
  inline BitMap(Index length, bool value);

  inline auto as_unique() const& -> BitMap;
  inline auto as_unique() && -> BitMap;
  inline auto length() const noexcept -> Index;
  inline auto make_inverted() const& noexcept -> BitMap;
  inline auto make_inverted() && noexcept -> BitMap;
  /// Returns `*this & ~other` without materializing the inverted `other`.
  inline auto and_not(const BitMap& other) const& -> BitMap;
  inline auto and_not(const BitMap& other) && -> BitMap;
  inline auto any() const -> bool;
  inline auto true_count() const -> Index;
  inline auto get(Index i) const -> bool;
  inline auto as_constant() const noexcept -> Option<bool>;
  inline auto data() const noexcept -> std::span<Word const>;

  class Builder {
  public:
    inline auto emplace_back(bool value) -> void;
    /// Appends `count` copies of `value`, working a word at a time.
    inline auto append_n(bool value, Index count) -> void;
    /// Removes the last bit and returns it.
    inline auto pop_back() -> bool;
    inline auto size() const -> Index;
    inline auto finish() -> BitMap;

  private:
    /// Materializes the `length_` implicit constant bits accumulated so far
    /// into `data_builder`.
    inline auto materialize_implicit_bits() -> void;

    DataOwner<Word[]>::Builder data_builder;
    Index length_ = 0;
    Index true_count_ = 0;
  };

  class Mutable {
  public:
    explicit inline Mutable(Index length);
    /// Takes over an existing bitmap for in-place modification, reusing its
    /// buffer if it is exclusively owned and materializing one otherwise.
    explicit inline Mutable(BitMap bitmap);
    inline Mutable(const Mutable&) = delete;
    inline auto operator=(const Mutable&) -> Mutable& = delete;
    inline Mutable(Mutable&&) = default;
    inline auto operator=(Mutable&&) -> Mutable& = default;
    inline ~Mutable() = default;

    inline auto length() const noexcept -> Index;
    inline auto true_count() const noexcept -> Index;
    inline auto get(Index i) const -> bool;
    inline auto set(Index i, bool value) -> void;
    inline auto copy_from(BitMap const& other) -> void;
    inline auto finish() && -> BitMap;
    inline auto operator|=(const BitMap& other) -> Mutable&;

  private:
    inline auto word_length() const noexcept -> Index;

    DataOwner<Word[]> data_;
    Index length_ = 0;
    Index true_count_ = 0;
  };

private:
  inline auto word_length() const noexcept -> Index;
  inline auto has_storage() const noexcept -> bool;
  inline auto all_true() const noexcept -> bool;
  static inline auto popcount(const DataOwner<Word[]>& data) -> Index;
  /// The mask of the bits that are in use in the final word, or `~Word{0}` if
  /// `length` ends exactly on a word boundary.
  static inline auto trailing_mask(Index length) -> Word;

  /// Applies `operation` word-wise to the first `length` bits of `a` and `b`,
  /// storing the result into `dst`, and returns the number of set bits in the
  /// result. `dst` may alias `a` or `b`. Bits past `length` in the final word
  /// are left zero in `dst` and excluded from the count.
  template <typename Operation>
  static inline auto combine_words(Word* dst, const Word* a, const Word* b,
                                   Index length, Operation operation) -> Index;

  /// Combines two equally long bitmaps that both have storage. Both operands
  /// are taken by value on purpose: `as_unique()` can then reuse an exclusively
  /// owned left-hand buffer and copies it otherwise.
  template <typename Operation>
  static inline auto combine(BitMap lhs, BitMap rhs, Operation operation)
    -> BitMap;

  DataOwner<Word[]> data_;
  Index length_ = 0;
  Index true_count_ = 0;

  friend inline auto operator&(const BitMap& lhs, const BitMap& rhs) -> BitMap {
    if (not lhs.has_storage() and not rhs.has_storage()) {
      return BitMap{std::max(lhs.length(), rhs.length()),
                    lhs.all_true() and rhs.all_true()};
    }
    if (not lhs.has_storage()) {
      return lhs.all_true() ? rhs : BitMap{rhs.length(), false};
    }
    if (not rhs.has_storage()) {
      return rhs.all_true() ? lhs : BitMap{lhs.length(), false};
    }
    return combine(lhs, rhs, std::bit_and<Word>{});
  }

  friend inline auto operator&(BitMap&& lhs, const BitMap& rhs) -> BitMap {
    if (not lhs.has_storage()) {
      return lhs.all_true() ? rhs : BitMap{rhs.length(), false};
    }
    if (not rhs.has_storage()) {
      return rhs.all_true() ? std::move(lhs) : BitMap{lhs.length_, false};
    }
    return combine(std::move(lhs), rhs, std::bit_and<Word>{});
  }

  friend inline auto operator&(const BitMap& lhs, BitMap&& rhs) -> BitMap {
    return std::move(rhs) & lhs;
  }

  friend inline auto operator&(BitMap&& lhs, BitMap&& rhs) -> BitMap {
    return std::move(lhs) & static_cast<BitMap const&>(rhs);
  }

  friend inline auto operator|(const BitMap& lhs, const BitMap& rhs) -> BitMap {
    if (not lhs.has_storage() and not rhs.has_storage()) {
      return BitMap{std::max(lhs.length(), rhs.length()),
                    lhs.all_true() or rhs.all_true()};
    }
    if (not lhs.has_storage()) {
      return lhs.all_true() ? BitMap{rhs.length(), true} : rhs;
    }
    if (not rhs.has_storage()) {
      return rhs.all_true() ? BitMap{lhs.length(), true} : lhs;
    }
    return combine(lhs, rhs, std::bit_or<Word>{});
  }

  friend inline auto operator|(BitMap&& lhs, const BitMap& rhs) -> BitMap {
    if (not lhs.has_storage()) {
      return lhs.all_true() ? BitMap{rhs.length(), true} : rhs;
    }
    if (not rhs.has_storage()) {
      return rhs.all_true() ? BitMap{lhs.length_, true} : std::move(lhs);
    }
    return combine(std::move(lhs), rhs, std::bit_or<Word>{});
  }

  friend inline auto operator|(const BitMap& lhs, BitMap&& rhs) -> BitMap {
    return std::move(rhs) | lhs;
  }

  friend inline auto operator|(BitMap&& lhs, BitMap&& rhs) -> BitMap {
    return std::move(lhs) | static_cast<BitMap const&>(rhs);
  }

  friend inline auto operator^(const BitMap& lhs, const BitMap& rhs) -> BitMap {
    if (not lhs.has_storage() and not rhs.has_storage()) {
      return BitMap{std::max(lhs.length(), rhs.length()),
                    lhs.all_true() != rhs.all_true()};
    }
    if (not lhs.has_storage()) {
      return lhs.all_true() ? rhs.make_inverted() : rhs;
    }
    if (not rhs.has_storage()) {
      return rhs.all_true() ? lhs.make_inverted() : lhs;
    }
    return combine(lhs, rhs, std::bit_xor<Word>{});
  }

  friend inline auto operator^(BitMap&& lhs, const BitMap& rhs) -> BitMap {
    if (not lhs.has_storage()) {
      return lhs.all_true() ? rhs.make_inverted() : rhs;
    }
    if (not rhs.has_storage()) {
      return rhs.all_true() ? std::move(lhs).make_inverted() : std::move(lhs);
    }
    return combine(std::move(lhs), rhs, std::bit_xor<Word>{});
  }

  friend inline auto operator^(const BitMap& lhs, BitMap&& rhs) -> BitMap {
    return std::move(rhs) ^ lhs;
  }

  friend inline auto operator^(BitMap&& lhs, BitMap&& rhs) -> BitMap {
    return std::move(lhs) ^ static_cast<BitMap const&>(rhs);
  }
};

inline auto BitMap::popcount(const DataOwner<Word[]>& data) -> Index {
  auto count = Index{0};
  for (const auto& word : data) {
    count += std::popcount(word);
  }
  return count;
}

inline auto BitMap::trailing_mask(Index length) -> Word {
  const auto bits = length % word_bits;
  return bits == 0 ? ~Word{0} : (Word{1} << bits) - 1;
}

template <typename Operation>
inline auto BitMap::combine_words(Word* dst, const Word* a, const Word* b,
                                  Index length, Operation operation) -> Index {
  const auto words = (length + word_bits - Index{1}) / word_bits;
  auto count = Index{0};
  for (auto i = Index{0}; i < words; ++i) {
    dst[i] = operation(a[i], b[i]);
    count += std::popcount(dst[i]);
  }
  if (words > 0) {
    // Clear the padding bits of the final word and discount them again, so
    // that this establishes the zero-padding invariant instead of relying on
    // both operands already upholding it.
    const auto mask = trailing_mask(length);
    count -= std::popcount(dst[words - 1] & ~mask);
    dst[words - 1] &= mask;
  }
  return count;
}

template <typename Operation>
inline auto BitMap::combine(BitMap lhs, BitMap rhs, Operation operation)
  -> BitMap {
  TENZIR_ASSERT_EQ(lhs.length_, rhs.length_);
  TENZIR_ASSERT(lhs.has_storage());
  TENZIR_ASSERT(rhs.has_storage());
  const auto length = lhs.length_;
  lhs = std::move(lhs).as_unique();
  lhs.true_count_ = combine_words(lhs.data_.begin(), lhs.data_.begin(),
                                  rhs.data_.begin(), length, operation);
  return lhs;
}

inline BitMap::BitMap(Index length, DataOwner<Word[]> data,
                      Option<Index> true_count)
  : data_{std::move(data)},
    length_{length},
    true_count_{true_count ? *true_count : popcount(data_)} {
}

inline BitMap::BitMap(Index length, bool value)
  : length_{length}, true_count_{value ? length : 0} {
}

inline auto BitMap::as_unique() const& -> BitMap {
  if (not has_storage()) {
    return BitMap{length_, all_true()};
  }
  return BitMap{length_, data_.as_unique(), true_count_};
}

inline auto BitMap::as_unique() && -> BitMap {
  if (not has_storage()) {
    const auto value = all_true();
    return BitMap{std::exchange(length_, 0), value};
  }
  data_ = std::move(data_).as_unique();
  return std::move(*this);
}

inline auto BitMap::length() const noexcept -> Index {
  return length_;
}

inline auto BitMap::make_inverted() const& noexcept -> BitMap {
  if (not has_storage()) {
    return {length_, not all_true()};
  }
  const auto word_count = word_length();
  auto result = DataOwner<Word[]>::make_value(word_count, Word{0});
  for (auto i = Index{0}; i < word_count; ++i) {
    result[i] = ~data_[i];
  }
  result[word_count - 1] &= trailing_mask(length_);
  return {length_, std::move(result), length_ - true_count_};
}

inline auto BitMap::make_inverted() && noexcept -> BitMap {
  if (not has_storage()) {
    const auto value = not all_true();
    return {std::exchange(length_, 0), value};
  }
  data_ = std::move(data_).as_unique();
  for (auto& word : data_) {
    word = ~word;
  }
  data_.back() &= trailing_mask(length_);
  true_count_ = length_ - true_count_;
  return std::move(*this);
}

inline auto BitMap::and_not(const BitMap& other) const& -> BitMap {
  if (not other.has_storage()) {
    return other.all_true() ? BitMap{length_, false} : *this;
  }
  if (not has_storage()) {
    return all_true() ? other.make_inverted() : *this;
  }
  return combine(*this, other, [](Word lhs, Word rhs) -> Word {
    return lhs & ~rhs;
  });
}

inline auto BitMap::and_not(const BitMap& other) && -> BitMap {
  if (not other.has_storage()) {
    return other.all_true() ? BitMap{length_, false} : std::move(*this);
  }
  if (not has_storage()) {
    return all_true() ? other.make_inverted() : std::move(*this);
  }
  return combine(std::move(*this), other, [](Word lhs, Word rhs) -> Word {
    return lhs & ~rhs;
  });
}

inline auto BitMap::any() const -> bool {
  return true_count_ > 0;
}

inline auto BitMap::true_count() const -> Index {
  return true_count_;
}

inline auto BitMap::get(Index i) const -> bool {
  if (all_true()) {
    return true;
  }
  if (true_count_ == 0) {
    return false;
  }
  return (data_.begin()[i >> word_shift] & (Word{1} << (i & bit_mask))) != 0;
}

inline auto BitMap::as_constant() const noexcept -> Option<bool> {
  if (true_count_ == length_) {
    return true;
  }
  if (true_count_ == 0) {
    return false;
  }
  return None{};
}

inline auto BitMap::data() const noexcept -> std::span<Word const> {
  return {data_.begin(), static_cast<std::size_t>(data_.length())};
}

inline auto BitMap::Builder::emplace_back(bool value) -> void {
  if (length_ == 0) {
    ++length_;
    true_count_ += Index{value};
    return;
  }
  if (data_builder.size() == 0 and value == (true_count_ == length_)) {
    ++length_;
    true_count_ += Index{value};
    return;
  }
  const auto bit_index = length_ & bit_mask;
  if (data_builder.size() == 0) {
    materialize_implicit_bits();
  }
  if (bit_index == 0) {
    data_builder.emplace_back(Word{0});
  }
  if (value) {
    data_builder.back() |= Word{1} << bit_index;
    ++true_count_;
  }
  ++length_;
}

inline auto BitMap::Builder::materialize_implicit_bits() -> void {
  TENZIR_ASSERT_EQ(data_builder.size(), 0);
  const auto value = true_count_ == length_;
  // Whole words of `value`, then a partial word for the remainder.
  const auto word_count = length_ >> word_shift;
  const auto bit_count = length_ & bit_mask;
  data_builder.append_n(word_count, value ? ~Word{0} : Word{0});
  if (bit_count > 0) {
    data_builder.emplace_back(value ? (Word{1} << bit_count) - 1 : Word{0});
  }
}

inline auto BitMap::Builder::append_n(bool value, Index count) -> void {
  TENZIR_ASSERT_LEQ(0, count);
  if (count == 0) {
    return;
  }
  const auto added_true = value ? count : Index{0};
  if (length_ == 0) {
    length_ = count;
    true_count_ = added_true;
    return;
  }
  if (data_builder.size() == 0 and value == (true_count_ == length_)) {
    length_ += count;
    true_count_ += added_true;
    return;
  }
  if (data_builder.size() == 0) {
    materialize_implicit_bits();
  }
  true_count_ += added_true;
  // Top up the partially filled final word first.
  if (const auto bit_index = length_ & bit_mask; bit_index != 0) {
    const auto room = word_bits - bit_index;
    const auto n = std::min(room, count);
    if (value) {
      // `n < word_bits` here, so the shift cannot overflow.
      data_builder.back() |= ((Word{1} << n) - 1) << bit_index;
    }
    length_ += n;
    count -= n;
  }
  // Then whole words, then the remainder.
  const auto whole_words = count >> word_shift;
  data_builder.append_n(whole_words, value ? ~Word{0} : Word{0});
  length_ += whole_words << word_shift;
  count &= bit_mask;
  if (count > 0) {
    data_builder.emplace_back(value ? (Word{1} << count) - 1 : Word{0});
    length_ += count;
  }
}

inline auto BitMap::Builder::pop_back() -> bool {
  TENZIR_ASSERT_GT(length_, 0);
  if (data_builder.size() == 0) {
    const auto value = true_count_ == length_;
    --length_;
    true_count_ -= Index{value};
    return value;
  }
  --length_;
  const auto bit_index = length_ & bit_mask;
  auto& word = data_builder.back();
  const auto value = ((word >> bit_index) & Word{1}) != 0;
  if (value) {
    word &= ~(Word{1} << bit_index);
    --true_count_;
  }
  if (bit_index == 0) {
    data_builder.pop_back();
  }
  return value;
}

inline auto BitMap::Builder::size() const -> Index {
  return length_;
}

inline auto BitMap::Builder::finish() -> BitMap {
  if (data_builder.size() == 0) {
    return BitMap{length_, true_count_ == length_};
  }
  return BitMap{length_, data_builder.finish(), true_count_};
}

inline BitMap::Mutable::Mutable(Index length)
  : data_{DataOwner<Word[]>::make_value(
      (length + word_bits - Index{1}) / word_bits, Word{0})},
    length_{length} {
}

inline BitMap::Mutable::Mutable(BitMap bitmap)
  : length_{bitmap.length_}, true_count_{bitmap.true_count_} {
  if (bitmap.has_storage()) {
    data_ = std::move(bitmap.data_).as_unique();
    return;
  }
  data_ = DataOwner<Word[]>::make_value(word_length(),
                                        bitmap.all_true() ? ~Word{0} : Word{0});
  if (bitmap.all_true() and word_length() > 0) {
    data_.back() &= trailing_mask(length_);
  }
}

inline auto BitMap::Mutable::length() const noexcept -> Index {
  return length_;
}

inline auto BitMap::Mutable::true_count() const noexcept -> Index {
  return true_count_;
}

inline auto BitMap::Mutable::get(Index i) const -> bool {
  TENZIR_ASSERT_LEQ_EXPENSIVE(0, i);
  TENZIR_ASSERT_LT_EXPENSIVE(i, length_);
  return (data_.begin()[i >> word_shift] & (Word{1} << (i & bit_mask))) != 0;
}

inline auto BitMap::Mutable::set(Index i, bool value) -> void {
  TENZIR_ASSERT_LEQ_EXPENSIVE(0, i);
  TENZIR_ASSERT_LT_EXPENSIVE(i, length_);
  auto& word = data_.begin()[i >> word_shift];
  const auto bit = Word{1} << (i & bit_mask);
  const auto was_set = (word & bit) != 0;
  if (value) {
    word |= bit;
  } else {
    word &= ~bit;
  }
  if (value and not was_set) {
    ++true_count_;
  } else if (not value and was_set) {
    --true_count_;
  }
}

inline auto BitMap::Mutable::copy_from(BitMap const& other) -> void {
  TENZIR_ASSERT_EQ(length_, other.length_);
  auto const byte_size = static_cast<std::size_t>(word_length()) * sizeof(Word);
  if (byte_size > 0) {
    if (other.has_storage()) {
      std::memcpy(data_.begin(), other.data_.begin(), byte_size);
    } else {
      std::memset(data_.begin(), other.all_true() ? 0xff : 0x00, byte_size);
      data_.back() &= trailing_mask(length_);
    }
  }
  true_count_ = other.true_count_;
}

inline auto BitMap::Mutable::finish() && -> BitMap {
  return BitMap{length_, std::move(data_), true_count_};
}

inline auto BitMap::Mutable::operator|=(const BitMap& other) -> Mutable& {
  TENZIR_ASSERT_EQ(length_, other.length_);
  const auto word_count = word_length();
  if (not other.has_storage()) {
    if (other.all_true()) {
      for (auto i = Index{0}; i < word_count; ++i) {
        data_.begin()[i] = ~Word{0};
      }
      if (word_count > 0) {
        // Keep the padding bits of the final word zero.
        data_.begin()[word_count - 1] &= trailing_mask(length_);
      }
      true_count_ = length_;
    }
    return *this;
  }
  true_count_ = combine_words(data_.begin(), data_.begin(), other.data_.begin(),
                              length_, std::bit_or<Word>{});
  return *this;
}

inline auto BitMap::Mutable::word_length() const noexcept -> Index {
  return (length_ + word_bits - Index{1}) / word_bits;
}

inline auto BitMap::word_length() const noexcept -> Index {
  return (length_ + word_bits - Index{1}) / word_bits;
}

inline auto BitMap::has_storage() const noexcept -> bool {
  return static_cast<bool>(data_);
}

inline auto BitMap::all_true() const noexcept -> bool {
  return true_count_ == length_;
}

static_assert(storage<BitMap>);

} // namespace tenzir::nova::storage
