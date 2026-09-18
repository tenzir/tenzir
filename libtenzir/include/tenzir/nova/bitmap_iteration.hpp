//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/nova/bitmap.hpp"
#include "tenzir/option.hpp"
#include "tenzir/ref.hpp"

#include <bit>
#include <concepts>
#include <functional>
#include <iterator>

namespace tenzir::nova::storage {

/// A range over a bitmap that yields the current index for set bits and `None`
/// for unset bits.
///
/// The referenced bitmap must outlive the range. Iteration advances through the
/// bitmap word by word and does not use division or modulo.
class BitMapIteration {
public:
  class Sentinel;

  class Iterator {
  public:
    using value_type = Option<Index>;
    using difference_type = Index;
    using iterator_concept = std::input_iterator_tag;
    using iterator_category = std::input_iterator_tag;

    Iterator() = default;

    auto operator*() const noexcept -> value_type {
      auto const is_set = word_
                            ? (*word_ & (BitMap::Word{1} << bit_index_)) != 0
                            : constant_value_;
      return is_set ? value_type{index_} : value_type{None{}};
    }

    auto operator++() noexcept -> Iterator& {
      ++index_;
      ++bit_index_;
      if (bit_index_ == BitMap::word_bits) {
        bit_index_ = 0;
        if (word_) {
          ++word_;
        }
      }
      return *this;
    }

    auto operator++(int) noexcept -> void {
      ++*this;
    }

    friend auto operator==(Iterator const& lhs, Sentinel const& rhs) noexcept
      -> bool;

  private:
    friend class BitMapIteration;

    explicit Iterator(BitMap::Word const* word, bool constant_value) noexcept
      : word_{word}, constant_value_{constant_value} {
    }

    BitMap::Word const* word_ = nullptr;
    Index index_ = 0;
    Index bit_index_ = 0;
    bool constant_value_ = false;
  };

  class Sentinel {
  public:
    Sentinel() = default;

  private:
    friend class BitMapIteration;
    friend auto operator==(Iterator const& lhs, Sentinel const& rhs) noexcept
      -> bool;

    explicit Sentinel(Index length) noexcept : length_{length} {
    }

    Index length_ = 0;
  };

  explicit BitMapIteration(BitMap const& bitmap) noexcept : bitmap_{bitmap} {
  }

  auto begin() const noexcept -> Iterator {
    auto const data = bitmap_->data();
    return Iterator{data.empty() ? nullptr : data.data(),
                    bitmap_->as_constant().value_or(false)};
  }

  auto end() const noexcept -> Sentinel {
    return Sentinel{bitmap_->length()};
  }

private:
  Ref<BitMap const> bitmap_;
};

inline auto operator==(BitMapIteration::Iterator const& lhs,
                       BitMapIteration::Sentinel const& rhs) noexcept -> bool {
  return lhs.index_ == rhs.length_;
}

inline auto bitmap_iteration(BitMap const& bitmap) noexcept -> BitMapIteration {
  return BitMapIteration{bitmap};
}

auto bitmap_iteration(BitMap&&) -> BitMapIteration = delete;
auto bitmap_iteration(BitMap const&&) -> BitMapIteration = delete;

/// A range over the indices of the set bits in a bitmap.
///
/// The referenced bitmap must outlive the range. Iteration skips unset words
/// and visits only the set bits in non-empty words.
class TrueBits {
public:
  class Sentinel;

  class Iterator {
  public:
    using value_type = Index;
    using difference_type = Index;
    using iterator_concept = std::input_iterator_tag;
    using iterator_category = std::input_iterator_tag;

    Iterator() = default;

    auto operator*() const noexcept -> value_type {
      return index_;
    }

    auto operator++() noexcept -> Iterator& {
      if (word_) {
        remaining_ &= remaining_ - 1;
        advance();
      } else {
        ++index_;
      }
      return *this;
    }

    auto operator++(int) noexcept -> void {
      ++*this;
    }

    friend auto operator==(Iterator const& lhs, Sentinel const& rhs) noexcept
      -> bool;

  private:
    friend class TrueBits;

    explicit Iterator(BitMap::Word const* word, BitMap::Word const* word_end,
                      Index length, bool constant_value) noexcept
      : word_{word}, word_end_{word_end}, length_{length} {
      if (word_) {
        remaining_ = *word_;
        advance();
      } else if (not constant_value) {
        index_ = length_;
      }
    }

    auto advance() noexcept -> void {
      while (remaining_ == 0) {
        ++word_;
        word_offset_ += BitMap::word_bits;
        if (word_ == word_end_) {
          index_ = length_;
          return;
        }
        remaining_ = *word_;
      }
      index_ = word_offset_ + static_cast<Index>(std::countr_zero(remaining_));
    }

    BitMap::Word const* word_ = nullptr;
    BitMap::Word const* word_end_ = nullptr;
    BitMap::Word remaining_ = 0;
    Index length_ = 0;
    Index index_ = 0;
    Index word_offset_ = 0;
  };

  class Sentinel {
  public:
    Sentinel() = default;

  private:
    friend class TrueBits;
    friend auto operator==(Iterator const& lhs, Sentinel const& rhs) noexcept
      -> bool;

    explicit Sentinel(Index length) noexcept : length_{length} {
    }

    Index length_ = 0;
  };

  explicit TrueBits(BitMap const& bitmap) noexcept : bitmap_{bitmap} {
  }

  auto begin() const noexcept -> Iterator {
    auto const data = bitmap_->data();
    return Iterator{data.empty() ? nullptr : data.data(),
                    data.empty() ? nullptr : data.data() + data.size(),
                    bitmap_->length(), bitmap_->as_constant().value_or(false)};
  }

  auto end() const noexcept -> Sentinel {
    return Sentinel{bitmap_->length()};
  }

private:
  Ref<BitMap const> bitmap_;
};

inline auto operator==(TrueBits::Iterator const& lhs,
                       TrueBits::Sentinel const& rhs) noexcept -> bool {
  return lhs.index_ == rhs.length_;
}

inline auto true_bits(BitMap const& bitmap) noexcept -> TrueBits {
  return TrueBits{bitmap};
}

auto true_bits(BitMap&&) -> TrueBits = delete;
auto true_bits(BitMap const&&) -> TrueBits = delete;

/// Invokes `f(index)` for every set bit of `bitmap`, in ascending order.
///
/// This is the preferred form for hot row loops: an all-true bitmap becomes a
/// plain counted loop, an all-false one returns immediately, and anything else
/// is walked one word at a time, skipping zero words and jumping between set
/// bits with `countr_zero` instead of testing every bit.
template <std::invocable<Index> F>
inline auto for_each_true(BitMap const& bitmap, F&& f) -> void {
  const auto length = bitmap.length();
  if (const auto constant = bitmap.as_constant()) {
    if (*constant) {
      for (auto i = Index{0}; i < length; ++i) {
        std::invoke(f, i);
      }
    }
    return;
  }
  auto offset = Index{0};
  for (auto word : bitmap.data()) {
    while (word != 0) {
      std::invoke(f, offset + static_cast<Index>(std::countr_zero(word)));
      word &= word - 1;
    }
    offset += BitMap::word_bits;
  }
}

} // namespace tenzir::nova::storage
