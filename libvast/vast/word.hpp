/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <cstdint>
#include <limits>
#include <type_traits>

#include "vast/detail/assert.hpp"
#include "vast/detail/type_traits.hpp"

namespace vast::detail {

template <class T>
using is_unsigned_integral = bool_constant<
  std::is_unsigned<T>::value && std::is_integral<T>::value
>;

// Type alias used for SFINAE of free functions below. If we would simply use
// typename word<T>::size_type, the static_assert would fire. With this work
// around, we avoid going through word<T> so that overload resolution can
// proceed.
template <class T>
using word_size_type = std::conditional_t<
  is_unsigned_integral<T>::value,
  uint64_t,
  void
>;

} // namespace vast::detail

namespace vast {

/// A fixed-size piece unsigned piece of data that supports various bitwise
/// operations.
template <class T>
struct word {
  static_assert(detail::is_unsigned_integral<T>::value,
                "bitwise operations require unsigned integral, types");

  // -- general ---------------------------------------------------------------

  /// The underlying block type.
  using value_type = T;

  /// The type to represent sizes.
  using size_type = detail::word_size_type<T>;

  /// The number of bits per block (aka. word size).
  static constexpr size_type width = std::numeric_limits<value_type>::digits;

  // Sanity check.
  static_assert(width <= 64);

  /// A value that represents an invalid or "not found" position.
  static constexpr size_type npos = ~size_type{0};

  // -- special block values --------------------------------------------------

  /// A block with all 0s.
  static constexpr value_type none = value_type{0};

  /// A block with all 1s.
  static constexpr value_type all = ~none;

  /// A block with only an MSB of 0.
  static constexpr value_type msb0 = all >> 1;

  /// A block with only an MSB of 1.
  static constexpr value_type msb1 = ~msb0;

  /// A block with only an LSB of 1.
  static constexpr value_type lsb1 = value_type{1};

  /// A block with only an LSB of 0.
  static constexpr value_type lsb0 = ~lsb1;

  // -- masks -----------------------------------------------------------------

  /// Computes a bitmask for a given position.
  /// @param i The position where the 1-bit should be.
  /// @return `1 << i`
  /// @pre `i < width`
  static constexpr value_type mask(size_type i) {
    return lsb1 << i;
  }

  /// Computes a bitmask with only the *i* least significant bits set to 1.
  /// @param i The number least significant bits to set to 1.
  /// @returns `~(all << i)`
  /// @pre `i < width`
  /// @relates lsb_fill
  static constexpr value_type lsb_mask(size_type i) {
    return ~(all << i);
  }

  /// Computes a bitmask with only the *i* least significant bits set to 1.
  /// @param i The number least significant bits to set to 1.
  /// @returns `all >> (width - i)`
  /// @pre `i > 0 && i <= width`
  /// @relates lsb_mask
  static constexpr value_type lsb_fill(size_type i) {
    return all >> (width - i);
  }

  /// Computes a bitmask with only the *i* most significant bits set to 1.
  /// @param i The number most significant bits to set to 1.
  /// @returns `~(all << i)`
  /// @pre `i < width`
  /// @relates msb_fill
  static constexpr value_type msb_mask(size_type i) {
    return ~(all >> i);
  }

  /// Computes a bitmask with only the *i* most significant bits set to 1.
  /// @param i The number most significant bits to set to 1.
  /// @returns `all << (width - i)`
  /// @pre `i > 0 && i <= width`
  /// @relates msb_mask
  static constexpr value_type msb_fill(size_type i) {
    return all << (width - i);
  }

  // -- tests -----------------------------------------------------------------

  /// Extracts the *i*-th bit in a block.
  /// @param x The block to test.
  /// @param i The bit to extract.
  /// @returns The value at position *i*, counted from the LSB.
  /// @pre `i < width`
  static constexpr bool test(value_type x, size_type i) {
    return (x & mask(i)) == mask(i);
  }

  /// Tests whether a block is either all 0 or all 1.
  /// @param x The block to test.
  /// @returns `x == all || x == none`
  static constexpr bool all_or_none(value_type x) {
    return ((x + 1) & lsb0) <= 1;
  }

  /// Tests whether the *k* least signficant bits block are all 0 or all 1.
  /// @param x The block to test.
  /// @param k The number of least significant bits to consider.
  /// @returns `x & lsb_mask(k) == all || x & lsb_mask(k) == none`
  /// @pre `k < width`
  static constexpr bool all_or_none(value_type x, size_type k) {
    return ((x + 1) & lsb_mask(k)) <= 1;
  }

  // -- manipulation ----------------------------------------------------------

  /// Sets a specific bit in a block to 0 or 1.
  /// @tparam Bit The bit value to set.
  /// @param x The block to set the bit in.
  /// @param i The position to set.
  /// @pre `i < width`
  template <bool Bit>
  static constexpr value_type set(value_type x, size_type i) {
    if constexpr (Bit)
      return x | mask(i);
    else
      return x & ~mask(i);
  }

  /// Sets a specific bit in a block to 0 or 1.
  /// @param x The block to set the bit in.
  /// @param i The position to set.
  /// @param b The bit value to set.
  /// @pre `i < width`
  static constexpr value_type set(value_type x, size_type i, bool b) {
    return b ? set<1>(x, i) : set<0>(x, i);
  }

  /// Flips a bit in a block at a given position.
  /// @param x The block to flip a bit in.
  /// @param i The position to flip.
  /// @returns `x ^ (1 << i)`
  /// @pre `i < width`
  static constexpr value_type flip(value_type x, size_type i) {
    return x ^ mask(i);
  }

  // -- searching -------------------------------------------------------------

  /// Locates the first index of a 1-bit.
  /// @param x The block value.
  /// @returns The index of the first 1-bit in *x*.
  /// @pre `x > 0`
  static constexpr size_type find_first_set(value_type x) {
    if constexpr (width <= 32)
      return __builtin_ffs(x);
    else
      return __builtin_ffsll(x);
  }

  // -- counting --------------------------------------------------------------

  /// Computes the population count (aka. *Hamming weight* or *popcount*) of a
  /// word.
  /// @param x The block value.
  /// @returns The number of set bits in *x*.
  static constexpr size_type popcount(value_type x) {
    if constexpr (width <= 32)
      return x == 0 ? 0 : __builtin_popcount(x);
    else
      return x == 0 ? 0 : __builtin_popcountll(x);
  }

  /// Counts the number of trailing zeros.
  /// @param x The block value.
  /// @returns The number trailing zeros in *x*.
  static constexpr size_type count_trailing_zeros(value_type x) {
    if constexpr (width <= 32)
      return x == 0 ? width : __builtin_ctz(x);
    else
      return x == 0 ? width : __builtin_ctzll(x);
  }

  /// Counts the number of trailing ones.
  /// @param x The block value.
  /// @returns The number trailing ones in *x*.
  static constexpr auto count_trailing_ones(value_type x) {
    return count_trailing_zeros(~x);
  }

  /// Counts the number of leading zeros.
  /// @param x The block value.
  /// @returns The number leading zeros in *x*.
  static constexpr size_type count_leading_zeros(value_type x) {
    if constexpr (width <= 32)
      // The compiler builtin always assumes a width of 32 bits. We have to
      // adapt the return value according to the actual block width.
      return x == 0 ? width : (__builtin_clz(x) - (32 - width));
    else
      return x == 0 ? width : __builtin_clzll(x);
  }

  /// Counts the number of leading ones.
  /// @param x The block value.
  /// @returns The number leading ones in *x*.
  static constexpr auto count_leading_ones(value_type x) {
    return count_leading_zeros(~x);
  }

  /// Computes the parity of a block, i.e., the number of 1-bits modulo 2.
  /// @param x The block value.
  /// @returns The parity of *x*.
  /// @pre `x > 0`
  static constexpr size_type parity(value_type x) {
    if constexpr (width <= 32)
      return __builtin_parity(x);
    else
      return __builtin_parityll(x);
  }

  // -- math ------------------------------------------------------------------

  /// Computes the binary logarithm (*log2*) for a given block.
  /// @param x The block value.
  /// @returns `log2(x)`
  /// @pre `x > 0`
  static constexpr auto log2(value_type x) {
    return width - count_leading_zeros(x) - 1;
  }
};

template <class T>
constexpr typename word<T>::size_type word<T>::width;

template <class T>
constexpr typename word<T>::size_type word<T>::npos;

template <class T>
constexpr typename word<T>::value_type word<T>::none;

template <class T>
constexpr typename word<T>::value_type word<T>::all;

template <class T>
constexpr typename word<T>::value_type word<T>::msb0;

template <class T>
constexpr typename word<T>::value_type word<T>::msb1;

template <class T>
constexpr typename word<T>::value_type word<T>::lsb0;

template <class T>
constexpr typename word<T>::value_type word<T>::lsb1;

// -- counting --------------------------------------------------------------

template <bool Bit = true, class T>
static constexpr auto rank(T x)
-> std::enable_if_t<
  detail::is_unsigned_integral<T>::value,
  detail::word_size_type<T>
> {
  if constexpr (Bit)
    return word<T>::popcount(x);
  else
    return word<T>::popcount(static_cast<T>(~x));
}

/// Computes *rank_i* of a block, i.e., the number of 1-bits up to and
/// including position *i*, counted from the LSB.
/// @param x The block to compute the rank for.
/// @param i The position up to where to count.
/// @returns *rank_i(x)*.
/// @pre `i < width`
template <bool Bit = true, class T>
static constexpr auto rank(T x, detail::word_size_type<T> i)
-> std::enable_if_t<
  Bit && detail::is_unsigned_integral<T>::value,
  detail::word_size_type<T>
> {
  T masked = x & word<T>::lsb_fill(i + 1);
  return rank<1>(masked);
}

template <bool Bit, class T>
static constexpr auto rank(T x, detail::word_size_type<T> i)
-> std::enable_if_t<
  !Bit && detail::is_unsigned_integral<T>::value,
  detail::word_size_type<T>
> {
  return rank<1>(static_cast<T>(~x), i);
}

// -- searching -------------------------------------------------------------

/// Finds the next 1-bit starting at position relative to the LSB.
/// @param x The block to search.
/// @param i The position relative to the LSB to start searching.
template <bool Bit = true, class T>
static constexpr auto find_first(T x)
-> std::enable_if_t<
  Bit && detail::is_unsigned_integral<T>::value,
  detail::word_size_type<T>
> {
  auto tzs = word<T>::count_trailing_zeros(x);
  return tzs == word<T>::width ? word<T>::npos : tzs;
}

template <bool Bit, class T>
static constexpr auto find_first(T x)
-> std::enable_if_t<
  !Bit && detail::is_unsigned_integral<T>::value,
  detail::word_size_type<T>
> {
  return find_first<1>(static_cast<T>(~x));
}

template <bool Bit = true, class T>
static constexpr auto find_last(T x)
-> std::enable_if_t<
  Bit && detail::is_unsigned_integral<T>::value,
  detail::word_size_type<T>
> {
  auto lzs = word<T>::count_leading_zeros(x);
  return lzs == word<T>::width ? word<T>::npos : (word<T>::width - lzs - 1);
}

template <bool Bit, class T>
static constexpr auto find_last(T x)
-> std::enable_if_t<
  !Bit && detail::is_unsigned_integral<T>::value,
  detail::word_size_type<T>
> {
  return find_last<1>(static_cast<T>(~x));
}

/// Finds the next 1-bit starting at position relative to the LSB.
/// @param x The block to search.
/// @param i The position relative to the LSB to start searching.
template <class T>
static constexpr auto find_next(T x, detail::word_size_type<T> i)
-> std::enable_if_t<
  detail::is_unsigned_integral<T>::value,
  detail::word_size_type<T>
  > {
  if (i == word<T>::width - 1)
    return word<T>::npos;
  T top = x & (word<T>::all << (i + 1));
  return top == 0 ? word<T>::npos : word<T>::count_trailing_zeros(top);
}

/// Finds the previous 1-bit starting at position relative to the LSB.
/// @param x The block to search.
/// @param i The position relative to the LSB to start searching.
/// @pre `i < width`
template <class T>
static constexpr auto find_prev(T x, detail::word_size_type<T> i)
-> std::enable_if_t<
  detail::is_unsigned_integral<T>::value,
  detail::word_size_type<T>
> {
  if (i == 0)
    return word<T>::npos;
  T low = x & ~(word<T>::all << i);
  return low == 0
    ? word<T>::npos
    : word<T>::width - word<T>::count_leading_zeros(low) - 1;
}

/// Computes the position of the i-th occurrence of a bit.
/// @param Bit The bit value.
/// @param x The block to search.
/// @param i The position of the *i*-th occurrence of *Bit* in *b*.
/// @pre `i > 0 && i <= width`
template <bool Bit = true, class T>
static constexpr auto select(T x, detail::word_size_type<T> i)
-> std::enable_if_t<
  Bit && detail::is_unsigned_integral<T>::value,
  detail::word_size_type<T>
> {
  // TODO: make this efficient and branch-free. There is one implementation
  // that counts from the right for 64-bit here:
  // http://graphics.stanford.edu/~seander/bithacks.html
  auto cum = 0u;
  for (auto j = 0u; j < word<T>::width; ++j)
    if (word<T>::test(x, j))
      if (++cum == i)
        return j;
  return word<T>::npos;
}

template <bool Bit, class T>
static constexpr auto select(T x, detail::word_size_type<T> i)
-> std::enable_if_t<
  !Bit && detail::is_unsigned_integral<T>::value,
  detail::word_size_type<T>
> {
  // TODO: see note above.
  auto cum = 0u;
  for (auto j = 0u; j < word<T>::width; ++j)
    if (!word<T>::test(x, j))
      if (++cum == i)
        return j;
  return word<T>::npos;
}

} // namespace vast

