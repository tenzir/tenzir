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

#include "vast/bitmap_algorithms.hpp"
#include "vast/bits.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/range.hpp"
#include "vast/die.hpp"
#include "vast/word.hpp"

namespace vast {

/// The base class for bitmaps. The concrete derived types must model the
/// *bitmap concept* looks as follows:
///
///    struct bitmap {
///      bitmap();
///      bitmap(size_type n, bool bit = false);
///
///      // Inspectors
///      bool empty() const;
///      size_type size() const;
///
///      // Modifiers
///      void append_bit(bool bit); // optional
///      void append_bits(bool bit, size_type n);
///      void append_block(block_type bits, size_type n);
///      void flip();
///    };
///
///    // Provides a range instance with .begin() and .end() member functions
///    // to iterate over the bitmap in terms of sequences of bits.
///    auto bit_range(const bitmap& bm);
///
/// If possible, derived types shall provide an optimized version of the
/// following operators:
///
/// - operator&=
/// - operator|=
/// - operator^=
/// - operator-=
/// - operator/=
///
/// These can lead to significantly faster bitwise operations.
template <class Derived>
class bitmap_base {
public:
  using super = bitmap_base;
  using block_type = uint64_t;
  using size_type = uint64_t;
  using word_type = word<block_type>;
  using bits_type = bits<block_type>;

  // We subtract 1 to let the last value represent an invalid bitmap position.
  static constexpr auto max_size = std::numeric_limits<size_type>::max() - 1;

  // -- modifiers -------------------------------------------------------------

  /// Appends the contents of any other bitmap to this one.
  /// @tparam The type of the other bitmap.
  /// @param other The other bitmap.
  /// @pre `size() + other.size()` <= max_size`
  template <class Bitmap>
  void append(const Bitmap& other) {
    VAST_ASSERT(derived().size() + other.size() <= max_size);
    for (auto bits : bit_range(other))
      append(bits);
  }

  /// Appends a single bit.
  /// @tparam Bit the bit value to append.
  template <bool Bit>
  void append() {
    derived().append_bit(Bit);
  }

  /// Appends multiple bits of a single value.
  /// @tparam Bit the bit value to append.
  /// @param n The number of bits with value *Bit* to append.
  template <bool Bit>
  void append(size_t n) {
    derived().append_bits(Bit, n);
  }

  /// Appends multiple bits of a single value.
  /// @param bit the bit value to append.
  /// @param n The number of bits with value *bit* to append.
  void append(bool bit, size_t n) {
    derived().append_bits(bit, n);
  }

  /// Appends a certain number of bits from a given block.
  /// @param xs The bits to append.
  void append(bits_type xs) {
    if (xs.is_run())
      derived().append_bits(xs.data(), xs.size());
    else if (!xs.empty())
      derived().append_block(xs.data(), xs.size());
  }

  // -- element access --------------------------------------------------------

  /// Accesses the *i*-th bit of a bitmap.
  /// @param i The index into the bitmap.
  /// @returns `true` iff bit *i* is 1.
  /// @pre `i < size()`
  bool operator[](size_type i) const {
    VAST_ASSERT(i < derived().size());
    auto n = size_type{0};
    for (auto bits : bit_range(derived())) {
      if (i >= n && i < n + bits.size())
        return bits[i - n];
      n += bits.size();
    }
    die("bitmap_base<Derived>::operator[]");
  }

  // -- bitwise operations ----------------------------------------------------

  /// Computes the complement of this bitmap.
  Derived operator~() const {
    Derived result{derived()};
    result.flip();
    return result;
  }

  /// Computes the bitwise AND of two bitmaps.
  template <class Rhs = Derived>
  friend auto operator&(const Derived& lhs, const Rhs& rhs) {
    return binary_and(lhs, rhs);
  }

  /// Computes the bitwise OR of two bitmaps.
  template <class Rhs = Derived>
  friend auto operator|(const Derived& lhs, const Rhs& rhs) {
    return binary_or(lhs, rhs);
  }

  /// Computes the bitwise XOR of two bitmaps.
  template <class Rhs = Derived>
  friend auto operator^(const Derived& lhs, const Rhs& rhs) {
    return binary_xor(lhs, rhs);
  }

  /// Computes the bitwise NAND of two bitmaps.
  template <class Rhs = Derived>
  friend auto operator-(const Derived& lhs, const Rhs& rhs) {
    return binary_nand(lhs, rhs);
  }

  /// Computes the bitwise NOR of two bitmaps.
  template <class Rhs = Derived>
  friend auto operator/(const Derived& lhs, const Rhs& rhs) {
    return binary_nor(lhs, rhs);
  }

  // -- inplace bitwise operations---------------------------------------------
  //
  // Derived types should provide an optimized version where possible.

  Derived& operator&=(Derived const rhs) {
    derived() = derived() & rhs;
    return derived();
  }

  Derived& operator|=(Derived const rhs) {
    derived() = derived() | rhs;
    return derived();
  }

  Derived& operator^=(Derived const rhs) {
    derived() = derived() ^ rhs;
    return derived();
  }

  Derived& operator-=(Derived const rhs) {
    derived() = derived() - rhs;
    return derived();
  }

  Derived& operator/=(Derived const rhs) {
    derived() = derived() / rhs;
    return derived();
  }

private:
  Derived& derived() {
    return *static_cast<Derived*>(this);
  }

  const Derived& derived() const {
    return *static_cast<const Derived*>(this);
  }
};

/// The base class for bitmap bit ranges.
template <class Derived, class Block>
class bit_range_base : public detail::range_facade<Derived> {
public:
  const bits<Block>& get() const {
    return bits_;
  }

protected:
  bits<Block> bits_;
};

} // namespace vast
