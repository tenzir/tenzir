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

#ifndef VAST_BITS_HPP
#define VAST_BITS_HPP

#include "vast/word.hpp"

namespace vast {

/// A sequence of bits represented by a single word. If the size is greater
/// than or equal to the word size, then the data block must be all 0s or
/// all 1s. Otherwise, only the N least-significant bits are active, and the
/// remaining bits in the block are guaranteed to be 0.
template <class T>
class bits {
  public:
  using word_type = word<T>;
  using value_type = typename word_type::value_type;
  using size_type = typename word_type::size_type;

  static constexpr value_type mask(value_type x, size_type n) {
    return n < word_type::width ? x & word_type::lsb_mask(n) : x;
  }

  /// Constructs a bit sequence.
  /// @param x The value of the sequence.
  /// @param n The length of the sequence.
  /// @pre `n > 0 && (n < w || all_or_none(x))` where *w* is the word width.
  bits(value_type x = 0, size_type n = word_type::width)
    : data_{mask(x, n)},
      size_{n} {
    VAST_ASSERT(n > 0);
    VAST_ASSERT(n <= word_type::width || word_type::all_or_none(x));
  }

  value_type data() const {
    return data_;
  }

  size_type size() const {
    return size_;
  }

  /// Accesses the i-th bit in the bit sequence.
  /// @param i The bit position counting from the LSB.
  /// @returns The i-th bit value from the LSB.
  /// @pre `i < size`.
  bool operator[](size_type i) const {
    VAST_ASSERT(i < size_);
    return i > word_type::width ? !!data_ : data_ & word_type::mask(i);
  }

  /// Checks whether all bits have the same value.
  /// @returns `true` if the bits are either all 0 or all 1.
  bool homogeneous() const {
    return size_ == word_type::width
      ? word_type::all_or_none(data_)
      : word_type::all_or_none(data_, size_);
  }

private:
  value_type data_;
  size_type size_;
};

// -- searching --------------------------------------------------------------

/// Finds the first bit of a particular value.
/// @tparam Bit The bit value to look for.
/// @returns The position of the first bit having value *Bit*, or `npos` if
///          not such position exists..
template <bool Bit = true, class T>
auto find_first(const bits<T>& b) {
  if constexpr (Bit) {
    if (b.size() > word<T>::width)
      return b.data() == word<T>::all ? 0 : word<T>::npos;
    return find_first<1>(b.data());
  } else {
    if (b.size() > word<T>::width)
      return b.data() == word<T>::none ? 0 : word<T>::npos;
    T masked = b.data() | word<T>::msb_mask(word<T>::width - b.size());
    return find_first<0>(masked);
  }
}

/// Finds the next bit after at a particular offset.
/// @param i The offset after where to begin searching.
/// @returns The position *p*, where *p > i*, of the bit having value *Bit*,
///          or `npos` if no such *p* exists.
template <bool Bit = true, class T>
auto find_next(const bits<T>& b, typename bits<T>::size_type i) {
  if constexpr (Bit) {
    if (i >= b.size() - 1)
      return word<T>::npos;
    if (b.size() > word<T>::width)
      return b.data() == word<T>::all ? i + 1 : word<T>::npos;
    return find_next(b.data(), i);
  } else {
    if (i >= b.size() - 1)
      return word<T>::npos;
    if (b.size() > word<T>::width)
      return b.data() == word<T>::none ? i + 1 : word<T>::npos;
    T masked = ~b.data() & word<T>::lsb_fill(b.size());
    return find_next(masked, i);
  }
}

/// Finds the last bit of a particular value.
/// @tparam Bit The bit value to look for.
/// @returns The position of the last bit having value *Bit*.
template <bool Bit = true, class T>
auto find_last(const bits<T>& b) {
  if constexpr (Bit) {
    if (b.size() > word<T>::width)
      return b.data() == word<T>::all ? b.size() - 1 : word<T>::npos;
    return find_last<1>(b.data());
  } else {
    if (b.size() > word<T>::width)
      return b.data() == word<T>::none ? b.size() - 1 : word<T>::npos;
    T masked = ~b.data() & word<T>::lsb_fill(b.size());
    return find_last<1>(masked);
  }
}

// -- counting ---------------------------------------------------------------

/// Computes the number of occurrences of a bit value.
/// @tparam Bit The bit value to count.
/// @param b The bit sequence to count.
/// @returns The population count of *b*.
template <bool Bit = true, class T>
auto rank(const bits<T>& b) {
  if constexpr (Bit) {
    if (b.size() > word<T>::width)
      return b.data() == word<T>::all ? b.size() : 0;
    return rank<1>(b.data());
  } else {
    if (b.size() > word<T>::width)
      return b.data() == word<T>::none ? b.size() : 0;
    T masked = ~b.data() & word<T>::lsb_fill(b.size());
    return rank<1>(masked);
  }
}

/// Computes the number of occurrences of a bit value in *[0,i]*.
/// @tparam Bit The bit value to count.
/// @param b The bit sequence to count.
/// @param i The offset where to end counting.
/// @returns The population count of *b* up to and including position *i*.
/// @pre `i < b.size()`
template <bool Bit = true, class T>
auto rank(const bits<T>& b, typename bits<T>::size_type i) {
  VAST_ASSERT(i < b.size());
  T data = Bit ? b.data() : ~b.data();
  if (b.size() > word<T>::width)
    return data == word<T>::none ? 0 : i + 1;
  if (i == word<T>::width - 1)
    return word<T>::popcount(data);
  return rank(data, i);
}

/// Computes the position of the i-th occurrence of a bit.
/// @tparam Bit the bit value to locate.
/// @param b The bit sequence to select from.
/// @param i The position of the *i*-th occurrence of *Bit* in *b*.
/// @pre `i > 0 && i <= b.size()`
template <bool Bit = true, class T>
auto select(const bits<T>& b, typename bits<T>::size_type i) {
  VAST_ASSERT(i > 0);
  VAST_ASSERT(i <= b.size());
  T data = Bit ? b.data() : ~b.data();
  if (b.size() > word<T>::width)
    return data == word<T>::all ? i - 1 : word<T>::npos;
  return select(data, i);
}

} // namespace vast

#endif
