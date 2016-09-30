#ifndef VAST_BITS_HPP
#define VAST_BITS_HPP

#include <limits>

#include "vast/detail/assert.hpp"

namespace vast {

/// An abstraction of a fixed sequence of bits.
template <class T>
struct bits {
  static_assert(std::is_unsigned<T>::value && std::is_integral<T>::value,
                "bitwise operations require unsigned integral, types");

  // -- types -----------------------------------------------------------------

  /// The underlying block type.
  using block = T;

  // -- special values --------------------------------------------------------

  /// The number of bits per block.
  static constexpr auto width = std::numeric_limits<block>::digits;

  /// A block with all 0s.
  static constexpr block none = block{0};

  /// A block with all 1s.
  static constexpr block all = ~none;

  /// A block with only an MSB of 0.
  static constexpr block msb0 = all >> 1;

  /// A block with only an MSB of 1.
  static constexpr block msb1 = ~msb0;

  /// A block with only an LSB of 1.
  static constexpr block lsb1 = block{1};

  /// A block with only an LSB of 0.
  static constexpr block lsb0 = ~lsb1;

  // -- manipulation ----------------------------------------------------------

  /// Computes a bitmask for a given position.
  /// @param i The position where the 1-bit should be.
  /// @return `1 << i`
  /// @pre `i < width`
  static constexpr block mask(block i) {
    return lsb1 << i;
  }

  /// Flips a bit in a block at a given position.
  /// @param x The block to flip a bit in.
  /// @param i The position to flip.
  /// @returns `x ^ (1 << i)`
  /// @pre `i < width`
  static constexpr block flip(block x, block i) {
    return x ^ mask(i);
  }

  /// Sets a specific bit in a block to 0 or 1.
  /// @param x The block to set the bit in.
  /// @param i The position to set.
  /// @pre `i < width`
  static constexpr block set(block x, block i, bool b) {
    return b ? x | mask(i) : x & ~mask(i);
  }

  // -- counting --------------------------------------------------------------

  template <class B>
  using enable_if_32 = std::enable_if_t<(bits<B>::width <= 32), B>;

  template <class B>
  using enable_if_64 = std::enable_if_t<(bits<B>::width == 64), B>;

  /// Counts the number of trailing zeros.
  /// @param x The block value.
  /// @returns The number trailing zeros in *x*.
  /// @pre `x > 0`
  template <class B = block>
  static constexpr auto count_trailing_zeros(block x) -> enable_if_32<B> {
    return __builtin_ctz(x);
  }

  template <class B = block>
  static constexpr auto count_trailing_zeros(block x) -> enable_if_64<B> {
    return __builtin_ctzll(x);
  }

  /// Counts the number of trailing ones.
  /// @param x The block value.
  /// @returns The number trailing ones in *x*.
  /// @pre `x > 0`
  static constexpr block count_trailing_ones(block x) {
    return count_trailing_zeros(~x);
  }

  /// Counts the number of leading zeros.
  /// @param x The block value.
  /// @returns The number leading zeros in *x*.
  /// @pre `x > 0`
  template <class B = block>
  static constexpr auto count_leading_zeros(block x) -> enable_if_32<B> {
    // The compiler builtin always assumes a width of 32 bits. We have to adapt
    // the return value according to the actual block width.
    return __builtin_clz(x) - (32 - width);
  }

  template <class B = block>
  static constexpr auto count_leading_zeros(block x) -> enable_if_64<B> {
    return __builtin_clzll(x);
  }

  /// Counts the number of leading ones.
  /// @param x The block value.
  /// @returns The number leading ones in *x*.
  /// @pre `x > 0`
  static constexpr block count_leading_ones(block x) {
    return count_leading_zeros(~x);
  }

  /// Counts the number of leading ones.
  /// @param x The block value.
  /// @returns The number leading ones in *x*.
  /// @pre `x > 0`
  template <class B = block>
  static constexpr auto popcount(block x) -> enable_if_32<B> {
    return __builtin_popcount(x);
  }

  template <class B = block>
  static constexpr auto popcount(block x) -> enable_if_64<B> {
    return __builtin_popcountll(x);
  }

  /// Computes the parity of a block, i.e., the number of 1-bits modulo 2.
  /// @param x The block value.
  /// @returns The parity of *x*.
  /// @pre `x > 0`
  template <class B = block>
  static constexpr auto parity(block x) -> enable_if_32<B> {
    return __builtin_parity(x);
  }

  template <class B = block>
  static constexpr auto parity(block x) -> enable_if_64<B> {
    return __builtin_parityll(x);
  }

  // -- math ------------------------------------------------------------------

  /// Computes the binary logarithm (*log2*) for a given block.
  /// @param x The block value.
  /// @returns `log2(x)`
  /// @pre `x > 0`
  static constexpr block log2(block x) {
    return width - count_leading_zeros(x) - 1;
  }
};

template <class T>
constexpr typename bits<T>::block bits<T>::none;

template <class T>
constexpr typename bits<T>::block bits<T>::all;

template <class T>
constexpr typename bits<T>::block bits<T>::msb0;

template <class T>
constexpr typename bits<T>::block bits<T>::msb1;

template <class T>
constexpr typename bits<T>::block bits<T>::lsb0;

template <class T>
constexpr typename bits<T>::block bits<T>::lsb1;

} // namespace vast

#endif
