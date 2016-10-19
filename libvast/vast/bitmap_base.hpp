#ifndef VAST_BITMAP_BASE_HPP
#define VAST_BITMAP_BASE_HPP

#include <cstdint>

#include "vast/bitmap_algorithms.hpp"
#include "vast/bits.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/range.hpp"
#include "vast/die.hpp"
#include "vast/word.hpp"

namespace vast {

/// The base class for bitmaps. The concrete derived class must model the
/// *bitmap concept* looks:
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
///    auto bit_range(bitmap const& bm);
///
template <class Derived>
class bitmap_base {
public:
  using super = bitmap_base;
  using block_type = uint64_t;
  using size_type = uint64_t;
  using word_type = word<block_type>;

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
  friend Derived operator&(Derived const& lhs, Rhs const& rhs) {
    return binary_and(lhs, rhs);
  }

  /// Computes the bitwise OR of two bitmaps.
  template <class Rhs = Derived>
  friend Derived operator|(Derived const& lhs, Rhs const& rhs) {
    return binary_or(lhs, rhs);
  }

  /// Computes the bitwise XOR of two bitmaps.
  template <class Rhs = Derived>
  friend Derived operator^(Derived const& lhs, Rhs const& rhs) {
    return binary_xor(lhs, rhs);
  }

  /// Computes the bitwise NAND of two bitmaps.
  template <class Rhs = Derived>
  friend Derived operator-(Derived const& lhs, Rhs const& rhs) {
    return binary_nand(lhs, rhs);
  }

  /// Computes the bitwise NOR of two bitmaps.
  template <class Rhs = Derived>
  friend Derived operator/(Derived const& lhs, Rhs const& rhs) {
    return binary_nor(lhs, rhs);
  }

private:
   void append(block_type data, size_type n) {
    if (n > word_type::width)
      derived().append_bits(data, n);
    else if (n == 1)
      derived().append_bit(data & word_type::lsb1);
    else
      derived().append_block(data, n);
  }

  Derived& derived() {
    return *static_cast<Derived*>(this);
  }

  Derived const& derived() const {
    return *static_cast<Derived const*>(this);
  }
};

/// The base class for bitmap bit ranges.
template <class Derived, class Block>
class bit_range_base : public detail::range_facade<Derived> {
public:
  bits<Block> const& get() const {
    return bits_;
  }

protected:
  bits<Block> bits_;
};

} // namespace vast

#endif
