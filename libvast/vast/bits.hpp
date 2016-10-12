#ifndef VAST_BITS_HPP
#define VAST_BITS_HPP

#include "vast/word.hpp"

namespace vast {

/// A sequence of bits represented by a single word. If the word is
/// *homogeneous*, i.e., either all 0s or all 1s, then the sequence can have
/// any size. Otherwise it must be less than or equal the word size.
template <class T>
class bits {
  public:
  using word = word<T>;
  using value_type = typename word::value_type;
  using size_type = uint64_t;

  bits(value_type x = 0, size_type n = word::width) : data_{x}, size_{n} {
    VAST_ASSERT(n > 0);
    VAST_ASSERT(n <= word::width || word::all_or_none(x));
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
    return i > word::width ? !!data_ : data_ & word::mask(i);
  }

  /// Checks whether all bits have the same value.
  /// @returns `true` if the bits are either all 0 or all 1.
  bool homogeneous() const {
    auto full = size_ == word::width;
    return full ? word::all_or_none(data_) : word::all_or_none(data_, size_);
  }

  /// Computes the number 1-bits.
  /// @returns The population count of this bit sequence.
  size_type count() const {
    if (size_ <= word::width)
      return word::popcount(data_ & word::lsb_fill(size_));
    return data_ == word::all ? size_ : 0;
  }

private:
  value_type data_;
  size_type size_;
};

} // namespace vast

#endif
