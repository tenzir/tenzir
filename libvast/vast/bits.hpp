#ifndef VAST_BITS_HPP
#define VAST_BITS_HPP

#include "vast/word.hpp"

namespace vast {

// An abstraction over a contiguous sequence of bits. A bit sequence can have
// two types: a *fill* sequence representing a homogenous bits, typically
// greater than or equal to the block size, and a *literal* sequence
// representing bits from a single block, typically less than or equal to the
// block size.
template <class T>
class bits {
public:
  using word = word<T>;
  using value_type = typename word::value_type;
  using size_type = typename word::size_type;

  bits(value_type x = 0, size_type n = word::width) : value{x}, size{n} {
  }

  value_type value;
  size_type size;
};

} // namespace vast

#endif
