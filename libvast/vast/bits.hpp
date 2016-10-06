#ifndef VAST_BITS_HPP
#define VAST_BITS_HPP

#include "vast/word.hpp"

namespace vast {

// An abstraction over a contiguous sequence of bits.
template <class T>
class bits {
public:
  using word = word<T>;
  using value_type = typename word::value_type;
  using size_type = typename word::size_type;

  bits(value_type x = 0, size_type n = word::width) : data{x}, size{n} {
  }

  value_type value() const {
    return data & word::lsb_mask(size);
  }

  bool homogeneous() const {
    auto full = size == word::width;
    return full ? word::all_or_none(data) : word::all_or_none(data, size);
  }

  value_type data;
  size_type size;
};

} // namespace vast

#endif
