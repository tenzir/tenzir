#ifndef VAST_OFFSET_HPP
#define VAST_OFFSET_HPP

#include <cstddef>

#include "vast/detail/stack_vector.hpp"

namespace vast {

/// A sequence of indexes to recursively access a type or value.
struct offset : detail::stack_vector<size_t, 64> {
  using super = detail::stack_vector<size_t, 64>;
  using super::super;
};

} // namespace vast

#endif
