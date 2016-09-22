#ifndef VAST_OFFSET_HPP
#define VAST_OFFSET_HPP

#include <cstddef>

#include "vast/detail/stack/vector.hpp"

namespace vast {

/// A sequence of indexes to recursively access a type or value.
struct offset : detail::stack::vector<4, size_t> {
  using super = detail::stack::vector<4, size_t>;
  using super::vector;
};

} // namespace vast

#endif
