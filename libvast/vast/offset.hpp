#ifndef VAST_OFFSET_HPP
#define VAST_OFFSET_HPP

#include <cstddef>

#include "vast/util/stack/vector.hpp"

namespace vast {

/// A sequence of indexes to recursively access a type or value.
struct offset : util::stack::vector<4, size_t> {
  using super = util::stack::vector<4, size_t>;
  using super::vector;
};

} // namespace vast

#endif
