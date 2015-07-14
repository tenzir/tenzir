#ifndef VAST_OFFSET_H
#define VAST_OFFSET_H

#include "vast/print.h"
#include "vast/util/stack/vector.h"

namespace vast {

/// A sequence of indexes to recursively access a type or value.
struct offset : util::stack::vector<4, size_t>
{
  using super = util::stack::vector<4, size_t>;
  using super::vector;

};

// TODO: Migrate to concepts location.
template <typename Iterator>
trial<void> print(offset const& o, Iterator&& out)
{
  return print_delimited(',', o.begin(), o.end(), out);
}

} // namespace vast

#endif
