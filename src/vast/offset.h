#ifndef VAST_OFFSET_H
#define VAST_OFFSET_H

#include <vector>

namespace vast {

/// A sequence of indexes to recursively access a record.
using offset = std::vector<size_t>;

} // namespace vast

#endif
