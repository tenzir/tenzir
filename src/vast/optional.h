#ifndef VAST_OPTIONAL_HPP
#define VAST_OPTIONAL_HPP

#include <caf/optional.hpp>

#include "vast/none.h"

namespace vast {

/// An optional value of `T` with similar semantics as `std::optional`.
template <typename T>
using optional = caf::optional<T>;

} // namespace vast

#endif
