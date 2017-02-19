#ifndef VAST_NONE_HPP
#define VAST_NONE_HPP

#include <caf/none.hpp>

namespace vast {

/// A class that models a null value.
using none = caf::none_t;

/// The only instance of ::none.
constexpr auto nil = caf::none;

} // namespace vast

#endif
