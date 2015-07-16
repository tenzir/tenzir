#ifndef VAST_UTIL_NONE
#define VAST_UTIL_NONE

#include <caf/none.hpp>

namespace vast {
namespace util {

/// A class that models a null value.
using none = caf::none_t;

/// The only instance of ::none.
constexpr auto nil = none{};

inline bool operator<(none const&, none const&)
{
  return true;
}

inline bool operator==(none const&, none const&)
{
  return true;
}

inline bool operator!=(none const&, none const&)
{
  return false;
}

} // namespace util
} // namespace vast

#endif
