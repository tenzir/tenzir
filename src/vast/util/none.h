#ifndef VAST_UTIL_NONE
#define VAST_UTIL_NONE

namespace vast {
namespace util {

/// A class that models a null value.
struct none {};

/// The only instance of ::none.
static constexpr auto nil = none{};

bool operator<(none const&, none const&)
{
  return true;
}

bool operator==(none const&, none const&)
{
  return true;
}

} // namespace util
} // namespace vast

#endif
