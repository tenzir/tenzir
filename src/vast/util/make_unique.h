#ifndef VAST_UTIL_MAKE_UNIQUE_H
#define VAST_UTIL_MAKE_UNIQUE_H

#include <memory>
#include <utility>

namespace vast {
namespace util {

/// Interim `std::make_unique` until the standard ships with an implementation.
template<typename T, typename ...Args>
std::unique_ptr<T> make_unique(Args&& ...args)
{
  return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}

} // namespace util

using util::make_unique;

} // namespace vast

#endif
