#ifndef VAST_UTIL_OPTIONAL_HPP
#define VAST_UTIL_OPTIONAL_HPP

#include <caf/optional.hpp>

namespace vast {
namespace util {

/// An optional value of `T` with similar semantics as `std::optional`.
template <typename T>
using optional = caf::optional<T>;

} // namespace util
} // namespace vast

#endif
