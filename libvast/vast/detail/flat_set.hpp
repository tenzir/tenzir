#ifndef VAST_DETAIL_FLAT_SET_HPP
#define VAST_DETAIL_FLAT_SET_HPP

#include <algorithm>
#include <functional>
#include <memory>
#include <utility>

#include "vast/detail/vector_set.hpp"

namespace vast {
namespace detail {

template <class Compare>
struct flat_set_policy {
  template <class Ts, class T>
  static auto add(Ts& xs, T&& x) {
    auto i = std::lower_bound(xs.begin(), xs.end(), x, Compare{});
    if (i == xs.end() || Compare{}(x, *i))
      return std::make_pair(xs.insert(i, std::forward<T>(x)), true);
    else
      return std::make_pair(i, false);
  }

  template <class Ts, class T>
  static auto lookup(Ts&& xs, const T& x) {
    auto i = std::lower_bound(xs.begin(), xs.end(), x, Compare{});
    return i != xs.end() && !Compare{}(x, *i) ? i : xs.end();
  }
};

/// A set abstraction over a sorted `std::vector`.
template <
  class T,
  class Compare = std::less<T>,
  class Allocator = std::allocator<T>
>
using flat_set = vector_set<T, Allocator, flat_set_policy<Compare>>;

} // namespace detail
} // namespace vast

#endif
