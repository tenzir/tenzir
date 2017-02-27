#ifndef VAST_DETAIL_FLAT_SET_HPP
#define VAST_DETAIL_FLAT_SET_HPP

#include "vast/detail/vector_set.hpp"

namespace vast {
namespace detail {

template <class Compare>
struct flat_set_policy {
  template <class Ts, class T>
  static auto add(Ts& xs, T&& x) {
    auto i = lookup(xs, x);
    if (i == xs.end() || Compare{}(x, *i))
      return std::make_pair(xs.insert(i, std::forward<T>(x)), true);
    else
      return std::make_pair(i, false);
  }

  template <class Ts, class T>
  static auto lookup(Ts&& xs, const T& x) {
    return std::lower_bound(xs.begin(), xs.end(), x, Compare{});
  }
};

/// A set abstraction over a sorted `std::vector`.
template <
  class T,
  class Compare = std::less<T>,
  class Allocator = std::allocator<T>
>
class flat_set : public vector_set<T, Allocator, flat_set_policy<Compare>> {
public:
  using vector_set<T, Allocator, flat_set_policy<Compare>>::vector_set;
};

} // namespace detail
} // namespace vast

#endif
