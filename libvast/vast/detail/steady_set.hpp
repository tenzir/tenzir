#ifndef VAST_DETAIL_STEADY_SET_HPP
#define VAST_DETAIL_STEADY_SET_HPP

#include "vast/detail/vector_set.hpp"

namespace vast {
namespace detail {

struct steady_set_policy {
  template <class Ts, class T>
  static auto add(Ts& xs, T&& x) {
    auto i = lookup(xs, x);
    if (i == xs.end())
      return std::make_pair(xs.insert(i, std::forward<T>(x)), true);
    else
      return std::make_pair(i, false);
  }

  template <class Ts, class T>
  static auto lookup(Ts&& xs, const T& x) {
    return std::find(xs.begin(), xs.end(), x);
  }
};

/// A set abstraction over an unsorted `std::vector`.
template <class T, class Allocator = std::allocator<T>>
class steady_set : public vector_set<T, Allocator, steady_set_policy> {
public:
  using vector_set<T, Allocator, steady_set_policy>::vector_set;
};

} // namespace detail
} // namespace vast

#endif
