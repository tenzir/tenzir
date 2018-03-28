/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <algorithm>
#include <functional>
#include <memory>
#include <utility>

#include "vast/detail/vector_set.hpp"

namespace vast::detail {

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

} // namespace vast::detail

