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

#ifndef VAST_DETAIL_STEADY_MAP_HPP
#define VAST_DETAIL_STEADY_MAP_HPP

#include "vast/detail/vector_map.hpp"

namespace vast::detail {

struct steady_map_policy {
  template <class Ts, class T>
  static auto add(Ts& xs, T&& x) {
    auto i = lookup(xs, x.first);
    if (i == xs.end())
      return std::make_pair(xs.insert(i, std::forward<T>(x)), true);
    else
      return std::make_pair(i, false);
  }

  template <class Ts, class T>
  static auto lookup(Ts&& xs, const T& x) {
    auto pred = [&](auto& p) { return p.first == x; };
    return std::find_if(xs.begin(), xs.end(), pred);
  }
};

/// A map abstraction over an unsorted `std::vector`.
template <
  class Key,
  class T,
  class Allocator = std::allocator<std::pair<Key, T>>
>
using steady_map = vector_map<Key, T, Allocator, steady_map_policy>;

} // namespace vast::detail

#endif
