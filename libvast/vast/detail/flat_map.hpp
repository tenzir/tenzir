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

#include "vast/detail/vector_map.hpp"

#include <algorithm>
#include <functional>
#include <memory>
#include <utility>

namespace vast::detail {

template <class Key, class T, class Compare>
struct flat_map_policy {
  static bool pair_compare(const std::pair<const Key, T>& x, const Key& y) {
    return Compare{}(x.first, y);
  }

  template <class Ts, class Pair>
  static auto add(Ts& xs, Pair&& x) {
    auto i = std::lower_bound(xs.begin(), xs.end(), x.first, pair_compare);
    if (i == xs.end() || pair_compare(x, i->first))
      return std::make_pair(xs.insert(i, std::forward<Pair>(x)), true);
    else
      return std::make_pair(i, false);
  }

  template <class Ts>
  static auto lookup(Ts&& xs, const Key& x) {
    auto i = std::lower_bound(xs.begin(), xs.end(), x, pair_compare);
    return i != xs.end() && !Compare{}(x, i->first) ? i : xs.end();
  }
};

/// A map abstraction over a sorted `std::vector`.
template <class Key, class T, class Compare = std::less<Key>,
          class Allocator = std::allocator<std::pair<Key, T>>>
using flat_map
  = vector_map<Key, T, Allocator, flat_map_policy<Key, T, Compare>>;

} // namespace vast::detail
