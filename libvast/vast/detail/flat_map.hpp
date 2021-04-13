//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/vector_map.hpp"

#include <algorithm>
#include <functional>
#include <memory>
#include <utility>

namespace vast::detail {

template <class Key, class T, class Compare>
struct flat_map_policy {
  static bool pair_compare(const std::pair<Key, T>& x, const Key& y) {
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

/// A map abstraction over a `std::vector`.
/// Guarantees that all entries are always stored in ascending order
/// according to `Compare`.
template <class Key, class T, class Compare = std::less<Key>,
          class Allocator = std::allocator<std::pair<Key, T>>>
using flat_map
  = vector_map<Key, T, Allocator, flat_map_policy<Key, T, Compare>>;

} // namespace vast::detail
