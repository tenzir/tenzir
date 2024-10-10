//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/vector_map.hpp"

#include <algorithm>
#include <functional>
#include <memory>
#include <tuple>
#include <utility>

namespace tenzir::detail {

template <class Key, class T, class Compare>
struct flat_map_policy {
  template <typename K1, typename K2>
  static bool key_compare(const K1& k1, const K2& k2) {
    return Compare{}(k1, k2);
  }

  static bool pair_compare(const std::pair<Key, T>& x, const Key& y) {
    return Compare{}(x.first, y);
  }

  template <class Ts, class Key_Like, class... Args>
  static auto try_emplace(Ts& xs, Key_Like&& k, Args&&... args) {
    using value_type = typename Ts::value_type;
    auto it = std::ranges::lower_bound(xs, k, Compare{}, &value_type::first);
    if (it == xs.end() || Compare{}(k, it->first)) {
      return std::make_pair(
        xs.emplace(it, std::piecewise_construct,
                   std::forward_as_tuple(std::forward<Key_Like>(k)),
                   std::forward_as_tuple(std::forward<Args>(args)...)),
        true); // TODO verify that we dont need the types here
    } else {
      return std::make_pair(it, false);
    }
  }

  template <class Ts, class Pair>
  static auto add(Ts& xs, Pair&& x) {
    auto i = std::lower_bound(xs.begin(), xs.end(), x.first, pair_compare);
    if (i == xs.end() || pair_compare(x, i->first)) {
      return std::make_pair(xs.insert(i, std::forward<Pair>(x)), true);
    } else {
      return std::make_pair(i, false);
    }
  }

  template <class Ts, class Key_Like>
  static auto lookup(Ts& xs, const Key_Like& k) {
    using value_type = typename Ts::value_type;
    auto it = std::ranges::lower_bound(xs, k, Compare{}, &value_type::first);
    return it != xs.end() && !Compare{}(k, it->first) ? it : xs.end();
  }
};

/// A map abstraction over a `std::vector`.
/// Guarantees that all entries are always stored in ascending order
/// according to `Compare`.
template <class Key, class T, class Compare = std::less<>,
          class Allocator = std::allocator<std::pair<Key, T>>>
using flat_map
  = vector_map<Key, T, Allocator, flat_map_policy<Key, T, Compare>>;

} // namespace tenzir::detail
