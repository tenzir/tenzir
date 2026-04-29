//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/vector_set.hpp"

#include <algorithm>
#include <functional>
#include <memory>
#include <utility>

namespace tenzir::detail {

template <class Compare>
struct flat_set_policy {
  template <class Ts, class T>
  static auto add(Ts& xs, T&& x) {
    auto i = std::lower_bound(xs.begin(), xs.end(), x, Compare{});
    if (i == xs.end() or Compare{}(x, *i)) {
      return std::make_pair(xs.insert(i, std::forward<T>(x)), true);
    } else {
      return std::make_pair(i, false);
    }
  }

  template <class Ts, class T>
  static auto lookup(Ts&& xs, const T& x) {
    auto i = std::lower_bound(xs.begin(), xs.end(), x, Compare{});
    return i != xs.end() and not Compare{}(x, *i) ? i : xs.end();
  }
};

/// A set abstraction over a sorted `std::vector`.
template <class T, class Compare = std::less<T>,
          class Allocator = std::allocator<T>>
using flat_set = vector_set<T, Allocator, flat_set_policy<Compare>>;

} // namespace tenzir::detail
