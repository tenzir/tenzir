//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/vector_set.hpp"

#include <algorithm>
#include <functional>
#include <memory>
#include <utility>

namespace vast::detail {

struct stable_set_policy {
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
using stable_set = vector_set<T, Allocator, stable_set_policy>;

} // namespace vast::detail
