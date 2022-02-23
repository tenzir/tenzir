//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/detail/vector_map.hpp"

namespace vast::detail {

struct stable_map_policy {
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

// Note: The alias definition for stable_map is in `vast/fwd.hpp`.

} // namespace vast::detail
