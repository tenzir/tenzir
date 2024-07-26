//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/detail/vector_map.hpp"

namespace tenzir::detail {

struct stable_map_policy {
  template <class Ts, class T>
  static auto add(Ts& xs, T&& x) {
    const auto i = lookup(xs, x.first);
    if (i == xs.end()) {
      return std::make_pair(xs.insert(i, std::forward<T>(x)), true);
    } else {
      return std::make_pair(i, false);
    }
  }

  template <class Ts, class Key_Like>
  static auto lookup(Ts& xs, const Key_Like& x) {
    return std::find_if( std::begin(xs), std::end(xs), [&x]( const auto& kvp ){ return kvp.first == x; } );
  }

  template <class Ts, class Key_Like, class... Args>
  static auto try_emplace(Ts& xs, Key_Like&& k, Args&&... args) {
    const auto it = lookup(xs, k);
    if (it == xs.end() || k == it->first) {
      return std::make_pair(
        xs.emplace(it, std::piecewise_construct,
                   std::forward_as_tuple(std::forward<Key_Like>(k)),
                   std::forward_as_tuple(std::forward<Args>(args)...)),
        true);
    } else {
      return std::make_pair(it, false);
    }
  }
};

// Note: The alias definition for stable_map is in `tenzir/fwd.hpp`.

} // namespace tenzir::detail
