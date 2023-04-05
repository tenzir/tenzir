//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <variant>

namespace vast {
/// Marker type for the given type.
template <typename T>
struct tag {
  using type = T;
};

/// Value of the marker type for the given type.
template <typename T>
constexpr auto tag_v = tag<T>{};

/// Variant of the marker types of the given types.
template <typename... Ts>
struct tag_variant : std::variant<tag<Ts>...> {
  using std::variant<tag<Ts>...>::variant;

  /// Returns whether this holds `tag<T>`.
  template <typename T>
  auto is() const -> bool {
    return std::holds_alternative<tag<T>>(*this);
  }
};
} // namespace vast
