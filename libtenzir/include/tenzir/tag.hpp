//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/variant.hpp"

namespace tenzir {

/// Marker type for the given type.
template <typename T>
struct tag {
  using type = T;

  auto operator<=>(const tag& other) const = default;

  friend auto inspect(auto& f, tag& x) -> bool {
    return f.object(x).pretty_name("tag").fields();
  }
};

/// Value of the marker type for the given type.
template <typename T>
inline constexpr auto tag_v = tag<T>{};

/// Variant of the marker types of the given types.
template <typename... Ts>
class tag_variant : public variant<tag<Ts>...> {
public:
  using super = variant<tag<Ts>...>;

  using super::super;

  template <class T>
  static constexpr auto make() -> tag_variant {
    return tag_variant{tag_v<T>};
  }

  template <class T>
  static constexpr auto of = make<T>();

  /// Returns whether this holds `tag<T>`.
  template <typename T>
  auto is() const -> bool {
    return std::holds_alternative<tag<T>>(*this);
  }

  template <typename T>
  auto is_not() const -> bool {
    return not is<T>();
  }

  template <typename... Us>
  auto is_any() const -> bool {
    return (std::holds_alternative<tag<Us>>(*this) || ...);
  }

  template <typename... Us>
  auto none_of() const -> bool {
    return not is_any<Us...>();
  }

  auto operator<=>(const tag_variant& other) const = default;

  template <class Inspector>
  friend auto inspect(Inspector& f, tag_variant& x) -> bool {
    return f.apply(static_cast<super&>(x));
  }
};

} // namespace tenzir
