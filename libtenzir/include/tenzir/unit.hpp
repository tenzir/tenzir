//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <concepts>
#include <type_traits>
#include <variant>

namespace tenzir {

using Unit = std::monostate;

template <class T>
using VoidToUnit = std::conditional_t<std::is_void_v<T>, Unit, T>;

template <class T>
using UnitToVoid
  = std::conditional_t<std::same_as<std::remove_cvref_t<T>, Unit>, void, T>;

/// Converts a `Unit` or non-`Unit` value back. Returns `void` for `Unit`,
/// otherwise forwards the value.
template <class T>
auto unit_to_void(T&& value) -> UnitToVoid<T> {
  if constexpr (std::same_as<std::remove_cvref_t<T>, Unit>) {
    (void)value;
  } else {
    return std::forward<T>(value);
  }
}

} // namespace tenzir
