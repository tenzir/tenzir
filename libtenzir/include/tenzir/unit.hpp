//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <concepts>
#include <functional>
#include <type_traits>
#include <utility>
#include <variant>

namespace tenzir {

using Unit = std::monostate;

template <class T>
using VoidToUnit = std::conditional_t<std::is_void_v<T>, Unit, T>;

template <class T>
using UnitToVoid
  = std::conditional_t<std::same_as<std::remove_cvref_t<T>, Unit>, void, T>;

/// Forwards a value, converting `Unit` to `void`.
template <class T>
auto unit_to_void(T&& value) -> UnitToVoid<T> {
  if constexpr (std::same_as<std::remove_cvref_t<T>, Unit>) {
    (void)value;
  } else {
    return std::forward<T>(value);
  }
}

/// Forwards a function's return value, converting `void` to `Unit`.
///
/// This takes a function to invoke as there are no `void` parameters.
template <class F>
auto void_to_unit(F&& f) -> VoidToUnit<std::invoke_result_t<F>> {
  if constexpr (std::is_void_v<std::invoke_result_t<F>>) {
    std::invoke(std::forward<F>(f));
    return Unit{};
  } else {
    return std::invoke(std::forward<F>(f));
  }
}

} // namespace tenzir
