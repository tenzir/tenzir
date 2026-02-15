//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <type_traits>
#include <variant>

namespace tenzir {

using Unit = std::monostate;

template <class T>
using VoidToUnit = std::conditional_t<std::is_void_v<T>, Unit, T>;

/// Converts a `VoidToUnit<T>` value back to `T`. When `T` is `void`, this
/// discards the `Unit` value and returns `void`. Otherwise, it forwards `T`.
template <class T>
auto unit_to_void(VoidToUnit<T>&& value) -> T {
  if constexpr (std::is_void_v<T>) {
    (void)value;
  } else {
    return std::move(value);
  }
}

} // namespace tenzir
