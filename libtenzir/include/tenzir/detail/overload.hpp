//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <utility>

namespace tenzir::detail {

/// Creates a set of overloaded functions. This utility struct allows for
/// writing inline visitors without having to result to inversion of control.
template <class... Ts>
struct overload : Ts... {
  using Ts::operator()...;
};

/// Explicit deduction guide for overload (not needed as of C++20).
template <class... Ts>
overload(Ts...) -> overload<Ts...>;

} // namespace tenzir::detail
