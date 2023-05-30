//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

namespace vast {

/// A helper that wraps a lambda for lazy evaluation in std::optional::value_or:
///
///   auto x = std::optional<int>{};
///   auto y = x.value_or(lazy{[]{ return 0; }});
///
/// This is useful when computing the fallback value is potentially expensive.
template <class Fun>
struct lazy {
  Fun fun;

  operator decltype(std::move(fun)())() && {
    return std::move(fun)();
  }
};

template <class Fun>
lazy(Fun) -> lazy<Fun>;

#ifndef VAST_LAZY
#  define VAST_LAZY(...)                                                       \
    ::vast::lazy {                                                             \
      [&]() noexcept(noexcept(__VA_ARGS__)) -> decltype(__VA_ARGS__) {         \
        return (__VA_ARGS__);                                                  \
      }                                                                        \
    }
#endif

} // namespace vast
