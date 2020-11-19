/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <tuple>
#include <type_traits>

namespace vast::detail {

/// A callable that does nothing. Keeps copies of variables passed to it in its
/// constructor. This is useful for sharing lifetimes in custom deleters.
template <class... Ts>
struct keeper final : private std::tuple<Ts...> {
  using std::tuple<Ts...>::tuple;

  template <class... Args>
  constexpr void operator()(Args&&...) const noexcept {
    // nop
  }
};

/// Explicit deduction guide for keeper (not needed as of C++20).
template <class... Ts>
keeper(Ts...) -> keeper<Ts...>;

/// @returns A callable that does nothing, but keeps copies of passed variables.
/// @param args Variables to keep copies of.
template <class... Args>
constexpr auto keep(Args&&... args) noexcept {
  return keeper{std::forward<Args>(args)...};
}

} // namespace vast::detail
