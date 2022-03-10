//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include <caf/actor_cast.hpp>

#include <utility>

namespace vast::detail {

/// A functor that delegates to `caf::actor_cast`. Instances of it behave the
/// same as the following code that will be valid come C++20:
///
///   []<typename Out>(auto&& in) {
///     return caf::actor_cast<Out>(
///       std::forward<decltype(in)>(in));
///   }
struct actor_cast_wrapper {
  template <class Out, class In>
  Out operator()(In&& in) {
    return caf::actor_cast<Out>(std::forward<In>(in));
  }
};

} // namespace vast::detail
