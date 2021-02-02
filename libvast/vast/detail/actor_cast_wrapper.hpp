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
