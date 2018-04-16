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

#include "vast/concept/hashable/hash_append.hpp"

namespace vast {

/// The universal hash function.
template <class Hasher>
class uhash {
public:
  using result_type = typename Hasher::result_type;

  template <class... Ts>
  uhash(Ts&&... xs) : h_(std::forward<Ts>(xs)...) {
  }

  template <class T>
  result_type operator()(const T& x) noexcept {
    hash_append(h_, x);
    return static_cast<result_type>(h_);
  }

private:
  Hasher h_;
};

} // namespace vast

