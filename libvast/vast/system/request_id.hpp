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

#include <caf/meta/type_name.hpp>

#include <utility>

namespace vast::system {

/// The unique identifier of a request.
struct request_id {
  /// Advances the request ID, and returns the current ID.
  request_id advance() {
    return std::exchange(*this, {key + 1});
  }

  /// The internal key of the request ID.
  uint64_t key = 0;

  /// Enable basic comparison for use as keys in containers.
  friend bool operator<(const request_id& lhs, const request_id& rhs) {
    return lhs.key < rhs.key;
  }

  /// Opt-in to CAF's type inspection API.
  template <class Inspector>
  friend auto inspect(Inspector& f, request_id& x) ->
    typename Inspector::result_type {
    return f(caf::meta::type_name("vast.system.request_id"), x.key);
  }
};

} // namespace vast::system
