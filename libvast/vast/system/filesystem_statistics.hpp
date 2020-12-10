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

#include <cstdint>

namespace vast::system {

/// Statistics about filesystem operations.
struct filesystem_statistics {
  struct ops {
    uint64_t successful = 0;
    uint64_t failed = 0;
    uint64_t bytes = 0;

    template <class Inspector>
    friend auto inspect(Inspector& f, ops& x) ->
      typename Inspector::result_type {
      return f(caf::meta::type_name("vast.system.filesystem_statistics.ops"),
               x.successful, x.failed, x.bytes);
    }
  };

  ops writes;
  ops reads;
  ops mmaps;

  template <class Inspector>
  friend auto inspect(Inspector& f, filesystem_statistics& x) ->
    typename Inspector::result_type {
    return f(caf::meta::type_name("vast.system.filesystem_statistics"),
             x.writes, x.reads, x.mmaps);
  }
};

} // namespace vast::system
