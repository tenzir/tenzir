//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

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

  ops checks;
  ops writes;
  ops reads;
  ops mmaps;
  ops erases;

  template <class Inspector>
  friend auto inspect(Inspector& f, filesystem_statistics& x) ->
    typename Inspector::result_type {
    return f(caf::meta::type_name("vast.system.filesystem_statistics"),
             x.checks, x.writes, x.reads, x.mmaps);
  }
};

} // namespace vast::system
