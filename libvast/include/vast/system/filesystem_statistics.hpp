//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include <cstdint>

namespace vast::system {

/// Statistics about filesystem operations.
struct filesystem_statistics {
  struct ops {
    uint64_t successful = 0;
    uint64_t failed = 0;
    uint64_t bytes = 0;

    template <class Inspector>
    friend auto inspect(Inspector& f, ops& x) {
      return f.object(x)
        .pretty_name("vast.system.filesystem_statistics.ops")
        .fields(f.field("successful", x.successful),
                f.field("failed", x.failed), f.field("bytes", x.bytes));
    }
  };

  ops checks;
  ops writes;
  ops reads;
  ops mmaps;
  ops erases;
  ops moves;

  template <class Inspector>
  friend auto inspect(Inspector& f, filesystem_statistics& x) {
    return f.object(x)
      .pretty_name("vast.system.filesystem_statistics")
      .fields(f.field("checks", x.checks), f.field("writes", x.writes),
              f.field("reads", x.reads), f.field("mmaps", x.mmaps),
              f.field("moves", x.moves));
  }
};

} // namespace vast::system
