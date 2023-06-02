//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/time.hpp"

#include <cstddef>
#include <cstdint>

namespace vast {

/// Statistics about a query.
struct query_status {
  duration runtime = {};  ///< Current runtime.
  size_t expected = 0;    ///< The count of candidate partitions.
  size_t scheduled = 0;   ///< The number of currently scheduled partitions
                          ///  at the INDEX.
  size_t received = 0;    ///< The number of already completed partitions.
  uint64_t processed = 0; ///< Processed candidate events.
  uint64_t shipped = 0;   ///< Shipped results to the SINK.
  uint64_t requested = 0; ///< User-requested pending results to extract.
  uint64_t cached = 0;    ///< Currently available results for the SINK.
};

template <class Inspector>
auto inspect(Inspector& f, query_status& qs) {
  return f.object(qs)
    .pretty_name("query_status")
    .fields(f.field("runtime", qs.runtime), f.field("expected", qs.expected),
            f.field("scheduled", qs.scheduled),
            f.field("received", qs.received),
            f.field("processed", qs.processed), f.field("shipped", qs.shipped),
            f.field("requested", qs.requested));
}

} // namespace vast
