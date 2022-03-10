//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/time.hpp"

#include <caf/meta/type_name.hpp>

#include <cstddef>
#include <cstdint>

namespace vast::system {

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
  return f(caf::meta::type_name("query_status"), qs.runtime, qs.expected,
           qs.scheduled, qs.received, qs.processed, qs.shipped, qs.requested);
}

} // namespace vast::system
