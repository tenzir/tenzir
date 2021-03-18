// SPDX-FileCopyrightText: (c) 2018 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/time.hpp"

#include <caf/meta/type_name.hpp>

#include <cstddef>
#include <cstdint>

namespace vast::system {

/// Statistics about a query.
struct query_status {
  duration runtime;            ///< Current runtime.
  size_t expected = 0;         ///< Expected ID sets from INDEX.
  size_t scheduled = 0;        ///< Scheduled partitions (ID sets) at INDEX.
  size_t received = 0;         ///< Received ID sets from INDEX.
  size_t lookups_issued = 0;   ///< Number of lookups sent to the ARCHIVE.
  size_t lookups_complete = 0; ///< Number of lookups returned by the ARCHIVE.
  uint64_t processed = 0;      ///< Processed candidates from ARCHIVE.
  uint64_t shipped = 0;        ///< Shipped results to the SINK.
  uint64_t requested = 0;      ///< User-requested pending results to extract.
  uint64_t cached = 0;         ///< Currently available results for the SINK.
};

template <class Inspector>
auto inspect(Inspector& f, query_status& qs) {
  return f(caf::meta::type_name("query_status"), qs.runtime, qs.expected,
           qs.scheduled, qs.received, qs.lookups_issued, qs.lookups_complete,
           qs.processed, qs.shipped, qs.requested);
}

} // namespace vast::system
