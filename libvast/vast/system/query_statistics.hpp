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

#ifndef VAST_SYSTEM_QUERY_STATISTICS_HPP
#define VAST_SYSTEM_QUERY_STATISTICS_HPP

#include <cstddef>
#include <cstdint>

#include "vast/time.hpp"

namespace vast {
namespace system {

/// Statistics about a query.
struct query_statistics {
  timespan runtime;         ///< Current runtime.
  size_t expected = 0;      ///< Expected bitmaps from INDEX.
  size_t scheduled = 0;     ///< Scheduled partitions (bitmaps) at INDEX.
  size_t received = 0;      ///< Received bitmaps from INDEX.
  uint64_t processed = 0;   ///< Processed candidates from ARCHIVE.
  uint64_t shipped = 0;     ///< Shipped results to sink.
  uint64_t requested = 0;   ///< User-requested pending results to extract.
};

template <class Inspector>
auto inspect(Inspector& f, query_statistics& qs) {
  return f(qs.runtime, qs.expected, qs.scheduled, qs.received, qs.processed,
           qs.shipped, qs.requested);
}

} // namespace system
} // namespace vast

#endif
