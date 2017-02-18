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
