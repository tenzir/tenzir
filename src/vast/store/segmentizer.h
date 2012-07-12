#ifndef VAST_STORE_SEGMENTIZER_H
#define VAST_STORE_SEGMENTIZER_H

#include <cppa/cppa.hpp>
#include "vast/store/segment.h"

namespace vast {
namespace store {

/// Writes events into a segment and relays baked segments to the archive.
class segmentizer : public cppa::sb_actor<segmentizer>, public ze::object
{
public:
  /// Spawns a segmentizer.
  /// @param max_events_per_chunk The maximum number of events per chunk.
  /// @param max_segment_size The maximum segment size in bytes.
  segmentizer(size_t max_events_per_chunk, size_t max_segment_size);

private:
  segment segment_;
};

} // namespace store
} // namespace vast

#endif
