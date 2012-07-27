#ifndef VAST_ARCHIVE_H
#define VAST_ARCHIVE_H

#include <cppa/cppa.hpp>

namespace vast {

/// The event archive. It stores events in the form of segments.
class archive : public cppa::sb_actor<archive>
{
  friend class cppa::sb_actor<archive>;

public:
  /// Spawns the archive.
  /// @param directory The root directory of the archive.
  /// @param max_events_per_chunk The maximum number of events per chunk.
  /// @param max_segment_size The maximum segment size in bytes.
  /// @param max_segments The maximum number of segments to keep in memory.
  archive(std::string const& directory,
          size_t max_events_per_chunk,
          size_t max_segment_size,
          size_t max_segments);

private:
  std::vector<cppa::actor_ptr> emitters_;
  cppa::actor_ptr segment_manager_;
  cppa::actor_ptr segmentizer_;
  cppa::behavior init_state;
};

} // namespace vast

#endif
