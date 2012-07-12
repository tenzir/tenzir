#ifndef VAST_STORE_ARCHIVE_H
#define VAST_STORE_ARCHIVE_H

#include <memory>
#include <ze/uuid.h>
#include "vast/store/segment_manager.h"
#include "vast/store/segmentizer.h"

namespace vast {
namespace store {

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
    /// Creates an emitter that sends its events to a given sink.
    /// @param sink The actor to send the events to.
    void create_emitter(cppa::actor_ptr sink);

    /// Removes an emitter.
    /// @param id The ID of the emitter.
    void remove_emitter(ze::uuid const& id);

    /// Scans through a directory for segments and records their path.
    /// @param directory The directory to scan.
    void scan(fs::path const& directory);

    /// Loads a segment into memory after a cache miss.
    std::shared_ptr<isegment> load(ze::uuid const& id);

    fs::path archive_root_;
    segment_manager segment_manager_;
    std::mutex segment_files_mutex_;
    std::unordered_map<ze::uuid, fs::path> segment_files_;
    std::vector<cppa::actor_ptr> emitters_;

    cppa::actor_ptr segmentizer_;
    cppa::actor_ptr writer_;
    cppa::behavior init_state;
};

} // namespace store
} // namespace vast

#endif
