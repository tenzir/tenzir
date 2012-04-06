#ifndef VAST_STORE_ARCHIVER_H
#define VAST_STORE_ARCHIVER_H

#include <mutex>
#include <ze/component.h>
#include <ze/sink.h>
#include "vast/fs/path.h"
#include "vast/store/forward.h"

namespace vast {
namespace store {

/// Writes events to disk.
class archiver : public ze::component<ze::event>::sink
{
    archiver(archiver const&) = delete;
    archiver& operator=(archiver const&) = delete;

public:
    /// Constructs an archiver.
    /// @param c The component the archiver belongs to.
    archiver(ze::component<ze::event>& c);

    /// Destroys an archiver.
    ~archiver();

    /// Initializes an archiver.
    /// @param directory The archive directory.
    /// @param max_events_per_chunk The maximum number of events per chunk.
    /// @param max_segment_size The maximum segment size in bytes.
    void init(fs::path const& directory,
              size_t max_events_per_chunk,
              size_t max_segment_size);

private:
    void archive(ze::event_ptr&& event);

    /// Flushes the current segment to the filesystem.
    /// @return A pointer to the flushed segment.
    std::unique_ptr<osegment> flush();

    std::mutex segment_mutex_;
    size_t max_segment_size_;
    size_t max_events_per_chunk_;
    fs::path archive_directory_;

    std::unique_ptr<osegment> segment_;
};

} // namespace store
} // namespace vast

#endif
