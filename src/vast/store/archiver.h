#ifndef VAST_STORE_ARCHIVER_H
#define VAST_STORE_ARCHIVER_H

#include <ze/component.h>
#include <ze/sink.h>
#include "vast/fs/path.h"
#include "vast/fs/fstream.h"
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

    ~archiver();

    /// Initializes the archiver.
    /// @param directory The directory in which to archive events.
    /// @param max_chunk_events The maximum number of events per chunk.
    /// @param max_segment_size The maximum segment size in bytes.
    void init(fs::path const& directory,
              size_t max_chunk_events,
              size_t max_segment_size);

private:
    void archive(ze::event_ptr&& event);

    std::unique_ptr<osegment> segment_;
    size_t max_segment_size_;
    fs::ofstream file_;
};

} // namespace store
} // namespace vast

#endif
