#ifndef VAST_STORE_SEGMENTIZER_H
#define VAST_STORE_SEGMENTIZER_H

#include <mutex>
#include <ze/vertex.h>
#include "vast/fs/path.h"
#include "vast/store/forward.h"

namespace vast {
namespace store {

/// Writes events into a segment.
class segmentizer
  : public ze::device<ze::subscriber<>, ze::publisher<osegment>>
{
    segmentizer(segmentizer const&) = delete;
    segmentizer& operator=(segmentizer const&) = delete;

public:
    typedef ze::device<ze::subscriber<>, ze::publisher<osegment>> device;

    /// Constructs a segmentizer.
    /// @param c The component the segmentizer belongs to.
    /// @param f The function to invoke after rotating a segment.
    segmentizer(ze::component& c);

    /// Destroys a segmentizer.
    ~segmentizer();

    /// Initializes a segmentizer.
    /// @param max_events_per_chunk The maximum number of events per chunk.
    /// @param max_segment_size The maximum segment size in bytes.
    void init(size_t max_events_per_chunk, size_t max_segment_size);

    /// Signals the segmentizer to finish outstanding operations. This involves
    /// flushing the current segment to disk.
    void stop();

private:
    /// Writes an event into the output segment.
    void write(ze::event_ptr event);

    bool terminating_;
    std::mutex segment_mutex_;
    size_t max_segment_size_;
    size_t max_events_per_chunk_;
    fs::path archive_directory_;

    ze::intrusive_ptr<osegment> segment_;
};

} // namespace store
} // namespace vast

#endif
