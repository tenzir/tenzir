#ifndef VAST_STORE_ARCHIVER_H
#define VAST_STORE_ARCHIVER_H

#include <ze/component.h>
#include <ze/sink.h>
#include "vast/fs/path.h"
#include "vast/store/segment.h"

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

    /// Initializes the archiver.
    /// @param directory The directory in which to archive events.
    void init(fs::path const& directory);

private:
    void archive(ze::event_ptr&& event);

    std::unique_ptr<osegment> segment_;
};

} // namespace store
} // namespace vast

#endif
