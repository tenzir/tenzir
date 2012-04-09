#ifndef VAST_STORE_EMITTER_H
#define VAST_STORE_EMITTER_H

#include <ze/source.h>
#include "vast/store/segment_cache.h"

namespace vast {
namespace store {

/// Reads events from the archive on disk.
class emitter : public ze::core_source<ze::event>
{
    emitter(emitter const&) = delete;
    emitter& operator=(emitter const&) = delete;

public:
    /// Constructs an emitter.
    /// @param c The component the emitter belongs to.
    /// @param cache The cache containing the segments.
    /// @param ids A vector of IDs for the segments to emit.
    emitter(ze::component& c,
            std::shared_ptr<segment_cache> cache,
            std::vector<ze::uuid> ids);

    /// Starts the emitter and blocks.
    void run();

private:
    void emit(ze::event_ptr&& event);

    std::shared_ptr<segment_cache> cache_;
    std::vector<ze::uuid> ids_;
};

} // namespace store
} // namespace vast

#endif
