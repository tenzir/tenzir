#ifndef VAST_STORE_EMITTER_H
#define VAST_STORE_EMITTER_H

#include <mutex>
#include <ze/vertex.h>
#include "vast/store/segment_cache.h"

namespace vast {
namespace store {

/// Reads events from archive's segment cache.
class emitter : public ze::publisher<>
              , public std::enable_shared_from_this<emitter>
{
    emitter(emitter const&) = delete;
    emitter& operator=(emitter const&) = delete;

public:
    /// Emitter state.
    enum state
    {
        stopped,
        paused,
        running,
        finished,
    };

    /// Constructs an emitter.
    /// @param c The component the emitter belongs to.
    /// @param cache The cache containing the segments.
    /// @param ids A vector of IDs representing the segments to emit.
    emitter(ze::component& c,
            std::shared_ptr<segment_cache> cache,
            std::vector<ze::uuid> ids);

    /// Stars the emission process by scheduling a task.
    state start();

    /// Temporarily stops the emission of events.
    state pause();

private:
    // Note: emitting chunks asynchronously could lead to segment thrashing in
    // the cache if the ingestion rate is very high.
    void emit();

    std::mutex mutex_;
    state state_ = stopped;
    std::shared_ptr<segment_cache> cache_;
    std::vector<ze::uuid> ids_;
    std::vector<ze::uuid>::const_iterator current_;
};

} // namespace store
} // namespace vast

#endif
