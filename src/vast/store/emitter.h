#ifndef VAST_STORE_EMITTER_H
#define VAST_STORE_EMITTER_H

#include <mutex>
#include <ze/vertex.h>
#include <ze/actor.h>
#include "vast/store/segment_cache.h"

namespace vast {
namespace store {

/// Reads events from archive's segment cache.
class emitter : public ze::publisher<>
              , public ze::actor<emitter>
{
    friend class ze::actor<emitter>;
    emitter(emitter const&) = delete;
    emitter& operator=(emitter const&) = delete;

public:
    /// Constructs an emitter.
    /// @param c The component the emitter belongs to.
    /// @param cache The cache containing the segments.
    /// @param ids A vector of IDs representing the segments to emit.
    emitter(ze::component& c,
            std::shared_ptr<segment_cache> cache,
            std::vector<ze::uuid> ids);

private:
    void act();

    std::shared_ptr<segment_cache> cache_;
    std::vector<ze::uuid> ids_;
    std::vector<ze::uuid>::const_iterator segment_id_;
    std::shared_ptr<isegment> segment_;
};

} // namespace store
} // namespace vast

#endif
