#ifndef VAST_STORE_SEGMENT_CACHE_H
#define VAST_STORE_SEGMENT_CACHE_H

#include <memory>
#include <ze/uuid.h>
#include "vast/store/forward.h"
#include "vast/util/lru_cache.h"

namespace vast {
namespace store {

typedef util::lru_cache<ze::uuid, std::shared_ptr<isegment>> segment_cache;

} // namespace store
} // namespace vast

#endif
