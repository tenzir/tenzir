#ifndef VAST_STORE_SEGMENT_MANAGER_H
#define VAST_STORE_SEGMENT_MANAGER_H

#include <memory>
#include <ze/uuid.h>
#include "vast/util/lru_cache.h"

namespace vast {
namespace store {

class isegment;
typedef util::lru_cache<ze::uuid, std::shared_ptr<isegment>> segment_manager;

} // namespace store
} // namespace vast

#endif
