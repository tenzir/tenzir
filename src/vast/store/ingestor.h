#ifndef VAST_STORE_INGESTOR_H
#define VAST_STORE_INGESTOR_H

#include "vast/comm/event_source.h"

namespace vast {
namespace store {

/// The ingestion component.
struct ingestor : public ze::component
{
    ingestor(ze::io& io);
    ingestor(ingestor const&) = delete;
    ingestor& operator=(ingestor) = delete;

    comm::event_source source;
};

} // namespace store
} // namespace vast

#endif
