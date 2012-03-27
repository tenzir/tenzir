#ifndef VAST_STORE_INGESTION_H
#define VAST_STORE_INGESTION_H

#include "vast/store/archiver.h"
#include "vast/comm/event_source.h"

namespace vast {
namespace store {

/// The ingestion component.
class ingestion : public comm::event_component
{
    ingestion(ingestion const&) = delete;
    ingestion& operator=(ingestion const&) = delete;

public:
    ingestion(ze::io& io);

    void init(std::string const& ip,
              unsigned port,
              fs::path const& directory);

    void stop();

private:
    comm::event_source event_source_;
    archiver archiver_;
};

} // namespace store
} // namespace vast

#endif
