#ifndef VAST_STORE_INGEST_H
#define VAST_STORE_INGEST_H

#include "vast/store/archiver.h"
#include "vast/comm/event_source.h"

namespace vast {
namespace store {

/// The ingestion component.
class ingest : public comm::event_component
{
    ingest(ingest const&) = delete;
    ingest& operator=(ingest const&) = delete;

public:
    ingest(ze::io& io);

    void init(std::string const& ip,
              unsigned port,
              std::vector<std::string> const& events,
              fs::path const& directory,
              size_t max_chunk_events,
              size_t max_segment_size);

    void stop();

private:
    comm::event_source event_source_;
    archiver archiver_;
};

} // namespace store
} // namespace vast

#endif
