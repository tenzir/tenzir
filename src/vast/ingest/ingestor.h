#ifndef VAST_INGEST_INGESTOR_H
#define VAST_INGEST_INGESTOR_H

#include "vast/fs/path.h"
#include "vast/comm/event_source.h"
#include "vast/ingest/forward.h"

namespace vast {
namespace ingest {

/// The ingestion component.
class ingestor : public ze::component
{
    ingestor(ingestor const&) = delete;
    ingestor& operator=(ingestor) = delete;

public:
    ingestor(ze::io& io);

    /// Ingests events from a file.
    /// @param filename The name of the file to ingest.
    std::shared_ptr<reader> make_reader(fs::path const& filename);

    comm::event_source source;
};

} // namespace ingest
} // namespace vast

#endif
