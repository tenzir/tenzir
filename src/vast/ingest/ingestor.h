#ifndef VAST_INGEST_INGESTOR_H
#define VAST_INGEST_INGESTOR_H

#include <cppa/cppa.hpp>

namespace vast {
namespace ingest {

/// The ingestion component.
class ingestor : cppa::sb_actor<ingestor>
{
public:
    /// Sets the initial behavior.
    /// @param archive The archive actor.
    ingestor(cppa::actor_ptr archive);

    cppa::behavior init_state;

private:
    cppa::actor_ptr bro_event_source_;
    std::vector<cppa::actor_ptr> readers_;

    cppa::actor_ptr archive_;
};

} // namespace ingest
} // namespace vast

#endif
