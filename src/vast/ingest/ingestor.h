#ifndef VAST_INGEST_INGESTOR_H
#define VAST_INGEST_INGESTOR_H

#include <queue>
#include <cppa/cppa.hpp>

namespace vast {
namespace ingest {

/// The ingestion component.
class ingestor : public cppa::sb_actor<ingestor>
{
  friend class cppa::sb_actor<ingestor>;

public:
  /// Sets the initial behavior.
  /// @param archive The archive actor.
  ingestor(cppa::actor_ptr archive);

private:
  std::queue<std::string> files_;
  cppa::actor_ptr archive_;
  cppa::actor_ptr bro_event_source_;
  cppa::actor_ptr reader_;
  cppa::behavior init_state;
};

} // namespace ingest
} // namespace vast

#endif
