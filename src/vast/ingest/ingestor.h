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

  /// Support file types for ingestion.
  enum class file_type
  {
    bro1,
    bro2
  };

public:
  /// Sets the initial behavior.
  /// @param archive The archive actor.
  /// @param id_file The event ID file for the ID tracker.
  ingestor(cppa::actor_ptr archive, std::string const& id_file);

private:
  std::queue<std::pair<file_type, std::string>> files_;
  cppa::actor_ptr archive_;
  cppa::actor_ptr id_tracker_;
  cppa::actor_ptr bro_event_source_;
  cppa::actor_ptr reader_;
  cppa::behavior init_state;
};

} // namespace ingest
} // namespace vast

#endif
