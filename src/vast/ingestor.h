#ifndef VAST_INGESTOR_H
#define VAST_INGESTOR_H

#include <queue>
#include <cppa/cppa.hpp>

namespace vast {

/// The ingestion component.
class ingestor : public cppa::sb_actor<ingestor>
{
  friend class cppa::sb_actor<ingestor>;

public:
  /// Sets the initial behavior.
  /// @param archive The archive actor.
  /// @param id_file The event ID file for the ID tracker.
  ingestor(cppa::actor_ptr archive, std::string const& id_file);

private:
  std::queue<cppa::actor_ptr> file_sources_;
  cppa::actor_ptr archive_;
  cppa::actor_ptr tracker_;
  cppa::actor_ptr broccoli_;
  cppa::behavior init_state;
};

} // namespace vast

#endif
