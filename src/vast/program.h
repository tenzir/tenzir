#ifndef VAST_PROGRAM_H
#define VAST_PROGRAM_H

#include <cppa/cppa.hpp>
#include "vast/configuration.h"

namespace vast {

/// The main program.
class program
{
  program(program const&);
  program& operator=(program);

public:
  /// Spawns the program.
  /// @param config The program configuration.
  program(configuration const& config);

  /// Starts all actors.
  /// @return `true` if starting the actors succeeded.
  bool start();

  /// Sends a shutdown message to all actors and blocks until the last 
  /// actor has terminated.
  void stop();

private:
  configuration const& config_;

  cppa::actor_ptr archive_;
  cppa::actor_ptr index_;
  cppa::actor_ptr tracker_;
  cppa::actor_ptr ingestor_;
  cppa::actor_ptr search_;
  cppa::actor_ptr query_client_;
  cppa::actor_ptr schema_manager_;
  cppa::actor_ptr profiler_;
};

} // namespace vast

#endif
