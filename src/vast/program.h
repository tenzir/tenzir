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
  /// Constructs the program.
  /// @param config The program configuration.
  program(configuration const& config);

  /// Starts the program and blocks until all actors have terminated.
  /// @return `true` if the program terminated without errors and `false`
  /// otherwise.
  bool run();

private:
  /// Starts all actors.
  /// @return `true` if starting the actors succeeded.
  bool start();

  /// Sends a shutdown message to all actors.
  void stop();

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
