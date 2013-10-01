#ifndef VAST_PROGRAM_H
#define VAST_PROGRAM_H

#include <cppa/cppa.hpp>
#include "vast/configuration.h"

namespace vast {

/// The main program.
class program
{
  program(program const&) = delete;
  program& operator=(program) = delete;

public:
  /// Constructs the program.
  /// @param config The program configuration.
  program(configuration const& config);

  /// Starts the program and blocks until all actors have terminated.
  /// @returns `true` if the program terminated without errors and `false`
  /// otherwise.
  bool run();

private:
  /// Starts all actors.
  /// @returns `true` if starting the actors succeeded.
  bool start();

  /// Sends a shutdown message to all actors.
  void stop();

  cppa::actor_ptr receiver_;
  cppa::actor_ptr archive_;
  cppa::actor_ptr index_;
  cppa::actor_ptr tracker_;
  cppa::actor_ptr ingestor_;
  cppa::actor_ptr search_;
  cppa::actor_ptr console_;
  cppa::actor_ptr schema_manager_;
  cppa::actor_ptr system_monitor_;
  cppa::actor_ptr profiler_;
  configuration const& config_;
  bool server_ = true;
};

} // namespace vast

#endif
