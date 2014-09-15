#ifndef VAST_PROGRAM_H
#define VAST_PROGRAM_H

#include "vast/actor.h"
#include "vast/configuration.h"

namespace vast {

class configuration;

/// The main program.
class program : public actor_base
{
public:
  /// Spawns the program.
  /// @param config The program configuration.
  program(configuration config);

  caf::message_handler act() final;
  std::string describe() const final;

private:
  void run();

  caf::actor receiver_;
  caf::actor identifier_;
  caf::actor archive_;
  caf::actor index_;
  caf::actor search_;
  configuration config_;
};

} // namespace vast

#endif
