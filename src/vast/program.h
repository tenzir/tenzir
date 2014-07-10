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

  cppa::partial_function act() final;
  std::string describe() const final;

  void run();

private:
  cppa::actor receiver_;
  cppa::actor tracker_;
  cppa::actor archive_;
  cppa::actor index_;
  cppa::actor search_;
  configuration config_;
};

} // namespace vast

#endif
