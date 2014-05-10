#ifndef VAST_PROGRAM_H
#define VAST_PROGRAM_H

#include "vast/actor.h"
#include "vast/configuration.h"

namespace vast {

/// The main program.
class program : public actor_base
{
public:
  /// Spawns the program.
  /// @param config The program configuration.
  program(configuration const& config);

  cppa::behavior act() final;
  char const* describe() const final;

private:
  configuration const& config_;
};

} // namespace vast

#endif
