#ifndef VAST_PROGRAM_H
#define VAST_PROGRAM_H

#include "vast/actor.h"
#include "vast/configuration.h"

namespace vast {

/// The main program.
class program : public actor<program>
{
  program(program const&) = delete;
  program& operator=(program) = delete;

public:
  /// Spawns the program.
  /// @param config The program configuration.
  program(configuration const& config);

  void act();
  char const* description() const;

private:
  configuration const& config_;
};

} // namespace vast

#endif
