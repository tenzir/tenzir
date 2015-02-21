#ifndef VAST_ACTOR_PROGRAM_H
#define VAST_ACTOR_PROGRAM_H

#include "vast/configuration.h"
#include "vast/actor/actor.h"
#include "vast/trial.h"

namespace vast {

/// The main program.
struct program : default_actor
{
  /// Spawns the program.
  /// @param config The program configuration.
  program(configuration config);

  void on_exit();
  caf::behavior make_behavior() override;

  trial<void> run();

  caf::actor receiver_;
  caf::actor tracker_;
  caf::actor archive_;
  caf::actor index_;
  caf::actor search_;
  caf::actor importer_;
  caf::actor exporter_;
  configuration config_;
};

} // namespace vast

#endif
