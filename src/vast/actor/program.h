#ifndef VAST_ACTOR_PROGRAM_H
#define VAST_ACTOR_PROGRAM_H

#include "vast/configuration.h"
#include "vast/actor/actor.h"
#include "vast/trial.h"

namespace vast {

/// The main program.
class program : public actor_mixin<program, sentinel>
{
public:
  /// Spawns the program.
  /// @param config The program configuration.
  program(configuration config);

  caf::message_handler make_handler();
  std::string name() const;

private:
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
