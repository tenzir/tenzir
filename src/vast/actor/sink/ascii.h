#ifndef VAST_ACTOR_SINK_ASCII_H
#define VAST_ACTOR_SINK_ASCII_H

#include <iosfwd>
#include <memory>

#include "vast/actor/sink/base.h"

namespace vast {
namespace sink {

struct ascii_state : state {
  ascii_state(local_actor *self);

  bool process(event const& e) override;

  void flush() override;

  std::unique_ptr<std::ostream> out;
};

/// A sink dumping events in plain ASCII.
/// @param self The actor handle.
/// @param out The stream to print received events into.
behavior ascii(stateful_actor<ascii_state>* self,
               std::unique_ptr<std::ostream> out);

} // namespace sink
} // namespace vast

#endif
