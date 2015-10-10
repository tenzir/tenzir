#ifndef VAST_ACTOR_SINK_csv_H
#define VAST_ACTOR_SINK_csv_H

#include <iosfwd>
#include <memory>

#include "vast/type.h"
#include "vast/actor/sink/base.h"

namespace vast {
namespace sink {

struct csv_state : state {
  csv_state(local_actor *self);

  bool process(event const& e) override;

  void flush() override;

  std::unique_ptr<std::ostream> out;
  vast::type type;
};

/// A sink dumping events as CSV.
/// @param self The actor handle.
/// @param out The stream to print received events into.
behavior csv(stateful_actor<csv_state>* self, std::ostream* out);

} // namespace sink
} // namespace vast

#endif
