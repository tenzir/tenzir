#ifndef VAST_ACTOR_SINK_csv_HPP
#define VAST_ACTOR_SINK_csv_HPP

#include <iosfwd>
#include <memory>

#include "vast/type.hpp"
#include "vast/actor/sink/base.hpp"

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
behavior csv(stateful_actor<csv_state>* self,
             std::unique_ptr<std::ostream> out);

} // namespace sink
} // namespace vast

#endif
