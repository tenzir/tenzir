#ifndef VAST_SINK_BASE_H
#define VAST_SINK_BASE_H

#include "vast/actor.h"
#include "vast/event.h"

namespace vast {
namespace sink {

/// The base class for event sinks.
template <typename Derived>
struct base : public actor_base
{
  cppa::partial_function act() final
  {
    return
    {
      [=](event const& e)
      {
        if (! static_cast<Derived*>(this)->process(e))
        {
          VAST_LOG_ACTOR_ERROR("failed to process event: " << e);
          quit(exit::error);
        }
      }
    };
  }
};

} // namespace sink
} // namespace vast

#endif
