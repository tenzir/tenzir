#ifndef VAST_ACTOR_SINK_BASE_H
#define VAST_ACTOR_SINK_BASE_H

#include "vast/actor/actor.h"
#include "vast/event.h"

namespace vast {
namespace sink {

/// The base class for event sinks.
template <typename Derived>
struct base : public default_actor
{
  base()
  {
    high_priority_exit(false);
  }

  caf::message_handler make_handler() override
  {
    using namespace caf;
    return
    {
      [=](event const& e)
      {
        if (! static_cast<Derived*>(this)->process(e))
        {
          VAST_ERROR(this, "failed to process event:", e);
          this->quit(exit::error);
        }
      },
      [=](std::vector<event> const& v)
      {
        for (auto& e : v)
          if (! static_cast<Derived*>(this)->process(e))
          {
            VAST_ERROR(this, "failed to process event:", e);
            this->quit(exit::error);
            return;
          }
      }
    };
  }
};

} // namespace sink
} // namespace vast

#endif
