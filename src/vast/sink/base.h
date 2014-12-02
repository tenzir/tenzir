#ifndef VAST_SINK_BASE_H
#define VAST_SINK_BASE_H

#include "vast/actor.h"
#include "vast/event.h"

namespace vast {
namespace sink {

/// The base class for event sinks.
template <typename Derived>
struct base : public actor_mixin<base<Derived>, sentinel>
{
  caf::message_handler make_handler()
  {
    using namespace caf;
    this->trap_exit(true);

    return
    {
      [=](exit_msg const& msg)
      {
        static_cast<Derived*>(this)->finalize();
        this->quit(msg.reason);
      },
      [=](event const& e)
      {
        if (! static_cast<Derived*>(this)->process(e))
        {
          VAST_LOG_ACTOR_ERROR("failed to process event: " << e);
          this->quit(exit::error);
        }
      },
      [=](std::vector<event> const& v)
      {
        for (auto& e : v)
          if (! static_cast<Derived*>(this)->process(e))
          {
            VAST_LOG_ACTOR_ERROR("failed to process event: " << e);
            this->quit(exit::error);
            return;
          }
      }
    };
  }

  void finalize()
  {
    // Do nothing, allow children to overwrite.
  }
};

} // namespace sink
} // namespace vast

#endif
