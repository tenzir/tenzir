#ifndef VAST_ACTOR_SINK_BASE_H
#define VAST_ACTOR_SINK_BASE_H

#include "vast/actor/actor.h"
#include "vast/event.h"
#include "vast/time.h"

namespace vast {
namespace sink {

/// The base class for event sinks.
template <typename Derived>
struct base : public default_actor
{
  base(char const* name = "sink")
    : default_actor{name}
  {
  }

  void on_exit()
  {
    accountant_ = caf::invalid_actor;
  }

  // Allows children too hook exit.
  virtual void finalize() { }

  caf::behavior make_behavior() override
  {
    using namespace caf;
    trap_exit(true);
    return
    {
      [=](exit_msg const& msg)
      {
        finalize();
        quit(msg.reason);
      },
      [=](accountant_atom, actor const& accountant)
      {
        VAST_DEBUG(this, "registers accountant", accountant);
        accountant_ = accountant;
        send(accountant_, label() + "-events", time::now());
      },
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
        assert(! v.empty());
        for (auto& e : v)
          if (! static_cast<Derived*>(this)->process(e))
          {
            VAST_ERROR(this, "failed to process event:", e);
            this->quit(exit::error);
            return;
          }
        if (accountant_)
          send(accountant_, uint64_t{v.size()}, time::snapshot());
      },
      catch_unexpected()
    };
  }

  caf::actor accountant_;
};

} // namespace sink
} // namespace vast

#endif
