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
  base()
  {
    high_priority_exit(false);
    attach_functor([=](uint32_t)
    {
      accountant_ = caf::invalid_actor;
    });
  }

  caf::message_handler make_handler() override
  {
    using namespace caf;
    return
    {
      [=](accountant_atom, actor const& accountant)
      {
        VAST_DEBUG(this, "registers accountant", accountant);
        accountant_ = accountant;
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
          send(accountant_, time::now(),
               description() + "-events", uint64_t{v.size()});
      }
    };
  }

  caf::actor accountant_;
};

} // namespace sink
} // namespace vast

#endif
