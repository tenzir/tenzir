#ifndef VAST_REPLICATOR_H
#define VAST_REPLICATOR_H

#include <caf/all.hpp>
#include "vast/actor.h"

namespace vast {

/// Replicates a message by relaying it to a set of workers.
class replicator : public actor_mixin<replicator, flow_controlled>
{
public:
  replicator()
  {
    attach_functor(
        [=](uint32_t reason)
        {
          for (auto& a : workers_)
            anon_send_exit(a, reason);
          workers_.clear();
        });
  }

  void at_down(caf::down_msg const& down)
  {
    workers_.erase(
        std::remove_if(
            workers_.begin(),
            workers_.end(),
            [=](caf::actor const& a) { return a == last_sender(); }),
        workers_.end());

    if (workers_.empty())
      quit(down.reason);
  }

  caf::message_handler make_handler()
  {
    using namespace caf;

    return
    {
      on(atom("add"), atom("worker"), arg_match) >> [=](actor a)
      {
        VAST_DEBUG(this, "adds worker", a);
        monitor(a);
        workers_.push_back(std::move(a));
      },
      others() >> [=]
      {
        for (auto& a : workers_)
          // FIXME: use a method of sending appropriate for 1-n communication.
          //send_tuple_as(last_sender(), a, last_dequeued());
          forward_to(a);

        return workers_;
      }
    };
  }

  std::string name() const
  {
    return "replicator";
  }

private:
  std::vector<caf::actor> workers_;
};

} // namespace vast

#endif
