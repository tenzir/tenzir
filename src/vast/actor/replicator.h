#ifndef VAST_ACTOR_REPLICATOR_H
#define VAST_ACTOR_REPLICATOR_H

#include <caf/all.hpp>
#include "vast/actor/actor.h"

namespace vast {

/// Replicates a message by relaying it to a set of workers.
class replicator : public flow_controlled_actor
{
public:
  replicator()
  {
    trap_unexpected(false);
    attach_functor([=](uint32_t) { workers_.clear(); });
  }

  void at(caf::down_msg const& msg) override
  {
    workers_.erase(
        std::remove_if(
            workers_.begin(),
            workers_.end(),
            [=](caf::actor const& a) { return a == last_sender(); }),
        workers_.end());

    if (workers_.empty())
      quit(msg.reason);
  }

  caf::message_handler make_handler() override
  {
    using namespace caf;

    return
    {
      [=](add_atom, worker_atom, actor a)
      {
        VAST_DEBUG(this, "adds worker", a);
        monitor(a);
        workers_.push_back(std::move(a));
      },
      [=](workers_atom)
      {
        return workers_;
      },
      others() >> [=]
      {
        auto sender = actor_cast<actor>(last_sender());
        for (auto& w : workers_)
          send_as(sender, w, last_dequeued());
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
