#ifndef VAST_ACTOR_REPLICATOR_H
#define VAST_ACTOR_REPLICATOR_H

#include <caf/all.hpp>
#include "vast/actor/actor.h"

namespace vast {

/// Replicates a message by relaying it to a set of workers.
struct replicator : flow_controlled_actor
{
  replicator()
    : flow_controlled_actor{"replicator"}
  {
  }

  void on_exit()
  {
    workers_.clear();
  };

  caf::behavior make_behavior() override
  {
    using namespace caf;
    trap_exit(true);
    return
    {
      forward_overload(),
      forward_underload(),
      register_upstream_node(),
      [=](exit_msg const&)
      {
        if (downgrade_exit())
          return;
      },
      [=](down_msg const& msg)
      {
        if (remove_upstream_node(msg.source))
          return;
        workers_.erase(
            std::remove_if(
                workers_.begin(),
                workers_.end(),
                [=](caf::actor const& a) { return a == current_sender(); }),
            workers_.end());
        if (workers_.empty())
          quit(msg.reason);
      },
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
        auto sender = actor_cast<actor>(current_sender());
        for (auto& w : workers_)
          send_as(sender, w, current_message());
      }
    };
  }

  std::vector<caf::actor> workers_;
};

} // namespace vast

#endif
