#ifndef VAST_ACTOR_LOAD_BALANCER_H
#define VAST_ACTOR_LOAD_BALANCER_H

#include <caf/all.hpp>
#include "vast/actor/actor.h"

namespace vast {

/// Relays a message to one specific worker in a round-robin fashion.
struct load_balancer : flow_controlled_actor
{
  load_balancer()
    : flow_controlled_actor{"load-balancer"}
  {
  }

  void on_exit()
  {
    workers_.clear();
    overloaded_.clear();
  }

  caf::behavior make_behavior() override
  {
    using namespace caf;
    trap_exit(true);
    return
    {
      [=](overload_atom)
      {
        VAST_DEBUG(this, "inserts", current_sender(), "into overload set");
        overloaded_.insert(current_sender());
        if (overloaded_.size() == workers_.size())
          overloaded(true);
      },
      [=](underload_atom)
      {
        VAST_DEBUG(this, "removes", current_sender(), "from overload set");
        overloaded_.erase(current_sender());
        overloaded(false);
      },
      register_upstream_node(),
      [=](exit_msg const& msg)
      {
        if (downgrade_exit())
          return;
        quit(msg.reason);
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
        overloaded_.erase(
            std::remove_if(
                overloaded_.begin(),
                overloaded_.end(),
                [=](caf::actor_addr const& addr)
                {
                  return addr == current_sender();
                }),
            overloaded_.end());
        if (workers_.empty())
          quit(msg.reason);
        else if (i_ == workers_.size())
          i_ = 0;
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
        assert(! workers_.empty());
        actor next;
        do
        {
          next = workers_[i_++];
          i_ %= workers_.size();
        }
        while (overloaded_.contains(next.address()) && ! overloaded());
        forward_to(next);
      }
    };
  }

  size_t i_ = 0;
  std::vector<caf::actor> workers_;
  util::flat_set<caf::actor_addr> overloaded_;
};

} // namespace vast

#endif
