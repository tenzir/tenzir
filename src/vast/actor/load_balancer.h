#ifndef VAST_ACTOR_LOAD_BALANCER_H
#define VAST_ACTOR_LOAD_BALANCER_H

#include <caf/all.hpp>
#include "vast/actor/actor.h"

namespace vast {

/// Relays a message to one specific worker in a round-robin fashion.
class load_balancer : public flow_controlled_actor
{
public:
  load_balancer()
  {
    trap_unexpected(false);
    attach_functor(
        [=](uint32_t)
        {
          workers_.clear();
          overloaded_.clear();
        });
  }

  void at(caf::down_msg const& msg) override
  {
    workers_.erase(
        std::remove_if(
            workers_.begin(),
            workers_.end(),
            [=](caf::actor const& a) { return a == last_sender(); }),
        workers_.end());

    overloaded_.erase(
        std::remove_if(
            overloaded_.begin(),
            overloaded_.end(),
            [=](caf::actor_addr const& addr) { return addr == last_sender(); }),
        overloaded_.end());

    if (workers_.empty())
      quit(msg.reason);
    else if (i_ == workers_.size())
      i_ = 0;
  }

  void on_overload(caf::actor_addr const& a) override
  {
    VAST_DEBUG(this, "inserts", a, "into overload set");
    overloaded_.insert(a);
    if (overloaded_.size() == workers_.size())
      become_overloaded();
  }

  void on_underload(caf::actor_addr const& a) override
  {
    VAST_DEBUG(this, "removes", a, "from overload set");
    overloaded_.erase(a);
    become_underloaded();
  }

  caf::message_handler make_handler()
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

  std::string name() const
  {
    return "load-balancer";
  }

private:
  size_t i_ = 0;
  std::vector<caf::actor> workers_;
  util::flat_set<caf::actor_addr> overloaded_;
};

} // namespace vast

#endif
