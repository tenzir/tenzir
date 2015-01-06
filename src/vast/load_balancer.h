#ifndef VAST_LOAD_BALANCER_H
#define VAST_LOAD_BALANCER_H

#include <caf/all.hpp>
#include "vast/actor.h"

namespace vast {

/// Relays a message to one specific worker in a round-robin fashion.
class load_balancer : public actor_mixin<replicator, flow_controlled>
{
public:
  load_balancer()
  {
    attach_functor(
        [=](uint32_t reason)
        {
          for (auto& a : workers_)
            anon_send_exit(a, reason);
          workers_.clear();
          overloaded.clear();
        });
  }

  void at_down(down_msg const& down)
  {
    workers_.erase(
        std::remove_if(
            workers_.begin(),
            workers_.end(),
            [=](actor const& a) { return a == last_sender() }),
        workers_.end());

    overloaded_.erase(
        std::remove_if(
            overloaded_.begin(),
            overloaded_.end(),
            [=](actor const& a) { return a == last_sender() }),
        overloaded_.end());

    if (workers_.empty())
      quit(down.reason);
    else if (i_ == workers_.size())
      i_ = 0;
  }

  void on_overload(base_actor* self)
  {
    using namespace caf;
    VAST_DEBUG(self, "inserts", a, "into overload set");
    overloaded_.insert(a);

    if (overloaded_.size() < workers_.size())
      return;

    flow_controlled::on_overload();

    self->become(
        keep_behavior,
        [=](flow_control::overload const&)
        {
          VAST_DEBUG(self, "ignores overload signal");
        },
        [=](flow_control::underload const&)
        {
          VAST_DEBUG(self, "removes", last_sender(), "from overload set");
          overloaded_.erase(last_sender());
          flow_controlled::on_underload();
          VAST_DEBUG(self, "unbecomes overloaded");
          unbecome();
        });
  }

  void on_underload(base_actor* self)
  {
    VAST_DEBUG(self, "removes", self->last_sender(), "from overload set");
    overloaded_.erase(self->last_sender());
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
        assert(! workers_.empty());
        actor next;
        do
        {
          next = workers_[i_++];
          i_ %= workers_.size();
        }
        while (overloaded_.contains(next.address()));

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
