#ifndef VAST_ACTOR_TASK_H
#define VAST_ACTOR_TASK_H

#include <set>
#include <caf/all.hpp>
#include "vast/actor/actor.h"
#include "vast/util/flat_set.h"

namespace vast {

/// An abstraction of a task where each work item consists of an actor. The
/// task completes as soon as all registered items send either a DONE atom or
/// terminate.
class task : public default_actor
{
public:
  /// Spawns a task.
  /// @param exit_reason The exit reason upon to use upon completion.
  task(uint32_t exit_reason = exit::done)
    : exit_reason_{exit_reason}
  {
    attach_functor(
      [=](uint32_t)
      {
        actors_.clear();
        subscribers_.clear();
        supervisors_.clear();
      });
  }

  void at(caf::down_msg const& msg) override
  {
    if (actors_.erase(msg.source) == 1)
      notify();
  }

  caf::message_handler make_handler() override
  {
    using namespace caf;
    return
    {
      [=](actor const& a)
      {
        VAST_TRACE(this, "registers actor", a);
        monitor(a);
        actors_.insert(a.address());
        ++total_;
      },
      on(atom("done"), arg_match) >> [=](actor const& a)
      {
        VAST_TRACE(this, "manually completed actor", a);
        if (actors_.erase(a.address()) == 1)
        {
          demonitor(a);
          notify();
        }
      },
      on(atom("done")) >> [=]
      {
        if (actors_.erase(last_sender()) == 1)
        {
          demonitor(last_sender());
          notify();
          return;
        }
        VAST_ERROR(this, "got DONE from unregistered actor:", last_sender());
        quit(exit::error);
      },
      on(atom("supervisor"), arg_match) >> [=](actor const& a)
      {
        VAST_TRACE(this, "notifies", a, "about task completion");
        supervisors_.insert(a);
      },
      on(atom("subscriber"), arg_match) >> [=](actor const& a)
      {
        VAST_TRACE(this, "notifies", a, "on task status chagne");
        subscribers_.insert(a);
      },
      on(atom("progress")) >> [=]
      {
        return make_message(uint64_t{actors_.size()}, total_);
      }
    };
  }

  std::string name() const
  {
    return "task";
  }

private:
  void notify()
  {
    using namespace caf;
    for (auto& s : subscribers_)
      send(s, atom("progress"), uint64_t{actors_.size()}, total_);
    if (actors_.empty())
    {
      auto done = make_message(atom("done"));
      for (auto& s : supervisors_)
        send(s, done);
      quit(exit_reason_);
    }
  }

  uint32_t exit_reason_;
  uint64_t total_ = 0;
  std::set<caf::actor_addr> actors_;
  util::flat_set<caf::actor> subscribers_;
  util::flat_set<caf::actor> supervisors_;
};

} // namespace vast

#endif
