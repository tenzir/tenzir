#ifndef VAST_ACTOR_TASK_H
#define VAST_ACTOR_TASK_H

#include <chrono>
#include <set>
#include <caf/all.hpp>
#include "vast/time.h"
#include "vast/actor/actor.h"
#include "vast/util/flat_set.h"

namespace vast {

/// An abstraction of a task where each work item consists of an actor. The
/// task completes as soon as all registered items send either a DONE atom or
/// terminate.
struct task : public default_actor
{
  /// Spawns a task.
  /// @param exit_reason The exit reason upon to use upon completion.
  task(uint32_t exit_reason = exit::done)
    : exit_reason_{exit_reason}
  {
    attach_functor(
      [=](uint32_t)
      {
        workers_.clear();
        subscribers_.clear();
        supervisors_.clear();
      });
  }

  void at(caf::down_msg const& msg) override
  {
    if (workers_.erase(msg.source) == 1)
      notify();
  }

  caf::message_handler make_handler() override
  {
    using namespace caf;
    begin_ = time::stopwatch();
    return
    {
      [=](actor const& a)
      {
        VAST_TRACE(this, "registers actor", a);
        monitor(a);
        workers_.insert(a.address());
        ++total_;
      },
      [=](done_atom, actor const& a)
      {
        VAST_TRACE(this, "manually completed actor", a);
        if (workers_.erase(a.address()) == 1)
        {
          demonitor(a);
          notify();
        }
      },
      [=](done_atom)
      {
        if (workers_.erase(last_sender()) == 1)
        {
          demonitor(last_sender());
          notify();
          return;
        }
        VAST_ERROR(this, "got DONE from unregistered actor:", last_sender());
        quit(exit::error);
      },
      [=](supervisor_atom, actor const& a)
      {
        VAST_TRACE(this, "notifies", a, "about task completion");
        supervisors_.insert(a);
      },
      [=](subscriber_atom, actor const& a)
      {
        VAST_TRACE(this, "notifies", a, "on task status change");
        subscribers_.insert(a);
      },
      [=](progress_atom)
      {
        return make_message(uint64_t{workers_.size()}, total_);
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
    auto runtime = time::stopwatch() - begin_;
    using namespace caf;
    for (auto& s : subscribers_)
      send(s, progress_atom::value, uint64_t{workers_.size()}, total_);
    if (workers_.empty())
    {
      auto done = make_message(done_atom::value, runtime);
      for (auto& s : supervisors_)
        send(s, done);
      quit(exit_reason_);
    }
  }

  uint32_t exit_reason_;
  uint64_t total_ = 0;
  time::point begin_;
  std::set<caf::actor_addr> workers_;
  util::flat_set<caf::actor> subscribers_;
  util::flat_set<caf::actor> supervisors_;
};

} // namespace vast

#endif
