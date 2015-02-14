#ifndef VAST_ACTOR_TASK_H
#define VAST_ACTOR_TASK_H

#include <chrono>
#include <map>
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

  void at(caf::exit_msg const& msg) override
  {
    // Only notify our supervisors when exiting.
    subscribers_.clear();
    notify();
    quit(msg.reason);
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
        ++workers_[a.address()];
        ++total_;
      },
      [=](actor const& a, uint64_t n)
      {
        VAST_TRACE(this, "registers actor", a, "for", n, "sub-tasks");
        monitor(a);
        workers_[a.address()] += n;
        ++total_;
      },
      [=](done_atom, actor const& a)
      {
        VAST_TRACE(this, "manually completed actor", a);
        complete(a->address());
      },
      [=](done_atom, actor_addr const& addr)
      {
        VAST_TRACE(this, "manually completed actor with address", addr);
        complete(addr);
      },
      [=](done_atom)
      {
        VAST_TRACE(this, "completed actor with address", last_sender());
        complete(last_sender());
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
  void complete(caf::actor_addr const& addr)
  {
    auto w = workers_.find(addr);
    if (w == workers_.end())
    {
      VAST_ERROR(this, "got completion signal from unregistered actor:", addr);
      quit(exit::error);
    }
    else if (--w->second == 0)
    {
      demonitor(addr);
      workers_.erase(w);
      notify();
    }
  }

  void notify()
  {
    using namespace caf;
    auto runtime = time::stopwatch() - begin_;
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
  std::map<caf::actor_addr, uint64_t> workers_;
  util::flat_set<caf::actor> subscribers_;
  util::flat_set<caf::actor> supervisors_;
};

} // namespace vast

#endif
