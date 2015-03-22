#ifndef VAST_ACTOR_SOURCE_SYNCHRONOUS_H
#define VAST_ACTOR_SOURCE_SYNCHRONOUS_H

#include <caf/all.hpp>
#include "vast/event.h"
#include "vast/actor/actor.h"
#include "vast/util/result.h"

namespace vast {
namespace source {

/// A synchronous source that extracts events one by one.
template <typename Derived>
class synchronous : public flow_controlled_actor
{
public:
  synchronous(char const* name)
    : flow_controlled_actor{name}
  {
  }

  void on_exit()
  {
    accountant_ = caf::invalid_actor;
    sinks_.clear();
  }

  caf::behavior make_behavior() override
  {
    using namespace caf;
    trap_exit(true);
    return
    {
      [=](exit_msg const& msg)
      {
        if (downgrade_exit())
          return;
        send_events();
        this->quit(msg.reason);
      },
      [=](overload_atom)
      {
        overloaded(true); // Stop after the next batch.
      },
      [=](underload_atom)
      {
        overloaded(false);
        if (! done())
          send(this, run_atom::value);
      },
      [=](batch_atom, uint64_t batch_size)
      {
        VAST_DEBUG(this, "sets batch size to", batch_size);
        batch_size_ = batch_size;
      },
      [=](sink_atom, actor const& sink)
      {
        VAST_DEBUG(this, "adds sink to", sink);
        sinks_.push_back(sink);
      },
      [=](accountant_atom, actor const& accountant)
      {
        VAST_DEBUG(this, "registers accountant", accountant);
        accountant_ = accountant;
        send(accountant_, label() + "-events", time::now());
      },
      [=](run_atom)
      {
        if (sinks_.empty())
        {
          VAST_ERROR(this, "cannot run without sinks");
          this->quit(exit::error);
          return;
        }
        while (events_.size() < batch_size_ && ! done())
        {
          result<event> r = static_cast<Derived*>(this)->extract();
          if (r)
          {
            events_.push_back(std::move(*r));
          }
          else if (r.failed())
          {
            VAST_ERROR(this, r.error());
            done(true);
            break;
          }
        }
        if (accountant_ != invalid_actor && ! events_.empty())
          send(accountant_, uint64_t{events_.size()}, time::snapshot());
        send_events();
        if (done())
          this->quit(exit::done);
        else if (! overloaded())
          this->send(this, this->current_message());
      },
      catch_unexpected(),
    };
  }

protected:
  bool done() const
  {
    return done_;
  }

  void done(bool flag)
  {
    done_ = flag;
  }

private:
  void send_events()
  {
    if (! events_.empty())
    {
      VAST_VERBOSE(this, "produced", events_.size(), "events");
      this->send(sinks_[next_sink_++ % sinks_.size()], std::move(events_));
      events_ = {};
    }
  }

  bool done_ = false;
  caf::actor accountant_;
  std::vector<caf::actor> sinks_;
  uint64_t batch_size_ = std::numeric_limits<uint16_t>::max();
  std::vector<event> events_;
  size_t next_sink_ = 0;
};

} // namespace source
} // namespace vast

#endif
