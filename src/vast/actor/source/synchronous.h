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
struct synchronous : public default_actor
{
public:
  synchronous()
  {
    high_priority_exit(false);
  }

  void at(caf::exit_msg const& msg) override
  {
    send_events();
    this->quit(msg.reason);
  }

  caf::message_handler make_handler() override
  {
    using namespace caf;
    this->attach_functor([=](uint32_t)
    {
      accountant_ = invalid_actor;
      sinks_.clear();
    });

    return
    {
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
      },
      [=](start_atom)
      {
        running_ = true;
        this->send(this, run_atom::value);
      },
      [=](stop_atom)
      {
        running_ = false;
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
          send(accountant_, time::now(),
               description() + "-events", uint64_t{events_.size()});
        send_events();
        if (done())
          this->quit(exit::done);
        else if (running_)
          this->send(this, this->last_dequeued());
      }
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
      for (auto& a : sinks_)
        this->send(a, std::move(events_));
      events_ = {};
    }
  }

  bool done_ = false;
  bool running_ = true;
  caf::actor accountant_;
  std::vector<caf::actor> sinks_;
  uint64_t batch_size_ = std::numeric_limits<uint16_t>::max();
  std::vector<event> events_;
};

} // namespace source
} // namespace vast

#endif
