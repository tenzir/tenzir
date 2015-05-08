#ifndef VAST_ACTOR_SINK_BASE_H
#define VAST_ACTOR_SINK_BASE_H

#include "vast/actor/actor.h"
#include "vast/event.h"
#include "vast/time.h"
#include "vast/uuid.h"

namespace vast {
namespace sink {

/// The base class for event sinks.
template <typename Derived>
class base : public default_actor
{
public:
  base(char const* name = "sink")
    : default_actor{name}
  {
  }

  void on_exit()
  {
    accountant_ = caf::invalid_actor;
  }

  // Allows derived classes to hook exit.
  virtual void finalize() { }

  caf::behavior make_behavior() override
  {
    using namespace caf;
    trap_exit(true);
    return
    {
      [=](exit_msg const& msg)
      {
        finalize();
        quit(msg.reason);
      },
      [=](limit_atom, uint64_t max)
      {
        VAST_DEBUG(this, "caps event export at", max, "events");
        if (processed_ < max)
          limit_ = max;
        else
          VAST_WARN(this, "ignores new limit of", max <<
                    ", already processed", processed_, " events");
      },
      [=](accountant_atom, actor const& accountant)
      {
        VAST_DEBUG(this, "registers accountant", accountant);
        accountant_ = accountant;
        send(accountant_, label() + "-events", time::now());
      },
      [=](uuid const&, event const& e)
      {
        handle(e);
      },
      [=](uuid const&, std::vector<event> const& v)
      {
        assert(! v.empty());
        for (auto& e : v)
          if (! handle(e))
            return;
        if (accountant_)
          send(accountant_, static_cast<uint64_t>(v.size()), time::snapshot());
      },
      [=](uuid const& id, progress_atom, double progress, uint64_t total_hits)
      {
        VAST_VERBOSE(this, "got progress from query ", id << ':',
                     total_hits, "hits (" << size_t(progress * 100) << "%)");
      },
      [=](uuid const& id, done_atom, time::duration runtime)
      {
        VAST_VERBOSE(this, "got DONE from query", id << ", took" << runtime);
        finalize();
        quit(exit::done);
      },
      catch_unexpected()
    };
  }

private:
  bool handle(event const& e)
  {
    if (! static_cast<Derived*>(this)->process(e))
    {
      VAST_ERROR(this, "failed to process event:", e);
      finalize();
      this->quit(exit::error);
      return false;
    }
    if (++processed_ == limit_)
    {
      VAST_VERBOSE(this, "reached limit: ", limit_, "events");
      finalize();
      this->quit(exit::done);
    }
    return true;
  }

  caf::actor accountant_;
  uint64_t processed_ = 0;
  uint64_t limit_ = 0;
};

} // namespace sink
} // namespace vast

#endif
