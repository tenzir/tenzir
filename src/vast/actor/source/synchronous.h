#ifndef VAST_ACTOR_SOURCE_SYNCHRONOUS_H
#define VAST_ACTOR_SOURCE_SYNCHRONOUS_H

#include <caf/all.hpp>
#include "vast/event.h"
#include "vast/chunk.h"
#include "vast/actor/actor.h"
#include "vast/util/result.h"

namespace vast {
namespace source {
namespace detail {

class chunkifier : public flow_controlled_actor
{
public:
  chunkifier(caf::actor source, io::compression method)
    : flow_controlled_actor{"chunkifier"},
      source_{source},
      compression_{method}
  {
    trap_exit(true);
  }

  void on_exit()
  {
    source_ = caf::invalid_actor;
  }

  caf::behavior make_behavior() override
  {
    using namespace caf;
    return
    {
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
      },
      [=](std::vector<event> const& events, caf::actor const& sink)
      {
        VAST_DEBUG(this, "forwards", events.size(), "events");
        send(sink, chunk{events, compression_});
        if (mailbox().count() > 50)
          overloaded(true);
        else if (overloaded())
          overloaded(false);
      }
    };
  }

private:
  caf::actor source_;
  io::compression compression_ = io::lz4;
};

} // namespace detail

/// A synchronous source that extracts events one by one.
template <typename Derived>
class synchronous : public flow_controlled_actor
{
public:
  synchronous(char const* name)
    : flow_controlled_actor{name}
  {
    trap_exit(true);
  }

  void on_exit()
  {
    accountant_ = caf::invalid_actor;
    chunkifier_ = caf::invalid_actor;
    sinks_.clear();
  }

  caf::behavior make_behavior() override
  {
    using namespace caf;
    return
    {
      [=](exit_msg const& msg)
      {
        if (downgrade_exit())
          return;
        if (! chunkifier_)
        {
          this->quit(msg.reason);
          return;
        }
        if (! events_.empty())
          send_events();
        send_exit(chunkifier_, msg.reason);
        become(
          [=](down_msg const& down)
          {
            if (down.source == chunkifier_)
              quit(msg.reason);
          });
      },
      [=](down_msg const& msg)
      {
        // Handle chunkifier termination.
        if (msg.source == chunkifier_)
        {
          quit(msg.reason);
          return;
        }
        // Handle sink termination.
        auto sink = std::find_if(
            sinks_.begin(), sinks_.end(),
            [this](auto& x) { return x->address() == current_sender(); });
        if (sink != sinks_.end())
          sinks_.erase(sink);
        if (sinks_.empty())
        {
          VAST_WARN(this, "has no more sinks");
          send_exit(*this, exit::done);
        }
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
      [=](io::compression method)
      {
        compression_ = method;
      },
      [=](schema_atom)
      {
        return static_cast<Derived*>(this)->sniff();
      },
      [=](schema const& sch)
      {
        static_cast<Derived*>(this)->set(sch);
      },
      [=](batch_atom, uint64_t batch_size)
      {
        VAST_DEBUG(this, "sets batch size to", batch_size);
        batch_size_ = batch_size;
      },
      [=](add_atom, sink_atom, actor const& sink)
      {
        VAST_DEBUG(this, "adds sink to", sink);
        monitor(sink);
        send(sink, upstream_atom::value, this);
        sinks_.push_back(sink);
        return ok_atom::value;
      },
      [=](accountant_atom, actor const& accountant)
      {
        VAST_DEBUG(this, "registers accountant", accountant);
        accountant_ = accountant;
        send(accountant_, label() + "-events", time::now());
      },
      [=](run_atom)
      {
        if (! chunkifier_)
        {
          chunkifier_ = spawn<detail::chunkifier, priority_aware + monitored>(
              this, compression_);
          send(chunkifier_, upstream_atom::value, this);
        }
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
        if (! events_.empty())
        {
          if (accountant_ != invalid_actor)
            send(accountant_, uint64_t{events_.size()}, time::snapshot());
          send_events();
        }
        if (done())
          send_exit(*this, exit::done);
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
    using namespace caf;
    assert(! events_.empty());
    VAST_VERBOSE(this, "produced", events_.size(), "events");
    auto& sink = sinks_[next_sink_++ % sinks_.size()];
    send(chunkifier_, std::move(events_), sink);
    events_ = {};
  }

  bool done_ = false;
  io::compression compression_ = io::lz4;
  caf::actor accountant_;
  caf::actor chunkifier_;
  std::vector<caf::actor> sinks_;
  size_t next_sink_ = 0;
  uint64_t batch_size_ = std::numeric_limits<uint16_t>::max();
  std::vector<event> events_;
};

} // namespace source
} // namespace vast

#endif
