#ifndef VAST_ACTOR_SOURCE_BASE_H
#define VAST_ACTOR_SOURCE_BASE_H

#include <caf/all.hpp>

#include "vast/event.h"
#include "vast/result.h"
#include "vast/actor/actor.h"
#include "vast/concept/printable/vast/error.h"
#include "vast/util/assert.h"

namespace vast {
namespace source {

/// The base class for data sources which synchronously extract events
/// one-by-one.
template <typename Derived>
class base : public flow_controlled_actor {
public:
  base(char const* name) : flow_controlled_actor{name} {
    trap_exit(true);
  }

  void on_exit() override {
    accountant_ = caf::invalid_actor;
    sinks_.clear();
  }

  caf::behavior make_behavior() override {
    using namespace caf;
    return {
      [=](exit_msg const& msg) {
        if (downgrade_exit())
          return;
        if (!events_.empty())
          send(sinks_[next_sink_++ % sinks_.size()], std::move(events_));
        quit(msg.reason);
      },
      [=](down_msg const& msg) {
        // Handle sink termination.
        auto sink = std::find_if(sinks_.begin(), sinks_.end(), [&](auto& x) {
          return x->address() == msg.source;
        });
        if (sink != sinks_.end())
          sinks_.erase(sink);
        if (sinks_.empty()) {
          VAST_WARN(this, "has no more sinks");
          send_exit(*this, exit::done);
        }
      },
      [=](overload_atom) {
        overloaded(true); // Stop after the next batch.
      },
      [=](underload_atom) {
        overloaded(false);
        if (!done())
          send(this, run_atom::value);
      },
      [=](batch_atom, uint64_t batch_size) {
        VAST_DEBUG(this, "sets batch size to", batch_size);
        batch_size_ = batch_size;
      },
      [=](get_atom, schema_atom) {
        return static_cast<Derived*>(this)->sniff();
      },
      [=](put_atom, schema const& sch) {
        static_cast<Derived*>(this)->set(sch);
      },
      [=](put_atom, sink_atom, actor const& sink) {
        VAST_DEBUG(this, "adds sink to", sink);
        monitor(sink);
        send(sink, upstream_atom::value, this);
        sinks_.push_back(sink);
      },
      [=](put_atom, accountant_atom, actor const& accountant) {
        VAST_DEBUG(this, "registers accountant", accountant);
        accountant_ = accountant;
        send(accountant_, label() + "-events", time::now());
      },
      [=](get_atom, sink_atom) { return sinks_; },
      [=](run_atom) {
        if (sinks_.empty()) {
          VAST_ERROR(this, "cannot run without sinks");
          this->quit(exit::error);
          return;
        }
        while (events_.size() < batch_size_ && !done()) {
          result<event> r = static_cast<Derived*>(this)->extract();
          if (r) {
            events_.push_back(std::move(*r));
          } else if (r.failed()) {
            VAST_ERROR(this, r.error());
            done(true);
            break;
          }
        }
        if (!events_.empty()) {
          VAST_VERBOSE(this, "produced", events_.size(), "events");
          if (accountant_ != invalid_actor)
            send(accountant_, uint64_t{events_.size()}, time::snapshot());
          send(sinks_[next_sink_++ % sinks_.size()], std::move(events_));
          events_ = {};
          // FIXME: if we do not give the stdlib implementation a hint to yield
          // here, this actor can monopolize all available resources. In
          // particular, we encountered a scenario where it prevented the BASP
          // broker from a getting a chance to operate, thereby queuing up
          // all event batches locally and running out of memory, as opposed to
          // sending them out as soon as possible. This yield fix temporarily
          // works around a deeper issue in CAF, which needs to be addressed in
          // the future.
          std::this_thread::yield();
        }
        if (done())
          send_exit(*this, exit::done);
        else if (!overloaded())
          this->send(this, this->current_message());
      },
      catch_unexpected(),
    };
  }

protected:
  bool done() const {
    return done_;
  }

  void done(bool flag) {
    done_ = flag;
  }

private:
  bool done_ = false;
  caf::actor accountant_;
  std::vector<caf::actor> sinks_;
  size_t next_sink_ = 0;
  uint64_t batch_size_ = std::numeric_limits<uint16_t>::max();
  std::vector<event> events_;
};

} // namespace source
} // namespace vast

#endif
