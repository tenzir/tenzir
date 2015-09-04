#ifndef VAST_ACTOR_SINK_BASE_H
#define VAST_ACTOR_SINK_BASE_H

#include "vast/concept/printable/vast/error.h"
#include "vast/concept/printable/vast/event.h"
#include "vast/concept/printable/vast/time.h"
#include "vast/concept/printable/vast/uuid.h"
#include "vast/actor/actor.h"
#include "vast/actor/accountant.h"
#include "vast/event.h"
#include "vast/time.h"
#include "vast/uuid.h"

namespace vast {
namespace sink {

/// The base class for event sinks.
template <typename Derived>
class base : public default_actor {
public:
  base(char const* name = "sink") : default_actor{name} {
    trap_exit(true);
  }

  void on_exit() override {
    static_cast<Derived*>(this)->flush();
  }

  behavior make_behavior() override {
    last_flush_ = time::snapshot();
    return {
      [=](exit_msg const& msg) { quit(msg.reason); },
      [=](limit_atom, uint64_t max) {
        VAST_DEBUG(this, "caps event export at", max, "events");
        if (processed_ < max)
          limit_ = max;
        else
          VAST_WARN(this, "ignores new limit of", max, "(already processed",
                    processed_, " events)");
      },
      [=](accountant::actor_type acc) {
        VAST_DEBUG_AT(this, "registers accountant#" << acc->id());
        accountant_ = acc;
      },
      [=](uuid const&, event const& e) { handle(e); },
      [=](uuid const&, std::vector<event> const& v) {
        assert(!v.empty());
        auto start = time::snapshot();
        for (auto& e : v)
          if (!handle(e))
            return;
        if (accountant_) {
          auto runtime = time::snapshot() - start;
          auto unit = time::duration_cast<time::microseconds>(runtime).count();
          auto rate = v.size() * 1e6 / unit;
          send(accountant_, "sink", "event-rate", rate);
        }
      },
      [=](uuid const& id, progress_atom, double progress, uint64_t total_hits) {
        VAST_VERBOSE(this, "got progress from query ", id << ':', total_hits,
                     "hits (" << size_t(progress * 100) << "%)");
      },
      [=](uuid const& id, done_atom, time::extent runtime) {
        VAST_VERBOSE(this, "got DONE from query", id << ", took", runtime);
      },
      catch_unexpected()};
  }

protected:
  void flush() {
    // Nothing by default.
  }

private:
  bool handle(event const& e) {
    if (!static_cast<Derived*>(this)->process(e)) {
      VAST_ERROR(this, "failed to process event:", e);
      this->quit(exit::error);
      return false;
    }
    if (++processed_ == limit_) {
      VAST_VERBOSE(this, "reached limit: ", limit_, "events");
      this->quit(exit::done);
    }
    auto now = time::snapshot();
    if (now - last_flush_ > flush_interval_) {
      static_cast<Derived*>(this)->flush();
      last_flush_ = now;
    }
    return true;
  }

  time::extent flush_interval_ = time::seconds(1); // TODO: make configurable
  time::moment last_flush_;
  accountant::actor_type accountant_;
  uint64_t processed_ = 0;
  uint64_t limit_ = 0;
};

} // namespace sink
} // namespace vast

#endif
