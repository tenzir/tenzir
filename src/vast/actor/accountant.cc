#include "vast/actor/accountant.h"
#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/vast/error.h"
#include "vast/concept/printable/vast/filesystem.h"

namespace vast {
namespace accountant {

state::state(local_actor* self) : basic_state{self, "accountant"} { }

optional<value_type> state::accumulate(context& ctx, value_type x,
                                              time::moment t) {
  ctx.x += x;
  if (ctx.last.time_since_epoch() == time::extent::zero()) {
    ctx.last = t;
    return {};
  }
  auto delta = t - ctx.last;
  if (delta < resolution_)
    return {};
  // We normalize at the value granularity of milliseconds, as more
  // fine-grained latencies will probably hard to get accurate in a
  // actor-based deployment.
  value_type normalized = ctx.x * resolution_.milliseconds()
                            / time::duration{delta}.milliseconds();
  ctx.accumulator.add(normalized);
  ctx.last = t;
  ctx.x = 0;
  return normalized;
}

void state::record(std::string const& context, value_type x, time::moment t) {
  auto& ctx = contexts_[context];
  if (ctx.begin == time::duration::zero())
    ctx.begin = time::now();
  auto a = accumulate(ctx, x, t);
  if (!a)
    return;
  // Initialize log file not done already.
  if (!file_.is_open()) {
    if (!exists(filename_.parent())) {
      auto t = mkdir(filename_.parent());
      if (!t) {
        VAST_ERROR_AT(self, t.error());
        self->quit(exit::error);
        return;
      }
    }
    VAST_DEBUG_AT(self, "opens log file:", filename_);
    file_.open(filename_.str());
    if (!file_.is_open()) {
      VAST_ERROR_AT(self, "failed to open file in:", filename_);
      self->quit(exit::error);
      return;
    }
    file_ << "time\tcontext\tvalue\t"
          << "count\tsum\tmin\tmax\tmean\tmedian\tvariance\n";
  }
  VAST_DEBUG_AT(self, "accumulated new value:",
                (context.empty() ? "" : context + " ="), *a);
  if (! file_) {
    VAST_ERROR_AT(self, "encountered error with log file", filename_);
    self->quit(exit::error);
    return;
  }
  auto ts = (ctx.begin + (t - ctx.last)).time_since_epoch().double_seconds();
  file_ << to_string(ts) << '\t'
        << (context.empty() ? "none" : context) << '\t'
        << *a << '\t'
        << ctx.accumulator.count() << '\t'
        << ctx.accumulator.sum() << '\t'
        << ctx.accumulator.min() << '\t'
        << ctx.accumulator.max() << '\t'
        << ctx.accumulator.mean() << '\t'
        << ctx.accumulator.median() << '\t'
        << ctx.accumulator.variance() << std::endl;
}

behavior_type actor(stateful_pointer self, path filename,
                    time::duration resolution) {
  VAST_ASSERT(!filename.empty());
  self->state.filename_ = std::move(filename);
  self->state.resolution_ = std::move(resolution);
  return {
    [=](std::string const& context, time::point first) {
      VAST_DEBUG_AT(self, "registers context", context, "from actor",
                    self->current_sender());
      self->state.actors_[self->current_sender()] = context;
      self->state.contexts_[context].begin = first;
    },
    [=](value_type x, time::moment timestamp) {
      self->state.record(self->state.actors_[self->current_sender()],
                         x, timestamp);
    },
    [=](std::string const& context, value_type x, time::moment timestamp) {
      self->state.record(context, x, timestamp);
    }
  };
}

} // namespace accountant
} // namespace vast
