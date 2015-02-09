#ifndef VAST_ACTOR_ACCOUNTANT_H
#define VAST_ACTOR_ACCOUNTANT_H

#include <fstream>
#include "vast/filesystem.h"
#include "vast/time.h"
#include "vast/actor/actor.h"
#include "vast/util/accumulator.h"

namespace vast {

/// Writes out accounting data into a log file.
template <typename T>
struct accountant : default_actor
{
  struct context
  {
    T x = 0;
    time::point last = time::duration{};
    util::accumulator<T> accumulator;
  };

  /// Spawns an accountant.
  /// @param filename The directory in which to write the logfile.
  /// @param resolution The granularity at which to track values which get
  ///                   submitted incrementally.
  accountant(path dir, time::duration resolution = time::seconds(1))
    : dir_{std::move(dir)},
      resolution_{resolution}
  {
  }

  caf::message_handler make_handler() override
  {
    if (dir_.empty())
    {
      VAST_ERROR(this, "require non-empty directory to write log file");
      quit(exit::error);
      return {};
    }
    if (! exists(dir_))
    {
      auto t = mkdir(dir_);
      if (! t)
      {
        VAST_ERROR(this, t.error());
        quit(exit::error);
        return {};
      }
    }
    file_.open((dir_ / "accounting.log").str());
    if (! file_)
    {
      VAST_ERROR("failed to open file in:", dir_);
      quit(exit::error);
      return {};
    }
    file_
      << "time\tcontext\tvalue\t"
      << "count\tsum\tmin\tmax\tmean\tmedian\tvariance\n";
    return
    {
      [=](time::point timestamp, std::string const& context, T x)
      {
        using std::to_string;
        auto& ctx = contexts_[context];
        if (auto a = accumulate(ctx, x, timestamp))
        {
          auto record = to_string(timestamp.since_epoch().microseconds())
            + '\t' + context
            + '\t' + to_string(*a)
            + '\t' + to_string(ctx.accumulator.count())
            + '\t' + to_string(ctx.accumulator.sum())
            + '\t' + to_string(ctx.accumulator.min())
            + '\t' + to_string(ctx.accumulator.max())
            + '\t' + to_string(ctx.accumulator.mean())
            + '\t' + to_string(ctx.accumulator.median())
            + '\t' + to_string(ctx.accumulator.variance());
          VAST_DEBUG(record);
          if (file_)
            file_ << record << std::endl;
        }
      }
    };
  }

  std::string name() const override
  {
    return "accumulator";
  }

  optional<T> accumulate(context& ctx, T x, time::point t)
  {
    ctx.x += x;
    if (ctx.last == time::duration{})
    {
      ctx.last = t;
      return {};
    }
    auto delta = t - ctx.last;
    if (delta < resolution_)
      return {};
    // We normalize at the value granularity of milliseconds, as more
    // fine-grained latencies will probably hard to get accurate in a
    // actor-based deployment.
    T normalized = ctx.x * resolution_.milliseconds() / delta.milliseconds();
    ctx.accumulator.add(normalized);
    ctx.last = t;
    ctx.x = 0;
    return normalized;
  }

  path const dir_;
  time::duration const resolution_;
  std::ofstream file_;
  std::unordered_map<std::string, context> contexts_;
};

} // namespace vast

#endif
