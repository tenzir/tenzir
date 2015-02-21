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
    time::point begin{time::duration::zero()};
    time::moment last{time::extent::zero()};
    util::accumulator<T> accumulator;
  };

  /// Spawns an accountant.
  /// @param filename The directory in which to write the logfile.
  /// @param resolution The granularity at which to track values which get
  ///                   submitted incrementally.
  accountant(path dir, time::duration resolution = time::seconds(1))
    : default_actor{"accountant"},
      dir_{std::move(dir)},
      resolution_{resolution}
  {
  }

  caf::behavior make_behavior() override
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
      [=](std::string const& context, time::point first)
      {
        actors_[current_sender()] = context;
        contexts_[context].begin = first;
      },
      [=](T x, time::moment timestamp)
      {
        record(actors_[current_sender()], x, timestamp);
      },
      [=](std::string const& context, T x, time::moment timestamp)
      {
        record(context, x, timestamp);
      },
      catch_unexpected()
    };
  }

  void record(std::string const& context, T x, time::moment t)
  {
    using std::to_string;
    auto& ctx = contexts_[context];
    if (ctx.begin == time::duration::zero())
      ctx.begin = time::now();
    if (auto a = accumulate(ctx, x, t))
    {
      auto ts = (ctx.begin + (t - ctx.last)).since_epoch().double_seconds();
      auto record = to_string(ts)
        + '\t' + (context.empty() ? "none" : context)
        + '\t' + to_string(*a)
        + '\t' + to_string(ctx.accumulator.count())
        + '\t' + to_string(ctx.accumulator.sum())
        + '\t' + to_string(ctx.accumulator.min())
        + '\t' + to_string(ctx.accumulator.max())
        + '\t' + to_string(ctx.accumulator.mean())
        + '\t' + to_string(ctx.accumulator.median())
        + '\t' + to_string(ctx.accumulator.variance());
      if (file_)
        file_ << record << std::endl;
    }
  }

  optional<T> accumulate(context& ctx, T x, time::moment t)
  {
    ctx.x += x;
    if (ctx.last.time_since_epoch() == time::extent::zero())
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
    T normalized = ctx.x * resolution_.milliseconds() /
      time::duration{delta}.milliseconds();
    ctx.accumulator.add(normalized);
    ctx.last = t;
    ctx.x = 0;
    return normalized;
  }

  path const dir_;
  time::duration const resolution_;
  std::ofstream file_;
  std::unordered_map<caf::actor_addr, std::string> actors_;
  std::unordered_map<std::string, context> contexts_;
};

} // namespace vast

#endif
