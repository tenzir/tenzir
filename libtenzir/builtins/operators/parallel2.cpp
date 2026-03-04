//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/view.hpp"

#include <algorithm>
#include <numeric>
#include <thread>

namespace tenzir::plugins::parallel2 {

namespace {

constexpr auto fairness_factor = 2.0;

struct ParallelArgs {
  std::optional<located<uint64_t>> jobs;
  std::optional<ast::expression> route_by;
  located<ir::pipeline> pipe;
};

/// Distributes `total` rows across `k` workers, leveling them up from the
/// least-loaded first. Workers with fewer rows assigned get more rows, bringing
/// everyone as close to equal as possible. Any leftover rows after leveling are
/// split evenly.
///
/// The `sorted_indices` must be sorted by ascending `rows_assigned`.
///
/// Example: rows_assigned = [100, 300, 500], total = 1000
///   Level up worker 0 by 200 to match worker 1 (cost: 200)
///   Level up workers 0,1 by 200 each to match worker 2 (cost: 400)
///   Remaining 400 split evenly: 134, 133, 133
///   Result: [534, 333, 133], new totals: [634, 633, 633]
auto water_fill(uint64_t total, std::span<const size_t> sorted_indices,
                std::span<const uint64_t> rows_assigned)
  -> std::vector<uint64_t> {
  auto k = sorted_indices.size();
  auto alloc = std::vector<uint64_t>(k, 0);
  auto remaining = total;
  for (auto level = size_t{0}; level + 1 < k; ++level) {
    auto gap = rows_assigned[sorted_indices[level + 1]]
               - rows_assigned[sorted_indices[level]];
    auto needed = gap * (level + 1);
    if (needed <= remaining) {
      for (auto j = size_t{0}; j <= level; ++j) {
        alloc[j] += gap;
      }
      remaining -= needed;
    } else {
      auto per = remaining / (level + 1);
      auto extra = remaining % (level + 1);
      for (auto j = size_t{0}; j <= level; ++j) {
        alloc[j] += per + (j < extra ? 1 : 0);
      }
      remaining = 0;
      break;
    }
  }
  if (remaining > 0) {
    auto per = remaining / k;
    auto extra = remaining % k;
    for (auto j = size_t{0}; j < k; ++j) {
      alloc[j] += per + (j < extra ? 1 : 0);
    }
  }
  return alloc;
}

/// Distributes `total_rows` across workers while maintaining fairness.
///
/// Tries to use as few workers as possible (for better locality) while keeping
/// the max/min ratio of total rows assigned across all workers within
/// `fairness_factor`. Starts by trying to send everything to the most-starved
/// worker (k=1), then considers spreading across 2, 3, ... workers until the
/// fairness constraint is satisfied. At k=n (all workers), always accepts.
///
/// Updates `rows_assigned` in place and returns (worker_index, row_count) pairs.
///
/// Example: 4 workers at [0, 0, 0, 0], distributing 1000 rows
///   k=1: all to worker 0 → [1000, 0, 0, 0], unfair → rejected
///   k=4: 250 each → [250, 250, 250, 250] → accepted
///
/// Example: 4 workers at [500, 300, 200, 100], distributing 400 rows
///   k=1: all to worker 3 → [500, 300, 200, 500], max/min = 2.5 → rejected
///   k=2: water-fill workers 3,2 → [500, 300, 300, 300], max/min = 1.67 → ok
auto distribute_adaptive(uint64_t total_rows,
                         std::vector<uint64_t>& rows_assigned)
  -> std::vector<std::pair<size_t, uint64_t>> {
  auto n = rows_assigned.size();
  // Sort worker indices by rows_assigned ascending.
  auto sorted = std::vector<size_t>(n);
  std::iota(sorted.begin(), sorted.end(), size_t{0});
  std::sort(sorted.begin(), sorted.end(), [&](size_t a, size_t b) {
    return rows_assigned[a] < rows_assigned[b];
  });
  auto alloc = std::vector<uint64_t>{};
  for (auto k = size_t{1}; k <= n; ++k) {
    alloc = water_fill(total_rows, std::span{sorted.data(), k}, rows_assigned);
    if (k == n) {
      break;
    }
    // Check whether this distribution satisfies the fairness constraint.
    auto new_totals = rows_assigned;
    for (auto i = size_t{0}; i < k; ++i) {
      new_totals[sorted[i]] += alloc[i];
    }
    auto [min_it, max_it]
      = std::minmax_element(new_totals.begin(), new_totals.end());
    auto is_fair = static_cast<double>(*max_it)
                   <= static_cast<double>(*min_it) * fairness_factor;
    if (is_fair) {
      break;
    }
  }
  auto result = std::vector<std::pair<size_t, uint64_t>>{};
  for (auto i = size_t{0}; i < alloc.size(); ++i) {
    if (alloc[i] > 0) {
      rows_assigned[sorted[i]] += alloc[i];
      result.emplace_back(sorted[i], alloc[i]);
    }
  }
  return result;
}

/// Shared implementation for both transform and sink variants.
class ParallelImpl {
public:
  explicit ParallelImpl(ParallelArgs args)
    : jobs_{args.jobs
              ? args.jobs->inner
              : std::max(uint64_t{1}, static_cast<uint64_t>(
                                        std::thread::hardware_concurrency()))},
      route_by_{std::move(args.route_by)},
      pipe_{std::move(args.pipe.inner)} {
  }

  auto start(OpCtx& ctx) -> Task<void> {
    rows_assigned_.resize(jobs_, 0);
    for (auto i = uint64_t{0}; i < jobs_; ++i) {
      auto copy = pipe_;
      auto sub_ctx = substitute_ctx{base_ctx{ctx.dh(), ctx.reg()}, nullptr};
      if (not copy.substitute(sub_ctx, true)) {
        co_return;
      }
      co_await ctx.spawn_sub_fused(data{int64_t(i)}, std::move(copy),
                                   tag_v<table_slice>);
    }
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> {
    if (route_by_) {
      co_await process_hash(std::move(input), ctx);
    } else {
      co_await process_round_robin(std::move(input), ctx);
    }
  }

  auto snapshot(Serde& serde) {
    serde("rows_assigned", rows_assigned_);
  }

  auto finalize(OpCtx& ctx) -> Task<void> {
    for (auto i = uint64_t{0}; i < jobs_; ++i) {
      auto sub = ctx.get_sub(int64_t(i));
      if (sub) {
        auto& pipe = as<OpenPipeline<table_slice>>(*sub);
        co_await pipe.close();
      }
    }
  }

private:
  auto process_hash(table_slice input, OpCtx& ctx) -> Task<void> {
    auto values = eval(*route_by_, input, ctx.dh());
    auto num_rows = static_cast<int64_t>(input.rows());
    // Find runs of same-bucket rows and push subslices.
    auto begin = int64_t{0};
    while (begin < num_rows) {
      auto bucket = std::hash<data_view>{}(values.value_at(begin)) % jobs_;
      auto end = begin + 1;
      while (end < num_rows
             and std::hash<data_view>{}(values.value_at(end)) % jobs_
                   == bucket) {
        ++end;
      }
      auto slice = subslice(input, begin, end);
      auto sub = ctx.get_sub(int64_t(bucket));
      TENZIR_ASSERT(sub);
      auto& pipe = as<OpenPipeline<table_slice>>(*sub);
      std::ignore = co_await pipe.push(std::move(slice));
      begin = end;
    }
  }

  auto process_round_robin(table_slice input, OpCtx& ctx) -> Task<void> {
    auto total_rows = static_cast<uint64_t>(input.rows());
    auto assignments = distribute_adaptive(total_rows, rows_assigned_);
    auto offset = size_t{0};
    for (auto [worker, count] : assignments) {
      auto slice = subslice(input, offset, offset + count);
      offset += count;
      auto sub = ctx.get_sub(int64_t(worker));
      TENZIR_ASSERT(sub);
      auto& pipe = as<OpenPipeline<table_slice>>(*sub);
      std::ignore = co_await pipe.push(std::move(slice));
    }
  }

  uint64_t jobs_;
  std::optional<ast::expression> route_by_;
  ir::pipeline pipe_;
  std::vector<uint64_t> rows_assigned_;
};

/// Shared implementation for void-input variants (source operators).
class ParallelSourceImpl {
public:
  explicit ParallelSourceImpl(ParallelArgs args)
    : jobs_{args.jobs
              ? args.jobs->inner
              : std::max(uint64_t{1}, static_cast<uint64_t>(
                                        std::thread::hardware_concurrency()))},
      pipe_{std::move(args.pipe.inner)} {
  }

  auto start(OpCtx& ctx) -> Task<void> {
    for (auto i = uint64_t{0}; i < jobs_; ++i) {
      auto copy = pipe_;
      auto sub_ctx = substitute_ctx{base_ctx{ctx.dh(), ctx.reg()}, nullptr};
      if (not copy.substitute(sub_ctx, true)) {
        co_return;
      }
      co_await ctx.spawn_sub_fused(data{int64_t(i)}, std::move(copy),
                                   tag_v<void>);
    }
  }

private:
  uint64_t jobs_;
  ir::pipeline pipe_;
};

/// Parallel operator that transforms events (subpipeline outputs events).
class ParallelTransform final : public Operator<table_slice, table_slice> {
public:
  explicit ParallelTransform(ParallelArgs args) : impl_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    return impl_.start(ctx);
  }

  auto snapshot(Serde& serde) -> void override {
    impl_.snapshot(serde);
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    return impl_.process(std::move(input), ctx);
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push);
    co_await impl_.finalize(ctx);
    co_return FinalizeBehavior::done;
  }

private:
  ParallelImpl impl_;
};

/// Parallel operator that sinks events (subpipeline outputs void).
class ParallelSink final : public Operator<table_slice, void> {
public:
  explicit ParallelSink(ParallelArgs args) : impl_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    return impl_.start(ctx);
  }

  auto snapshot(Serde& serde) -> void override {
    impl_.snapshot(serde);
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    return impl_.process(std::move(input), ctx);
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    co_await impl_.finalize(ctx);
    co_return FinalizeBehavior::done;
  }

private:
  ParallelImpl impl_;
};

/// Parallel source operator (subpipeline outputs events).
class ParallelSource final : public Operator<void, table_slice> {
public:
  explicit ParallelSource(ParallelArgs args) : impl_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    return impl_.start(ctx);
  }

  auto state() -> OperatorState override {
    return OperatorState::done;
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push, ctx);
    co_return FinalizeBehavior::done;
  }

private:
  ParallelSourceImpl impl_;
};

/// Parallel source operator (subpipeline outputs void).
class ParallelVoid final : public Operator<void, void> {
public:
  explicit ParallelVoid(ParallelArgs args) : impl_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    return impl_.start(ctx);
  }

  auto state() -> OperatorState override {
    return OperatorState::done;
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(ctx);
    co_return FinalizeBehavior::done;
  }

private:
  ParallelSourceImpl impl_;
};

class plugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.parallel";
  }

  auto describe() const -> Description override {
    auto d = Describer<ParallelArgs>{};
    auto jobs = d.positional("jobs", &ParallelArgs::jobs);
    auto route_by = d.named("route_by", &ParallelArgs::route_by, "any");
    auto pipe = d.pipeline(&ParallelArgs::pipe);
    d.validate([jobs](DescribeCtx& ctx) -> Empty {
      if (auto j = ctx.get(jobs); j and j->inner == 0) {
        diagnostic::error("`jobs` must not be zero").primary(*j).emit(ctx);
      }
      return {};
    });
    d.spawner([pipe, route_by]<class Input>(DescribeCtx& ctx)
                -> failure_or<Option<SpawnWith<ParallelArgs, Input>>> {
      TRY(auto pipe, ctx.get(pipe));
      TRY(auto output, pipe.inner.infer_type(tag_v<Input>, ctx));
      if constexpr (std::same_as<Input, table_slice>) {
        if (output == tag_v<table_slice>) {
          return [](ParallelArgs args) {
            return ParallelTransform{std::move(args)};
          };
        }
        if (output == tag_v<void>) {
          return [](ParallelArgs args) {
            return ParallelSink{std::move(args)};
          };
        }
      } else if constexpr (std::same_as<Input, void>) {
        if (auto route = ctx.get_location(route_by)) {
          diagnostic::error("`route_by` cannot be used when `parallel` is "
                            "used as a source")
            .primary(*route)
            .emit(ctx);
          return failure::promise();
        }
        if (output == tag_v<table_slice>) {
          return [](ParallelArgs args) {
            return ParallelSource{std::move(args)};
          };
        }
        if (output == tag_v<void>) {
          return [](ParallelArgs args) {
            return ParallelVoid{std::move(args)};
          };
        }
      } else {
        return {};
      }
      diagnostic::error("subpipeline must not produce bytes")
        .primary(pipe.source)
        .emit(ctx);
      return failure::promise();
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::parallel2

TENZIR_REGISTER_PLUGIN(tenzir::plugins::parallel2::plugin)
