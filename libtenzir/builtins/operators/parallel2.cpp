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

/// Water-fills `total` rows across the `k` most-starved workers.
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

/// Returns (worker_index, row_count) pairs for adaptive round-robin.
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
      co_await ctx.spawn_sub(data{int64_t(i)}, std::move(copy),
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
    // Partition rows by bucket.
    auto row_buckets = std::vector<size_t>(num_rows);
    for (auto row = int64_t{0}; row < num_rows; ++row) {
      auto hash = std::hash<data_view>{}(values.value_at(row));
      row_buckets[row] = hash % jobs_;
    }
    // Find runs of same-bucket rows and push subslices.
    auto begin = size_t{0};
    while (begin < static_cast<size_t>(num_rows)) {
      auto bucket = row_buckets[begin];
      auto end = begin + 1;
      while (end < static_cast<size_t>(num_rows)
             and row_buckets[end] == bucket) {
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

class plugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.parallel";
  }

  auto describe() const -> Description override {
    auto d = Describer<ParallelArgs, ParallelTransform, ParallelSink>{};
    auto jobs = d.named("jobs", &ParallelArgs::jobs);
    d.named("route_by", &ParallelArgs::route_by, "expression");
    auto pipe_arg = d.pipeline(&ParallelArgs::pipe);
    d.infer_output([pipe_arg](element_type_tag input, DescribeCtx& ctx)
                     -> failure_or<std::optional<element_type_tag>> {
      auto pipe = ctx.get(pipe_arg);
      if (not pipe) {
        return std::nullopt;
      }
      return pipe->inner.infer_type(input, ctx);
    });
    d.validate([jobs](DescribeCtx& ctx) -> Empty {
      if (auto j = ctx.get(jobs); j and j->inner == 0) {
        diagnostic::error("`jobs` must not be zero").primary(*j).emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::parallel2

TENZIR_REGISTER_PLUGIN(tenzir::plugins::parallel2::plugin)
