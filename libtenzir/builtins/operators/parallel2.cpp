//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"
#include "tenzir/async/routing.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/eval.hpp"

#include <thread>

namespace tenzir::plugins::parallel2 {

namespace {

struct ParallelArgs {
  Option<located<uint64_t>> jobs;
  Option<ast::expression> route_by;
  bool fuse = true;
  located<ir::pipeline> pipe;
};

/// Shared implementation for both transform and sink variants.
class ParallelImpl {
public:
  explicit ParallelImpl(ParallelArgs args)
    : jobs_{args.jobs
              ? args.jobs->inner
              : std::max(uint64_t{1}, static_cast<uint64_t>(
                                        std::thread::hardware_concurrency()))},
      route_by_{std::move(args.route_by)},
      fuse_{args.fuse},
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
      if (fuse_) {
        co_await ctx.spawn_sub_fused<table_slice>(data{int64_t(i)},
                                                  std::move(copy));
      } else {
        co_await ctx.spawn_sub<table_slice>(data{int64_t(i)}, std::move(copy));
      }
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

  auto finalize(OpCtx& ctx) const -> Task<void> {
    for (auto i = uint64_t{0}; i < jobs_; ++i) {
      auto sub = ctx.get_sub(int64_t(i));
      if (sub) {
        auto& pipe = as<SubHandle<table_slice>>(*sub);
        co_await pipe.close();
      }
    }
  }

private:
  auto process_hash(table_slice input, OpCtx& ctx) -> Task<void> {
    auto values = eval(*route_by_, input, ctx.dh());
    // Find runs of same-bucket rows and push subslices.
    for (auto [bucket, begin, end] : routing::hash_runs(values, jobs_)) {
      auto slice = subslice(input, begin, end);
      auto sub = ctx.get_sub(int64_t(bucket));
      TENZIR_ASSERT(sub);
      auto& pipe = as<SubHandle<table_slice>>(*sub);
      std::ignore = co_await pipe.push(std::move(slice));
    }
  }

  auto process_round_robin(table_slice input, OpCtx& ctx) -> Task<void> {
    auto total_rows = static_cast<uint64_t>(input.rows());
    auto assignments = routing::distribute_adaptive(total_rows, rows_assigned_);
    auto offset = size_t{0};
    for (auto [worker, count] : assignments) {
      auto slice = subslice(input, offset, offset + count);
      offset += count;
      auto sub = ctx.get_sub(int64_t(worker));
      TENZIR_ASSERT(sub);
      auto& pipe = as<SubHandle<table_slice>>(*sub);
      std::ignore = co_await pipe.push(std::move(slice));
    }
  }

  uint64_t jobs_;
  Option<ast::expression> route_by_;
  bool fuse_;
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
      fuse_{args.fuse},
      pipe_{std::move(args.pipe.inner)} {
  }

  auto start(OpCtx& ctx) -> Task<void> {
    for (auto i = uint64_t{0}; i < jobs_; ++i) {
      auto copy = pipe_;
      auto sub_ctx = substitute_ctx{base_ctx{ctx.dh(), ctx.reg()}, nullptr};
      if (not copy.substitute(sub_ctx, true)) {
        co_return;
      }
      if (fuse_) {
        co_await ctx.spawn_sub_fused<void>(data{int64_t(i)}, std::move(copy));
      } else {
        co_await ctx.spawn_sub<void>(data{int64_t(i)}, std::move(copy));
      }
    }
  }

private:
  uint64_t jobs_;
  bool fuse_;
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
    d.named_optional("_fuse", &ParallelArgs::fuse);
    auto pipe = d.pipeline(&ParallelArgs::pipe, SubOptimize::from_downstream);
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
    // We use `invariant_order()` so that residual filters do not escape, but
    // are reinserted at the front of the subpipeline instead.
    return d.invariant_order();
  }
};

} // namespace

} // namespace tenzir::plugins::parallel2

TENZIR_REGISTER_PLUGIN(tenzir::plugins::parallel2::plugin)
