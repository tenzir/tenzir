//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/compile_ctx.hpp"
#include "tenzir/detail/scope_guard.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/panic.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <algorithm>
#include <limits>
#include <thread>

namespace tenzir::plugins::parallel2 {

namespace {

/// The upper bound on the degree used when `jobs` is omitted. Spawning one
/// instance per hardware thread rarely pays off for the section of a pipeline
/// that `parallel` wraps, so we cap the implicit degree here.
constexpr auto max_default_jobs = uint64_t{8};

/// The largest degree of partitioning that `limit_partitions` accepts.
constexpr auto max_limit_partitions
  = uint64_t{std::numeric_limits<uint16_t>::max()};

/// The degree used when `jobs` is omitted: `max_default_jobs`, limited to the
/// number of hardware threads when the implementation can determine it.
auto default_jobs() -> uint64_t {
  const auto concurrency = std::thread::hardware_concurrency();
  if (concurrency == 0) {
    return max_default_jobs;
  }
  return std::min(max_default_jobs, uint64_t{concurrency});
}

struct ParallelArgs {
  location keyword;
  Option<located<uint64_t>> jobs; ///< None → `default_jobs`
  /// None → inherit from the enclosing parallelism scope.
  Option<located<std::string>> fuse;
  Option<located<uint64_t>> limit_partitions; ///< None → inherit
  Option<located<bool>> legacy_fuse;          ///< Deprecated alias for `fuse`.
  Option<ast::expression> route_by;           ///< Deprecated and ignored.

  /// The fusing mode, if given. Validated during argument parsing.
  auto fusing() const -> Option<located<ir::parallelism::Fusing>> {
    if (legacy_fuse) {
      return located{legacy_fuse->inner ? ir::parallelism::Fusing::parallel
                                        : ir::parallelism::Fusing::none,
                     legacy_fuse->source};
    }
    if (not fuse) {
      return None{};
    }
    auto parsed = ir::parallelism::parse_fusing(fuse->inner);
    TENZIR_ASSERT(parsed);
    return located{*parsed, fuse->source};
  }
};

auto describe_parallel() -> Description {
  auto d = Describer<ParallelArgs>{};
  d.name("parallel");
  d.operator_location(&ParallelArgs::keyword);
  auto jobs = d.positional("jobs", &ParallelArgs::jobs, "int");
  auto fuse = d.named("fuse", &ParallelArgs::fuse, "string");
  auto limit_partitions
    = d.named("limit_partitions", &ParallelArgs::limit_partitions, "int");
  auto legacy_fuse = d.named("_fuse", &ParallelArgs::legacy_fuse, "bool");
  auto route_by = d.named("route_by", &ParallelArgs::route_by, "field");
  // `ParallelIr::optimize()` optimizes the subpipeline itself because it must
  // also decide whether the operators within may reorder.
  d.pipeline(SubOptimize::off);
  d.inline_pipeline();
  d.validate([=](DescribeCtx& ctx) -> Empty {
    if (auto value = ctx.get(jobs); value and value->inner == 0) {
      diagnostic::error("`jobs` must be greater than zero")
        .primary(*value)
        .emit(ctx);
    }
    if (auto value = ctx.get(limit_partitions);
        value and (value->inner == 0 or value->inner > max_limit_partitions)) {
      diagnostic::error("`limit_partitions` must be between 1 and {}",
                        max_limit_partitions)
        .primary(*value)
        .emit(ctx);
    }
    auto fuse_value = ctx.get(fuse);
    if (fuse_value and not ir::parallelism::parse_fusing(fuse_value->inner)) {
      diagnostic::error("`fuse` must be one of `none`, `parallel`, or `all`")
        .primary(*fuse_value)
        .emit(ctx);
    }
    if (auto value = ctx.get(legacy_fuse)) {
      diagnostic::warning("`_fuse` is deprecated")
        .primary(*value)
        .note("use `fuse=\"{}\"` instead", value->inner ? "parallel" : "none")
        .emit(ctx);
      if (fuse_value) {
        diagnostic::error("cannot combine `_fuse` and `fuse`")
          .primary(*value)
          .secondary(*fuse_value, "`fuse` provided here")
          .emit(ctx);
      }
    }
    if (auto value = ctx.get(route_by)) {
      // Accepted so that existing pipelines keep compiling. `parallel` no
      // longer routes events itself; operators that need their input
      // partitioned, such as `summarize` and `deduplicate`, declare their own
      // keys and receive a matching exchange.
      diagnostic::warning("`route_by` is no longer needed")
        .primary(*value)
        .note("events are now routed to the correct operator instance "
              "automatically")
        .emit(ctx);
    }
    return {};
  });
  return d.only_arguments();
}

using ParallelArguments = OperatorArguments<ParallelArgs, describe_parallel>;

class ParallelIr final : public ir::Operator {
public:
  ParallelIr() = default;

  explicit ParallelIr(ParallelArguments args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "parallel_ir";
  }

  auto copy() const -> Box<ir::Operator> override {
    return ParallelIr{args_};
  }

  auto move() && -> Box<ir::Operator> override {
    return ParallelIr{std::move(args_)};
  }

  auto jobs() const -> uint64_t {
    auto jobs = args_.get().jobs;
    return jobs ? jobs->inner : default_jobs();
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<element_type_tag> override {
    auto pipe = args_.pipe();
    TENZIR_ASSERT(pipe);
    if (input.is<chunk_ptr>()) {
      diagnostic::error("`parallel` does not accept bytes as input")
        .primary(args_.main_location())
        .emit(dh);
      return failure::promise();
    }
    TRY(auto output, pipe->inner.infer_type(input, dh));
    if (output.is<chunk_ptr>()) {
      diagnostic::error("`parallel` subpipeline must not produce bytes")
        .primary(pipe->source)
        .emit(dh);
      return failure::promise();
    }
    return output;
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    return args_.substitute(ctx, instantiate);
  }

  auto optimize(ir::OptimizeRequest req,
                const ir::OptimizeCtx&) && -> ir::OptimizeResult override {
    // Determine whether operators inside this parallel block may reorder.
    auto sub_octx = ir::OptimizeCtx{
      .can_any_op_reorder = jobs() > 1,
    };
    auto pipe = args_.pipe();
    TENZIR_ASSERT(pipe);
    // Apply downstream requirements into the subpipeline. Every replica would
    // interpret a limit independently, so a limit must not escape to the
    // shared upstream. In contrast, every replica's input projection is the
    // same requirement on that shared input.
    auto sub = std::move(pipe->inner)
                 .optimize(
                   ir::OptimizeRequest{
                     .filter = std::move(req.filter),
                     .order = req.order,
                     .projection = std::move(req.projection),
                   },
                   sub_octx);
    // Reinsert residual filters at the front of the subpipeline so they don't
    // escape past `parallel` (no filter propagation upstream).
    pipe->inner = std::move(sub.replacement);
    pipe->inner.prepend(std::move(sub.filter));
    // Return this operator as the replacement; don't propagate filter upstream.
    auto replacement = std::vector<Box<ir::Operator>>{};
    replacement.push_back(std::move(*this).move());
    return {
      .filter = {},
      .order = sub.order,
      .replacement = ir::pipeline{{}, std::move(replacement)},
      .projection = std::move(sub.projection),
    };
  }

  auto spawn(element_type_tag) const -> AnyOperator override {
    panic("parallel must be lowered into the plan before spawning");
  }

  auto plan(ir::PlanBuilder& builder, ir::PlanPorts input,
            diagnostic_handler& dh) && -> failure_or<ir::PlanPorts> override {
    auto pipe = args_.take_pipe();
    auto args = args_.get();
    auto degree = args.jobs ? args.jobs->inner : default_jobs();
    if (degree > 1) {
      // The subpipeline's head may run at a degree greater than one, which the
      // external input cannot feed directly.
      input = builder.scatter_external_input(std::move(input));
    }
    auto fuse = args.fusing();
    auto scope = ir::Parallelism{
      .degree = degree,
      .limit_partitions
      = args.limit_partitions
          ? static_cast<uint16_t>(args.limit_partitions->inner)
          : builder.par().limit_partitions,
      .fuse = fuse ? fuse->inner : builder.par().fuse,
    };
    builder.push_par_scope(scope);
    auto guard = detail::scope_guard([&]() noexcept {
      builder.pop_par_scope();
    });
    return builder.lower_pipeline(std::move(pipe.inner), std::move(input), dh);
  }

  auto main_location() const -> location override {
    return args_.main_location();
  }

  friend auto inspect(auto& f, ParallelIr& x) -> bool {
    return f.apply(x.args_);
  }

private:
  ParallelArguments args_;
};

class plugin final : public virtual operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.parallel";
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::CompileResult> override {
    TRY(auto args, ParallelArguments::parse(std::move(inv), ctx));
    return ParallelIr{std::move(args)};
  }
};

using parallel_ir_plugin = inspection_plugin<ir::Operator, ParallelIr>;

} // namespace

} // namespace tenzir::plugins::parallel2

TENZIR_REGISTER_PLUGIN(tenzir::plugins::parallel2::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::parallel2::parallel_ir_plugin)
