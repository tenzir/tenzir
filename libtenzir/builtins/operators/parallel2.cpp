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
#include "tenzir/panic.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <thread>

namespace tenzir::plugins::parallel2 {

namespace {

/// The degree used when `jobs` is omitted. We use the number of hardware
/// threads, falling back to a fixed value when the implementation cannot
/// determine the available concurrency.
constexpr auto fallback_jobs = uint64_t{8};

auto default_jobs() -> uint64_t {
  const auto concurrency = std::thread::hardware_concurrency();
  return concurrency > 0 ? uint64_t{concurrency} : fallback_jobs;
}

struct ParallelArgs {
  location keyword;
  Option<located<uint64_t>> jobs; ///< None → `default_jobs`
  bool fuse = true; ///< true → Fusing::parallel, false → Fusing::none
  located<ir::pipeline> pipe;

  friend auto inspect(auto& f, ParallelArgs& x) -> bool {
    return f.object(x).fields(f.field("keyword", x.keyword),
                              f.field("jobs", x.jobs), f.field("fuse", x.fuse),
                              f.field("pipe", x.pipe));
  }
};

class ParallelIr final : public ir::Operator {
public:
  ParallelIr() = default;

  explicit ParallelIr(ParallelArgs args) : args_{std::move(args)} {
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

  auto jobs() const& -> uint64_t {
    return args_.jobs ? args_.jobs->inner : default_jobs();
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<element_type_tag> override {
    if (input.is<chunk_ptr>()) {
      diagnostic::error("`parallel` does not accept bytes as input")
        .primary(args_.keyword)
        .emit(dh);
      return failure::promise();
    }
    TRY(auto output, args_.pipe.inner.infer_type(input, dh));
    if (output.is<chunk_ptr>()) {
      diagnostic::error("`parallel` subpipeline must not produce bytes")
        .primary(args_.pipe.source)
        .emit(dh);
      return failure::promise();
    }
    return output;
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    return args_.pipe.inner.substitute(ctx, instantiate);
  }

  auto optimize(ir::optimize_filter filter, event_order order,
                const ir::OptimizeCtx&) && -> ir::optimize_result override {
    // Determine whether operators inside this parallel block may reorder.
    auto degree = jobs();
    auto sub_octx = ir::OptimizeCtx{
      .can_any_op_reorder = degree > 1,
    };
    // Apply downstream filter and order into the subpipeline (from_downstream).
    auto sub
      = std::move(args_.pipe.inner).optimize(std::move(filter), order, sub_octx);
    // Reinsert residual filters at the front of the subpipeline so they don't
    // escape past `parallel` (invariant_order: no filter propagation upstream).
    args_.pipe.inner = std::move(sub.replacement);
    args_.pipe.inner.prepend(std::move(sub.filter));
    // Return this operator as the replacement; don't propagate filter upstream.
    auto replacement = std::vector<Box<ir::Operator>>{};
    replacement.push_back(std::move(*this).move());
    return {
      .filter = {},
      .order = sub.order,
      .replacement = ir::pipeline{{}, std::move(replacement)},
    };
  }

  auto spawn(element_type_tag) const -> AnyOperator override {
    panic("parallel must be lowered into the plan before spawning");
  }

  auto plan(ir::PlanBuilder& builder, ir::PlanPorts input,
            diagnostic_handler& dh) && -> failure_or<ir::PlanPorts> override {
    auto degree = jobs();
    if (degree > 1) {
      // The subpipeline's head may run at a degree greater than one, which the
      // external input cannot feed directly.
      input = builder.scatter_external_input(std::move(input));
    }
    auto fuse = args_.fuse ? ir::parallelism::Fusing::parallel
                           : ir::parallelism::Fusing::none;
    auto scope = ir::Parallelism{
      .degree = degree,
      .limit_partitions = builder.par().limit_partitions,
      .fused = fuse,
    };
    builder.push_par_scope(scope);
    auto guard = detail::scope_guard([&]() noexcept {
      builder.pop_par_scope();
    });
    return builder.lower_pipeline(std::move(args_.pipe.inner), std::move(input),
                                  dh);
  }

  auto main_location() const -> location override {
    return args_.keyword;
  }

  friend auto inspect(auto& f, ParallelIr& x) -> bool {
    return f.apply(x.args_);
  }

private:
  ParallelArgs args_;
};

class plugin final : public virtual operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.parallel";
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::CompileResult> override {
    auto args = ParallelArgs{};
    args.keyword = inv.op.get_location();

    auto* pipe_expr = static_cast<ast::pipeline_expr*>(nullptr);
    auto pipe_location = Option<location>{};
    auto fuse_location = Option<location>{};
    auto route_by_location = Option<location>{};
    auto duplicate
      = [&](std::string_view what, location previous, location current) {
          diagnostic::error("duplicate `{}` argument", what)
            .primary(current)
            .secondary(previous, "previously provided here")
            .emit(ctx);
        };
    for (auto& arg : inv.args) {
      if (auto* p = try_as<ast::pipeline_expr>(arg)) {
        if (pipe_location) {
          duplicate("pipeline", *pipe_location, arg.get_location());
          return failure::promise();
        }
        pipe_location = arg.get_location();
        pipe_expr = p;
      } else if (auto* assign = try_as<ast::assignment>(arg)) {
        // named argument
        auto* name = try_as<ast::root_field>(*assign->left.kind);
        if (not name) {
          diagnostic::error("unexpected argument").primary(arg).emit(ctx);
          return failure::promise();
        }
        if (name->id.name == "_fuse") {
          if (fuse_location) {
            duplicate("_fuse", *fuse_location, arg.get_location());
            return failure::promise();
          }
          fuse_location = arg.get_location();
          TRY(auto val, const_eval(assign->right, ctx));
          auto* b = try_as<bool>(val.inner);
          if (not b) {
            diagnostic::error("`_fuse` must be a boolean")
              .primary(assign->right)
              .emit(ctx);
            return failure::promise();
          }
          args.fuse = *b;
        } else if (name->id.name == "route_by") {
          if (route_by_location) {
            duplicate("route_by", *route_by_location, arg.get_location());
            return failure::promise();
          }
          route_by_location = arg.get_location();
          // Accepted so that existing pipelines keep compiling. `parallel` no
          // longer routes events itself; operators that need their input
          // partitioned, such as `summarize` and `deduplicate`, declare their
          // own keys and receive a matching exchange.
          diagnostic::warning("`route_by` is no longer needed")
            .primary(arg)
            .note("events are now routed to the correct operator instance "
                  "automatically")
            .emit(ctx);
        } else {
          diagnostic::error("unknown argument `{}`", name->id.name)
            .primary(arg)
            .emit(ctx);
          return failure::promise();
        }
      } else {
        // positional argument: jobs
        if (args.jobs) {
          duplicate("jobs", args.jobs->source, arg.get_location());
          return failure::promise();
        }
        TRY(auto val, const_eval(arg, ctx));
        auto* i = try_as<int64_t>(val.inner);
        if (not i) {
          diagnostic::error("`jobs` must be an integer").primary(arg).emit(ctx);
          return failure::promise();
        }
        if (*i <= 0) {
          diagnostic::error("`jobs` must be greater than zero")
            .primary(arg)
            .emit(ctx);
          return failure::promise();
        }
        args.jobs
          = located<uint64_t>{static_cast<uint64_t>(*i), arg.get_location()};
      }
    }

    if (not pipe_expr) {
      diagnostic::error("`parallel` requires a pipeline argument `{{ … }}`")
        .primary(args.keyword)
        .emit(ctx);
      return failure::promise();
    }

    args.pipe.source = pipe_expr->get_location();
    TRY(args.pipe.inner, std::move(pipe_expr->inner).compile(ctx));
    return ParallelIr{std::move(args)};
  }
};

using parallel_ir_plugin = inspection_plugin<ir::Operator, ParallelIr>;

} // namespace

} // namespace tenzir::plugins::parallel2

TENZIR_REGISTER_PLUGIN(tenzir::plugins::parallel2::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::parallel2::parallel_ir_plugin)
