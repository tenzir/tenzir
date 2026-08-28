//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/argument_parser2.hpp"
#include "tenzir/compile_ctx.hpp"
#include "tenzir/detail/scope_guard.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/panic.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/session.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tql2/eval.hpp"
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
  Option<located<ir::parallelism::Fusing>> fuse;
  Option<located<uint16_t>> limit_partitions; ///< None → inherit
  located<ir::pipeline> pipe;

  friend auto inspect(auto& f, ParallelArgs& x) -> bool {
    return f.object(x).fields(f.field("keyword", x.keyword),
                              f.field("jobs", x.jobs), f.field("fuse", x.fuse),
                              f.field("limit_partitions", x.limit_partitions),
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
    auto scope = ir::Parallelism{
      .degree = degree,
      .limit_partitions = args_.limit_partitions
                            ? args_.limit_partitions->inner
                            : builder.par().limit_partitions,
      .fuse = args_.fuse ? args_.fuse->inner : builder.par().fuse,
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
    // `argument_parser2` compiles subpipelines into runtime pipelines, but we
    // need the IR. Take the pipeline argument out of the invocation and let the
    // parser handle everything else.
    auto* pipe_expr = static_cast<ast::pipeline_expr*>(nullptr);
    auto rest = std::vector<ast::expression>{};
    for (auto& arg : inv.args) {
      auto* p = try_as<ast::pipeline_expr>(arg);
      if (not p) {
        rest.push_back(std::move(arg));
        continue;
      }
      if (pipe_expr) {
        diagnostic::error("duplicate `pipeline` argument")
          .primary(arg)
          .secondary(pipe_expr->get_location(), "previously provided here")
          .emit(ctx);
        return failure::promise();
      }
      pipe_expr = p;
    }
    if (not pipe_expr) {
      diagnostic::error("`parallel` requires a pipeline argument `{{ … }}`")
        .primary(args.keyword)
        .emit(ctx);
      return failure::promise();
    }
    auto jobs = Option<located<uint64_t>>{};
    auto fuse = Option<located<std::string>>{};
    auto limit_partitions = Option<located<uint64_t>>{};
    auto legacy_fuse = Option<located<bool>>{};
    auto route_by = Option<ast::expression>{};
    // TODO: `Describer` is the intended argument machinery, but it is only
    // reachable through `OperatorPlugin`/`GenericIr`, which cannot plan a
    // parallelism scope. Until it grows a planning hook, use the legacy parser
    // with a session shim, as `where` and `top_rare` do.
    auto provider = session_provider::make(ctx);
    TRY(argument_parser2::operator_("parallel")
          .positional("jobs", jobs, "int")
          .named("fuse", fuse, "string")
          .named("limit_partitions", limit_partitions, "int")
          .named("_fuse", legacy_fuse, "bool")
          .named("route_by", route_by, "field")
          .parse(operator_factory_invocation{inv.op, rest},
                 provider.as_session()));
    if (jobs) {
      if (jobs->inner == 0) {
        diagnostic::error("`jobs` must be greater than zero")
          .primary(*jobs)
          .emit(ctx);
        return failure::promise();
      }
      args.jobs = *jobs;
    }
    if (limit_partitions) {
      constexpr auto max = std::numeric_limits<uint16_t>::max();
      if (limit_partitions->inner == 0 or limit_partitions->inner > max) {
        diagnostic::error("`limit_partitions` must be between 1 and {}", max)
          .primary(*limit_partitions)
          .emit(ctx);
        return failure::promise();
      }
      args.limit_partitions = located{
        static_cast<uint16_t>(limit_partitions->inner),
        limit_partitions->source,
      };
    }
    if (fuse) {
      auto parsed = ir::parallelism::parse_fusing(fuse->inner);
      if (not parsed) {
        diagnostic::error("`fuse` must be one of `none`, `parallel`, or `all`")
          .primary(*fuse)
          .emit(ctx);
        return failure::promise();
      }
      args.fuse = located{*parsed, fuse->source};
    }
    if (legacy_fuse) {
      diagnostic::warning("`_fuse` is deprecated")
        .primary(*legacy_fuse)
        .note("use `fuse=\"{}\"` instead",
              legacy_fuse->inner ? "parallel" : "none")
        .emit(ctx);
      if (fuse) {
        diagnostic::error("cannot combine `_fuse` and `fuse`")
          .primary(*legacy_fuse)
          .secondary(*fuse, "`fuse` provided here")
          .emit(ctx);
        return failure::promise();
      }
      args.fuse = located{legacy_fuse->inner ? ir::parallelism::Fusing::parallel
                                             : ir::parallelism::Fusing::none,
                          legacy_fuse->source};
    }
    if (route_by) {
      // Accepted so that existing pipelines keep compiling. `parallel` no
      // longer routes events itself; operators that need their input
      // partitioned, such as `summarize` and `deduplicate`, declare their own
      // keys and receive a matching exchange.
      diagnostic::warning("`route_by` is no longer needed")
        .primary(*route_by)
        .note("events are now routed to the correct operator instance "
              "automatically")
        .emit(ctx);
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
