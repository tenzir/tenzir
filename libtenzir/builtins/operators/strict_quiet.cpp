//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/detail/string_literal.hpp>
#include <tenzir/error.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/type.h>

namespace tenzir::plugins::strict_quiet {

namespace {

// --- New-executor implementation ---

struct DiagnosticScopeArgs {
  located<ir::pipeline> pipe;
};

// Primary template: non-void input, non-void output.
template <DiagnosticBehavior Behavior, class Input, class Output>
class DiagnosticScopeOp final : public Operator<Input, Output> {
public:
  explicit DiagnosticScopeOp(DiagnosticScopeArgs args)
    : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (not co_await ctx.plan_and_spawn_sub<Input>(int64_t{0}, args_.pipe.inner,
                                                   Behavior)) {
      co_return;
    }
  }

  auto process(Input input, Push<Output>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    auto sub = ctx.get_sub(int64_t{0});
    if (not sub) {
      co_return;
    }
    std::ignore = co_await as<SubHandle<Input>>(*sub).push(std::move(input));
  }

  auto process_sub(SubKeyView key, chunk_ptr chunk, Push<Output>& push,
                   OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(key, ctx);
    if constexpr (std::same_as<Output, chunk_ptr>) {
      co_await push(std::move(chunk));
    } else {
      TENZIR_UNREACHABLE();
    }
  }

private:
  DiagnosticScopeArgs args_;
};

// Partial specialisation: void output, non-void input.
template <DiagnosticBehavior Behavior, class Input>
class DiagnosticScopeOp<Behavior, Input, void> final
  : public Operator<Input, void> {
public:
  explicit DiagnosticScopeOp(DiagnosticScopeArgs args)
    : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (not co_await ctx.plan_and_spawn_sub<Input>(int64_t{0}, args_.pipe.inner,
                                                   Behavior)) {
      co_return;
    }
  }

  auto process(Input input, OpCtx& ctx) -> Task<void> override {
    auto sub = ctx.get_sub(int64_t{0});
    if (not sub) {
      co_return;
    }
    std::ignore = co_await as<SubHandle<Input>>(*sub).push(std::move(input));
  }

private:
  DiagnosticScopeArgs args_;
};

// Partial specialisation: void input, non-void output.
template <DiagnosticBehavior Behavior, class Output>
class DiagnosticScopeOp<Behavior, void, Output> final
  : public Operator<void, Output> {
public:
  explicit DiagnosticScopeOp(DiagnosticScopeArgs args)
    : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (not co_await ctx.plan_and_spawn_sub<void>(int64_t{0}, args_.pipe.inner,
                                                  Behavior)) {
      co_return;
    }
  }

  auto state() -> OperatorState override {
    return OperatorState::done;
  }

  auto process_sub(SubKeyView key, chunk_ptr chunk, Push<Output>& push,
                   OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(key, ctx);
    if constexpr (std::same_as<Output, chunk_ptr>) {
      co_await push(std::move(chunk));
    } else {
      TENZIR_UNREACHABLE();
    }
  }

private:
  DiagnosticScopeArgs args_;
};

// Full specialisation: void input, void output.
template <DiagnosticBehavior Behavior>
class DiagnosticScopeOp<Behavior, void, void> final
  : public Operator<void, void> {
public:
  explicit DiagnosticScopeOp(DiagnosticScopeArgs args)
    : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (not co_await ctx.plan_and_spawn_sub<void>(int64_t{0}, args_.pipe.inner,
                                                  Behavior)) {
      co_return;
    }
  }

  auto state() -> OperatorState override {
    return OperatorState::done;
  }

private:
  DiagnosticScopeArgs args_;
};

template <DiagnosticBehavior Behavior>
auto describe_diagnostic_scope() -> Description {
  auto d = Describer<DiagnosticScopeArgs>{};
  // Optimize the subpipeline independently in plan_and_spawn_sub(). Pushing
  // downstream predicates into it would change their diagnostic behavior.
  auto pipe = d.pipeline(&DiagnosticScopeArgs::pipe, SubOptimize::off);
  d.spawner([pipe]<class Input>(DescribeCtx& ctx)
              -> failure_or<Option<SpawnWith<DiagnosticScopeArgs, Input>>> {
    TRY(auto p, ctx.get(pipe));
    TRY(auto output, p.inner.infer_type(tag_v<Input>, ctx));
    return match(
      output,
      [&](tag<table_slice>)
        -> failure_or<Option<SpawnWith<DiagnosticScopeArgs, Input>>> {
        if constexpr (std::same_as<Input, nova::Events>) {
          diagnostic::error("subpipeline must not produce events")
            .primary(p.source)
            .emit(ctx);
          return failure::promise();
        } else {
          return [](DiagnosticScopeArgs args) {
            return DiagnosticScopeOp<Behavior, Input, table_slice>{
              std::move(args)};
          };
        }
      },
      [](tag<chunk_ptr>)
        -> failure_or<Option<SpawnWith<DiagnosticScopeArgs, Input>>> {
        return [](DiagnosticScopeArgs args) {
          return DiagnosticScopeOp<Behavior, Input, chunk_ptr>{std::move(args)};
        };
      },
      [](tag<void>)
        -> failure_or<Option<SpawnWith<DiagnosticScopeArgs, Input>>> {
        return [](DiagnosticScopeArgs args) {
          return DiagnosticScopeOp<Behavior, Input, void>{std::move(args)};
        };
      },
      [&](tag<nova::Events>)
        -> failure_or<Option<SpawnWith<DiagnosticScopeArgs, Input>>> {
        if constexpr (concepts::one_of<Input, void, chunk_ptr, nova::Events>) {
          return [](DiagnosticScopeArgs args) {
            return DiagnosticScopeOp<Behavior, Input, nova::Events>{
              std::move(args)};
          };
        } else {
          diagnostic::error("subpipeline must not produce nova_events")
            .primary(p.source)
            .emit(ctx);
          return failure::promise();
        }
      });
  });
  return d.without_optimize();
}

// --- Plugin ---

struct strict : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "strict";
  }

  auto describe() const -> Description override {
    return describe_diagnostic_scope<DiagnosticBehavior::WarningToError>();
  }
};

// Experimental: suppress runtime warnings until selective diagnostic handling
// and dead-letter queues have a defined contract.
class QuietPlugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "quiet";
  }

  auto describe() const -> Description override {
    return describe_diagnostic_scope<DiagnosticBehavior::SuppressWarnings>();
  }
};

} // namespace

} // namespace tenzir::plugins::strict_quiet

TENZIR_REGISTER_PLUGIN(tenzir::plugins::strict_quiet::strict)

TENZIR_REGISTER_PLUGIN(tenzir::plugins::strict_quiet::QuietPlugin)
