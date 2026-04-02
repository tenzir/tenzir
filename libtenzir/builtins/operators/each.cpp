//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/view3.hpp>

namespace tenzir::plugins::each {

namespace {

struct EachArgs {
  located<ir::pipeline> pipe;
  let_id this_id;
  uint64_t parallel = 10;
};

struct EachImpl {
  explicit EachImpl(EachArgs args) : args_{std::move(args)} {
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> {
    TENZIR_ASSERT(todo_.rows() == 0);
    TENZIR_ASSERT(running_subs_ < args_.parallel);
    auto take = args_.parallel - running_subs_;
    auto [head, tail] = split(input, take);
    todo_ = std::move(tail);
    for (auto row_view : values3(head)) {
      co_await spawn_for(row_view, ctx);
    }
  }

  auto spawn_for(record_view3 input, OpCtx& ctx) -> Task<void> {
    auto env = substitute_ctx::env_t{};
    env[args_.this_id] = materialize(input);
    auto sub_ctx = substitute_ctx{ctx, &env};
    auto copy = args_.pipe.inner;
    if (not copy.substitute(sub_ctx, true)) {
      co_await assert_cancelled();
      TENZIR_UNREACHABLE();
    }
    co_await ctx.spawn_sub<void>(data{next_key_}, std::move(copy));
    next_key_ += 1;
    running_subs_ += 1;
  }

  auto finish_sub(SubKeyView key, OpCtx& ctx) -> Task<void> {
    TENZIR_UNUSED(key, ctx);
    TENZIR_ASSERT(running_subs_ > 0);
    running_subs_ -= 1;
    if (todo_.rows() > 0) {
      auto input = view_at(*check(to_record_batch(todo_)->ToStructArray()), 0);
      TENZIR_ASSERT(input);
      co_await spawn_for(*input, ctx);
      todo_ = tail(todo_, todo_.rows() - 1);
    }
  }

  auto state() -> OperatorState {
    return running_subs_ < args_.parallel ? OperatorState::normal
                                          : OperatorState::blocked;
  }

  auto snapshot(Serde& serde) -> void {
    serde("next_key", next_key_);
    serde("running_subs", running_subs_);
    serde("todo", todo_);
  }

  EachArgs args_;
  uint64_t next_key_ = 0;
  size_t running_subs_ = 0;
  table_slice todo_;
};

/// Spawns a source subpipeline per event, forwarding their event output.
class Each final : public Operator<table_slice, table_slice>, private EachImpl {
public:
  using EachImpl::EachImpl;

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    return EachImpl::process(std::move(input), ctx);
  }

  auto finish_sub(SubKeyView key, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    return EachImpl::finish_sub(key, ctx);
  }

  auto state() -> OperatorState override {
    return EachImpl::state();
  }

  auto snapshot(Serde& serde) -> void override {
    EachImpl::snapshot(serde);
  }
};

/// Spawns a source subpipeline per event; subpipelines sink their own output.
class EachSink final : public Operator<table_slice, void>, private EachImpl {
public:
  using EachImpl::EachImpl;

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    return EachImpl::process(std::move(input), ctx);
  }

  auto finish_sub(SubKeyView key, OpCtx& ctx) -> Task<void> override {
    return EachImpl::finish_sub(key, ctx);
  }

  auto state() -> OperatorState override {
    return EachImpl::state();
  }

  auto snapshot(Serde& serde) -> void override {
    EachImpl::snapshot(serde);
  }
};

class EachPlugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "each";
  }

  auto describe() const -> Description override {
    auto d = Describer<EachArgs, Each>{};
    auto parallel = d.named_optional("parallel", &EachArgs::parallel);
    auto pipe = d.pipeline(&EachArgs::pipe, {{"this", &EachArgs::this_id}});
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto parallel_value, ctx.get(parallel));
      if (parallel_value < 1) {
        diagnostic::error("`parallel` must be at least 1")
          .primary(ctx.get_location(parallel).value())
          .emit(ctx);
      }
      return {};
    });
    d.spawner([=]<class Input>(DescribeCtx& ctx)
                -> failure_or<Option<SpawnWith<EachArgs, Input>>> {
      if constexpr (not std::same_as<Input, table_slice>) {
        return {};
      } else {
        TRY(auto p, ctx.get(pipe));
        auto null_dh = null_diagnostic_handler{};
        auto result = p.inner.infer_type(tag_v<void>, null_dh);
        if (not result or not *result) {
          diagnostic::error("pipeline inside `each` must be a source")
            .primary(p.source)
            .emit(ctx);
          return failure::promise();
        }
        if (**result == tag_v<table_slice>) {
          return [](EachArgs args) {
            return Each{std::move(args)};
          };
        }
        if (**result == tag_v<void>) {
          return [](EachArgs args) {
            return EachSink{std::move(args)};
          };
        }
        diagnostic::error("pipeline inside `each` must not produce bytes")
          .primary(p.source)
          .emit(ctx);
        return failure::promise();
      }
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::each

TENZIR_REGISTER_PLUGIN(tenzir::plugins::each::EachPlugin)
