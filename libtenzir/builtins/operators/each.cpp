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

template <class Output>
class Each final : public Operator<table_slice, Output> {
public:
  explicit Each(EachArgs args) : args_{std::move(args)} {
  }

  auto process(table_slice input, Push<Output>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
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

  auto process_sub(SubKeyView key, chunk_ptr chunk, Push<Output>& push,
                   OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(key, ctx);
    if constexpr (std::same_as<Output, chunk_ptr>) {
      co_await push(std::move(chunk));
    }
  }

  auto finish_sub(SubKeyView key, Push<Output>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(key, push, ctx);
    TENZIR_ASSERT(running_subs_ > 0);
    running_subs_ -= 1;
    if (todo_.rows() > 0) {
      auto input = view_at(*check(to_record_batch(todo_)->ToStructArray()), 0);
      TENZIR_ASSERT(input);
      co_await spawn_for(*input, ctx);
      todo_ = tail(todo_, todo_.rows() - 1);
    }
  }

  auto state() -> OperatorState override {
    return running_subs_ < args_.parallel ? OperatorState::normal
                                          : OperatorState::blocked;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("next_key", next_key_);
    serde("running_subs", running_subs_);
    serde("todo", todo_);
  }

private:
  EachArgs args_;
  /// Counter to give every subpipeline a unique key.
  uint64_t next_key_ = 0;
  /// Number of currently running subpipelines.
  size_t running_subs_ = 0;
  /// Rows that still need to have a subpipeline spawned.
  table_slice todo_;
};

class EachPlugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "each";
  }

  auto describe() const -> Description override {
    auto d = Describer<EachArgs, Each<table_slice>>{};
    auto parallel = d.named_optional("parallel", &EachArgs::parallel);
    auto pipe = d.pipeline(&EachArgs::pipe, {{"this", &EachArgs::this_id}});
    d.validate([=](DescribeCtx& ctx) -> Empty {
      if (auto parallel_value = ctx.get(parallel)) {
        if (*parallel_value < 1) {
          diagnostic::error("`parallel` must be at least 1")
            .primary(ctx.get_location(parallel).value())
            .emit(ctx);
        }
      }
      return {};
    });
    d.spawner([pipe]<class Input>(DescribeCtx& ctx)
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
          return std::function<Box<Operator<Input, table_slice>>(EachArgs)>{
            [](EachArgs args) {
              return Each<table_slice>{std::move(args)};
            }};
        }
        if (**result == tag_v<chunk_ptr>) {
          return std::function<Box<Operator<Input, chunk_ptr>>(EachArgs)>{
            [](EachArgs args) {
              return Each<chunk_ptr>{std::move(args)};
            }};
        }
        diagnostic::error("pipeline inside `each` must produce events or bytes")
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
