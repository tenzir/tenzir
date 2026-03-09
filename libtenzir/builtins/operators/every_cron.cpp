//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/compile_ctx.hpp>
#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/detail/croncpp.hpp>
#include <tenzir/detail/string_literal.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/parser_interface.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/try.hpp>

#include <folly/coro/Sleep.h>

#include <string_view>

namespace tenzir::plugins::every_cron {

namespace {

auto sleep(duration d) -> Task<void> {
  return folly::coro::sleep(
    std::chrono::duration_cast<folly::HighResDuration>(d));
}

auto sleep_until(time t) -> Task<void> {
  auto now = time::clock::now();
  // The check is needed because `-` can overflow and yield unexpected results.
  auto diff = t < now ? duration{0} : t - time::clock::now();
  return sleep(diff);
}

template <class Input>
class EveryBase : public Operator<Input, table_slice> {
public:
  EveryBase(duration interval, ir::pipeline ir)
    : interval_{interval}, ir_{std::move(ir)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (not ctx.get_sub(int64_t{next_})) {
      co_await spawn_new(ctx);
    }
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    auto next = last_started_ + interval_;
    co_await sleep_until(next);
    co_return {};
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(result, push);
    sleep_done_ = true;
    // For transformation sub-pipelines, close the current one so it
    // finishes and triggers finish_sub.
    if constexpr (not std::same_as<Input, void>) {
      TENZIR_ASSERT(next_ > 0);
      auto sub = ctx.get_sub(int64_t{next_ - 1});
      if (sub) {
        auto& pipe = as<OpenPipeline<Input>>(*sub);
        co_await pipe.close();
      }
    }
    co_await maybe_respawn(ctx);
  }

  auto finish_sub(SubKeyView key, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(key, push);
    sub_finished_ = true;
    co_await maybe_respawn(ctx);
  }

  // TODO: Clean this up with the upcoming subpipelines PR.
  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push, ctx);
    done_ = true;
    co_return FinalizeBehavior::done;
  }

  auto snapshot(Serde& s) -> void override {
    s("next", next_);
    s("last_started", last_started_);
  }

protected:
  auto spawn_new(OpCtx& ctx) -> Task<void> {
    last_started_ = time::clock::now();
    sleep_done_ = false;
    sub_finished_ = false;
    co_await ctx.spawn_sub(int64_t{next_}, ir_, tag_v<Input>);
    ++next_;
  }

  auto maybe_respawn(OpCtx& ctx) -> Task<void> {
    if (sleep_done_ and sub_finished_ and not done_) {
      co_await spawn_new(ctx);
    }
  }

  duration interval_;
  ir::pipeline ir_;
  time last_started_ = time::min();
  int64_t next_ = 0;
  bool sleep_done_ = false;
  bool sub_finished_ = false;
  bool done_ = false;
};

template <class Input>
class Every;

template <>
class Every<table_slice> final : public EveryBase<table_slice> {
public:
  using EveryBase::EveryBase;

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    TENZIR_ASSERT(this->next_ > 0);
    // TODO: This can drop events at the boundary between closing the old
    // sub-pipeline and spawning the new one.
    auto sub = ctx.get_sub(int64_t{this->next_ - 1});
    if (not sub) {
      co_return;
    }
    auto& pipe = as<OpenPipeline<table_slice>>(*sub);
    std::ignore = co_await pipe.push(std::move(input));
  }
};

template <>
class Every<void> final : public EveryBase<void> {
public:
  using EveryBase<void>::EveryBase;
};

class every_ir final : public ir::Operator {
public:
  every_ir() = default;

  every_ir(ast::expression interval, ir::pipeline pipe)
    : interval_{std::move(interval)}, pipe_{std::move(pipe)} {
  }

  auto name() const -> std::string override {
    return "every_ir";
  }

  auto spawn(element_type_tag input) && -> AnyOperator override {
    // We know that this succeeds because instantiation must happen before.
    auto interval = as<duration>(interval_);
    return match(
      input,
      [&]<class Input>(tag<Input>) -> AnyOperator {
        return Every<Input>{interval, std::move(pipe_)};
      },
      [&](tag<chunk_ptr>) -> AnyOperator {
        TENZIR_UNREACHABLE();
      });
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    TRY(match(
      interval_,
      [&](ast::expression& expr) -> failure_or<void> {
        TRY(expr.substitute(ctx));
        if (instantiate or expr.is_deterministic(ctx)) {
          TRY(auto value, const_eval(expr, ctx));
          auto cast = try_as<duration>(value);
          if (not cast) {
            auto got = match(
              value,
              []<class T>(const T&) -> type_kind {
                return tag_v<data_to_type_t<T>>;
              },
              [](const pattern&) -> type_kind {
                TENZIR_UNREACHABLE();
              });
            diagnostic::error("expected `duration`, got `{}`", got)
              .primary(expr)
              .emit(ctx);
            return failure::promise();
          }
          // We can also do some extended validation here...
          if (*cast <= duration::zero()) {
            diagnostic::error("expected a positive duration")
              .primary(expr)
              .emit(ctx);
            return failure::promise();
          }
          interval_ = *cast;
        }
        return {};
      },
      [&](duration&) -> failure_or<void> {
        return {};
      }));
    TRY(pipe_.substitute(ctx, false));
    return {};
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>> override {
    return pipe_.infer_type(input, dh);
  }

  friend auto inspect(auto& f, every_ir& x) -> bool {
    return f.object(x).fields(f.field("interval", x.interval_),
                              f.field("pipe", x.pipe_));
  }

private:
  variant<ast::expression, duration> interval_;
  ir::pipeline pipe_;
};

using every_ir_plugin = inspection_plugin<ir::Operator, every_ir>;

class every_compiler_plugin final : public operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.every";
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<Box<ir::Operator>> override {
    // TODO: Improve this with argument parser.
    if (inv.args.size() != 2) {
      diagnostic::error("expected exactly two arguments")
        .primary(inv.op)
        .emit(ctx);
      return failure::promise();
    }
    TRY(inv.args[0].bind(ctx));
    auto pipe = as<ast::pipeline_expr>(inv.args[1]);
    TRY(auto pipe_ir, std::move(pipe.inner).compile(ctx));
    return every_ir{
      std::move(inv.args[0]),
      std::move(pipe_ir),
    };
  }
};

} // namespace

} // namespace tenzir::plugins::every_cron

TENZIR_REGISTER_PLUGIN(tenzir::plugins::every_cron::every_ir_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::every_cron::every_compiler_plugin)
