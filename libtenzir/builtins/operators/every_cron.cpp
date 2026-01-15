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

#include <string_view>

namespace tenzir::plugins::every_cron {

namespace {

// TODO: Consider moving these into a common place.
#define TENZIR_TRY_COMMON_(var, expr, ret)                                     \
  auto var = (expr);                                                           \
  if (not tenzir::tryable<decltype(var)>::is_success(var)) [[unlikely]] {      \
    ret tenzir::tryable<decltype(var)>::get_error(std::move(var));             \
  }

#define TENZIR_TRY_EXTRACT_(decl, var, expr, ret)                              \
  TENZIR_TRY_COMMON_(var, expr, ret);                                          \
  decl = tenzir::tryable<decltype(var)>::get_success(std::move(var))

#define TENZIR_TRY_DISCARD_(var, expr, ret)                                    \
  TENZIR_TRY_COMMON_(var, expr, ret)                                           \
  if (false) {                                                                 \
    /* trigger [[nodiscard]] */                                                \
    tenzir::tryable<decltype(var)>::get_success(std::move(var));               \
  }

#define TENZIR_TRY_X_1(expr, ret)                                              \
  TENZIR_TRY_DISCARD_(TENZIR_PP_PASTE2(_try, __COUNTER__), expr, ret)

#define TENZIR_TRY_X_2(decl, expr, ret)                                        \
  TENZIR_TRY_EXTRACT_(decl, TENZIR_PP_PASTE2(_try, __COUNTER__), expr, ret)

#define TENZIR_CO_TRY(...)                                                     \
  TENZIR_PP_OVERLOAD(TENZIR_TRY_X_, __VA_ARGS__)(__VA_ARGS__, co_return)

#define CO_TRY TENZIR_CO_TRY

auto sleep(duration d) -> Task<void> {
  return folly::coro::sleep(
    std::chrono::duration_cast<folly::HighResDuration>(d));
}

auto sleep_until(time t) -> Task<void> {
  auto now = time::clock::now();
  // The check is needed because `-` can overflow and yield unexpected results.
  auto diff = t < now ? duration{0} : t - time::clock::now();
  TENZIR_WARN("diff = {}", diff);
  return sleep(diff);
}

template <class Input>
class EveryBase : public Operator<Input, table_slice> {
public:
  EveryBase(duration interval, ir::pipeline ir)
    : interval_{interval}, ir_{std::move(ir)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    // TODO: Do we directly want to spawn one here? What if we restore?
    // (co_await spawn_new(ctx)).ignore();
    co_return;
  }

  auto await_task() const -> Task<std::any> override {
    auto next = last_started_ + interval_;
    TENZIR_WARN("sleeping in every until {}", next);
    co_await sleep_until(next);
    TENZIR_WARN("waking every");
    co_return {};
  }

  auto process_task(std::any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    last_started_ = time::clock::now();
    if (next_ > 0) {
      auto last_pipe = ctx.get_sub(next_ - 1);
      if (last_pipe) {
        auto pipe = as<OpenPipeline<Input>>(*last_pipe);
        // TODO: Does this get rid of it?
        pipe.close();
      } else {
        // FIXME
      }
    }
    auto spawn_result = co_await spawn_new(ctx);
    spawn_result.ignore();
  }

  auto spawn_new(OpCtx& ctx) -> Task<failure_or<AnyOpenPipeline>> {
    auto copy = ir_;
    // FIXME: Don't do this.
    auto reg = global_registry();
    auto b_ctx = base_ctx{ctx, *reg, ctx.actor_system()};
    CO_TRY(copy.substitute(substitute_ctx{b_ctx, nullptr}, true));
    auto id = next_;
    next_ += 1;
    co_return co_await ctx.spawn_sub(id, std::move(copy), tag_v<Input>);
  }

  auto snapshot(Serde& s) -> void override {
    s("next", next_);
    s("last_started", last_started_);
  }

protected:
  duration interval_;
  ir::pipeline ir_;

  time last_started_ = time::min();
  size_t next_ = 0;
};

template <class Input>
class Every;

template <>
class Every<table_slice> final : public EveryBase<table_slice> {
public:
  using EveryBase::EveryBase;

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_ERROR("got input in every: {}", input.rows());
    TENZIR_UNUSED(push);
    // TODO: Do we know that we have a subpipeline running?
    TENZIR_ASSERT(this->next_ > 0);
    auto sub
      = as<OpenPipeline<table_slice>>(check(ctx.get_sub(this->next_ - 1)));
    (co_await sub.push(std::move(input))).expect("not closed?");
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
