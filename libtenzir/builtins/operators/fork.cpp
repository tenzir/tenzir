//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"
#include "tenzir/compile_ctx.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/panic.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/substitute_ctx.hpp"

#include <tenzir/pipeline_executor.hpp>
#include <tenzir/scope_linked.hpp>
#include <tenzir/source.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <caf/actor_from_state.hpp>

#include <queue>

namespace tenzir::plugins::fork {

namespace {

struct side_channel_actor_traits {
  using signatures = caf::type_list<
    // Push events into the side-channel.
    auto(atom::push, table_slice events)->caf::result<void>,
    // Pull events from the side-channel.
    auto(atom::pull)->caf::result<table_slice>>
    // Forward metrics.
    ::append_from<metrics_receiver_actor::signatures>
    // Forward diagnostics.
    ::append_from<receiver_actor<diagnostic>::signatures>;
};

using side_channel_actor = caf::typed_actor<side_channel_actor_traits>;

class side_channel {
public:
  [[maybe_unused]] static constexpr auto name = "side-channel";

  side_channel(side_channel_actor::pointer self,
               shared_diagnostic_handler diagnostics_handler,
               metrics_receiver_actor metrics_receiver,
               uint64_t parent_operator_index)
    : self_{self},
      diagnostics_handler_{std::move(diagnostics_handler)},
      metrics_receiver_{std::move(metrics_receiver)},
      parent_operator_index_{parent_operator_index} {
  }

  auto make_behavior() -> side_channel_actor::behavior_type {
    return {
      [this](atom::push, table_slice events) -> caf::result<void> {
        TENZIR_ASSERT(not push_rp_.pending());
        if (pull_rp_.pending()) {
          TENZIR_ASSERT(buffer_.empty());
          pull_rp_.deliver(std::move(events));
          return {};
        }
        buffer_.push(std::move(events));
        if (buffer_.size() < max_buffered) {
          return {};
        }
        push_rp_ = self_->make_response_promise<void>();
        return push_rp_;
      },
      [this](atom::pull) -> caf::result<table_slice> {
        TENZIR_ASSERT(not pull_rp_.pending());
        if (buffer_.empty()) {
          pull_rp_ = self_->make_response_promise<table_slice>();
          return pull_rp_;
        }
        if (push_rp_.pending()) {
          TENZIR_ASSERT(buffer_.size() == max_buffered);
          push_rp_.deliver();
        }
        auto output = std::move(buffer_.front());
        buffer_.pop();
        return output;
      },
      [this](uint64_t operator_index, uuid metrics_id,
             type schema) -> caf::result<void> {
        TENZIR_UNUSED(operator_index);
        return self_
          ->mail(parent_operator_index_, metrics_id, std::move(schema))
          .delegate(metrics_receiver_);
      },
      [this](uint64_t operator_index, uuid metrics_id,
             record metrics) -> caf::result<void> {
        TENZIR_UNUSED(operator_index);
        return self_
          ->mail(parent_operator_index_, metrics_id, std::move(metrics))
          .delegate(metrics_receiver_);
      },
      [](const operator_metric& metrics) -> caf::result<void> {
        // Operator metrics cannot be forwarded.
        TENZIR_UNUSED(metrics);
        return {};
      },
      [this](diagnostic diag) -> caf::result<void> {
        diagnostics_handler_.emit(std::move(diag));
        return {};
      },
    };
  }

private:
  static constexpr auto max_buffered = size_t{10};
  caf::typed_response_promise<void> push_rp_;
  caf::typed_response_promise<table_slice> pull_rp_;
  std::queue<table_slice> buffer_;

  side_channel_actor::pointer self_;
  shared_diagnostic_handler diagnostics_handler_;
  metrics_receiver_actor metrics_receiver_;
  uint64_t parent_operator_index_;
};

class internal_fork_source_operator final
  : public crtp_operator<internal_fork_source_operator> {
public:
  internal_fork_source_operator() = default;

  internal_fork_source_operator(side_channel_actor side_channel)
    : side_channel_{std::move(side_channel)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    // Signal the start immediately as the parent pipeline won't deliver results
    // before the nested pipeline has started up.
    co_yield {};
    auto result = table_slice{};
    while (true) {
      ctrl.self()
        .mail(atom::pull_v)
        .request(side_channel_, caf::infinite)
        .then(
          [&](table_slice output) {
            result = std::move(output);
            ctrl.set_waiting(false);
          },
          [&](caf::error err) {
            diagnostic::error(std::move(err))
              .note("failed to accept forwarded events")
              .emit(ctrl.diagnostics());
          });
      ctrl.set_waiting(true);
      co_yield {};
      if (result.rows() == 0) {
        co_return;
      }
      co_yield std::move(result);
    }
  }

  auto name() const -> std::string override {
    return "internal-fork-source";
  }

  auto optimize(const expression&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, internal_fork_source_operator& x) -> bool {
    return f.object(x).fields(f.field("side_channel", x.side_channel_));
  }

private:
  side_channel_actor side_channel_;
};

class fork_operator final : public crtp_operator<fork_operator> {
public:
  fork_operator() = default;

  explicit fork_operator(located<pipeline> pipe) : pipe_{std::move(pipe)} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto side_channel = scope_linked{ctrl.self().spawn(
      caf::actor_from_state<class side_channel>, ctrl.shared_diagnostics(),
      ctrl.metrics_receiver(), ctrl.operator_index())};
    auto pipe = pipe_.inner;
    pipe.prepend(
      std::make_unique<internal_fork_source_operator>(side_channel.get()));
    const auto pipeline_executor = scope_linked{ctrl.self().spawn(
      tenzir::pipeline_executor, std::move(pipe), ctrl.definition(),
      side_channel.get(), side_channel.get(), ctrl.node(), ctrl.has_terminal(),
      ctrl.is_hidden(), std::string{ctrl.pipeline_id()})};
    ctrl.self().monitor(pipeline_executor.get(), [&](caf::error err) {
      if (err.valid() and err != caf::exit_reason::user_shutdown) {
        diagnostic::error(std::move(err))
          .primary(pipe_, "pipeline failed")
          .emit(ctrl.diagnostics());
        return;
      }
      ctrl.set_waiting(false);
    });
    ctrl.self()
      .mail(atom::start_v)
      .request(pipeline_executor.get(), caf::infinite)
      .then(
        [&]() {
          ctrl.set_waiting(false);
        },
        [&](caf::error err) {
          diagnostic::error(std::move(err))
            .primary(pipe_, "failed to start")
            .emit(ctrl.diagnostics());
        });
    ctrl.set_waiting(true);
    co_yield {};
    for (auto events : input) {
      if (events.rows() == 0) {
        co_yield {};
        continue;
      }
      ctrl.self()
        .mail(atom::push_v, events)
        .request(side_channel.get(), caf::infinite)
        .then(
          [&]() {
            ctrl.set_waiting(false);
          },
          [&](caf::error err) {
            diagnostic::error(std::move(err))
              .primary(pipe_, "failed to forward events")
              .emit(ctrl.diagnostics());
          });
      ctrl.set_waiting(true);
      co_yield std::move(events);
    }
    // Signal the end of the input by sending an empty batch.
    ctrl.self()
      .mail(atom::push_v, table_slice{})
      .request(side_channel.get(), caf::infinite)
      .then(
        [&]() {
          ctrl.set_waiting(false);
        },
        [&](caf::error err) {
          diagnostic::error(std::move(err))
            .primary(pipe_, "failed to forward signal end of input")
            .emit(ctrl.diagnostics());
        });
    ctrl.set_waiting(true);
    co_yield {};
    // Wait until the nested pipeline has finished or errored.
    ctrl.set_waiting(true);
    co_yield {};
  }

  auto name() const -> std::string override {
    return "tql2.fork";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  friend auto inspect(auto& f, fork_operator& x) -> bool {
    return f.object(x).fields(f.field("pipe", x.pipe_));
  }

private:
  located<pipeline> pipe_;
};

struct ForkIrArgs {
  location keyword;
  location pipe_location;
  ir::pipeline pipe;

  friend auto inspect(auto& f, ForkIrArgs& x) -> bool {
    return f.object(x).fields(f.field("keyword", x.keyword),
                              f.field("pipe_location", x.pipe_location),
                              f.field("pipe", x.pipe));
  }
};

class ForkIr final : public ir::Operator {
public:
  ForkIr() = default;

  explicit ForkIr(ForkIrArgs args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "fork_ir";
  }

  auto copy() const -> Box<ir::Operator> override {
    return ForkIr{args_};
  }

  auto move() && -> Box<ir::Operator> override {
    return ForkIr{std::move(args_)};
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    return args_.pipe.substitute(ctx, instantiate);
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<element_type_tag> override {
    if (input.is_not<table_slice>()) {
      diagnostic::error("`fork` expects events as input")
        .primary(args_.keyword)
        .emit(dh);
      return failure::promise();
    }
    TRY(auto branch_ty, args_.pipe.infer_type(input, dh));
    if (branch_ty.is_not<void>()) {
      diagnostic::error("`fork` subpipeline must end in a sink")
        .primary(args_.pipe_location)
        .emit(dh);
      return failure::promise();
    }
    return tag_v<table_slice>;
  }

  auto spawn(element_type_tag) const -> AnyOperator override {
    panic("cannot spawn fork; it must be lowered into the plan");
  }

  auto plan(ir::PlanBuilder& builder, ir::PlanPorts input,
            diagnostic_handler& dh) && -> failure_or<ir::PlanPorts> override {
    // Broadcast the input once: one lane continues the main pipeline unchanged,
    // the other drives the side-effect subpipeline.
    auto src = builder.into_single(input);
    auto ty = tag_v<table_slice>;
    auto passthrough = builder.add_identity(ty);
    auto side_effect = builder.add_identity(ty);
    TRY(auto tail, builder.lower_pipeline(
                     std::move(args_.pipe),
                     ir::PlanPorts{ir::PlanPort{side_effect, ty}}, dh));
    TENZIR_ASSERT(tail.empty());
    builder.add_broadcast(src, {passthrough, side_effect});
    return ir::PlanPorts{ir::PlanPort{passthrough, ty}};
  }

  friend auto inspect(auto& f, ForkIr& x) -> bool {
    return f.apply(x.args_);
  }

private:
  ForkIrArgs args_;
};

class fork_plugin final : public virtual operator_plugin2<fork_operator> {
public:
  auto name() const -> std::string override {
    return "tql2.fork";
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto pipe = located<pipeline>{};
    TRY(argument_parser2::operator_("fork")
          .positional("{ … }", pipe)
          .parse(inv, ctx));
    return std::make_unique<fork_operator>(std::move(pipe));
  }
};

class fork_ir_plugin final : public virtual operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return "fork";
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::CompileResult> override {
    auto args = ForkIrArgs{};
    args.keyword = inv.op.get_location();
    if (inv.args.size() != 1) {
      diagnostic::error("`fork` expects exactly one pipeline argument")
        .primary(args.keyword)
        .hint("use `fork { … }`")
        .emit(ctx);
      return failure::promise();
    }
    auto* pipe_expr = try_as<ast::pipeline_expr>(inv.args.front());
    if (not pipe_expr) {
      diagnostic::error("`fork` expects a pipeline argument `{{ … }}`")
        .primary(inv.args.front())
        .emit(ctx);
      return failure::promise();
    }
    args.pipe_location = pipe_expr->get_location();
    TRY(auto pipe_ir, std::move(pipe_expr->inner).compile(ctx));
    args.pipe = std::move(pipe_ir);
    return ForkIr{std::move(args)};
  }
};

using internal_fork_source_plugin
  = operator_inspection_plugin<internal_fork_source_operator>;
using fork_ir_inspection_plugin = inspection_plugin<ir::Operator, ForkIr>;

} // namespace

} // namespace tenzir::plugins::fork

TENZIR_REGISTER_PLUGIN(tenzir::plugins::fork::fork_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::fork::fork_ir_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::fork::fork_ir_inspection_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::fork::internal_fork_source_plugin)
