//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/exec/operator_base.hpp"
#include "tenzir/exec/pipeline.hpp"
#include "tenzir/plan/operator_spawn_args.hpp"

#include <tenzir/compile_ctx.hpp>
#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/detail/croncpp.hpp>
#include <tenzir/detail/string_literal.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/error.hpp>
#include <tenzir/finalize_ctx.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/parser_interface.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plan/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/report.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/type.h>
#include <caf/typed_event_based_actor.hpp>

#include <string_view>

namespace tenzir::plugins::every_cron {

namespace {

struct every_actor_traits {
  using signatures = exec::operator_actor::signatures::append_from<
    exec::shutdown_actor::signatures>;
};

using every_actor = caf::typed_actor<every_actor_traits>;

class every2 {
public:
  friend auto inspect(auto& f, every2& x) -> bool {
    return f.object(x).fields(f.field("interval", x.interval_),
                              f.field("ir", x.ir_));
  }

  auto on_push(table_slice slice) {
    pipeline_handle->push(std::move(slice));
  }

  auto spawn_subpipeline() {
    auto plan = instantiate_subpipeline();
    spawn_it_for_me(std::move(plan), [](table_slice slice) -> future<void> {
      co_await push(std::move(slice));
    });
  }

  auto instantiate_subpipeline() -> plan::pipeline {
    auto copy = ir_;
    auto result = copy.substitute(substitute_ctx{ctx_, nullptr}, true);
    // TODO
    TENZIR_ASSERT(result);
    auto plan = std::move(copy).finalize(finalize_ctx{ctx_}).unwrap();
    return plan;
  }

private:
  duration interval_;
  ir::pipeline ir_;
  void* pipeline_handle;
};

class every_exec : public exec::basic_operator<every_actor> {
public:
  [[maybe_unused]] static constexpr auto name = "every_exec";

  every_exec(every_actor::pointer self, duration interval, ir::pipeline ir,
             std::optional<plan::restore_t> restore, base_ctx ctx)
    : basic_operator{self}, interval_{interval}, ir_{std::move(ir)}, ctx_{ctx} {
    if (not restore) {
      return;
    }
    spawn_pipe(std::move(restore));
  }

  auto make_behavior() -> every_actor::behavior_type {
    return extend_behavior(std::tuple{
      [this](atom::shutdown) {
        on_shutdown();
      },
    });
  }

  void on_shutdown() {
    TENZIR_WARN("subpipeline is requesting shutdown");
    // TODO: Or can we just ignore it?
    // TODO: Don't we have to wait for checkpoints etc?
    // self_->send_exit(sub_, caf::exit_reason::user_shutdown);
    // Somehow store that we killed it so that we don't continue sending things
    // to it?
  }

  auto on_connect() -> caf::result<void> override {
    return {};
    // TENZIR_WARN("??");
    // auto rp = self_->make_response_promise<void>();
    // TENZIR_ASSERT(sub_);
    // self_
    //   ->mail(exec::connect_t{
    //     exec::upstream_actor{self_},
    //     exec::downstream_actor{self_},
    //     checkpoint_receiver(),
    //     // TODO: Shutdown actor is self?
    //     exec::shutdown_actor{self_},
    //   })
    //   .request(sub_, caf::infinite)
    //   .then(
    //     [rp] mutable {
    //       rp.deliver();
    //     },
    //     [this](caf::error& err) {
    //       TENZIR_ERROR("failed to connect to subpipeline: {}", err);
    //       self_->quit(err);
    //     });
    // return rp;
  }

  auto connect_pipe(std::function<void()> callback) {
    TENZIR_ASSERT(sub_);
    self_
      ->mail(exec::connect_t{
        exec::upstream_actor{self_},
        exec::downstream_actor{self_},
        checkpoint_receiver(),
        // TODO: Shutdown actor is self?
        exec::shutdown_actor{self_},
      })
      .request(sub_, caf::infinite)
      .then(std::move(callback), TENZIR_REPORT);
  }

  auto on_start() -> caf::result<void> override {
    // TODO: Wait for subpipeline to start before returning?
    if (sub_) {
      // We restored and can thus immediately start the subpipeline.
      self_->mail(atom::start_v)
        .request(sub_, caf::infinite)
        .then([] {}, TENZIR_REPORT);
      return {};
    }
    spawn_pipe(std::nullopt);
    connect_pipe([this] {
      self_->mail(atom::start_v)
        .request(sub_, caf::infinite)
        .then([] {}, TENZIR_REPORT);
    });
    return {};
  }

  void on_commit() override {
    self_->mail(atom::commit_v)
      .request(sub_, caf::infinite)
      .then([] {}, TENZIR_REPORT);
  }

  // downstream_actor_traits handlers
  void on_push(table_slice slice) override {
    if (self_->current_sender() == upstream()) {
      self_->mail(atom::push_v, exec::payload{std::move(slice)})
        .request(sub_, caf::infinite)
        .then([] {}, TENZIR_REPORT);
    } else {
      self_->mail(atom::push_v, exec::payload{std::move(slice)})
        .request(downstream(), caf::infinite)
        .then([] {}, TENZIR_REPORT);
    }
  }

  void on_push(chunk_ptr chunk) override {
    TENZIR_WARN("?");
    TENZIR_TODO();
  }

  /// We have the following dynamic state:
  /// - The plan of the currently executing pipeline
  /// - When this pipeline was started (questionable)
  auto serialize() -> chunk_ptr override {
    auto buffer = caf::byte_buffer{};
    auto ser = caf::binary_serializer{buffer};
    auto ok = ser.apply(plan_);
    TENZIR_ASSERT(ok);
    return chunk::make(std::move(buffer));
  }

  void on_persist(exec::checkpoint checkpoint) override {
    if (self_->current_sender() == sub_) {
      // We got our checkpoint back from the subpipeline, so we forward it now.
      persist(checkpoint);
      return;
    }
    // Otherwise we got the checkpoint from upstream.
    TENZIR_ASSERT(self_->current_sender() == upstream());
    auto serialized = serialize();
    self_->mail(checkpoint, serialize())
      .request(checkpoint_receiver(), caf::infinite)
      .then(
        [this, checkpoint] {
          self_->mail(atom::persist_v, checkpoint)
            .request(sub_, caf::infinite)
            .then([] {}, TENZIR_REPORT);
        },
        TENZIR_REPORT);
  }

  void on_done() override {
    if (self_->current_sender() == sub_) {
      // Inner pipeline is done.
      TENZIR_WARN("inner pipeline is done");
      // TODO: What now?
      return;
    }
    TENZIR_WARN("upstream is done");
    TENZIR_ASSERT(self_->current_sender() == upstream());
    // Wait for the inner pipeline to terminate.
    TENZIR_TODO();
  }

  // upstream_actor_traits handlers
  void on_pull(uint64_t items) override {
    if (self_->current_sender() == sub_) {
      pull(items);
      return;
    }
    TENZIR_ASSERT(self_->current_sender() == downstream());
    self_->mail(atom::pull_v, items)
      .request(sub_, caf::infinite)
      .then([] {}, TENZIR_REPORT);
  }

  void on_stop() override {
    if (self_->current_sender() == sub_) {
      // TODO: Anything else?
      return;
    }
    TENZIR_WARN("?");
    TENZIR_ASSERT(self_->current_sender() == downstream());
    TENZIR_TODO();
  }

private:
  void spawn_pipe(std::optional<plan::restore_t> restore) {
    TENZIR_ASSERT(not sub_);
    auto plan = plan::pipeline{};
    if (restore) {
      auto bytes = as_bytes(restore->chunk);
      auto f = caf::binary_deserializer{
        caf::const_byte_span{bytes.data(), bytes.size()}};
      auto plan = plan::pipeline{};
      auto ok = f.apply(plan);
      TENZIR_ASSERT(ok);
      sub_ = exec::make_subpipeline(std::move(plan), restore->checkpoint_reader,
                                    ctx_);
    } else {
      auto copy = ir_;
      auto result = copy.substitute(substitute_ctx{ctx_, nullptr}, true);
      // TODO
      TENZIR_ASSERT(result);
      auto plan = std::move(copy).finalize(finalize_ctx{ctx_}).unwrap();
      sub_ = exec::make_subpipeline(
        std::move(plan),
        restore ? std::optional{restore->checkpoint_reader} : std::nullopt,
        ctx_);
    }
  }

  // Plan
  duration interval_;
  ir::pipeline ir_;

  // Indirect
  base_ctx ctx_;

  // Serialized
  plan::pipeline plan_;
  exec::subpipeline_actor sub_;
};

class every_plan final : public plan::operator_base {
public:
  every_plan() = default;

  every_plan(duration interval, ir::pipeline pipe)
    : interval_{interval}, pipe_{std::move(pipe)} {
  }

  auto name() const -> std::string override {
    return "every_plan";
  }

  auto spawn(plan::operator_spawn_args args) const
    -> exec::operator_actor override {
    return args.sys.spawn(caf::actor_from_state<every_exec>, interval_, pipe_,
                          std::move(args.restore), args.ctx);
  }

  friend auto inspect(auto& f, every_plan& x) -> bool {
    return f.object(x).fields(f.field("interval", x.interval_),
                              f.field("pipe", x.pipe_));
  }

private:
  // TODO: This needs to be part of the actor.
  auto start_new(base_ctx ctx) const -> failure_or<plan::pipeline> {
    auto copy = pipe_;
    TRY(copy.substitute(substitute_ctx{ctx, nullptr}, true));
    // TODO: Where is the type check?
    return std::move(copy).finalize(finalize_ctx{ctx});
  }

  duration interval_{};
  ir::pipeline pipe_;
};

using every_exec_plugin = inspection_plugin<plan::operator_base, every_plan>;

class every_ir final : public ir::operator_base {
public:
  every_ir() = default;

  every_ir(ast::expression interval, ir::pipeline pipe)
    : interval_{std::move(interval)}, pipe_{std::move(pipe)} {
  }

  auto name() const -> std::string override {
    return "every_ir";
  }

  auto finalize(finalize_ctx ctx) && -> failure_or<plan::pipeline> override {
    (void)ctx;
    // TODO: Test the instantiation of the subpipeline? But in general,
    // instantiation is done later by the actor.
    // TRY(auto pipe, tenzir::instantiate(std::move(pipe_), ctx));
    // We know that this succeeds because instantiation must happen before.
    auto interval = as<duration>(interval_);
    return std::make_unique<every_plan>(interval, std::move(pipe_));
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

using every_ir_plugin = inspection_plugin<ir::operator_base, every_ir>;

class every_compiler_plugin final : public operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.every";
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::operator_ptr> override {
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
    return std::make_unique<every_ir>(std::move(inv.args[0]),
                                      std::move(pipe_ir));
  }
};

} // namespace

} // namespace tenzir::plugins::every_cron

TENZIR_REGISTER_PLUGIN(tenzir::plugins::every_cron::every_exec_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::every_cron::every_ir_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::every_cron::every_compiler_plugin)
