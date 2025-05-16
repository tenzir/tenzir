//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/actors.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/croncpp.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/operator_control_plane.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/pipeline_executor.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/shared_diagnostic_handler.hpp"
#include "tenzir/shutdown.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/exec.hpp"

#include <tenzir/tql2/plugin.hpp>

#include <caf/actor_from_state.hpp>
#include <caf/exit_reason.hpp>

#include <chrono>
#include <queue>

namespace tenzir::plugins::every_cron {

TENZIR_ENUM(mode, every, cron);

namespace {

struct scheduler_traits {
  using signatures = caf::type_list<
    // Accepts events from the subpipeline.
    auto(atom::push, table_slice events)->caf::result<void>,
    // Forwards events into the parent pipeline.
    auto(atom::pull)->caf::result<table_slice>>
    // Accept diagnostics.
    ::append_from<receiver_actor<diagnostic>::signatures>
    // Accept metrics.
    ::append_from<metrics_receiver_actor::signatures>;
};

using scheduler_actor = caf::typed_actor<scheduler_traits>;

class internal_scheduler_sink_operator final
  : public crtp_operator<internal_scheduler_sink_operator> {
public:
  internal_scheduler_sink_operator() = default;

  explicit internal_scheduler_sink_operator(scheduler_actor every_scheduler)
    : scheduler_{std::move(every_scheduler)} {
  }

  auto name() const -> std::string override {
    return "internal-scheduler-sink";
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    for (auto events : input) {
      if (events.rows() == 0) {
        co_yield {};
        continue;
      }
      ctrl.self()
        .mail(atom::push_v, std::move(events))
        .request(scheduler_, caf::infinite)
        .then(
          [&]() {
            ctrl.set_waiting(false);
          },
          [&](caf::error err) {
            diagnostic::error(std::move(err))
              .note("failed to forward events")
              .emit(ctrl.diagnostics());
          });
      ctrl.set_waiting(true);
      co_yield {};
    }
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, internal_scheduler_sink_operator& x) -> bool {
    return f.object(x).fields(f.field("scheduler", x.scheduler_));
  }

private:
  scheduler_actor scheduler_;
};

class scheduler {
public:
  [[maybe_unused]] static constexpr auto name = "scheduler";

  // Given a timestamp, returns the next timestamp and a hint whether to kick
  // off the next run immediately because the next timestamp had already passed.
  using scheduler_impl_type
    = std::function<auto(std::optional<time> last)->std::pair<time, bool>>;

  scheduler(scheduler_actor::pointer self, scheduler_impl_type scheduler_impl,
            located<uint64_t> parallel, located<pipeline> pipe,
            std::string definition, node_actor node, bool has_terminal,
            bool is_hidden, shared_diagnostic_handler dh,
            metrics_receiver_actor mh, uint64_t op_index)
    : self_{self},
      scheduler_impl_{std::move(scheduler_impl)},
      parallel_{parallel},
      pipe_{std::move(pipe)},
      definition_{std::move(definition)},
      node_{std::move(node)},
      has_terminal_{has_terminal},
      is_hidden_{is_hidden},
      dh_{std::move(dh)},
      mh_{std::move(mh)},
      op_index_{op_index} {
    if (not pipe_.inner.is_closed()) {
      pipe_.inner.append(std::make_unique<internal_scheduler_sink_operator>(
        static_cast<scheduler_actor>(self_)));
      TENZIR_ASSERT(pipe_.inner.is_closed());
    }
  }

  auto make_behavior() -> scheduler_actor::behavior_type {
    for (auto i = uint64_t{0}; i < parallel_.inner; ++i) {
      schedule_start();
    }
    return {
      [this](atom::push, table_slice events) -> caf::result<void> {
        TENZIR_ASSERT(push_rps_.size() < parallel_.inner);
        if (pull_rp_.pending()) {
          TENZIR_ASSERT(buffer_.empty());
          pull_rp_.deliver(std::move(events));
          return {};
        }
        buffer_.push(std::move(events));
        if (buffer_.size() < max_buffered) {
          return {};
        }
        return push_rps_.emplace(self_->make_response_promise<void>());
      },
      [this](atom::pull) -> caf::result<table_slice> {
        TENZIR_ASSERT(not pull_rp_.pending());
        if (buffer_.empty()) {
          TENZIR_ASSERT(push_rps_.empty());
          pull_rp_ = self_->make_response_promise<table_slice>();
          return pull_rp_;
        }
        if (not push_rps_.empty()) {
          TENZIR_ASSERT(push_rps_.front().pending());
          push_rps_.front().deliver();
          push_rps_.pop();
        }
        auto events = std::move(buffer_.front());
        buffer_.pop();
        return events;
      },
      [this](diagnostic diag) -> caf::result<void> {
        dh_.emit(std::move(diag));
        return {};
      },
      [this](uint64_t op_index, uuid metrics_id,
             type schema) -> caf::result<void> {
        TENZIR_UNUSED(op_index);
        return self_->mail(op_index_, metrics_id, std::move(schema))
          .delegate(mh_);
      },
      [this](uint64_t op_index, uuid metrics_id,
             record metrics) -> caf::result<void> {
        TENZIR_UNUSED(op_index);
        return self_->mail(op_index_, metrics_id, std::move(metrics))
          .delegate(mh_);
      },
      [](const operator_metric& metrics) -> caf::result<void> {
        TENZIR_UNUSED(metrics);
        return {};
      },
      [this](caf::exit_msg msg) {
        auto handles = std::vector<caf::actor>{};
        for (const auto& handle : running_) {
          handles.push_back(caf::actor_cast<caf::actor>(handle));
        }
        shutdown<policy::parallel>(self_, handles, std::move(msg.reason));
      },
    };
  }

private:
  auto schedule_start() -> void {
    const auto [next_start, immediate] = scheduler_impl_(last_start_);
    last_start_ = next_start;
    const auto start = [&] {
      TENZIR_ASSERT(running_.size() < parallel_.inner);
      auto handle = self_->spawn(pipeline_executor, pipe_.inner, definition_,
                                 receiver_actor<diagnostic>{self_},
                                 metrics_receiver_actor{self_}, node_,
                                 has_terminal_, is_hidden_);
      self_->monitor(handle, [this, id = handle->id()](caf::error err) {
        const auto found
          = std::ranges::find(running_, id, &pipeline_executor_actor::id);
        TENZIR_ASSERT(found != running_.end());
        running_.erase(found);
        schedule_start();
        if (err) {
          diagnostic::warning(std::move(err))
            .primary(pipe_, "failed at runtime")
            .emit(dh_);
        }
      });
      self_->mail(atom::start_v)
        .request(handle, caf::infinite)
        .then(
          [] {
            // Yay :)
          },
          [this](caf::error err) {
            if (err == ec::silent or err == ec::diagnostic
                or err == caf::exit_reason::user_shutdown) {
              // Nothing to do; the pipeline executor will shut down on its own.
              return;
            }
            // The error is unexpected so we shut down everything. This is
            // likely a system error, so there isn't much we can do about it.
            self_->quit(diagnostic::error(std::move(err))
                          .primary(pipe_, "failed to start")
                          .to_error());
          });
      running_.push_back(std::move(handle));
    };
    if (immediate) {
      start();
      return;
    }
    self_->run_scheduled_weak(next_start, start);
  }

  scheduler_actor::pointer self_;

  std::vector<pipeline_executor_actor> running_;
  std::optional<time> last_start_;
  scheduler_impl_type scheduler_impl_;

  located<uint64_t> parallel_;
  located<pipeline> pipe_;
  std::string definition_;
  node_actor node_;
  bool has_terminal_;
  bool is_hidden_;
  shared_diagnostic_handler dh_;
  metrics_receiver_actor mh_;
  uint64_t op_index_;

  static constexpr auto max_buffered = 10;
  std::queue<table_slice> buffer_;
  std::queue<caf::typed_response_promise<void>> push_rps_;
  caf::typed_response_promise<table_slice> pull_rp_;
};

TENZIR_ENUM(mode, every, cron);

template <mode Mode>
class every_cron_operator final
  : public crtp_operator<every_cron_operator<Mode>> {
public:
  using arg_type
    = std::conditional_t<Mode == mode::every, duration, std::string>;

  every_cron_operator() = default;

  explicit every_cron_operator(located<arg_type> scheduler_arg,
                               located<uint64_t> parallel,
                               located<pipeline> pipe)
    : scheduler_arg_{std::move(scheduler_arg)},
      parallel_{parallel},
      pipe_{std::move(pipe)} {
  }

  auto name() const -> std::string override {
    return fmt::to_string(Mode);
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    const auto make_scheduler_impl = [&] {
      if constexpr (Mode == mode::every) {
        return [interval = scheduler_arg_.inner](
                 std::optional<time> last) -> std::pair<time, bool> {
          const auto now = time::clock::now();
          if (not last) {
            return std::make_pair(now, true);
          }
          if (now - *last > interval) {
            return std::make_pair(now, true);
          }
          return std::make_pair(*last + interval, false);
        };
      } else {
        // The cronexpr was already validated in the operator's parser, so we
        // can safely assume that it is valid here and don't need to set up
        // exceptions again. We can't store the parsed cronexpr directly,
        // unfortunately, because the type is not easy to make inspectable.
        return [expr = detail::cron::make_cron(scheduler_arg_.inner)](
                 std::optional<time> last) -> std::pair<time, bool> {
          const auto now = last ? *last : time::clock::now();
          const auto tt = time::clock::to_time_t(
            std::chrono::time_point_cast<
              std::chrono::system_clock::time_point::duration>(now));
          const auto next
            = time::clock::from_time_t(detail::cron::cron_next(expr, tt));
          return std::make_pair(next, next <= now);
        };
      }
    };
    const auto handle = ctrl.self().spawn<caf::linked>(
      caf::actor_from_state<class scheduler>, make_scheduler_impl(), parallel_,
      pipe_, std::string{ctrl.definition()}, ctrl.node(), ctrl.has_terminal(),
      ctrl.is_hidden(), ctrl.shared_diagnostics(), ctrl.metrics_receiver(),
      ctrl.operator_index());
    auto output = table_slice{};
    while (true) {
      ctrl.self()
        .mail(atom::pull_v)
        .request(handle, caf::infinite)
        .then(
          [&](table_slice events) {
            output = std::move(events);
            ctrl.set_waiting(false);
          },
          [&](caf::error err) {
            diagnostic::error(std::move(err))
              .primary(pipe_, "failed to forward result")
              .emit(ctrl.diagnostics());
          });
      ctrl.set_waiting(true);
      co_yield {};
      co_yield std::move(output);
    }
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    auto result = pipe_.inner.optimize(filter, order);
    if (not result.replacement) {
      return result;
    }
    auto* pipe = dynamic_cast<pipeline*>(result.replacement.get());
    TENZIR_ASSERT(pipe);
    result.replacement = std::make_unique<every_cron_operator<Mode>>(
      scheduler_arg_, parallel_, located{std::move(*pipe), pipe_.source});
    return result;
  }

  auto location() const -> operator_location override {
    const auto requires_node = [](const auto& ops) {
      return std::ranges::find(ops, operator_location::remote,
                               &operator_base::location)
             != ops.end();
    };
    return requires_node(pipe_.inner.operators()) ? operator_location::remote
                                                  : operator_location::anywhere;
  }

  friend auto inspect(auto& f, every_cron_operator& x) -> bool {
    return f.object(x).fields(f.field("scheduler_arg", x.scheduler_arg_),
                              f.field("parallel", x.parallel_),
                              f.field("pipe", x.pipe_));
  }

private:
  located<arg_type> scheduler_arg_;
  located<uint64_t> parallel_;
  located<pipeline> pipe_;
};

class cron_plugin final
  : public virtual operator_plugin2<every_cron_operator<mode::cron>> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto cronexpr = located<std::string>{};
    auto parallel = located<uint64_t>{1, location::unknown};
    auto pipe = located<pipeline>{};
    TRY(argument_parser2::operator_(name())
          .positional("interval", cronexpr)
          .named_optional("parallel", parallel)
          .positional("{ … }", pipe)
          .parse(inv, ctx));
    try {
      (void)detail::cron::make_cron(cronexpr.inner);
    } catch (const detail::cron::bad_cronexpr& ex) {
      // The croncpp library re-throws the exception message from the
      // `std::stoul` call on failure. This happens for most cases of invalid
      // expressions, i.e. ones that do not contain unsigned integers or allowed
      // literals. libstdc++ and libc++ exception messages both contain the
      // string "stoul" in their what() strings. We can check for this and
      // provide a slightly better error message back to the user.
      if (std::string_view{ex.what()}.find("stoul") != std::string_view::npos) {
        diagnostic::error(
          "bad cron expression: invalid value for at least one field")
          .primary(cronexpr)
          .emit(ctx);
        return failure::promise();
      }
      diagnostic::error("bad cron expression: \"{}\"", ex.what())
        .primary(cronexpr)
        .emit(ctx);
      return failure::promise();
    }
    if (parallel.inner == 0) {
      diagnostic::error("parallel level must be greater than zero")
        .primary(parallel)
        .emit(ctx);
      return failure::promise();
    }
    auto output = pipe.inner.infer_type(tag_v<void>);
    if (not output) {
      diagnostic::error("pipeline must accept `void`").primary(pipe).emit(ctx);
      return failure::promise();
    }
    if (output->is<chunk_ptr>()) {
      diagnostic::error("pipeline must return `events` or `void`")
        .primary(pipe, "returns `{}`", operator_type_name(*output))
        .emit(ctx);
      return failure::promise();
    }
    auto result = std::make_unique<pipeline>();
    result->append(std::make_unique<every_cron_operator<mode::cron>>(
      cronexpr, parallel, std::move(pipe)));
    if (output->is<void>()) {
      // If the nested pipeline returns `void` then we "cheat" a tiny bit and
      // add a discard operator after the nested pipeline. This reduces the
      // typing logic inside the operator implementation.
      const auto* discard_op
        = plugins::find<operator_factory_plugin>("discard");
      TENZIR_ASSERT(discard_op);
      TRY(auto discard_pipe,
          discard_op->make({.self = inv.self, .args = {}}, ctx));
      result->append(std::move(discard_pipe));
    }
    return result;
  }
};

class every_plugin final
  : public virtual operator_plugin2<every_cron_operator<mode::every>> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto interval = located<duration>{};
    auto parallel = located<uint64_t>{1, location::unknown};
    // We take an expression for the pipeline rather than an already compiled
    // pipeline because we must be able to pass it to the `window` operator for
    // backwards compatibility.
    auto pipe_expr = std::optional<ast::expression>{};
    auto parser = argument_parser2::operator_(name());
    parser.positional("interval", interval);
    parser.named_optional("parallel", parallel);
    parser.positional("pipeline", pipe_expr, "{ … }");
    TRY(parser.parse(inv, ctx));
    if (not pipe_expr) {
      // The argument parser has a bug that makes it impossible to specify a
      // required positional pipeline argument after optional named arguments.
      // We work around this by making the pipeline an optional positional
      // argument, and then manually checking if it was provided.
      diagnostic::error("missing required `pipe` argument")
        .docs(parser.docs())
        .usage(parser.usage())
        .emit(ctx);
      return failure::promise();
    }
    auto* pipe_ast = try_as<ast::pipeline_expr>(*pipe_expr);
    if (not pipe_ast) {
      diagnostic::error("expected pipeline").primary(*pipe_expr).emit(ctx);
      return failure::promise();
    }
    TRY(auto pipe, compile(auto{pipe_ast->inner}, ctx));
    if (interval.inner <= duration::zero()) {
      diagnostic::error("expected a positive duration, got {}", interval.inner)
        .primary(interval)
        .emit(ctx);
      return failure::promise();
    }
    if (parallel.inner == 0) {
      diagnostic::error("parallel level must be greater than zero")
        .primary(parallel)
        .emit(ctx);
      return failure::promise();
    }
    // Historically, the `every` operator could be used as a source and as a
    // transformation. In the latter case, it effectively created tumbling
    // windows with a fixed time interval. This has turned out to be a problem
    // for operators like `shell`, which can be both a source and a
    // transformation, with the latter being preferred in type inference. For
    // a pipeline like `every … { shell … }`, the general assumption is that
    // `shell` acts as a source operator and not as a transformation. Because
    // of this, we first check whether the nested pipeline works as a source,
    // breaking with the usual type inference order of `events` > `bytes` >
    // `void`, and emit a warning and fall back to the newly added `window` if
    // the pipeline does not have a source.
    if (auto output = pipe.infer_type(tag_v<void>)) {
      if (output->is<chunk_ptr>()) {
        diagnostic::error("pipeline must return `events` or `void`")
          .primary(*pipe_expr, "returns `{}`", operator_type_name(*output))
          .emit(ctx);
        return failure::promise();
      }
      auto result = std::make_unique<pipeline>();
      result->append(std::make_unique<every_cron_operator<mode::every>>(
        interval, parallel,
        located{std::move(pipe), pipe_expr->get_location()}));
      if (output->is<void>()) {
        // If the nested pipeline returns `void` then we "cheat" a tiny bit
        // and add a discard operator after the nested pipeline. This reduces
        // the typing logic inside the operator implementation.
        const auto* discard_op
          = plugins::find<operator_factory_plugin>("discard");
        TENZIR_ASSERT(discard_op);
        TRY(auto discard_pipe,
            discard_op->make({.self = inv.self, .args = {}}, ctx));
        result->append(std::move(discard_pipe));
      }
      return result;
    }
    auto window_inv = operator_factory_plugin::invocation{};
    window_inv.self = inv.self;
    window_inv.args.emplace_back(ast::assignment{
      ast::field_path::from(located{"timeout", interval.source}),
      location::unknown,
      ast::constant{interval.inner, interval.source},
    });
    window_inv.args.emplace_back(ast::assignment{
      ast::field_path::from(located{"parallel", parallel.source}),
      location::unknown,
      ast::constant{parallel.inner, parallel.source},
    });
    window_inv.args.emplace_back(ast::assignment{
      ast::field_path::from(located{"_nonblocking", location::unknown}),
      location::unknown,
      ast::constant{true, location::unknown},
    });
    window_inv.args.push_back(std::move(*pipe_expr));
    const auto* window_op = plugins::find<operator_factory_plugin>("window");
    TENZIR_ASSERT(window_op);
    auto dh = transforming_diagnostic_handler{
      ctx.dh(),
      [&](diagnostic diag) -> diagnostic {
        for (auto& note : diag.notes) {
          switch (note.kind) {
            case tenzir::diagnostic_note_kind::note:
            case tenzir::diagnostic_note_kind::hint:
              break;
            case tenzir::diagnostic_note_kind::docs: {
              note.message = parser.docs();
              break;
            }
            case tenzir::diagnostic_note_kind::usage: {
              note.message = parser.usage();
              break;
            }
          }
        }
        return diag;
      },
    };
    auto sp = session_provider::make(dh);
    return window_op->make(std::move(window_inv), sp.as_session());
  }
};

using internal_scheduler_sink_plugin
  = operator_inspection_plugin<internal_scheduler_sink_operator>;

} // namespace

} // namespace tenzir::plugins::every_cron
TENZIR_REGISTER_PLUGIN(
  tenzir::plugins::every_cron::internal_scheduler_sink_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::every_cron::cron_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::every_cron::every_plugin)
