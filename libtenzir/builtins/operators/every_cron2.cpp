//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/assert.hpp"

#include <tenzir/argument_parser2.hpp>
#include <tenzir/detail/croncpp.hpp>
#include <tenzir/detail/flat_map.hpp>
#include <tenzir/detail/string_literal.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/operator_control_plane.hpp>
#include <tenzir/pipeline_executor.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <caf/actor_from_state.hpp>
#include <caf/disposable.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/expected.hpp>
#include <caf/send.hpp>
#include <caf/typed_actor.hpp>

#include <deque>
#include <memory>
#include <utility>
#include <variant>

namespace tenzir::plugins::every_cron2 {

namespace {

using timepoint = decltype(time::clock::now());

struct transceiver_actor_traits final {
  using signatures = caf::type_list<
    /// Push events from parent into self.
    auto(atom::push, table_slice)->caf::result<void>,
    /// Push events from subpipeline into self.
    auto(atom::internal, atom::push, table_slice)->caf::result<void>,
    /// Get events from self to subpipeline.
    auto(atom::internal, atom::pull)->caf::result<std::vector<table_slice>>,
    /// Get events from self to parent.
    auto(atom::pull)->caf::result<std::vector<table_slice>>,
    /// Get events from self to parent immediately.
    auto(atom::pull, atom::flush)->caf::result<std::vector<table_slice>>>
    /// Support the diagnostic receiver interface.
    ::append_from<receiver_actor<diagnostic>::signatures>
    /// Support the metrics receiver interface for the branch pipelines.
    ::append_from<metrics_receiver_actor::signatures>;
};

using transceiver_actor = caf::typed_actor<transceiver_actor_traits>;

struct transceiver_state {
  transceiver_state(transceiver_actor::pointer self,
                    shared_diagnostic_handler dh,
                    metrics_receiver_actor metrics, uint64_t operator_index,
                    exec_node_actor spawner)
    : self_{self},
      dh_{std::move(dh)},
      metrics_receiver_{std::move(metrics)},
      operator_index_{operator_index} {
    self_->monitor(std::move(spawner), [&](caf::error e) {
      TENZIR_DEBUG("[transceiver_actor] spawner shut down, exiting");
      self_->quit(std::move(e));
    });
  }

  auto make_behavior() -> transceiver_actor::behavior_type {
    return {
      [&](atom::push, table_slice output) -> caf::result<void> {
        TENZIR_ASSERT(not push_rp_.pending());
        if (internal_pull_rp_.pending()) {
          internal_pull_rp_.deliver(std::vector{std::move(output)});
          return {};
        }
        input_.push_back(std::move(output));
        if (input_.size() >= 40) {
          push_rp_ = self_->make_response_promise<void>();
          return push_rp_;
        }
        return {};
      },
      [&](atom::internal, atom::push, table_slice output) -> caf::result<void> {
        if (pull_rp_.pending()) {
          pull_rp_.deliver(std::vector{std::move(output)});
          return {};
        }
        output_.push_back(std::move(output));
        if (output_.size() >= 40) {
          internal_push_rp_ = self_->make_response_promise<void>();
          return internal_push_rp_;
        }
        return {};
      },
      [&](atom::internal, atom::pull) -> caf::result<std::vector<table_slice>> {
        TENZIR_ASSERT(not internal_pull_rp_.pending());
        if (push_rp_.pending()) {
          push_rp_.deliver();
        }
        if (input_.empty()) {
          internal_pull_rp_
            = self_->make_response_promise<std::vector<table_slice>>();
          return internal_pull_rp_;
        }
        return std::exchange(input_, {});
      },
      [&](atom::pull) -> caf::result<std::vector<table_slice>> {
        TENZIR_ASSERT(not pull_rp_.pending());
        if (internal_push_rp_.pending()) {
          internal_push_rp_.deliver();
        }
        if (output_.empty()) {
          pull_rp_ = self_->make_response_promise<std::vector<table_slice>>();
          return pull_rp_;
        }
        return std::exchange(output_, {});
      },
      [&](atom::pull, atom::flush) -> caf::result<std::vector<table_slice>> {
        return std::exchange(output_, {});
      },
      [this](diagnostic diag) {
        dh_.emit(std::move(diag));
      },
      [this](uint64_t nested_operator_index, uuid nested_metrics_id,
             type schema) {
        auto& id
          = registered_metrics_[nested_operator_index][nested_metrics_id];
        id = uuid::random();
        return self_->mail(operator_index_, id, std::move(schema))
          .delegate(metrics_receiver_);
      },
      [this](uint64_t nested_operator_index, uuid nested_metrics_id,
             record metrics) {
        const auto& id
          = registered_metrics_[nested_operator_index][nested_metrics_id];
        return self_->mail(operator_index_, id, std::move(metrics))
          .delegate(metrics_receiver_);
      },
      [](const operator_metric&) {},
      [this](const caf::exit_msg& msg) {
        TENZIR_DEBUG("[transceiver_actor] received exit: {}", msg.reason);
        if (msg.reason) {
          self_->quit(msg.reason);
        }
      },
    };
  }

private:
  transceiver_actor::pointer self_;
  shared_diagnostic_handler dh_;
  metrics_receiver_actor metrics_receiver_;
  uint64_t operator_index_ = 0;
  detail::flat_map<uint64_t, detail::flat_map<uuid, uuid>> registered_metrics_;
  std::vector<table_slice> output_;
  std::vector<table_slice> input_;
  caf::typed_response_promise<void> push_rp_;
  caf::typed_response_promise<void> internal_push_rp_;
  caf::typed_response_promise<std::vector<table_slice>> pull_rp_;
  caf::typed_response_promise<std::vector<table_slice>> internal_pull_rp_;
};

struct internal_source final : public crtp_operator<internal_source> {
  internal_source() = default;

  internal_source(transceiver_actor actor) : actor_{std::move(actor)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto slices = std::vector<table_slice>{};
    while (true) {
      TENZIR_DEBUG("[internal-transceiver-source] requesting slice");
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::internal_v, atom::pull_v)
        .request(actor_, caf::infinite)
        .then(
          [&](std::vector<table_slice> input) {
            TENZIR_DEBUG("[internal-transceiver-source] received {} slices",
                         input.size());
            ctrl.set_waiting(false);
            slices = std::move(input);
          },
          [&](const caf::error& e) {
            diagnostic::error(e).emit(ctrl.diagnostics());
          });
      co_yield {};
      for (auto&& slice : slices) {
        if (slice.rows() == 0) {
          TENZIR_DEBUG("[internal-transceiver-source] exiting");
          co_return;
        }
        co_yield std::move(slice);
      }
      slices.clear();
    }
  }

  auto name() const -> std::string override {
    return "internal-transceiver-source";
  }

  auto optimize(const expression&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, internal_source& x) {
    return f.apply(x.actor_);
  }

private:
  transceiver_actor actor_;
};

struct internal_sink final : public crtp_operator<internal_sink> {
  internal_sink() = default;

  internal_sink(transceiver_actor actor, tenzir::location op)
    : hdl_{std::move(actor)}, op_{op} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    ctrl.self().link_to(hdl_);
    for (auto slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      TENZIR_DEBUG("[internal-transceiver-sink] pushing slice");
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::internal_v, atom::push_v, std::move(slice))
        .request(hdl_, caf::infinite)
        .then(
          [&] {
            TENZIR_DEBUG("[internal-transceiver-sink] pushed slice");
            ctrl.set_waiting(false);
          },
          [&](const caf::error& e) {
            diagnostic::error("failed to push events: {}", e)
              .primary(op_)
              .emit(ctrl.diagnostics());
          });
      co_yield {};
    }
    TENZIR_DEBUG("[internal-scheduler-sink] pushing final slice");
    ctrl.set_waiting(true);
    ctrl.self()
      .mail(atom::internal_v, atom::push_v, table_slice{})
      .request(hdl_, caf::infinite)
      .then(
        [&] {
          TENZIR_DEBUG("[internal-scheduler-sink] pushed final slice");
          ctrl.set_waiting(false);
        },
        [&](const caf::error& e) {
          diagnostic::error("failed to push events: {}", e)
            .primary(op_)
            .emit(ctrl.diagnostics());
        });
    co_yield {};
  }

  auto name() const -> std::string override {
    return "internal-transceiver-sink";
  }

  auto optimize(const expression&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, internal_sink& x) {
    return f.object(x).fields(f.field("hdl_", x.hdl_), f.field("op_", x.op_));
  }

private:
  transceiver_actor hdl_;
  tenzir::location op_;
};

struct every_cron_args {
  tenzir::location op;
  located<duration> every;
  located<std::string> cron;
  located<pipeline> pipe;
  bool is_every{};

  auto validate(session ctx) const -> failure_or<void> {
    if (is_every) {
      if (every.inner <= duration::zero()) {
        diagnostic::error("interval must be a positive duration")
          .primary(every)
          .emit(ctx);
        return failure::promise();
      }
      return {};
    }
    try {
      detail::cron::make_cron(cron.inner);
    } catch (const detail::cron::bad_cronexpr& ex) {
      if (std::string_view{ex.what()}.find("stoul") != std::string_view::npos) {
        diagnostic::error(
          "bad cron expression: invalid value for at least one field")
          .primary(cron)
          .emit(ctx);
        return failure::promise();
      }
      diagnostic::error("bad cron expression: {}", ex.what())
        .primary(cron)
        .emit(ctx);
      return failure::promise();
    }
    return {};
  }

  friend auto inspect(auto& f, every_cron_args& x) -> bool {
    return f.object(x).fields(f.field("op", x.op), f.field("every", x.every),
                              f.field("cron", x.cron), f.field("pipe", x.pipe),
                              f.field("is_every", x.is_every));
  }
};

template <detail::string_literal Name>
struct every_cron_operator final : public operator_base {
  every_cron_operator() = default;

  explicit every_cron_operator(every_cron_args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_input input, operator_control_plane& ctrl) const
    -> caf::expected<operator_output> override {
    using Out = caf::expected<operator_output>;
    TRY(auto out, infer_type_impl(to_operator_type(input)));
    TENZIR_ASSERT(out.template is_not<chunk_ptr>());
    return match(
      std::move(input),
      [&](std::monostate) -> Out {
        if (out.template is<void>()) {
          return run(ctrl);
        }
        return run(ctrl, tag_v<table_slice>);
      },
      [&](generator<table_slice> in) -> Out {
        if (out.template is<void>()) {
          return run(std::move(in), ctrl);
        }
        return run(std::move(in), ctrl, tag_v<table_slice>);
      },
      [](generator<chunk_ptr>) -> Out {
        TENZIR_UNREACHABLE();
      });
  }

  auto run(operator_control_plane& ctrl) const -> generator<std::monostate> {
    const auto cron = make_cronexpr();
    const auto handle = spawn_transceiver(ctrl);
    auto start = timepoint{};
    auto finish = timepoint{};
    spawn_pipeline(ctrl, handle, start, finish, cron);
    ctrl.set_waiting(true);
    co_yield {};
    TENZIR_UNREACHABLE();
  }

  auto run(operator_control_plane& ctrl, tag<table_slice>) const
    -> generator<table_slice> {
    const auto cron = make_cronexpr();
    const auto handle = spawn_transceiver(ctrl);
    auto start = timepoint{};
    auto finish = timepoint{};
    auto slices = std::vector<table_slice>{};
    spawn_pipeline(ctrl, handle, start, finish, cron);
    while (true) {
      TENZIR_DEBUG("[every_cron source] requesting slice");
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::pull_v)
        .request(handle, caf::infinite)
        .then(
          [&](std::vector<table_slice> x) {
            TENZIR_DEBUG("[every_cron source] received {} slices", x.size());
            ctrl.set_waiting(false);
            slices = std::move(x);
          },
          [&](const caf::error& e) {
            diagnostic::error(e).primary(args_.op).emit(ctrl.diagnostics());
          });
      co_yield {};
      for (const auto& slice : slices) {
        co_yield slice;
      }
      slices.clear();
    }
  }

  auto run(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    const auto cron = make_cronexpr();
    const auto handle = spawn_transceiver(ctrl);
    auto count = uint64_t{};
    auto start = timepoint{};
    auto finish = timepoint{};
    spawn_pipeline(ctrl, handle, start, finish, cron, count);
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      TENZIR_DEBUG("[every_cron sink] pushing slice");
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::push_v, std::move(slice))
        .request(handle, caf::infinite)
        .then(
          [&] {
            TENZIR_DEBUG("[every_cron sink] pushed slice");
            ctrl.set_waiting(false);
          },
          [&](const caf::error& e) {
            diagnostic::error(e).primary(args_.op).emit(ctrl.diagnostics());
          });
      co_yield {};
    }
    TENZIR_DEBUG("[every_cron sink] pushing final slice");
    ctrl.set_waiting(true);
    ctrl.self()
      .mail(atom::push_v, table_slice{})
      .request(handle, caf::infinite)
      .then(
        [&] {
          TENZIR_DEBUG("[every_cron sink] pushed final slice");
          ctrl.set_waiting(false);
        },
        [&](const caf::error& e) {
          diagnostic::error(e).primary(args_.op).emit(ctrl.diagnostics());
        });
    co_yield {};
  }

  auto run(generator<table_slice> input, operator_control_plane& ctrl,
           tag<table_slice>) const -> generator<table_slice> {
    const auto cron = make_cronexpr();
    const auto handle = spawn_transceiver(ctrl);
    auto count = uint64_t{};
    auto start = timepoint{};
    auto finish = timepoint{};
    auto slices = std::vector<table_slice>{};
    spawn_pipeline(ctrl, handle, start, finish, cron, count);
    for (auto&& slice : input) {
      if (slice.rows() != 0) {
        TENZIR_DEBUG("[every_cron] pushing slice");
        ctrl.set_waiting(true);
        ctrl.self()
          .mail(atom::push_v, std::move(slice))
          .request(handle, caf::infinite)
          .then(
            [&] {
              TENZIR_DEBUG("[every_cron] pushed slice");
              ctrl.set_waiting(false);
            },
            [&](const caf::error& e) {
              diagnostic::error(e).primary(args_.op).emit(ctrl.diagnostics());
            });
        co_yield {};
      }
      TENZIR_DEBUG("[every_cron] requesting slice");
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::pull_v, atom::flush_v)
        .request(handle, caf::infinite)
        .then(
          [&](std::vector<table_slice> x) {
            TENZIR_DEBUG("[every_cron] received {} slices", x.size());
            ctrl.set_waiting(false);
            slices = std::move(x);
          },
          [&](const caf::error& e) {
            diagnostic::error(e).primary(args_.op).emit(ctrl.diagnostics());
          });
      co_yield {};

      for (const auto& slice : slices) {
        co_yield slice;
      }
      slices.clear();
    }
    TENZIR_DEBUG("[every_cron] pushing final slice");
    ctrl.self()
      .mail(atom::push_v, table_slice{})
      .request(handle, caf::infinite)
      .then(
        [&] {
          TENZIR_DEBUG("[every_cron] pushed final slice");
        },
        [&](const caf::error& e) {
          diagnostic::error(e).primary(args_.op).emit(ctrl.diagnostics());
        });
    while (true) {
      TENZIR_DEBUG("[every_cron] requesting slice");
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::pull_v)
        .request(handle, caf::infinite)
        .then(
          [&](std::vector<table_slice> x) {
            TENZIR_DEBUG("[every_cron] received {} slices", x.size());
            ctrl.set_waiting(false);
            slices = std::move(x);
          },
          [&](const caf::error& e) {
            diagnostic::error(e).primary(args_.op).emit(ctrl.diagnostics());
          });
      co_yield {};
      for (const auto& slice : slices) {
        co_yield slice;
      }
      if (not slices.empty() and slices.back().rows() == 0) {
        co_return;
      }
      slices.clear();
    }
  }

  auto make_cronexpr() const -> std::optional<detail::cron::cronexpr> {
    if (args_.is_every) {
      return std::nullopt;
    }
    return detail::cron::make_cron(args_.cron.inner);
  }

  template <concepts::one_of<void, table_slice> In>
  auto make_pipeline(const transceiver_actor& hdl) const -> pipeline {
    auto pipe = args_.pipe.inner;
    auto out = pipe.infer_type<In>();
    TENZIR_ASSERT(out);
    if (out->template is<table_slice>()) {
      pipe.append(std::make_unique<internal_sink>(hdl, args_.op));
    }
    if constexpr (std::same_as<table_slice, In>) {
      pipe.prepend(std::make_unique<internal_source>(hdl));
    }
    return pipe;
  }

  auto spawn_transceiver(operator_control_plane& ctrl) const
    -> transceiver_actor {
    auto hdl
      = ctrl.self().spawn(caf::actor_from_state<transceiver_state>,
                          ctrl.shared_diagnostics(), ctrl.metrics_receiver(),
                          ctrl.operator_index(), &ctrl.self());
    ctrl.self().monitor(hdl, [&](caf::error e) {
      diagnostic::error(std::move(e))
        .compose(add_diagnostic_location())
        .emit(ctrl.diagnostics());
    });
    return hdl;
  }

  auto
  spawn_pipeline(operator_control_plane& ctrl, const transceiver_actor& hdl,
                 timepoint& start, timepoint& finish,
                 const std::optional<detail::cron::cronexpr>& cron) const
    -> void {
    const auto now = time::clock::now();
    start = now > finish ? now : finish;
    finish = next_ts(cron, start);
    ctrl.self().delay_for_fn(start - now, [&] {
      auto pipe = make_pipeline<void>(hdl);
      const auto exec
        = ctrl.self().spawn(pipeline_executor, std::move(pipe),
                            std::string{ctrl.definition()}, hdl, hdl,
                            ctrl.node(), ctrl.has_terminal(), ctrl.is_hidden());
      ctrl.self().monitor(exec, [&, exec](const caf::error&) {
        spawn_pipeline(ctrl, hdl, start, finish, cron);
      });
      TENZIR_DEBUG("[every_cron] requesting subpipeline start");
      ctrl.self()
        .mail(atom::start_v)
        .request(exec, caf::infinite)
        .then(
          [] {
            TENZIR_DEBUG("[every_cron] subpipeline started");
          },
          [&ctrl, this](const caf::error& e) {
            diagnostic::error(e)
              .compose(add_diagnostic_location())
              .emit(ctrl.diagnostics());
          });
    });
  }

  auto
  spawn_pipeline(operator_control_plane& ctrl, const transceiver_actor& hdl,
                 timepoint& start, timepoint& finish,
                 const std::optional<detail::cron::cronexpr>& cron,
                 uint64_t& count) const -> void {
    const auto now = time::clock::now();
    start = now > finish ? now : finish;
    finish = next_ts(cron, start);
    ctrl.self().delay_for_fn(start - now, [&] {
      auto pipe = make_pipeline<table_slice>(hdl);
      const auto exec
        = ctrl.self().spawn(pipeline_executor, std::move(pipe),
                            std::string{ctrl.definition()}, hdl, hdl,
                            ctrl.node(), ctrl.has_terminal(), ctrl.is_hidden());
      ctrl.self().monitor(exec, [&, exec](const caf::error&) {
        ++count;
        spawn_pipeline(ctrl, hdl, start, finish, cron, count);
      });
      TENZIR_DEBUG("[every_cron] requesting subpipeline start");
      ctrl.self()
        .mail(atom::start_v)
        .request(exec, caf::infinite)
        .then(
          [&] {
            TENZIR_DEBUG("[every_cron] subpipeline started");
            ctrl.self().delay_for_fn(finish - start, [&, curr = count] {
              if (count != curr) {
                return;
              }
              TENZIR_DEBUG("[every_cron] closing input source");
              ctrl.self()
                .mail(atom::push_v, table_slice{})
                .request(hdl, caf::infinite)
                .then(
                  [] {
                    TENZIR_DEBUG("[every_cron] closed input source");
                  },
                  [&](const caf::error& e) {
                    diagnostic::error(e).primary(args_.op).emit(
                      ctrl.diagnostics());
                  });
            });
          },
          [&ctrl, this](const caf::error& e) {
            diagnostic::error(e)
              .compose(add_diagnostic_location())
              .emit(ctrl.diagnostics());
          });
    });
  }

  auto next_ts(const std::optional<detail::cron::cronexpr>& cron,
               timepoint last) const -> timepoint {
    if (not cron) {
      return time_point_cast<timepoint::duration>(last + args_.every.inner);
    }
    const auto tp = time_point_cast<std::chrono::system_clock::duration>(last);
    return detail::cron::cron_next(*cron, tp);
  }

  auto infer_type_impl(operator_type input) const
    -> caf::expected<operator_type> override {
    if (input.is<chunk_ptr>()) {
      return diagnostic::error("`{}` does not accept bytes as input",
                               args_.is_every ? "every" : "cron")
        .primary(args_.op)
        .to_error();
    }
    return args_.pipe.inner.infer_type_impl(input)
      .transform_or([&](caf::error e) {
        return diagnostic::error(std::move(e))
          .compose(add_diagnostic_location())
          .to_error();
      })
      .and_then([&](operator_type out) -> caf::expected<operator_type> {
        if (out.is<chunk_ptr>()) {
          return diagnostic::error("subpipeline must not return bytes")
            .primary(args_.pipe)
            .to_error();
        }
        return out;
      });
  }

  auto add_diagnostic_location() const {
    return
      [loc = args_.pipe.source](diagnostic_builder x) -> diagnostic_builder {
        if (x.inner().annotations.empty()) {
          return std::move(x).primary(loc);
        }
        return x;
      };
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    auto args = args_;
    auto result = args.pipe.inner.optimize(filter, order);
    args.pipe.inner = static_cast<pipeline&&>(*result.replacement);
    result.replacement = std::make_unique<every_cron_operator>(std::move(args));
    return result;
  }

  auto name() const -> std::string override {
    return Name;
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto copy() const -> operator_ptr override {
    return std::make_unique<every_cron_operator>(*this);
  }

  friend auto inspect(auto& f, every_cron_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  every_cron_args args_;
};

using every_operator = every_cron_operator<"every">;

struct every_plugin final : public operator_plugin2<every_operator> {
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = every_cron_args{};
    args.op = inv.self.get_location();
    args.is_every = true;
    TRY(argument_parser2::operator_(name())
          .positional("interval", args.every)
          .positional("{ … }", args.pipe)
          .parse(inv, ctx));
    TRY(args.validate(ctx));
    return std::make_unique<every_operator>(std::move(args));
  }
};

using cron_operator = every_cron_operator<"cron">;

struct cron_plugin final : public operator_plugin2<cron_operator> {
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = every_cron_args{};
    args.op = inv.self.get_location();
    TRY(argument_parser2::operator_(name())
          .positional("schedule", args.cron)
          .positional("{ … }", args.pipe)
          .parse(inv, ctx));
    TRY(args.validate(ctx));
    return std::make_unique<cron_operator>(std::move(args));
  }
};

using internal_source_plugin = operator_inspection_plugin<internal_source>;
using internal_sink_plugin = operator_inspection_plugin<internal_sink>;

} // namespace

} // namespace tenzir::plugins::every_cron2

TENZIR_REGISTER_PLUGIN(tenzir::plugins::every_cron2::every_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::every_cron2::cron_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::every_cron2::internal_source_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::every_cron2::internal_sink_plugin)
