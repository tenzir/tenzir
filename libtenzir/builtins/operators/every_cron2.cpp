//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/fwd.hpp"

#include "tenzir/argument_parser2.hpp"
#include "tenzir/detail/flat_map.hpp"
#include "tenzir/operator_control_plane.hpp"
#include "tenzir/pipeline_executor.hpp"

#include <tenzir/detail/croncpp.hpp>
#include <tenzir/detail/string_literal.hpp>
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

namespace tenzir::plugins::every_cron2 {

namespace {

using timepoint = decltype(time::clock::now());

struct scheduler_actor_traits final {
  using signatures = caf::type_list<
    /// Push events from parent into self.
    auto(atom::push, table_slice output)->caf::result<void>,
    /// Push events from subpipeline into self.
    auto(atom::internal, atom::push, table_slice output)->caf::result<void>,
    /// Get events from self to subpipeline.
    auto(atom::internal, atom::pull)->caf::result<table_slice>,
    /// Get events from self to parent.
    auto(atom::pull)->caf::result<std::vector<table_slice>>,
    /// Get events from self to parent immediately.
    auto(atom::pull, atom::flush)->caf::result<std::vector<table_slice>>>
    /// Support the diagnostic receiver interface.
    ::append_from<receiver_actor<diagnostic>::signatures>
    /// Support the metrics receiver interface for the branch pipelines.
    ::append_from<metrics_receiver_actor::signatures>;
};

using scheduler_actor = caf::typed_actor<scheduler_actor_traits>;

struct scheduler_state {
  scheduler_state(scheduler_actor::pointer self, shared_diagnostic_handler dh,
                  metrics_receiver_actor metrics, uint64_t operator_index,
                  exec_node_actor spawner)
    : self_{self},
      dh_{std::move(dh)},
      metrics_receiver_{std::move(metrics)},
      operator_index_{operator_index} {
    self_->monitor(std::move(spawner), [&](const caf::error& e) {
      self_->quit(e);
    });
  }

  ~scheduler_state() {
    if (slice_rp_.pending()) {
      slice_rp_.deliver(std::vector<table_slice>{});
    }
    if (external_slice_rp_.pending()) {
      external_slice_rp_.deliver(table_slice{});
    }
  }

  auto make_behavior() -> scheduler_actor::behavior_type {
    return {
      [&](atom::push, table_slice output) {
        if (external_slice_rp_.pending()) {
          external_slice_rp_.deliver(std::move(output));
          return;
        }
        external_slices_.push_back(std::move(output));
      },
      [&](atom::internal, atom::push, table_slice output) {
        if (slice_rp_.pending()) {
          slice_rp_.deliver(std::vector{std::move(output)});
          return;
        }
        slices_.push_back(std::move(output));
      },
      [&](atom::internal, atom::pull) -> caf::result<table_slice> {
        if (external_slices_.empty()) {
          external_slice_rp_ = self_->make_response_promise<table_slice>();
          return external_slice_rp_;
        }
        auto x = std::move(external_slices_.front());
        external_slices_.pop_front();
        return x;
      },
      [&](atom::pull) -> caf::result<std::vector<table_slice>> {
        TENZIR_ASSERT(not slice_rp_.pending());
        if (slices_.empty()) {
          slice_rp_ = self_->make_response_promise<std::vector<table_slice>>();
          return slice_rp_;
        }
        return std::exchange(slices_, {});
      },
      [&](atom::pull, atom::flush) -> caf::result<std::vector<table_slice>> {
        return std::exchange(slices_, {});
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
        TENZIR_DEBUG("[scheduler_actor] received exit: {}", msg.reason);
        if (msg.reason) {
          self_->quit(msg.reason);
        }
      },
    };
  }

private:
  scheduler_actor::pointer self_;
  shared_diagnostic_handler dh_;
  metrics_receiver_actor metrics_receiver_;
  uint64_t operator_index_ = 0;
  detail::flat_map<uint64_t, detail::flat_map<uuid, uuid>> registered_metrics_;
  std::vector<table_slice> slices_;
  std::deque<table_slice> external_slices_;
  caf::typed_response_promise<std::vector<table_slice>> slice_rp_;
  caf::typed_response_promise<table_slice> external_slice_rp_;
};

struct internal_source final : public crtp_operator<internal_source> {
  internal_source() = default;

  internal_source(scheduler_actor actor) : actor_{std::move(actor)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto slice = table_slice{};
    while (true) {
      TENZIR_DEBUG("[internal-scheduler-source] requesting slice");
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::internal_v, atom::pull_v)
        .request(actor_, caf::infinite)
        .then(
          [&](table_slice input) {
            ctrl.set_waiting(false);
            TENZIR_DEBUG("[internal-scheduler-source] received slice");
            slice = std::move(input);
          },
          [&](const caf::error& e) {
            diagnostic::error(e).emit(ctrl.diagnostics());
          });
      co_yield {};
      co_yield slice;
      if (slice.rows() == 0) {
        TENZIR_DEBUG("[internal-scheduler-source] exiting");
        break;
      }
    }
  }

  auto name() const -> std::string override {
    return "internal-scheduler-source";
  }

  auto optimize(const expression&, event_order) const
    -> optimize_result override {
    // TODO: Maybe we can optimize this
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, internal_source& x) {
    return f.apply(x.actor_);
  }

private:
  scheduler_actor actor_;
};

struct internal_sink final : public crtp_operator<internal_sink> {
  internal_sink() = default;

  internal_sink(scheduler_actor actor, tenzir::location op,
                std::optional<expression> filter)
    : actor_{std::move(actor)}, op_{op}, filter_{std::move(filter)} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    for (auto slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      TENZIR_DEBUG("[internal-scheduler-sink] pushing slice");
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::internal_v, atom::push_v, std::move(slice))
        .request(actor_, caf::infinite)
        .then(
          [&] {
            TENZIR_DEBUG("[internal-scheduler-sink] pushed slice");
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
      .request(actor_, caf::infinite)
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
    return "internal-scheduler-sink";
  }

  auto optimize(const expression&, event_order) const
    -> optimize_result override {
    // TODO: Maybe we can optimize this
    return {filter_, event_order::ordered, copy()};
  }

  friend auto inspect(auto& f, internal_sink& x) {
    return f.object(x).fields(f.field("actor_", x.actor_),
                              f.field("op_", x.op_),
                              f.field("filter_", x.filter_));
  }

private:
  scheduler_actor actor_;
  tenzir::location op_;
  std::optional<expression> filter_;
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
    auto out = infer_type_impl(to_operator_type(input));
    return match(
      std::move(input),
      [](auto) -> caf::expected<operator_output> {
        TENZIR_UNREACHABLE();
      },
      [&](std::monostate) -> caf::expected<operator_output> {
        TENZIR_ASSERT(out->template is<table_slice>());
        return run(ctrl);
      },
      [&](generator<table_slice> in) -> caf::expected<operator_output> {
        if (out->template is<table_slice>()) {
          return run<table_slice>(std::move(in), ctrl);
        }
        return run<std::monostate>(std::move(in), ctrl);
      });
  }

  auto run(operator_control_plane& ctrl) const -> generator<table_slice> {
    auto& dh = ctrl.diagnostics();
    auto cron = std::optional<detail::cron::cronexpr>{};
    if (not args_.is_every) {
      cron = detail::cron::make_cron(args_.cron.inner);
    }
    auto slices = std::vector<table_slice>{};
    auto handle = spawn_scheduler(ctrl);
    auto ts = time::clock::now();
    auto next = next_ts(cron, ts);
    spawn_pipeline<void>(ctrl, handle, ts, next, cron);
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
            diagnostic::error(e).primary(args_.op).emit(dh);
          });
      co_yield {};
      for (auto&& slice : slices) {
        if (slice.rows() == 0) {
          next = next_ts(cron, ts);
          if (time::clock::now() >= next) {
            ts = time::clock::now();
            while (ts >= next) {
              next = next_ts(cron, next);
            }
            spawn_pipeline<void>(ctrl, handle, ts, next, cron);
            break;
          }
          ctrl.self().delay_for_fn(next - ts, [&] {
            ts = time::clock::now();
            spawn_pipeline<void>(ctrl, handle, ts, next, cron);
          });
          break;
        }
        co_yield slice;
      }
      slices.clear();
    }
  }

  template <typename T>
  auto run(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<T> {
    auto& dh = ctrl.diagnostics();
    auto cron = std::optional<detail::cron::cronexpr>{};
    if (not args_.is_every) {
      cron = detail::cron::make_cron(args_.cron.inner);
    }
    auto slices = std::vector<table_slice>{};
    auto handle = spawn_scheduler(ctrl);
    auto ts = time::clock::now();
    auto next = next_ts(cron, ts);
    spawn_pipeline<table_slice>(ctrl, handle, ts, next, cron);
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
              diagnostic::error(e).primary(args_.op).emit(dh);
            });
        co_yield {};
      }
      if constexpr (std::same_as<table_slice, T>) {
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
              diagnostic::error(e).primary(args_.op).emit(dh);
            });
        co_yield {};
        for (auto&& slice : slices) {
          if (slice.rows() == 0) {
            next = next_ts(cron, ts);
            if (time::clock::now() >= next) {
              ts = time::clock::now();
              while (ts >= next) {
                next = next_ts(cron, next);
              }
              spawn_pipeline<table_slice>(ctrl, handle, ts, next, cron);
              break;
            }
            ctrl.self().delay_for_fn(next - ts, [&] {
              ts = time::clock::now();
              next = next_ts(cron, ts);
              spawn_pipeline<table_slice>(ctrl, handle, ts, next, cron);
            });
            break;
          }
          co_yield slice;
        }
        slices.clear();
      }
      co_yield {};
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
          diagnostic::error(e).primary(args_.op).emit(dh);
        });
    if constexpr (std::same_as<table_slice, T>) {
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
              diagnostic::error(e).primary(args_.op).emit(dh);
            });
        co_yield {};
        for (auto&& slice : slices) {
          if (slice.rows() == 0) {
            co_return;
          }
          co_yield slice;
        }
        slices.clear();
      }
    }
  }

  auto spawn_scheduler(operator_control_plane& ctrl) const -> scheduler_actor {
    auto handle
      = ctrl.self().spawn(caf::actor_from_state<scheduler_state>,
                          ctrl.shared_diagnostics(), ctrl.metrics_receiver(),
                          ctrl.operator_index(), &ctrl.self());
    ctrl.self().monitor(handle, [&](const caf::error& e) {
      if (e) {
        diagnostic::error(e)
          .compose(add_diagnostic_location())
          .emit(ctrl.diagnostics());
      }
    });
    return handle;
  }

  template <typename T>
  auto spawn_pipeline(operator_control_plane& ctrl, scheduler_actor hdl,
                      timepoint& ts, timepoint& next,
                      const std::optional<detail::cron::cronexpr>& cron) const
    -> void {
    auto pipe = args_.pipe.inner;
    auto output_type = pipe.infer_type<T>();
    TENZIR_ASSERT(output_type);
    if (output_type->template is_not<void>()) {
      pipe.append(std::make_unique<internal_sink>(hdl, args_.op, std::nullopt));
    }
    if constexpr (std::same_as<T, table_slice>) {
      pipe.prepend(std::make_unique<internal_source>(hdl));
      ctrl.self().delay_for_fn(next - ts, [&, hdl] {
        if (ts >= time::clock::now()) {
          return;
        }
        ts = time::clock::now();
        next = next_ts(cron, ts);
        ctrl.self()
          .mail(atom::push_v, table_slice{})
          .request(hdl, caf::infinite)
          .then(
            [] {
              TENZIR_DEBUG("[every_cron] closed input source");
            },
            [&](const caf::error& e) {
              diagnostic::error("failed to close input source: {}", e)
                .primary(args_.op)
                .emit(ctrl.diagnostics());
            });
      });
    }
    auto exec
      = ctrl.self().spawn(pipeline_executor,
                          std::move(pipe).optimize_if_closed(),
                          std::string{ctrl.definition()}, hdl, hdl, ctrl.node(),
                          ctrl.has_terminal(), ctrl.is_hidden());
    exec->link_to(hdl);
    hdl->attach_functor([exec] {
      caf::anon_send_exit(exec, caf::exit_reason::user_shutdown);
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
  }

  auto next_ts(const std::optional<detail::cron::cronexpr>& cron,
               timepoint last) const -> timepoint {
    if (cron) {
      auto casted = time_point_cast<std::chrono::system_clock::duration>(last);
      return detail::cron::cron_next(*cron, casted);
    }
    auto next = last + args_.every.inner;
    return std::chrono::time_point_cast<timepoint::duration>(next);
  }

  auto infer_type_impl(operator_type input) const
    -> caf::expected<operator_type> override {
    if (input.is<chunk_ptr>()) {
      return diagnostic::error("`{}` does accept bytes as input",
                               args_.is_every ? "every" : "cron")
        .primary(args_.op)
        .to_error();
    }
    return args_.pipe.inner.infer_type_impl(input)
      .transform_or([&](caf::error e) {
        return diagnostic::error(std::move(e)).primary(args_.pipe).to_error();
      })
      .and_then([&](operator_type out) -> caf::expected<operator_type> {
        if (out.is<chunk_ptr>()) {
          return diagnostic::error("subpipeline must not return bytes")
            .primary(args_.pipe)
            .to_error();
        }
        if (input.is<void>() and out.is<void>()) {
          return diagnostic::error("subpipeline must not be void-to-void")
            .primary(args_.pipe)
            .to_error();
        }
        return out;
      });
  }

  auto add_diagnostic_location() const {
    return [&](diagnostic_builder x) -> diagnostic_builder {
      if (x.inner().annotations.empty()) {
        return std::move(x).primary(args_.pipe);
      }
      return x;
    };
  }

  auto name() const -> std::string override {
    return Name;
  }

  auto copy() const -> operator_ptr override {
    return std::make_unique<every_cron_operator>(*this);
  }

  auto optimize(const expression&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, every_cron_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  every_cron_args args_;
};

using every_operator = every_cron_operator<"every">;

struct every_plugin final : public operator_plugin2<every_operator> {
  auto name() const -> std::string override {
    return "every";
  }

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
          .positional("interval", args.cron)
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
