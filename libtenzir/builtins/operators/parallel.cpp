//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser2.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/flat_map.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/operator_control_plane.hpp>
#include <tenzir/pipeline_executor.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <caf/actor_from_state.hpp>
#include <caf/actor_registry.hpp>
#include <caf/disposable.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/expected.hpp>
#include <caf/typed_actor.hpp>

#include <memory>
#include <utility>
#include <variant>

namespace tenzir::plugins::parallel {

namespace {

template <typename T>
[[nodiscard]] constexpr auto take(std::optional<T>& x) -> T {
  TENZIR_ASSERT(x);
  return std::exchange(x, std::nullopt).value();
}

struct transceiver_actor_traits final {
  using signatures = caf::type_list<
    /// Push events from parent into self.
    auto(atom::push, table_slice)->caf::result<void>,
    /// Push events from subpipeline into self.
    auto(atom::internal, atom::push, table_slice)->caf::result<void>,
    /// Get events from self to subpipeline.
    auto(atom::internal, atom::pull)->caf::result<table_slice>,
    /// Get events from self to parent.
    auto(atom::pull)->caf::result<table_slice>,
    /// Signal all subpipelines have stopped.
    auto(atom::stop)->caf::result<void>,
    /// Signal input end.
    auto(atom::done)->caf::result<void>>
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
    : operator_index_{operator_index},
      self_{self},
      dh_{std::move(dh)},
      metrics_receiver_{std::move(metrics)} {
    self_->monitor(std::move(spawner), [&](caf::error e) {
      TENZIR_TRACE("[transceiver_actor] spawner shut down, exiting");
      self_->quit(std::move(e));
    });
  }

  auto make_behavior() -> transceiver_actor::behavior_type {
    return {
      [this](atom::push, table_slice input) -> caf::result<void> {
        TENZIR_ASSERT(not done_);
        TENZIR_ASSERT(not push_rp_.pending());
        TENZIR_ASSERT(not input_);
        if (not internal_pull_rps_.empty()) {
          internal_pull_rps_.front().deliver(std::move(input));
          internal_pull_rps_.pop_front();
          return {};
        }
        input_ = std::move(input);
        push_rp_ = self_->make_response_promise<void>();
        return push_rp_;
      },
      [this](atom::internal, atom::push,
             table_slice output) -> caf::result<void> {
        if (pull_rp_.pending()) {
          pull_rp_.deliver(std::move(output));
          return {};
        }
        outputs_.push_back(std::move(output));
        auto& rp = internal_push_rps_.emplace_back(
          self_->make_response_promise<void>());
        return rp;
      },
      [this](atom::internal, atom::pull) -> caf::result<table_slice> {
        TENZIR_ASSERT(not stop_);
        if (push_rp_.pending()) {
          push_rp_.deliver();
        }
        if (input_) {
          return take(input_);
        }
        if (done_) {
          return table_slice{};
        }
        auto& rp = internal_pull_rps_.emplace_back(
          self_->make_response_promise<table_slice>());
        return rp;
      },
      [this](atom::pull) -> caf::result<table_slice> {
        TENZIR_ASSERT(not pull_rp_.pending());
        if (not internal_push_rps_.empty()) {
          internal_push_rps_.front().deliver();
          internal_push_rps_.pop_front();
        }
        if (not outputs_.empty()) {
          auto output = std::move(outputs_.front());
          outputs_.pop_front();
          return output;
        }
        if (stop_) {
          return table_slice{};
        }
        pull_rp_ = self_->make_response_promise<table_slice>();
        return pull_rp_;
      },
      [this](atom::stop) -> caf::result<void> {
        TENZIR_ASSERT(internal_pull_rps_.empty());
        TENZIR_ASSERT(internal_push_rps_.empty());
        if (pull_rp_.pending()) {
          pull_rp_.deliver(table_slice{});
        }
        stop_ = true;
        return {};
      },
      [this](atom::done) -> caf::result<void> {
        TENZIR_ASSERT(not push_rp_.pending());
        done_ = true;
        if (not internal_pull_rps_.empty()) {
          for (auto& rp : internal_pull_rps_) {
            rp.deliver(table_slice{});
          }
          internal_pull_rps_.clear();
        }
        return {};
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
        TENZIR_TRACE("[transceiver_actor] received exit: {}", msg.reason);
        if (msg.reason.valid()) {
          self_->quit(msg.reason);
        }
      },
    };
  }

private:
  bool stop_ = false;
  bool done_ = false;
  uint64_t operator_index_ = 0;
  transceiver_actor::pointer self_;
  shared_diagnostic_handler dh_;
  metrics_receiver_actor metrics_receiver_;
  std::deque<table_slice> outputs_;
  std::optional<table_slice> input_;
  caf::typed_response_promise<void> push_rp_;
  std::deque<caf::typed_response_promise<void>> internal_push_rps_;
  caf::typed_response_promise<table_slice> pull_rp_;
  std::deque<caf::typed_response_promise<table_slice>> internal_pull_rps_;
  detail::flat_map<uint64_t, detail::flat_map<uuid, uuid>> registered_metrics_;
};

struct internal_source final : public crtp_operator<internal_source> {
  internal_source() = default;

  internal_source(transceiver_actor actor) : actor_{std::move(actor)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto slice = table_slice{};
    while (true) {
      TENZIR_TRACE("[internal-transceiver-source] requesting slice");
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::internal_v, atom::pull_v)
        .request(actor_, caf::infinite)
        .then(
          [&](table_slice input) {
            TENZIR_TRACE("[internal-transceiver-source] received slice");
            ctrl.set_waiting(false);
            slice = std::move(input);
          },
          [&](const caf::error& e) {
            diagnostic::error(e).emit(ctrl.diagnostics());
          });
      co_yield {};
      if (slice.rows() == 0) {
        TENZIR_TRACE("[internal-transceiver-source] exiting");
        co_return;
      }
      co_yield std::move(slice);
    }
  }

  auto name() const -> std::string override {
    return "parallel-internal-transceiver-source";
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
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      TENZIR_TRACE("[internal-transceiver-sink] pushing slice");
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::internal_v, atom::push_v, std::move(slice))
        .request(hdl_, caf::infinite)
        .then(
          [&] {
            TENZIR_TRACE("[internal-transceiver-sink] pushed slice");
            ctrl.set_waiting(false);
          },
          [&](const caf::error& e) {
            diagnostic::error("failed to push events: {}", e)
              .primary(op_)
              .emit(ctrl.diagnostics());
          });
      co_yield {};
    }
  }

  auto name() const -> std::string override {
    return "parallel-internal-transceiver-sink";
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

struct parallel_args {
  uuid id{};
  tenzir::location op{};
  located<pipeline> pipe;
  located<uint64_t> jobs;
  located<uint64_t> split_at{5'000, location::unknown};

  auto validate(session ctx) const -> failure_or<void> {
    if (jobs.inner == 0) {
      diagnostic::error("`jobs` must not be zero").primary(jobs).emit(ctx);
      return failure::promise();
    }
    if (split_at.inner == 0) {
      diagnostic::error("`_split_at` must not be zero")
        .primary(split_at)
        .emit(ctx);
      return failure::promise();
    }
    return {};
  }

  friend auto inspect(auto& f, parallel_args& x) -> bool {
    return f.object(x).fields(f.field("id", x.id), f.field("op", x.op),
                              f.field("pipe", x.pipe), f.field("jobs", x.jobs),
                              f.field("split_at", x.split_at));
  }
};

struct execution_state {
  uint64_t count = 0;
};

struct parallel_operator final : public operator_base {
  parallel_operator() = default;

  explicit parallel_operator(parallel_args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_input input, operator_control_plane& ctrl) const
    -> caf::expected<operator_output> override {
    using Out = caf::expected<operator_output>;
    TRY(auto out, infer_type_impl(to_operator_type(input)));
    TENZIR_ASSERT(out.template is_not<chunk_ptr>());
    return match(
      std::move(input),
      [&](std::monostate) -> Out {
        TENZIR_UNREACHABLE();
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

  auto run(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    const auto handle = spawn_transceiver(ctrl);
    auto state = execution_state{};
    for (auto i = uint64_t{}; i < args_.jobs.inner; ++i) {
      spawn_pipeline(ctrl, handle, state);
    }
    input = split_slices(std::move(input));
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      TENZIR_TRACE("[parallel sink] pushing slice");
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::push_v, std::move(slice))
        .request(handle, caf::infinite)
        .then(
          [&] {
            TENZIR_TRACE("[parallel sink] pushed slice");
            ctrl.set_waiting(false);
          },
          [&](const caf::error& e) {
            diagnostic::error(e).primary(args_.op).emit(ctrl.diagnostics());
          });
      co_yield {};
    }
    TENZIR_TRACE("[parallel sink] finishing input");
    ctrl.set_waiting(true);
    ctrl.self()
      .mail(atom::done_v)
      .request(handle, caf::infinite)
      .then(
        [&] {
          TENZIR_TRACE("[parallel sink] finished input");
        },
        [&](const caf::error& e) {
          diagnostic::error(e).primary(args_.op).emit(ctrl.diagnostics());
        });
    co_yield {};
  }

  auto run(generator<table_slice> input, operator_control_plane& ctrl,
           tag<table_slice>) const -> generator<table_slice> {
    const auto key
      = fmt::format("tenzir.parallel_sink.{}.{}", args_.id, ctrl.run_id());
    const auto handle
      = ctrl.self().system().registry().get<transceiver_actor>(key);
    ctrl.self().system().registry().erase(handle.id());
    TENZIR_ASSERT(handle);
    auto state = execution_state{};
    for (auto i = uint64_t{}; i < args_.jobs.inner; ++i) {
      spawn_pipeline(ctrl, handle, state);
    }
    input = split_slices(std::move(input));
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      TENZIR_TRACE("[parallel] pushing slice");
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::push_v, std::move(slice))
        .request(handle, caf::infinite)
        .then(
          [&] {
            TENZIR_TRACE("[parallel] pushed slice");
            ctrl.set_waiting(false);
          },
          [&](const caf::error& e) {
            diagnostic::error(e).primary(args_.op).emit(ctrl.diagnostics());
          });
      co_yield {};
    }
    TENZIR_TRACE("[parallel] finishing input");
    ctrl.set_waiting(true);
    ctrl.self()
      .mail(atom::done_v)
      .request(handle, caf::infinite)
      .then(
        [&] {
          TENZIR_TRACE("[parallel] finished input");
        },
        [&](const caf::error& e) {
          diagnostic::error(e).primary(args_.op).emit(ctrl.diagnostics());
        });
    co_yield {};
  }

  auto split_slices(generator<table_slice> input) const
    -> generator<table_slice> {
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      const auto rows = slice.rows();
      const auto correction
        = static_cast<uint8_t>(rows % args_.split_at.inner != 0);
      const auto splits = (rows / args_.split_at.inner) + correction;
      TENZIR_ASSERT(splits != 0);
      const auto size = (rows / splits) + correction;
      for (auto i = uint64_t{}; i < splits; ++i) {
        co_yield subslice(slice, size * i, std::min(rows, size * (i + 1)));
      }
    }
  }

  template <concepts::one_of<void, table_slice> In>
  auto make_pipeline(const transceiver_actor& hdl) const -> pipeline {
    auto pipe = args_.pipe.inner;
    auto out = pipe.template infer_type<In>();
    TENZIR_ASSERT(out);
    if (out->template is<table_slice>()) {
      pipe.append(std::make_unique<internal_sink>(hdl, args_.op));
    }
    if constexpr (std::same_as<table_slice, In>) {
      pipe.prepend(std::make_unique<internal_source>(hdl));
    }
    return pipe;
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
    ctrl.self().attach_functor([hdl] {
      caf::anon_send_exit(hdl, caf::exit_reason::user_shutdown);
    });
    return hdl;
  }

  auto
  spawn_pipeline(operator_control_plane& ctrl, const transceiver_actor& hdl,
                 execution_state& state) const -> void {
    auto pipe = make_pipeline<table_slice>(hdl);
    const auto exec
      = ctrl.self().spawn(pipeline_executor, std::move(pipe),
                          std::string{ctrl.definition()}, hdl, hdl, ctrl.node(),
                          ctrl.has_terminal(), ctrl.is_hidden(),
                          std::string{ctrl.pipeline_id()});
    ctrl.self().attach_functor([exec] {
      caf::anon_send_exit(exec, caf::exit_reason::user_shutdown);
    });
    ctrl.self().monitor(exec, [&, exec](const caf::error& err) {
      TENZIR_TRACE("[parallel] subpipeline shut down");
      if (err.valid()) {
        diagnostic::error(err)
          .compose(add_diagnostic_location())
          .emit(ctrl.diagnostics());
      }
      ++state.count;
      if (state.count == args_.jobs.inner) {
        ctrl.self().mail(atom::stop_v).send(hdl);
        ctrl.self().quit();
      }
    });
    TENZIR_TRACE("[parallel] requesting subpipeline start");
    ctrl.self()
      .mail(atom::start_v)
      .request(exec, caf::infinite)
      .then(
        [&] {
          TENZIR_TRACE("[parallel] subpipeline started");
        },
        [&ctrl, this](const caf::error& e) {
          diagnostic::error(e)
            .compose(add_diagnostic_location())
            .emit(ctrl.diagnostics());
        });
  }

  auto infer_type_impl(operator_type input) const
    -> caf::expected<operator_type> override {
    if (input.is<void>()) {
      return diagnostic::error("`{}` cannot be used as a source", name())
        .primary(args_.op)
        .to_error();
    }
    if (input.is<chunk_ptr>()) {
      return diagnostic::error("`{}` does not accept bytes as input", name())
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

  auto optimize(const expression& filter, event_order) const
    -> optimize_result override {
    auto result = args_.pipe.inner.optimize(filter, event_order::unordered);
    auto args = args_;
    args.pipe.inner = static_cast<pipeline&&>(*result.replacement);
    return {
      std::move(result.filter),
      event_order::unordered,
      std::make_unique<parallel_operator>(std::move(args)),
    };
  }

  auto name() const -> std::string override {
    return "parallel";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto copy() const -> operator_ptr override {
    return std::make_unique<parallel_operator>(*this);
  }

  friend auto inspect(auto& f, parallel_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  parallel_args args_;
};

class parallel_sink_operator final
  : public crtp_operator<parallel_sink_operator> {
public:
  parallel_sink_operator() = default;

  parallel_sink_operator(uuid id, tenzir::location loc) : id_{id}, loc_{loc} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    const auto handle = spawn_transceiver(ctrl);
    const auto key
      = fmt::format("tenzir.parallel_sink.{}.{}", id_, ctrl.run_id());
    ctrl.self().system().registry().put(key, handle);
    co_yield {};
    auto output = table_slice{};
    auto done = false;
    while (not done) {
      if (const auto stub = input.next()) {
        TENZIR_ASSERT(stub->rows() == 0);
      }
      TENZIR_TRACE("[parallel_sink] requesting slice");
      ctrl.self()
        .mail(atom::pull_v)
        .request(handle, caf::infinite)
        .then(
          [&](table_slice slice) {
            ctrl.set_waiting(false);
            TENZIR_TRACE("[parallel_sink] got slice");
            done = slice.rows() == 0;
            output = std::move(slice);
          },
          [&](caf::error err) {
            diagnostic::error(std::move(err))
              .primary(loc_)
              .emit(ctrl.diagnostics());
          });
      ctrl.set_waiting(true);
      co_yield {};
      co_yield std::move(output);
    }
  }

  auto add_diagnostic_location() const {
    return [loc = loc_](diagnostic_builder x) -> diagnostic_builder {
      if (x.inner().annotations.empty()) {
        return std::move(x).primary(loc);
      }
      return x;
    };
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
    ctrl.self().attach_functor([hdl] {
      caf::anon_send_exit(hdl, caf::exit_reason::user_shutdown);
    });
    return hdl;
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    return {filter, order, copy()};
  }

  auto name() const -> std::string override {
    return "parallel_sink";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  friend auto inspect(auto& f, parallel_sink_operator& x) -> bool {
    return f.object(x).fields(f.field("id_", x.id_), f.field("loc_", x.loc_));
  }

private:
  uuid id_{};
  tenzir::location loc_{};
};

struct parallel final : public operator_plugin2<parallel_operator> {
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = parallel_args{};
    args.op = inv.self.get_location();
    auto pipe = std::optional<located<pipeline>>{};
    auto p = argument_parser2::operator_(name())
               .positional("jobs", args.jobs)
               .named_optional("_split_at", args.split_at)
               .positional("{ … }", pipe);
    TRY(p.parse(inv, ctx));
    if (not pipe) {
      diagnostic::error("missing positional argument `{{ … }}`")
        .usage("parallel jobs:int { … }")
        .docs(p.docs())
        .primary(inv.self.get_location())
        .emit(ctx);
      return failure::promise();
    }
    args.pipe = std::move(*pipe);
    TRY(args.validate(ctx));
    if (const auto out = args.pipe.inner.infer_type(tag_v<table_slice>);
        out and out->is<table_slice>()) {
      const auto loc = args.pipe.source;
      const auto id = uuid::random();
      args.id = id;
      auto result = std::make_unique<pipeline>();
      result->append(std::make_unique<parallel_operator>(std::move(args)));
      result->append(std::make_unique<parallel_sink_operator>(id, loc));
      return failure_or<operator_ptr>{std::move(result)};
    }
    return std::make_unique<parallel_operator>(std::move(args));
  }
};

using internal_source_plugin = operator_inspection_plugin<internal_source>;
using internal_sink_plugin = operator_inspection_plugin<internal_sink>;
using parallel_sink = operator_inspection_plugin<parallel_sink_operator>;

} // namespace

} // namespace tenzir::plugins::parallel

TENZIR_REGISTER_PLUGIN(tenzir::plugins::parallel::internal_source_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::parallel::internal_sink_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::parallel::parallel_sink)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::parallel::parallel)
