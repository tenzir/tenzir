//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/pipeline_executor.hpp>
#include <tenzir/scope_linked.hpp>
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
      tenzir::pipeline_executor, std::move(pipe),
      std::string{ctrl.definition()}, std::string{ctrl.pipeline_id()},
      side_channel.get(), side_channel.get(), ctrl.node(), ctrl.has_terminal(),
      ctrl.is_hidden())};
    ctrl.self().monitor(pipeline_executor.get(), [&](caf::error err) {
      if (err and err != caf::exit_reason::user_shutdown) {
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

class fork_plugin final : public virtual operator_plugin2<fork_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto pipe = located<pipeline>{};
    TRY(argument_parser2::operator_("fork")
          .positional("{ … }", pipe)
          .parse(inv, ctx));
    return std::make_unique<fork_operator>(std::move(pipe));
  }
};

using internal_fork_source_plugin
  = operator_inspection_plugin<internal_fork_source_operator>;

} // namespace

} // namespace tenzir::plugins::fork

TENZIR_REGISTER_PLUGIN(tenzir::plugins::fork::fork_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::fork::internal_fork_source_plugin)
