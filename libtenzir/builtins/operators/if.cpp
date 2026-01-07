//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/rebatch.hpp"

#include <tenzir/detail/flat_map.hpp>
#include <tenzir/multi_series.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/pipeline_executor.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/scope_linked.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/exec.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <caf/actor_from_state.hpp>
#include <caf/actor_registry.hpp>
#include <caf/exit_reason.hpp>
#include <caf/scheduled_actor/flow.hpp>
#include <caf/typed_response_promise.hpp>

#include <deque>

namespace tenzir::plugins::if_ {

namespace {

/// Splits a batch of events into two based on an array of bools. Treats null as
/// false. The first element of the returned pair are the values for which the
/// predicate returned true, and the second element are the other values.
auto split_at_predicate(const table_slice& events,
                        const arrow::BooleanArray& predicate)
  -> std::pair<table_slice, table_slice> {
  TENZIR_ASSERT(events.rows() > 0);
  TENZIR_ASSERT(predicate.length() == detail::narrow<int64_t>(events.rows()));
  auto lhs = std::vector<table_slice>{};
  auto rhs = std::vector<table_slice>{};
  const auto pred_at = [&](int64_t i) {
    return predicate.IsValid(i) and predicate.GetView(i);
  };
  auto range_offset = int64_t{0};
  auto range_value = pred_at(0);
  const auto append = [&](int64_t i) {
    auto& result = (range_value ? lhs : rhs);
    result.push_back(subslice(events, range_offset, i));
    range_offset = i;
    range_value = not range_value;
  };
  for (auto i = range_offset + 1; i < predicate.length(); ++i) {
    if (range_value != pred_at(i)) {
      append(i);
    }
  }
  append(predicate.length());
  return {
    concatenate(std::move(lhs)),
    concatenate(std::move(rhs)),
  };
}

struct branch_actor_traits {
  using signatures = caf::type_list<
    // Push events from the parent pipeline into the branch pipelines.
    auto(atom::push, table_slice input)->caf::result<void>,
    // Pull evaluated events into the branch pipelines.
    auto(atom::internal, atom::pull, bool predicate)->caf::result<table_slice>,
    // Push events from the branch pipelines into the parent.
    auto(atom::internal, atom::push, bool predicate, table_slice output)
      ->caf::result<void>,
    // Get resulting events from the branch pipelines into the parent pipeline.
    auto(atom::pull)->caf::result<table_slice>>
    // Support the diagnostic receiver interface for the branch pipelines.
    ::append_from<receiver_actor<diagnostic>::signatures>
    // Support the metrics receiver interface for the branch pipelines.
    ::append_from<metrics_receiver_actor::signatures>;
};

using branch_actor = caf::typed_actor<branch_actor_traits>;

/// The source operator used within branches of the `if` statement.
class branch_source_operator final
  : public crtp_operator<branch_source_operator> {
public:
  branch_source_operator() = default;

  branch_source_operator(branch_actor branch, bool predicate,
                         tenzir::location source)
    : branch_{std::move(branch)}, predicate_{predicate}, source_{source} {
  }

  auto name() const -> std::string override {
    return "internal-branch-source";
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    // Branching necessarily throws off the event order, so we can allow the
    // nested pipelines to do ordering optimizations.
    return optimize_result{std::nullopt, event_order::unordered, this->copy()};
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto done = false;
    auto result = table_slice{};
    while (not done) {
      ctrl.self()
        .mail(atom::internal_v, atom::pull_v, predicate_)
        .request(branch_, caf::infinite)
        .then(
          [&](table_slice input) {
            done = input.rows() == 0;
            result = std::move(input);
            ctrl.set_waiting(false);
          },
          [&](caf::error err) {
            if (err.empty() or err == caf::sec::request_receiver_down
                or err == caf::exit_reason::remote_link_unreachable) {
              done = true;
              result = table_slice{};
              ctrl.set_waiting(false);
              return;
            }
            diagnostic::error(std::move(err))
              .note("failed to pull events into branch")
              .primary(source_)
              .emit(ctrl.diagnostics());
          });
      ctrl.set_waiting(true);
      co_yield {};
      if (not done) {
        co_yield std::move(result);
      }
    }
  }

  friend auto inspect(auto& f, branch_source_operator& x) -> bool {
    return f.object(x).fields(f.field("branch", x.branch_),
                              f.field("predicate", x.predicate_),
                              f.field("source", x.source_));
  }

private:
  branch_actor branch_;
  bool predicate_ = false;
  tenzir::location source_;
};

/// The sink operator used within branches of the `if` statement if the branch
/// had no sink of its own.
class branch_sink_operator final : public crtp_operator<branch_sink_operator> {
public:
  branch_sink_operator() = default;

  explicit branch_sink_operator(branch_actor branch, bool predicate,
                                tenzir::location source)
    : branch_{std::move(branch)}, predicate_{predicate}, source_{source} {
  }

  auto name() const -> std::string override {
    return "internal-branch-sink";
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    // Branching necessarily throws off the event order, so we can allow the
    // nested pipelines to do ordering optimizations.
    return optimize_result{std::nullopt, event_order::unordered, this->copy()};
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
        .mail(atom::internal_v, atom::push_v, predicate_, std::move(events))
        .request(branch_, caf::infinite)
        .then(
          [&]() {
            ctrl.set_waiting(false);
          },
          [&](caf::error err) {
            if (err.empty() or err == caf::sec::request_receiver_down
                or err == caf::exit_reason::remote_link_unreachable) {
              ctrl.set_waiting(false);
              return;
            }
            diagnostic::error(std::move(err))
              .note("failed to push events from branch")
              .primary(source_)
              .emit(ctrl.diagnostics());
          });
      ctrl.set_waiting(true);
      co_yield {};
    }
  }

  friend auto inspect(auto& f, branch_sink_operator& x) -> bool {
    return f.object(x).fields(f.field("branch", x.branch_),
                              f.field("predicate", x.predicate_),
                              f.field("source", x.source_));
  }

private:
  branch_actor branch_;
  bool predicate_ = false;
  tenzir::location source_;
};

/// An actor managing the nested pipelines of an `if` statement.
class branch {
public:
  branch(branch_actor::pointer self, std::string definition, node_actor node,
         shared_diagnostic_handler dh, metrics_receiver_actor metrics_receiver,
         bool is_hidden, uint64_t operator_index, std::string pipeline_id,
         ast::expression predicate_expr, located<pipeline> then_pipe,
         std::optional<located<pipeline>> else_pipe)
    : self_{self},
      definition_{std::move(definition)},
      node_{std::move(node)},
      dh_{std::move(dh)},
      metrics_receiver_{std::move(metrics_receiver)},
      is_hidden_{is_hidden},
      operator_index_{operator_index},
      pipeline_id_{std::move(pipeline_id)},
      predicate_expr_{std::move(predicate_expr)},
      then_branch_{check(spawn_branch(std::move(then_pipe), true))},
      else_branch_{spawn_branch(std::move(else_pipe), false)} {
  }

  auto make_behavior() -> branch_actor::behavior_type {
    start_branch(then_branch_);
    start_branch(else_branch_);
    return {
      [this](atom::push, const table_slice& input) {
        return handle_input(input);
      },
      [this](atom::internal, atom::pull, bool predicate) {
        return forward_to_branch(predicate);
      },
      [this](atom::internal, atom::push, bool predicate, table_slice output) {
        return handle_output(predicate, std::move(output));
      },
      [this](atom::pull) {
        return forward_to_parent_pipeline();
      },
      [this](diagnostic diag) {
        return handle_diagnostic(std::move(diag));
      },
      [this](uint64_t nested_operator_index, uuid nested_metrics_id,
             type schema) {
        return register_metrics(nested_operator_index, nested_metrics_id,
                                std::move(schema));
      },
      [this](uint64_t nested_operator_index, uuid nested_metrics_id,
             record metrics) {
        return handle_metrics(nested_operator_index, nested_metrics_id,
                              std::move(metrics));
      },
      [](const operator_metric& metrics) {
        // We deliberately ignore operator metrics. There's no good way to
        // forward them from nested pipelines, and nowadays operator metrics are
        // really only relevant for generating pipeline metrics. If there's a
        // sink in the then-branch we're unfortunately losing its egress metrics
        // at the moment.
        TENZIR_UNUSED(metrics);
      },
    };
  }

private:
  auto spawn_branch(std::optional<located<pipeline>> pipe, bool predicate)
    -> std::optional<located<pipeline_executor_actor>> {
    if (not pipe) {
      return {};
    }
    pipe->inner.prepend(std::make_unique<branch_source_operator>(
      branch_actor{self_}, predicate, pipe->source));
    if (not pipe->inner.is_closed()) {
      pipe->inner.append(std::make_unique<branch_sink_operator>(
        branch_actor{self_}, predicate, pipe->source));
      TENZIR_ASSERT(pipe->inner.is_closed());
    }
    auto handle = self_->spawn(pipeline_executor,
                               std::move(pipe->inner).optimize_if_closed(),
                               definition_, receiver_actor<diagnostic>{self_},
                               metrics_receiver_actor{self_}, node_, false,
                               is_hidden_, pipeline_id_);
    ++running_branches_;
    self_->monitor(handle, [this, source = pipe->source](caf::error err) {
      if (err.valid()) {
        self_->quit(diagnostic::error(std::move(err))
                      .primary(source, "nested pipeline failed")
                      .to_error());
        return;
      }
      TENZIR_ASSERT(running_branches_ > 0);
      --running_branches_;
      if (running_branches_ == 0) {
        // We insert an empty batch as a sentinel value to signal that the
        // operator may shut down.
        if (to_endif_rp_.pending()) {
          TENZIR_ASSERT(outputs_.empty());
          to_endif_rp_.deliver(table_slice{});
          return;
        }
        outputs_.emplace_back();
      }
    });
    return located{std::move(handle), pipe->source};
  }

  auto start_branch(std::optional<located<pipeline_executor_actor>> branch)
    -> void {
    if (not branch) {
      return;
    }
    self_->mail(atom::start_v)
      .request(branch->inner, caf::infinite)
      .then([]() {},
            [this, source = branch->source](caf::error err) {
              self_->quit(diagnostic::error(std::move(err))
                            .primary(source, "failed to start nested pipeline")
                            .to_error());
            });
  }

  auto push_then(table_slice input) -> void {
    TENZIR_ASSERT(input.rows() > 0);
    if (to_then_branch_rp_.pending()) {
      TENZIR_ASSERT(then_inputs_.empty());
      to_then_branch_rp_.deliver(std::move(input));
      return;
    }
    then_inputs_.push_back(std::move(input));
  }

  auto push_else(table_slice input) -> void {
    TENZIR_ASSERT(input.rows() > 0);
    if (not else_branch_) {
      push_output(std::move(input));
      return;
    }
    if (to_else_branch_rp_.pending()) {
      TENZIR_ASSERT(else_inputs_.empty());
      to_else_branch_rp_.deliver(std::move(input));
      return;
    }
    else_inputs_.push_back(std::move(input));
  }

  auto push_output(table_slice output) -> void {
    TENZIR_ASSERT(output.rows() > 0);
    if (to_endif_rp_.pending()) {
      TENZIR_ASSERT(outputs_.empty());
      to_endif_rp_.deliver(std::move(output));
      return;
    }
    outputs_.push_back(std::move(output));
  }
  auto can_push_more() const -> bool {
    return then_inputs_.size() < max_queued
           and (else_branch_ ? else_inputs_.size() : outputs_.size())
                 < max_queued;
  }

  auto handle_input(const table_slice& input) -> caf::result<void> {
    TENZIR_ASSERT(not from_if_rp_.pending());
    if (input.rows() == 0) {
      const auto eoi = [](caf::typed_response_promise<table_slice>& rp,
                          std::deque<table_slice>& inputs) {
        if (rp.pending()) {
          TENZIR_ASSERT(inputs.empty());
          rp.deliver(table_slice{});
          return;
        }
        inputs.emplace_back();
      };
      eoi(to_then_branch_rp_, then_inputs_);
      if (else_branch_) {
        eoi(to_else_branch_rp_, else_inputs_);
      }
      return {};
    }
    auto end = int64_t{0};
    for (const auto& [predicate] :
         split_multi_series(eval(predicate_expr_, input, dh_))) {
      const auto start = std::exchange(end, end + predicate.length());
      TENZIR_ASSERT(end > start);
      const auto sliced_input = subslice(input, start, end);
      const auto typed_predicate = predicate.as<bool_type>();
      if (not typed_predicate) {
        diagnostic::warning("expected `bool`, but got `{}`",
                            predicate.type.kind())
          .primary(predicate_expr_)
          .emit(dh_);
        TENZIR_ASSERT(sliced_input.rows() > 0);
        push_else(sliced_input);
        continue;
      }
      if (typed_predicate->array->null_count() > 0) {
        diagnostic::warning("expected `bool`, but got `null`")
          .primary(predicate_expr_)
          .emit(dh_);
      }
      auto [lhs, rhs]
        = split_at_predicate(sliced_input, *typed_predicate->array);
      TENZIR_ASSERT(lhs.rows() + rhs.rows() == sliced_input.rows());
      if (lhs.rows() > 0) {
        push_then(std::move(lhs));
      }
      if (rhs.rows() > 0) {
        push_else(std::move(rhs));
      }
    }
    if (can_push_more()) {
      return {};
    }
    from_if_rp_ = self_->make_response_promise<void>();
    return from_if_rp_;
  }

  auto forward_to_branch(bool predicate) -> caf::result<table_slice> {
    auto& pull_rp = predicate ? to_then_branch_rp_ : to_else_branch_rp_;
    auto& inputs = predicate ? then_inputs_ : else_inputs_;
    TENZIR_ASSERT(not pull_rp.pending());
    if (inputs.empty()) {
      pull_rp = self_->make_response_promise<table_slice>();
      return pull_rp;
    }
    inputs = rebatch<std::deque>(std::move(inputs));
    auto input = std::move(inputs.front());
    inputs.pop_front();
    if (from_if_rp_.pending() and can_push_more()) {
      from_if_rp_.deliver();
    }
    return input;
  }

  auto handle_output(bool predicate, table_slice output) -> caf::result<void> {
    TENZIR_ASSERT(output.rows() > 0);
    if (to_endif_rp_.pending()) {
      TENZIR_ASSERT(outputs_.empty());
      to_endif_rp_.deliver(std::move(output));
      return {};
    }
    outputs_.push_back(std::move(output));
    if (outputs_.size() < max_queued + 1) {
      return {};
    }
    auto& push_rp = predicate ? from_then_branch_rp_ : from_else_branch_rp_;
    TENZIR_ASSERT(not push_rp.pending());
    push_rp = self_->make_response_promise<void>();
    return push_rp;
  }

  auto forward_to_parent_pipeline() -> caf::result<table_slice> {
    TENZIR_ASSERT(not to_endif_rp_.pending());
    if (outputs_.empty()) {
      to_endif_rp_ = self_->make_response_promise<table_slice>();
      return to_endif_rp_;
    }
    outputs_ = rebatch<std::deque>(std::move(outputs_));
    auto output = std::move(outputs_.front());
    outputs_.pop_front();
    if (outputs_.size() < max_queued) {
      if (from_then_branch_rp_.pending()) {
        from_then_branch_rp_.deliver();
      }
      if (from_else_branch_rp_.pending()) {
        from_else_branch_rp_.deliver();
      }
      if (from_if_rp_.pending() and can_push_more()) {
        from_if_rp_.deliver();
      }
    }
    return output;
  }

  auto handle_diagnostic(diagnostic diag) -> caf::result<void> {
    dh_.emit(std::move(diag));
    return {};
  }

  auto register_metrics(uint64_t nested_operator_index, uuid nested_metrics_id,
                        type schema) -> caf::result<void> {
    (void)nested_operator_index;
    return self_->mail(operator_index_, nested_metrics_id, std::move(schema))
      .delegate(metrics_receiver_);
  }

  auto handle_metrics(uint64_t nested_operator_index, uuid nested_metrics_id,
                      record metrics) -> caf::result<void> {
    (void)nested_operator_index;
    return self_->mail(operator_index_, nested_metrics_id, std::move(metrics))
      .delegate(metrics_receiver_);
  }

  branch_actor::pointer self_;

  std::string definition_;

  node_actor node_;
  shared_diagnostic_handler dh_;
  metrics_receiver_actor metrics_receiver_;
  bool is_hidden_;

  uint64_t operator_index_ = 0;
  std::string pipeline_id_;

  size_t running_branches_ = 0;
  ast::expression predicate_expr_;
  located<pipeline_executor_actor> then_branch_;
  std::optional<located<pipeline_executor_actor>> else_branch_;

  static constexpr size_t max_queued = 10;
  std::deque<table_slice> then_inputs_;
  std::deque<table_slice> else_inputs_;
  std::deque<table_slice> outputs_;

  caf::typed_response_promise<void> from_if_rp_;
  caf::typed_response_promise<table_slice> to_then_branch_rp_;
  caf::typed_response_promise<table_slice> to_else_branch_rp_;
  caf::typed_response_promise<void> from_then_branch_rp_;
  caf::typed_response_promise<void> from_else_branch_rp_;
  caf::typed_response_promise<table_slice> to_endif_rp_;
};

/// The left half of the `if` operator.
class internal_if_operator final : public crtp_operator<internal_if_operator> {
public:
  internal_if_operator() = default;

  internal_if_operator(uuid id) : id_{id} {
  }

  auto name() const -> std::string override {
    return "internal-if";
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    // Branching necessarily throws off the event order, so we can allow the
    // ested pipelines to do ordering optimizations.
    // TODO: We could push up a disjunction of the two filters.
    return optimize_result{std::nullopt, event_order::unordered, this->copy()};
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    co_yield {};
    auto branch = ctrl.self().system().registry().get<branch_actor>(
      fmt::format("tenzir.branch.{}.{}", id_, ctrl.run_id()));
    TENZIR_ASSERT(branch);
    ctrl.self().system().registry().erase(branch->id());
    for (auto events : input) {
      if (events.rows() == 0) {
        co_yield {};
        continue;
      }
      ctrl.self()
        .mail(atom::push_v, std::move(events))
        .request(branch, caf::infinite)
        .then(
          [&] {
            ctrl.set_waiting(false);
          },
          [&](caf::error err) {
            diagnostic::error(std::move(err))
              .note("failed to push events to branch")
              .emit(ctrl.diagnostics());
          });
      ctrl.set_waiting(true);
      co_yield {};
    }
    ctrl.self()
      .mail(atom::push_v, table_slice{})
      .request(branch, caf::infinite)
      .then(
        [&] {
          ctrl.set_waiting(false);
        },
        [&](caf::error err) {
          diagnostic::error(std::move(err))
            .note("failed to push sentinel to branch")
            .emit(ctrl.diagnostics());
        });
    ctrl.set_waiting(true);
    co_yield {};
  }

  friend auto inspect(auto& f, internal_if_operator& x) -> bool {
    return f.object(x).fields(f.field("id", x.id_));
  }

private:
  uuid id_ = {};
};

/// The right half of the `if` operator.
class internal_endif_operator final
  : public crtp_operator<internal_endif_operator> {
public:
  internal_endif_operator() = default;

  internal_endif_operator(uuid id, ast::expression predicate,
                          located<pipeline> then_pipe,
                          std::optional<located<pipeline>> else_pipe)
    : id_{id},
      predicate_{std::move(predicate)},
      then_pipe_{std::move(then_pipe)},
      else_pipe_{std::move(else_pipe)} {
  }

  auto name() const -> std::string override {
    return "internal-endif";
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    // Branching necessarily throws off the event order, so we can allow the
    // nested pipelines to do ordering optimizations.
    return optimize_result{std::nullopt, event_order::unordered, this->copy()};
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    // We spawn pipelines from right-to-left, so we can safely spawn this
    // operator in the internal-endif operator before and store it in the
    // registry as long as we do it before yielding for the first time.
    auto branch = scope_linked{ctrl.self().spawn<caf::linked>(
      caf::actor_from_state<class branch>, std::string{ctrl.definition()},
      ctrl.node(), ctrl.shared_diagnostics(), ctrl.metrics_receiver(),
      ctrl.is_hidden(), ctrl.operator_index(), std::string{ctrl.pipeline_id()},
      predicate_, then_pipe_, else_pipe_)};
    ctrl.self().system().registry().put(
      fmt::format("tenzir.branch.{}.{}", id_, ctrl.run_id()), branch.get());
    co_yield {};
    auto output = table_slice{};
    auto done = false;
    while (not done) {
      if (auto stub = input.next()) {
        // The actual input is coming from a side-channel, so we're only getting
        // stub batchs here.
        TENZIR_ASSERT(stub->rows() == 0);
      }
      ctrl.self()
        .mail(atom::pull_v)
        .request(branch.get(), caf::infinite)
        .then(
          [&](table_slice events) {
            ctrl.set_waiting(false);
            done = events.rows() == 0;
            output = std::move(events);
          },
          [&](caf::error err) {
            diagnostic::error(std::move(err))
              .note("failed to pull events from branch")
              .emit(ctrl.diagnostics());
          });
      ctrl.set_waiting(true);
      co_yield {};
      co_yield std::move(output);
    }
  }

  auto location() const -> operator_location override {
    // We pass in `ctrl.node()` to the branch actor, so if any of the nested
    // operators have a remote location, then we probably want to run the
    // `internal-endif` operator remotely as well.
    const auto requires_node = [](const auto& ops) {
      return std::ranges::find(ops, operator_location::remote,
                               &operator_base::location)
             != ops.end();
    };
    const auto should_be_remote
      = requires_node(then_pipe_.inner.operators())
        or (else_pipe_ and requires_node(else_pipe_->inner.operators()));
    return should_be_remote ? operator_location::remote
                            : operator_location::anywhere;
  }

  friend auto inspect(auto& f, internal_endif_operator& x) -> bool {
    return f.object(x).fields(f.field("id", x.id_),
                              f.field("predicate", x.predicate_),
                              f.field("then", x.then_pipe_),
                              f.field("else", x.else_pipe_));
  }

private:
  uuid id_ = {};
  ast::expression predicate_;
  located<pipeline> then_pipe_;
  std::optional<located<pipeline>> else_pipe_;
};

class if_plugin final : public virtual operator_factory_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.if";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    // NOTE: This operator is never called by the user directly. Its arguments
    // are dispatched through the pipeline compilation function. Hence, we can
    // safely assert that the we get two or three arguments:
    // 1. The predicate.
    // 2. The pipeline-expression for the if-branch.
    // 3. The pipeline-expression for the else-branch, iff the branch exists.
    TENZIR_ASSERT(inv.args.size() == 2 || inv.args.size() == 3);
    auto pred_expr = std::move(inv.args[0]);
    auto then_expr = as<ast::pipeline_expr>(std::move(*inv.args[1].kind));
    auto else_expr
      = inv.args.size() == 3
          ? std::optional{as<ast::pipeline_expr>(std::move(*inv.args[2].kind))}
          : std::optional<ast::pipeline_expr>{};
    // A few helper functions to avoid repetition.
    const auto make_pipeline
      = [&](ast::pipeline_expr&& expr) -> failure_or<located<pipeline>> {
      const auto source = expr.get_location();
      TRY(auto pipe, compile(std::move(expr).inner, ctx));
      return located{std::move(pipe), source};
    };
    const auto is_discard = [&](const ast::pipeline_expr& expr) {
      const auto& body = expr.inner.body;
      if (body.size() != 1) {
        return false;
      }
      const auto* invocation = try_as<ast::invocation>(body.front());
      if (not invocation) {
        return false;
      }
      return invocation->args.empty() and invocation->op.path.size() == 1
             and invocation->op.path.front().name == "discard";
    };
    const auto negate_pred = [](ast::expression pred) -> ast::expression {
      return ast::unary_expr{
        located{ast::unary_op::not_, location::unknown},
        std::move(pred),
      };
    };
    const auto make_if_pipeline = [&](ast::expression predicate,
                                      located<pipeline> then_pipe,
                                      std::optional<located<pipeline>> else_pipe
                                      = {}) {
      TENZIR_ASSERT((check(then_pipe.inner.infer_type(tag_v<table_slice>))
                       .is_any<table_slice, void>()),
                    "then-branch must return events or void after "
                    "optimizations");
      TENZIR_ASSERT(not else_pipe
                      or check(else_pipe->inner.infer_type(tag_v<table_slice>))
                           .is<table_slice>(),
                    "else-branch must not exist or return events after "
                    "optimizations");
      const auto id = uuid::random();
      auto if_pipe = std::make_unique<pipeline>();
      if_pipe->append(std::make_unique<internal_if_operator>(id));
      if_pipe->append(std::make_unique<internal_endif_operator>(
        id, std::move(predicate), std::move(then_pipe), std::move(else_pipe)));
      return if_pipe;
    };
    // Optimization: If the condition is a constant, we evaluate it and return
    // the appropriate branch only.
    if (auto pred = try_const_eval(pred_expr, ctx)) {
      const auto* typed_pred = try_as<bool>(*pred);
      if (not typed_pred) {
        diagnostic::error("expected `bool`, but got `{}`",
                          type::infer(*pred).value_or(type{}).kind())
          .primary(pred_expr)
          .emit(ctx);
        return failure::promise();
      }
      if (*typed_pred) {
        TRY(auto then_pipe, make_pipeline(std::move(then_expr)));
        return std::make_unique<pipeline>(std::move(then_pipe.inner));
      }
      if (else_expr) {
        TRY(auto else_pipe, make_pipeline(std::move(*else_expr)));
        return std::make_unique<pipeline>(std::move(else_pipe.inner));
      }
      return std::make_unique<pipeline>();
    }
    // Optimization: If either of the branches is just `discard`, then we can
    // flatten the pipeline with `where`. We empirically noticed that users
    // wrote such pipelines frequently, and the flattened pipeline is a lot more
    // efficient due to predicate pushdown we have implemented for `where`.
    const auto then_is_discard = is_discard(then_expr);
    const auto else_is_discard = else_expr and is_discard(*else_expr);
    if (then_is_discard or else_is_discard) {
      const auto* where_op
        = plugins::find<operator_factory_plugin>("tql2.where");
      TENZIR_ASSERT(where_op);
      if (then_is_discard) {
        TRY(auto where_pipe,
            where_op->make({inv.self, {negate_pred(std::move(pred_expr))}},
                           ctx));
        if (else_expr) {
          TRY(auto else_pipe, make_pipeline(std::move(*else_expr)));
          else_pipe.inner.prepend(std::move(where_pipe));
          return std::make_unique<pipeline>(std::move(else_pipe.inner));
        }
        return where_pipe;
      }
      TENZIR_ASSERT(else_expr);
      TRY(auto where_pipe,
          where_op->make({inv.self, {std::move(pred_expr)}}, ctx));
      TRY(auto then_pipe, make_pipeline(std::move(then_expr)));
      then_pipe.inner.prepend(std::move(where_pipe));
      return std::make_unique<pipeline>(std::move(then_pipe.inner));
    }
    // At this point, we can always compile the pipelines for both branches.
    TRY(auto then_pipe, make_pipeline(std::move(then_expr)));
    auto else_pipe = std::optional<located<pipeline>>{};
    if (else_expr) {
      TRY(else_pipe, make_pipeline(std::move(*else_expr)));
    }
    // Optimization: If at least one branch contains a sink, we can move the
    // other branch to after the pipeline. This makes it so that we only need to
    // implement the `if` operator as a transformation, which reduces the
    // complexity of its implementation a lot.
    const auto then_type = then_pipe.inner.infer_type(tag_v<table_slice>);
    if (not then_type) {
      diagnostic::error(then_type.error()).primary(then_expr).emit(ctx);
      return failure::promise();
    }
    if (then_type->is<chunk_ptr>()) {
      diagnostic::error("branches must return `void` or `events`")
        .primary(then_expr)
        .emit(ctx);
      return failure::promise();
    }
    if (else_pipe) {
      const auto else_type = else_pipe->inner.infer_type(tag_v<table_slice>);
      if (not else_type) {
        diagnostic::error(else_type.error()).primary(*else_expr).emit(ctx);
        return failure::promise();
      }
      if (else_type->is<chunk_ptr>()) {
        diagnostic::error("branches must return `void` or `events`")
          .primary(*else_expr)
          .emit(ctx);
        return failure::promise();
      }
      if (then_type->is<void>()) {
        else_pipe->inner.prepend(
          make_if_pipeline(std::move(pred_expr), std::move(then_pipe)));
        return std::make_unique<pipeline>(std::move(else_pipe->inner));
      }
      if (else_type->is<void>()) {
        then_pipe.inner.prepend(make_if_pipeline(
          negate_pred(std::move(pred_expr)), std::move(*else_pipe)));
        return std::make_unique<pipeline>(std::move(then_pipe.inner));
      }
    }
    return make_if_pipeline(std::move(pred_expr), std::move(then_pipe),
                            std::move(else_pipe));
  }
};

using branch_source_plugin = operator_inspection_plugin<branch_source_operator>;
using branch_sink_plugin = operator_inspection_plugin<branch_sink_operator>;
using internal_if_plugin = operator_inspection_plugin<internal_if_operator>;
using internal_endif_plugin
  = operator_inspection_plugin<internal_endif_operator>;

} // namespace

} // namespace tenzir::plugins::if_

TENZIR_REGISTER_PLUGIN(tenzir::plugins::if_::if_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::if_::branch_source_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::if_::branch_sink_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::if_::internal_if_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::if_::internal_endif_plugin)
