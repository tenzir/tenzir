//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/collect.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/pipeline_executor.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/exec.hpp"

#include <tenzir/operator_control_plane.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <caf/exit_reason.hpp>

namespace tenzir::plugins::if_ {

namespace {

auto array_select(const table_slice& slice, const arrow::BooleanArray& array,
                  bool target, int64_t offset) -> generator<table_slice> {
  TENZIR_ASSERT(array.length() > 0);
  auto length = array.length();
  auto current_value = array.Value(0) == target;
  auto current_begin = int64_t{0};
  // Add `false` at index `length` to flush.
  for (auto i = current_begin + 1; i < length + 1; ++i) {
    // TODO: Null?
    auto next = i != length && array.IsValid(i) && array.Value(i) == target;
    if (current_value == next) {
      continue;
    }
    if (current_value) {
      co_yield subslice(slice, offset + current_begin, offset + i);
    }
    current_value = next;
    current_begin = i;
  }
}

auto mask_slice(const table_slice& slice, const arrow::BooleanArray& array,
                bool target, int64_t offset) -> table_slice {
  return concatenate(collect(array_select(slice, array, target, offset)));
}

TENZIR_ENUM(bridge_location, left, right);

template <bridge_location Location, class Element>
class if_bridge_operator final
  : public crtp_operator<if_bridge_operator<Location, Element>> {
public:
  if_bridge_operator() = default;

  auto name() const -> std::string override {
    return fmt::format("internal-if-bridge-{}-{}", Location,
                       operator_type_name(tag_v<Element>));
  }

  auto operator()(operator_control_plane& ctrl) const -> generator<Element>
    requires(Location == bridge_location::left)
  {
    TENZIR_UNUSED(ctrl);
    TENZIR_TODO();
  }

  auto operator()(generator<Element> input, operator_control_plane& ctrl) const
    -> generator<std::monostate>
    requires(Location == bridge_location::right)
  {
    TENZIR_UNUSED(input, ctrl);
    TENZIR_TODO();
  }

  auto location() const -> operator_location override {
    // Local here means local to the `if` operator's nested executor.
    return operator_location::local;
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    // Branching necessarily throws off the event order, so we can allow the
    // nested pipelines to do ordering optimizations.
    return optimize_result{std::nullopt, event_order::unordered, this->copy()};
  }

  friend auto inspect(auto& f, if_bridge_operator& x) -> bool {
    return f.object(x).fields();
  }
};

class if_operator final : public operator_base {
public:
  if_operator() = default;

  if_operator(ast::expression condition, pipeline then,
              std::optional<pipeline> else_, operator_location location)
    : predicate_{std::move(condition)},
      then_{std::move(then)},
      else_{std::move(else_)},
      location_{location} {
  }

  auto name() const -> std::string override {
    return "tql2.if";
  }

  auto location() const -> operator_location override {
    return location_;
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    // Branching necessarily throws off the event order, so we can allow the
    // nested pipelines to do ordering optimizations.
    // TODO: We could push up a disjunction of the two filters.
    return optimize_result{std::nullopt, event_order::unordered, this->copy()};
  }

  auto infer_type_impl(operator_type input) const
    -> caf::expected<operator_type> override {
    if (input.is_not<table_slice>()) {
      return diagnostic::error("`if` does not accept `{}` as input",
                               operator_type_name(input))
        .to_error();
    }
    TRY(auto then_type, then_.infer_type(input));
    if (not else_) {
      if (then_type.is<void>()) {
        return tag_v<table_slice>;
      }
      return then_type;
    }
    TRY(auto else_type, else_->infer_type(input));
    if (then_type.is<void>()) {
      return else_type;
    }
    if (else_type.is<void>()) {
      return then_type;
    }
    if (then_type == else_type) {
      return then_type;
    }
    return diagnostic::error(
             "`if … else` requires its branches to have matching return types")
      .note("if-branch returns `{}`", operator_type_name(then_type))
      .note("else-branch returns `{}`", operator_type_name(else_type))
      .to_error();
  }

  auto instantiate(operator_input input, operator_control_plane& ctrl) const
    -> caf::expected<operator_output> override {
    // We verified already that the input must be events.
    auto typed_input = as<generator<table_slice>>(std::move(input));
    // TODO: Spawn an actor that handles metrics from the nested pipelines.
    const auto then_type = check(then_.infer_type(tag_v<table_slice>));
    if (not else_) {
      if (then_type.is<void>()) {
        // Pass matching events into the then-pipeline, and yield other events
        // immediately.
        return [](generator<table_slice> input, operator_control_plane& ctrl,
                  pipeline pipe) -> generator<table_slice> {
          pipe.prepend(
            std::make_unique<
              if_bridge_operator<bridge_location::left, table_slice>>());
          auto exec = ctrl.self().spawn(pipeline_executor, std::move(pipe),
                                        receiver_actor<diagnostic>{},
                                        metrics_receiver_actor{}, ctrl.node(),
                                        ctrl.has_terminal(), ctrl.is_hidden());
          ctrl.self().monitor(exec, [&](const caf::error& err) {
            // FIXME: do we care about this? probably already emitted it
            TENZIR_UNUSED(err);
            ctrl.set_waiting(false);
          });
          for (auto events : input) {
            // TODO: filter events
          }
          // Lastly, wait for the nested pipeline to finish.
          ctrl.self().send_exit(exec, caf::exit_reason::user_shutdown);
          ctrl.set_waiting(true);
          co_yield {};
        }(std::move(typed_input), ctrl, then_);
      }
      // Pass matching events into the then-pipeline, yielding its results, and
      // yield other events immediately.
      TENZIR_TODO();
    }
    const auto else_type = check(else_->infer_type(tag_v<table_slice>));
    if (then_type.is<void>() and else_type.is<void>()) {
      // Pass matching events into the then-pipeline, and other events into the
      // else-pipeline.
      TENZIR_TODO();
    }
    if (then_type.is<void>()) {
      // Pass matching events into the then-pipeline, and other events into the
      // else-pipeline, yielding its results.
      TENZIR_TODO();
    }
    if (else_type.is<void>()) {
      // Pass matching events into the then-pipeline, yielding its results, and
      // other events into the else-pipeline.
      TENZIR_TODO();
    }
    TENZIR_ASSERT(then_type == else_type);
    // Pass matching events into the then-pipeline, yielding its results, and
    // other events into the else-pipeline, yielding its results as well.
    TENZIR_TODO();
  }

  friend auto inspect(auto& f, if_operator& x) -> bool {
    return f.object(x).fields(f.field("prediacte", x.predicate_),
                              f.field("then", x.then_),
                              f.field("else", x.else_),
                              f.field("location", x.location_));
  }

private:
  ast::expression predicate_;
  pipeline then_;
  std::optional<pipeline> else_;
  operator_location location_ = {};
};

class plugin final : public virtual operator_plugin2<if_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    // TODO: This operator is never called by the user directly. It's arguments
    // are dispatched through the pipeline compilation function. But we still
    // need to use the plugin interface to implement it.
    TENZIR_ASSERT(inv.args.size() == 2 || inv.args.size() == 3);
    const auto has_else = inv.args.size() == 3;
    auto pred_expr = std::move(inv.args[0]);
    const auto make_pipeline = [&](size_t idx) {
      TENZIR_ASSERT(idx < inv.args.size());
      return compile(
        as<ast::pipeline_expr>(std::move(*inv.args[idx].kind)).inner, ctx);
    };
    const auto is_discard = [&](size_t idx) {
      TENZIR_ASSERT(idx < inv.args.size());
      const auto& body = as<ast::pipeline_expr>(*inv.args[idx].kind).inner.body;
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
        TRY(auto then, make_pipeline(1));
        return std::make_unique<pipeline>(std::move(then));
      }
      if (has_else) {
        TRY(auto else_, make_pipeline(2));
        return std::make_unique<pipeline>(std::move(else_));
      }
      return std::make_unique<pipeline>();
    }
    // Optimization: If either of the branches is just `discard`, then we can
    // flatten the pipeline with `where`. We empirically noticed that users
    // wrote such pipelines frequently, and the flattened pipeline is a lot more
    // efficient due to predicate pushdown we have implemented for `where`.
    const auto then_is_discard = is_discard(1);
    const auto else_is_discard = has_else and is_discard(2);
    if (then_is_discard or else_is_discard) {
      const auto* where_op
        = plugins::find<operator_factory_plugin>("tql2.where");
      TENZIR_ASSERT(where_op);
      if (then_is_discard) {
        pred_expr = ast::unary_expr{
          located{ast::unary_op::not_, location::unknown},
          std::move(pred_expr),
        };
        TRY(auto where,
            where_op->make({.self = inv.self, .args = {std::move(pred_expr)}},
                           ctx));
        if (has_else) {
          TRY(auto result, make_pipeline(2));
          result.prepend(std::move(where));
          return std::make_unique<pipeline>(std::move(result));
        }
        return where;
      }
      TENZIR_ASSERT(has_else);
      TRY(auto where,
          where_op->make({.self = inv.self, .args = {std::move(pred_expr)}},
                         ctx));
      TRY(auto result, make_pipeline(1));
      result.prepend(std::move(where));
      return std::make_unique<pipeline>(std::move(result));
    }
    auto then = make_pipeline(1);
    auto else_ = failure_or<pipeline>{};
    if (has_else) {
      else_ = make_pipeline(2);
    }
    // TODO: Improve this code (or better: get rid of this limitation).
    auto location = operator_location::anywhere;
    if (then) {
      for (const auto& op : then->operators()) {
        auto op_location = op->location();
        if (location == operator_location::anywhere) {
          location = op_location;
        } else if (op_location != operator_location::anywhere
                   && location != op_location) {
          diagnostic::error(
            "operator location conflict between local and remote")
            .primary(inv.self)
            .emit(ctx);
          then = failure::promise();
          break;
        }
      }
    }
    if (else_) {
      for (const auto& op : else_->operators()) {
        auto op_location = op->location();
        if (location == operator_location::anywhere) {
          location = op_location;
        } else if (op_location != operator_location::anywhere
                   && location != op_location) {
          diagnostic::error(
            "operator location conflict between local and remote")
            .primary(inv.self)
            .emit(ctx);
          else_ = failure::promise();
          break;
        }
      }
    }
    TRY(then);
    TRY(else_);
    return std::make_unique<if_operator>(std::move(pred_expr), std::move(*then),
                                         std::move(*else_), location);
  }
};

using left_bridge_events_plugin = operator_inspection_plugin<
  if_bridge_operator<bridge_location::left, table_slice>>;
using right_bridge_bytes_plugin = operator_inspection_plugin<
  if_bridge_operator<bridge_location::right, chunk_ptr>>;
using right_bridge_events_plugin = operator_inspection_plugin<
  if_bridge_operator<bridge_location::right, table_slice>>;

} // namespace

} // namespace tenzir::plugins::if_

TENZIR_REGISTER_PLUGIN(tenzir::plugins::if_::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::if_::left_bridge_events_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::if_::right_bridge_bytes_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::if_::right_bridge_events_plugin)
