//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/collect.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/exec.hpp"

#include <tenzir/operator_control_plane.hpp>
#include <tenzir/tql2/plugin.hpp>

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

class if_operator final : public crtp_operator<if_operator> {
public:
  if_operator() = default;

  if_operator(ast::expression condition, pipeline then,
              std::optional<pipeline> else_, operator_location location)
    : condition_{std::move(condition)},
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

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    // TODO: All of this is quite bad! Needs to be rewritten.
    auto transpose_gen = [](operator_output gen)
      -> generator<variant<table_slice, chunk_ptr, std::monostate>> {
      return std::visit(
        []<class T>(generator<T> gen)
          -> generator<variant<table_slice, chunk_ptr, std::monostate>> {
          for (auto&& x : gen) {
            co_yield std::move(x);
          }
        },
        std::move(gen));
    };
    // We use empty optional to signal exhaustion.
    auto then_input = std::optional<table_slice>{table_slice{}};
    auto else_input = std::optional<table_slice>{table_slice{}};
    auto make_input
      = [](std::optional<table_slice>& input) -> generator<table_slice> {
      while (input.has_value()) {
        co_yield std::exchange(*input, table_slice{});
      }
    };
    auto then_result = then_.instantiate(make_input(then_input), ctrl);
    if (not then_result) {
      diagnostic::error(then_result.error()).emit(ctrl.diagnostics());
      co_return;
    }
    auto then_gen = transpose_gen(std::move(*then_result));
    auto else_gen = generator<table_slice>{};
    if (else_) {
      auto else_result = else_->instantiate(make_input(else_input), ctrl);
      if (not else_result) {
        diagnostic::error(else_result.error()).emit(ctrl.diagnostics());
        co_return;
      }
      // TODO: Do not force `else` output to be table slice.
      auto* else_gen_ptr = std::get_if<generator<table_slice>>(&*else_result);
      if (not else_gen_ptr) {
        // TODO: Wrong location. Also, we want to lift this limitation.
        diagnostic::error("expected `else` branch to yield events")
          .primary(condition_)
          .note("this limitation will be removed eventually")
          .emit(ctrl.diagnostics());
        co_return;
      }
      else_gen = std::move(*else_gen_ptr);
    } else {
      else_gen = make_input(else_input);
    }
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        // TODO: Probably need to advance here as well.
        co_yield {};
        continue;
      }
      auto yielded = false;
      auto masks = eval(condition_, slice, ctrl.diagnostics());
      auto offset = int64_t{0};
      for (auto& mask : masks.parts()) {
        // TODO: Null array should also work.
        auto* array = try_as<arrow::BooleanArray>(&*mask.array);
        if (not array) {
          diagnostic::warning("condition must be `bool`, not `{}`",
                              mask.type.kind())
            .primary(condition_)
            .emit(ctrl.diagnostics());
          co_yield {};
          offset += mask.array->length();
          continue;
        }
        TENZIR_ASSERT(array);
        then_input = mask_slice(slice, *array, true, offset);
        while (then_input->rows() > 0) {
          if (auto next = then_gen.next()) {
            if (auto* output = std::get_if<table_slice>(&*next)) {
              co_yield std::move(*output);
              yielded = true;
            }
            // TODO: Other outputs are just dropped.
          } else {
            break;
          }
        }
        else_input = mask_slice(slice, *array, false, offset);
        while (else_input->rows() > 0) {
          if (auto next = else_gen.next()) {
            co_yield std::move(*next);
            yielded = true;
          } else {
            break;
          }
        }
        offset += array->length();
      }
      if (not yielded) {
        co_yield {};
      }
    }
    then_input.reset();
    else_input.reset();
    while (auto next = then_gen.next()) {
      if (auto* output = std::get_if<table_slice>(&*next)) {
        co_yield std::move(*output);
      }
    }
    while (auto next = else_gen.next()) {
      co_yield std::move(*next);
    }
  }

  friend auto inspect(auto& f, if_operator& x) -> bool {
    return f.object(x).fields(f.field("condition", x.condition_),
                              f.field("then", x.then_),
                              f.field("else", x.else_),
                              f.field("location", x.location_));
  }

private:
  ast::expression condition_;
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

} // namespace

} // namespace tenzir::plugins::if_

TENZIR_REGISTER_PLUGIN(tenzir::plugins::if_::plugin)
