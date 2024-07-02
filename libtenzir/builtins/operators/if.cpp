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
                  bool target) -> generator<table_slice> {
  TENZIR_ASSERT(slice.rows() == detail::narrow<uint64_t>(array.length()));
  auto length = array.length();
  auto current_value = array.Value(0) == target;
  auto current_begin = int64_t{0};
  // Add `false` at index `length` to flush.
  for (auto i = int64_t{1}; i < length + 1; ++i) {
    // TODO: Null?
    auto next = i != length && array.Value(i) == target;
    if (current_value == next) {
      continue;
    }
    if (current_value) {
      co_yield subslice(slice, current_begin, i);
    }
    current_value = next;
    current_begin = i;
  }
}

auto mask_slice(const table_slice& slice, const arrow::BooleanArray& array,
                bool target) -> table_slice {
  return concatenate(collect(array_select(slice, array, target)));
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
      auto else_gen_ptr = std::get_if<generator<table_slice>>(&*else_result);
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
      auto mask = eval(condition_, slice, ctrl.diagnostics());
      // TODO: Null array should also work.
      auto array = caf::get_if<arrow::BooleanArray>(&*mask.array);
      if (not array) {
        diagnostic::warning("condition must be `bool`, not `{}`",
                            mask.type.kind())
          .primary(condition_)
          .emit(ctrl.diagnostics());
        co_yield {};
        continue;
      }
      TENZIR_ASSERT(array); // TODO
      then_input = mask_slice(slice, *array, true);
      auto yielded = false;
      while (then_input->rows() > 0) {
        if (auto next = then_gen.next()) {
          if (auto output = std::get_if<table_slice>(&*next)) {
            co_yield std::move(*output);
            yielded = true;
          }
          // TODO: Other outputs are just dropped.
        } else {
          break;
        }
      }
      else_input = mask_slice(slice, *array, false);
      while (else_input->rows() > 0) {
        if (auto next = else_gen.next()) {
          co_yield std::move(*next);
          yielded = true;
        } else {
          break;
        }
      }
      if (not yielded) {
        co_yield {};
      }
    }
    then_input.reset();
    else_input.reset();
    while (auto next = then_gen.next()) {
      if (auto output = std::get_if<table_slice>(&*next)) {
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
  operator_location location_;
};

class plugin final : public virtual operator_plugin2<if_operator> {
public:
  auto make(invocation inv, session ctx) const -> operator_ptr override {
    // TODO: Very hacky!
    TENZIR_ASSERT(inv.args.size() == 2 || inv.args.size() == 3);
    auto condition = std::move(inv.args[0]);
    auto then = prepare_pipeline(
      std::get<ast::pipeline_expr>(std::move(*inv.args[1].kind)).inner, ctx);
    auto else_ = std::optional<pipeline>{};
    if (inv.args.size() == 3) {
      else_ = prepare_pipeline(
        std::get<ast::pipeline_expr>(std::move(*inv.args[2].kind)).inner, ctx);
    }
    // TODO: Improve this code (or better: get rid of this limitation).
    auto location = operator_location::anywhere;
    for (auto& op : then.operators()) {
      auto op_location = op->location();
      if (location == operator_location::anywhere) {
        location = op_location;
      } else if (op_location != operator_location::anywhere
                 && location != op_location) {
        diagnostic::error("operator location conflict between local and remote")
          .primary(inv.self)
          .emit(ctx);
      }
    }
    if (else_) {
      for (auto& op : else_->operators()) {
        auto op_location = op->location();
        if (location == operator_location::anywhere) {
          location = op_location;
        } else if (op_location != operator_location::anywhere
                   && location != op_location) {
          diagnostic::error(
            "operator location conflict between local and remote")
            .primary(inv.self)
            .emit(ctx);
        }
      }
    }
    return std::make_unique<if_operator>(std::move(condition), std::move(then),
                                         std::move(else_), location);
  }
};

} // namespace

} // namespace tenzir::plugins::if_

TENZIR_REGISTER_PLUGIN(tenzir::plugins::if_::plugin)
