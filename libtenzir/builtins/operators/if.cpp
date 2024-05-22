//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/collect.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/exec.hpp"

#include <tenzir/operator_control_plane.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::if_ {

namespace {

using namespace tql2;

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
      // emit
      co_yield subslice(slice, current_begin, i);
    } else {
      // discard
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
              std::optional<pipeline> else_)
    : condition_{std::move(condition)},
      then_{std::move(then)},
      else_{std::move(else_)} {
  }

  auto name() const -> std::string override {
    return "tql2.if";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    // TODO: All of this is quite bad!
    // We use empty optional to signal finish!
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
    auto then_gen = std::get<generator<table_slice>>(std::move(*then_result));
    auto else_gen = std::optional<generator<table_slice>>{};
    if (else_) {
      auto else_result = else_->instantiate(make_input(else_input), ctrl);
      if (not else_result) {
        diagnostic::error(else_result.error()).emit(ctrl.diagnostics());
        co_return;
      }
      else_gen = std::get<generator<table_slice>>(std::move(*else_result));
    }
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        // TODO: Probably need to advance here as well.
        co_yield {};
        continue;
      }
      auto mask = eval(condition_, slice, ctrl.diagnostics());
      TENZIR_WARN("mask = {}", mask.array->ToString());
      auto array = caf::get_if<arrow::BooleanArray>(&*mask.array);
      if (not array) {
        diagnostic::warning("condition must be `bool`, not `{}`",
                            mask.type.kind())
          .primary(condition_.get_location())
          .emit(ctrl.diagnostics());
        co_yield {};
        continue;
      }
      TENZIR_ASSERT(array); // TODO
      then_input = mask_slice(slice, *array, true);
      while (then_gen.unsafe_current() != then_gen.end()
             && then_input->rows() > 0) {
        ++then_gen.unsafe_current();
        co_yield *then_gen.unsafe_current();
      }
      else_input = mask_slice(slice, *array, false);
      if (else_gen) {
        while (else_gen->unsafe_current() != else_gen->end()
               && else_input->rows() > 0) {
          ++else_gen->unsafe_current();
          co_yield *else_gen->unsafe_current();
        }
      } else {
        co_yield std::move(*else_input);
      }
    }
    then_input.reset();
    else_input.reset();
    while (then_gen.unsafe_current() != then_gen.end()) {
      ++then_gen.unsafe_current();
      co_yield *then_gen.unsafe_current();
    }
    if (else_gen) {
      while (else_gen->unsafe_current() != else_gen->end()) {
        ++else_gen->unsafe_current();
        co_yield *else_gen->unsafe_current();
      }
    }
  }

  friend auto inspect(auto& f, if_operator& x) -> bool {
    return f.object(x).fields(f.field("condition", x.condition_),
                              f.field("then", x.then_),
                              f.field("else", x.else_));
  }

private:
  ast::expression condition_;
  pipeline then_;
  std::optional<pipeline> else_;
};

class plugin final : public virtual tql2::operator_plugin<if_operator> {
public:
  auto make_operator(invocation inv, session ctx) const
    -> operator_ptr override {
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
    return std::make_unique<if_operator>(std::move(condition), std::move(then),
                                         std::move(else_));
  }
};

} // namespace

} // namespace tenzir::plugins::if_

TENZIR_REGISTER_PLUGIN(tenzir::plugins::if_::plugin)
