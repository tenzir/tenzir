//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ir_set.hpp"

#include "tenzir/async.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/option.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/rebatch.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/set.hpp"

#include <algorithm>
#include <ranges>

namespace tenzir {

namespace {

class Set final : public Operator<table_slice, table_slice> {
public:
  Set(std::vector<ast::assignment> assignments, event_order order)
    : assignments_{std::move(assignments)}, order_{order} {
    for (auto& assignment : assignments_) {
      auto [pruned_assignment, moved_fields]
        = resolve_move_keyword(std::move(assignment));
      assignment = std::move(pruned_assignment);
      std::ranges::move(moved_fields, std::back_inserter(moved_fields_));
    }
    // Compilation rejects assignment targets that do not describe a selector,
    // so the conversion cannot fail here anymore.
    lefts_.reserve(assignments_.size());
    for (const auto& assignment : assignments_) {
      auto left = ast::selector::try_from(assignment.left);
      TENZIR_ASSERT(left);
      lefts_.push_back(std::move(*left));
    }
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> {
    auto slice = std::move(input);
    // The right-hand side is always evaluated with the original input, because
    // side-effects from preceding assignments shall not be reflected when
    // calculating the value of the left-hand side.
    auto values = std::vector<multi_series>{};
    for (const auto& assignment : assignments_) {
      values.push_back(eval(assignment.right, slice, ctx));
    }
    slice = drop(slice, moved_fields_, ctx, false);
    // After we know all the multi series values on the right, we can split the
    // input table slice and perform the actual assignment.
    auto begin = int64_t{0};
    auto results = std::vector<table_slice>{};
    for (auto values_slice : split_multi_series(values)) {
      TENZIR_ASSERT(not values_slice.empty());
      auto end = begin + values_slice[0].length();
      // We could still perform further splits if metadata is assigned.
      auto state = std::vector<table_slice>{};
      state.push_back(subslice(slice, begin, end));
      begin = end;
      auto new_state = std::vector<table_slice>{};
      for (auto [left, value] : std::views::zip(lefts_, values_slice)) {
        auto begin = int64_t{0};
        for (auto& entry : state) {
          auto entry_rows = detail::narrow<int64_t>(entry.rows());
          auto assigned
            = assign(left, value.slice(begin, entry_rows), entry, ctx);
          begin += entry_rows;
          new_state.insert(new_state.end(),
                           std::move_iterator{assigned.begin()},
                           std::move_iterator{assigned.end()});
        }
        std::swap(state, new_state);
        new_state.clear();
      }
      std::ranges::move(state, std::back_inserter(results));
    }
    // TODO: Consider adding a property to function plugins that let's them
    // indicate whether they want their outputs to be strictly ordered. If any
    // of the called functions has this requirement, then we should not be
    // making this optimization. This will become relevant in the future once we
    // allow functions to be stateful.
    if (order_ != event_order::ordered) {
      std::ranges::stable_sort(results, std::ranges::less{},
                               &table_slice::schema);
    }
    for (auto& result : rebatch(std::move(results))) {
      co_await push(std::move(result));
    }
  }

private:
  std::vector<ast::assignment> assignments_;
  std::vector<ast::selector> lefts_;
  event_order order_{};
  std::vector<ast::field_path> moved_fields_;
};

} // namespace

ir::SetIr::SetIr() : order_{event_order::ordered} {
}

ir::SetIr::SetIr(std::vector<ast::assignment> assignments)
  : assignments_{std::move(assignments)}, order_{event_order::ordered} {
}

auto ir::SetIr::name() const -> std::string {
  return "SetIr";
}

auto ir::SetIr::copy() const -> Box<ir::Operator> {
  return SetIr{*this};
}

auto ir::SetIr::move() && -> Box<ir::Operator> {
  return SetIr{std::move(*this)};
}

auto ir::SetIr::substitute(substitute_ctx ctx, bool instantiate)
  -> failure_or<void> {
  (void)instantiate;
  for (auto& x : assignments_) {
    // The left-hand side is resolved to a selector at compile time and cannot
    // contain `$`-variables. UDO parameters are resolved even before that.
    TRY(x.right.substitute(ctx));
  }
  return {};
}

auto ir::SetIr::spawn(element_type_tag input) const -> AnyOperator {
  TENZIR_ASSERT(input.is<table_slice>());
  return Set{assignments_, order_}.with_name("set");
}

namespace {

auto touched_fields_for_set(const std::vector<ast::assignment>& assignments)
  -> Option<std::vector<ast::field_path>> {
  auto result = std::vector<ast::field_path>{};
  for (const auto& assignment : assignments) {
    auto [resolved, moved_fields] = resolve_move_keyword(assignment);
    std::ranges::move(moved_fields, std::back_inserter(result));
    auto left = ast::selector::try_from(resolved.left);
    const auto* path = left ? try_as<ast::field_path>(&*left) : nullptr;
    if (path == nullptr or path->path().empty()) {
      return None{};
    }
    result.push_back(*path);
  }
  return result;
}

} // namespace

auto ir::SetIr::optimize(ir::optimize_filter filter,
                         event_order order) && -> ir::optimize_result {
  order_ = weaker_event_order(order_, order);
  auto touched_paths = touched_fields_for_set(assignments_);
  auto split = touched_paths
                 ? ir::split_filter_by_dependents(
                     std::move(filter),
                     ast::ExprRefs{.field_paths = std::move(*touched_paths)})
                 : ir::split_filter_result{{}, std::move(filter)};
  auto [filter_upstream, filter_self] = std::move(split);
  auto ops = std::vector<Box<ir::Operator>>{};
  ops.reserve(1 + filter_self.size());
  ops.emplace_back(ir::SetIr{std::move(*this)});
  for (auto& expr : filter_self) {
    ops.push_back(make_where_ir(expr));
  }
  return {
    std::move(filter_upstream),
    order_,
    ir::pipeline{{}, std::move(ops)},
  };
}

auto ir::SetIr::infer_type(element_type_tag input, diagnostic_handler& dh) const
  -> failure_or<element_type_tag> {
  if (input.is_not<table_slice>()) {
    diagnostic::error("set operator expected events").emit(dh);
    return failure::promise();
  }
  return input;
}

namespace ir {

template <class Inspector>
auto inspect(Inspector& f, SetIr& x) -> bool {
  return f.object(x).fields(f.field("assignments", x.assignments_),
                            f.field("order", x.order_));
}

} // namespace ir

auto make_set_ir(ast::assignment x) -> Box<ir::Operator> {
  auto assignments = std::vector<ast::assignment>{};
  assignments.push_back(std::move(x));
  return ir::SetIr{std::move(assignments)};
}

auto make_set_ir(std::vector<ast::assignment> assignments)
  -> Box<ir::Operator> {
  return ir::SetIr{std::move(assignments)};
}

} // namespace tenzir

TENZIR_REGISTER_PLUGIN(
  tenzir::inspection_plugin<tenzir::ir::Operator, tenzir::ir::SetIr>)
