//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/detail/narrow.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/rebatch.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>

#include <ranges>

namespace tenzir::plugins::set_select {

namespace {

struct SelectArgs {
  std::vector<ast::expression> fields;
};

auto make_select_assignments(std::vector<ast::expression> args,
                             diagnostic_handler* dh)
  -> std::optional<std::vector<ast::assignment>> {
  auto assignments = std::vector<ast::assignment>{};
  assignments.reserve(1 + args.size());
  assignments.emplace_back(
    ast::field_path::try_from(ast::this_{}).value(), location::unknown,
    ast::record{location::unknown, {}, location::unknown});
  for (auto& arg : args) {
    if (auto* assignment = std::get_if<ast::assignment>(&*arg.kind)) {
      auto* selector = std::get_if<ast::field_path>(&assignment->left);
      if (not selector) {
        if (dh) {
          diagnostic::error("expected selector")
            .primary(assignment->left)
            .emit(*dh);
        }
        return std::nullopt;
      }
      assignments.push_back(std::move(*assignment));
      continue;
    }
    auto original_arg = arg;
    auto selector = ast::field_path::try_from(std::move(arg));
    if (not selector) {
      if (dh) {
        diagnostic::error("expected selector").primary(original_arg).emit(*dh);
      }
      return std::nullopt;
    }
    auto rhs = selector->inner();
    assignments.emplace_back(std::move(*selector), location::unknown,
                             std::move(rhs));
  }
  return assignments;
}

class SelectSet final : public Operator<table_slice, table_slice> {
public:
  explicit SelectSet(SelectArgs args) {
    auto assignments = make_select_assignments(std::move(args.fields), nullptr);
    TENZIR_ASSERT(assignments);
    assignments_ = std::move(*assignments);
    for (auto& assignment : assignments_) {
      auto [pruned_assignment, moved_fields]
        = resolve_move_keyword(std::move(assignment));
      assignment = std::move(pruned_assignment);
      std::ranges::move(moved_fields, std::back_inserter(moved_fields_));
    }
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto slice = std::move(input);
    // The right-hand side is always evaluated with the original input, because
    // side-effects from preceding assignments shall not be reflected when
    // calculating the value of the left-hand side.
    auto values = std::vector<multi_series>{};
    for (auto const& assignment : assignments_) {
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
      for (auto [assignment, value] :
           std::views::zip(assignments_, values_slice)) {
        auto offset = int64_t{0};
        for (auto& entry : state) {
          auto entry_rows = detail::narrow<int64_t>(entry.rows());
          auto assigned = assign(assignment.left,
                                 value.slice(offset, entry_rows), entry, ctx);
          offset += entry_rows;
          new_state.insert(new_state.end(),
                           std::move_iterator{assigned.begin()},
                           std::move_iterator{assigned.end()});
        }
        std::swap(state, new_state);
        new_state.clear();
      }
      std::ranges::move(state, std::back_inserter(results));
    }
    for (auto& result : rebatch(std::move(results))) {
      co_await push(std::move(result));
    }
  }

private:
  std::vector<ast::assignment> assignments_;
  std::vector<ast::field_path> moved_fields_;
};

class set final : public operator_plugin2<set_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto usage = "set <path>=<expr>...";
    auto docs = "https://docs.tenzir.com/reference/operators/set";
    auto assignments = std::vector<ast::assignment>{};
    for (auto& arg : inv.args) {
      arg.match(
        [&](ast::assignment& x) {
          assignments.push_back(std::move(x));
        },
        [&](auto&) {
          diagnostic::error("expected assignment")
            .primary(arg)
            .usage(usage)
            .docs(docs)
            .emit(ctx.dh());
        });
    }
    return std::make_unique<set_operator>(std::move(assignments));
  }
};

class select final : public virtual operator_plugin2<set_operator>,
                     public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.select";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto assignments = make_select_assignments(std::move(inv.args), &ctx.dh());
    if (not assignments) {
      return failure::promise();
    }
    return std::make_unique<set_operator>(std::move(*assignments));
  }

  auto describe() const -> Description override {
    auto d = Describer<SelectArgs, SelectSet>{};
    auto fields = d.optional_variadic("field", &SelectArgs::fields, "selector");
    d.validate([fields](ValidateCtx& ctx) -> Empty {
      auto args = ctx.get_all(fields);
      auto fields = std::vector<ast::expression>{};
      fields.reserve(args.size());
      for (auto& arg : args) {
        if (not arg) {
          continue;
        }
        fields.push_back(std::move(*arg));
      }
      auto ignored = make_select_assignments(
        std::move(fields), &static_cast<diagnostic_handler&>(ctx));
      (void)ignored;
      return {};
    });
    d.assignments_are_positional();
    return d.order_invariant();
  }
};

} // namespace

} // namespace tenzir::plugins::set_select

TENZIR_REGISTER_PLUGIN(tenzir::plugins::set_select::set)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::set_select::select)
