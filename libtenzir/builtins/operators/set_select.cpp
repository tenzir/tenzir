//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/compile_ctx.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/rebatch.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/registry.hpp>
#include <tenzir/tql2/set.hpp>

#include <ranges>

namespace tenzir::plugins::set_select {

namespace {

/// Create a `where` IR operator with the given expression.
auto make_where_ir(ast::expression filter) -> Box<ir::Operator> {
  auto const* where = plugins::find<operator_compiler_plugin>("tql2.where");
  TENZIR_ASSERT(where);
  auto args = std::vector<ast::expression>{};
  args.push_back(std::move(filter));
  // TODO: This should reuse the existing compile context.
  auto dh = null_diagnostic_handler{};
  auto reg = global_registry();
  auto ctx = compile_ctx::make_root(base_ctx{dh, *reg});
  return where->compile(ast::invocation{ast::entity{{}}, std::move(args)}, ctx)
    .unwrap();
}

class SelectSet final : public Operator<table_slice, table_slice> {
public:
  SelectSet(std::vector<ast::assignment> assignments, event_order order)
    : assignments_{std::move(assignments)}, order_{order} {
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
    // This can reshuffle output schemas when strict ordering is not required.
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
  event_order order_ = event_order::ordered;
  std::vector<ast::field_path> moved_fields_;
};

class select_ir final : public ir::Operator {
public:
  select_ir() = default;

  explicit select_ir(std::vector<ast::assignment> assignments)
    : assignments_{std::move(assignments)} {
  }

  auto name() const -> std::string override {
    return "select_ir";
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    (void)instantiate;
    for (auto& x : assignments_) {
      TRY(x.right.substitute(ctx));
    }
    return {};
  }

  auto optimize(ir::optimize_filter filter,
                event_order order) && -> ir::optimize_result override {
    // Remember the order for potential rebatches.
    order_ = order;
    auto ops = std::vector<Box<ir::Operator>>{};
    if (not filter.empty()) {
      // TODO: Propagate all filters.
      TENZIR_ASSERT(filter.size() == 1);
      ops.reserve(2);
      auto where = make_where_ir(filter[0]);
      ops.push_back(std::move(where));
    }
    ops.emplace_back(select_ir{std::move(*this)});
    auto replacement = ir::pipeline{std::vector<ir::let>{}, std::move(ops)};
    return {{}, order_, std::move(replacement)};
  }

  auto spawn(element_type_tag input) && -> AnyOperator override {
    TENZIR_ASSERT(input.is<table_slice>());
    return SelectSet{std::move(assignments_), order_};
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>> override {
    if (input.is_not<table_slice>()) {
      diagnostic::error("select operator expected events").emit(dh);
      return failure::promise();
    }
    return input;
  }

  friend auto inspect(auto& f, select_ir& x) -> bool {
    return f.object(x).fields(f.field("assignments", x.assignments_));
  }

private:
  std::vector<ast::assignment> assignments_;
  event_order order_ = event_order::ordered;
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

class select final : public virtual operator_factory_plugin,
                     public virtual operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.select";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto assignments = std::vector<ast::assignment>{};
    assignments.reserve(1 + inv.args.size());
    assignments.emplace_back(
      ast::field_path::try_from(ast::this_{}).value(), location::unknown,
      ast::record{location::unknown, {}, location::unknown});
    for (auto& arg : inv.args) {
      if (auto assignment = std::get_if<ast::assignment>(&*arg.kind)) {
        auto selector = std::get_if<ast::field_path>(&assignment->left);
        if (not selector) {
          diagnostic::error("expected selector")
            .primary(assignment->left)
            .emit(ctx);
          continue;
        }
        assignments.push_back(std::move(*assignment));
      } else {
        auto selector = ast::field_path::try_from(arg);
        if (not selector) {
          diagnostic::error("expected selector").primary(arg).emit(ctx);
          continue;
        }
        // TODO: This is a hack.
        assignments.emplace_back(std::move(*selector), location::unknown,
                                 std::move(arg));
      }
    }
    return std::make_unique<set_operator>(std::move(assignments));
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<Box<ir::Operator>> override {
    auto assignments = std::vector<ast::assignment>{};
    assignments.reserve(1 + inv.args.size());
    // Start with `this = {}` to clear the record.
    assignments.emplace_back(
      ast::field_path::try_from(ast::this_{}).value(), location::unknown,
      ast::record{location::unknown, {}, location::unknown});
    for (auto& arg : inv.args) {
      if (auto* assignment = std::get_if<ast::assignment>(&*arg.kind)) {
        auto* selector = std::get_if<ast::field_path>(&assignment->left);
        if (not selector) {
          diagnostic::error("expected selector")
            .primary(assignment->left)
            .emit(ctx);
          continue;
        }
        TRY(assignment->right.bind(ctx));
        assignments.push_back(std::move(*assignment));
      } else {
        auto selector = ast::field_path::try_from(arg);
        if (not selector) {
          diagnostic::error("expected selector").primary(arg).emit(ctx);
          continue;
        }
        TRY(arg.bind(ctx));
        // TODO: This is a hack.
        assignments.emplace_back(std::move(*selector), location::unknown,
                                 std::move(arg));
      }
    }
    return select_ir{std::move(assignments)};
  }
};

using select_ir_plugin = inspection_plugin<ir::Operator, select_ir>;

} // namespace

} // namespace tenzir::plugins::set_select

TENZIR_REGISTER_PLUGIN(tenzir::plugins::set_select::set)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::set_select::select)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::set_select::select_ir_plugin)
