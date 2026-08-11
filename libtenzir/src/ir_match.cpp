//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ir_match.hpp"

#include "tenzir/async.hpp"
#include "tenzir/compile_ctx.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/view3.hpp"

#include <algorithm>

namespace tenzir {

namespace {

auto combine_branch_types(element_type_tag lhs, element_type_tag rhs,
                          location primary, diagnostic_handler& dh)
  -> failure_or<element_type_tag> {
  if (lhs == rhs) {
    return lhs;
  }
  if (lhs.is<void>()) {
    return rhs;
  }
  if (rhs.is<void>()) {
    return lhs;
  }
  diagnostic::error("incompatible branch output types: {} and {}",
                    operator_type_name(lhs), operator_type_name(rhs))
    .primary(primary)
    .emit(dh);
  return failure::promise();
}

struct MatchPattern {
  struct Wildcard {};
  struct Constant {
    data value;
  };
  struct Range {
    data lower;
    data upper;
  };
  using kind_type = variant<Wildcard, Constant, Range>;

  kind_type kind;

  friend auto inspect(auto& f, Wildcard& x) -> bool {
    TENZIR_UNUSED(x);
    return f.object(x).fields();
  }

  friend auto inspect(auto& f, Constant& x) -> bool {
    return f.object(x).fields(f.field("value", x.value));
  }

  friend auto inspect(auto& f, Range& x) -> bool {
    return f.object(x).fields(f.field("lower", x.lower),
                              f.field("upper", x.upper));
  }

  friend auto inspect(auto& f, MatchPattern& x) -> bool {
    return f.object(x).fields(f.field("kind", x.kind));
  }
};

struct MatchArgs {
  struct Arm {
    location source;
    std::vector<ast::match_pattern> pattern_exprs;
    std::vector<MatchPattern> patterns;
    Option<ast::expression> guard;
    ir::pipeline pipeline;
    bool wildcard = false;

    friend auto inspect(auto& f, Arm& x) -> bool {
      return f.object(x).fields(
        f.field("source", x.source), f.field("pattern_exprs", x.pattern_exprs),
        f.field("patterns", x.patterns), f.field("guard", x.guard),
        f.field("pipeline", x.pipeline), f.field("wildcard", x.wildcard));
    }
  };

  location match_keyword;
  ast::expression scrutinee;
  std::vector<Arm> arms;

  friend auto inspect(auto& f, MatchArgs& x) -> bool {
    return f.object(x).fields(f.field("match_keyword", x.match_keyword),
                              f.field("scrutinee", x.scrutinee),
                              f.field("arms", x.arms));
  }
};

auto matches_pattern(data_view3 value, MatchPattern const& pattern) -> bool;

auto make_boolean_array(std::vector<bool> const& mask)
  -> std::shared_ptr<arrow::BooleanArray> {
  auto builder = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
  check(builder.Reserve(mask.size()));
  for (auto value : mask) {
    builder.UnsafeAppend(value);
  }
  return finish(builder);
}

/// Runtime operator for `match`: routes each row to the first arm whose pattern
/// matches and whose guard passes (one output port per arm); the mandatory
/// wildcard arm catches any remaining rows. Mirrors `match`'s first-match
/// semantics, evaluating the scrutinee and guards once per slice.
class MatchOp final : public Operator<table_slice, table_slice> {
public:
  explicit MatchOp(MatchArgs args) : args_{std::move(args)} {
  }

  auto needs_output_ports() const -> bool override {
    return true;
  }

  auto process(table_slice, Push<table_slice>&, OpCtx&) -> Task<void> override {
    TENZIR_UNREACHABLE();
  }

  auto process(table_slice input, PushPorts<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    for (auto& [lane, slice] : classify(std::move(input), ctx.dh())) {
      co_await push(lane, std::move(slice));
    }
  }

private:
  /// Classifies the rows of `input` into `(arm index, subslice)` runs covering
  /// each matched row exactly once.
  auto classify(table_slice input, diagnostic_handler& dh) const
    -> std::vector<std::pair<size_t, table_slice>> {
    auto runs = std::vector<std::pair<size_t, table_slice>>{};
    auto matched = std::vector<bool>(input.rows(), false);
    auto scrutinee = eval(args_.scrutinee, input, dh);
    for (auto arm_index = size_t{0}; arm_index < args_.arms.size();
         ++arm_index) {
      auto const& arm = args_.arms[arm_index];
      auto candidate_rows = std::vector<size_t>{};
      auto offset = int64_t{0};
      for (auto& part : scrutinee) {
        for (auto value : part.values3()) {
          auto const row = detail::narrow<size_t>(offset++);
          if (matched[row]) {
            continue;
          }
          if (arm.wildcard
              or std::ranges::any_of(arm.patterns, [&](auto const& pattern) {
                   return matches_pattern(value, pattern);
                 })) {
            candidate_rows.push_back(row);
          }
        }
      }
      TENZIR_ASSERT_EQ(offset, static_cast<int64_t>(input.rows()));
      if (candidate_rows.empty()) {
        continue;
      }
      auto guard_mask = std::vector<bool>(candidate_rows.size(), true);
      if (arm.guard) {
        auto candidate_mask = std::vector<bool>(input.rows(), false);
        for (auto row : candidate_rows) {
          candidate_mask[row] = true;
        }
        auto candidate_input
          = filter(input, *make_boolean_array(candidate_mask));
        auto end = int64_t{0};
        for (auto const& predicate : eval(*arm.guard, candidate_input, dh)) {
          auto const start = std::exchange(end, end + predicate.length());
          auto const typed_predicate = predicate.as<bool_type>();
          if (not typed_predicate) {
            if (not is<null_type>(predicate.type)) {
              diagnostic::warning("expected `bool`, but got `{}`",
                                  predicate.type.kind())
                .primary(*arm.guard)
                .emit(dh);
            }
            std::fill(guard_mask.begin() + start, guard_mask.begin() + end,
                      false);
            continue;
          }
          auto const& array = *typed_predicate->array;
          for (auto row = start; row < end; ++row) {
            guard_mask[row]
              = not array.IsNull(row - start) and array.GetView(row - start);
          }
        }
        TENZIR_ASSERT_EQ(end, static_cast<int64_t>(candidate_rows.size()));
      }
      auto arm_mask = std::vector<bool>(input.rows(), false);
      for (auto index = size_t{0}; index < candidate_rows.size(); ++index) {
        if (guard_mask[index]) {
          auto row = candidate_rows[index];
          arm_mask[row] = true;
          matched[row] = true;
        }
      }
      auto filtered = filter(input, *make_boolean_array(arm_mask));
      if (filtered.rows() == 0) {
        continue;
      }
      runs.emplace_back(arm_index, std::move(filtered));
    }
    return runs;
  }

  MatchArgs args_;
};

auto const_eval_match_expression(ast::expression const& expr, location source,
                                 diagnostic_handler& dh) -> failure_or<data> {
  auto diagnostics = collecting_diagnostic_handler{};
  auto value = const_eval(expr, diagnostics);
  if (value.is_error() or not diagnostics.empty()) {
    diagnostic::error("match patterns must be constant expressions")
      .primary(source)
      .hint("use a literal, a constant expression, or `_`")
      .emit(dh);
    return failure::promise();
  }
  return std::move(value->inner);
}

auto is_irrefutable_match_pattern(ast::match_pattern const& pattern) -> bool;

auto bind_match_pattern(ast::match_pattern& pattern, compile_ctx& ctx)
  -> failure_or<void>;

auto compare_range_bounds(data const& lower, data const& upper)
  -> std::partial_ordering;

auto substitute_match_pattern(ast::match_pattern& pattern, substitute_ctx ctx,
                              bool instantiate) -> failure_or<void>;

auto lower_match_pattern(ast::match_pattern const& pattern,
                         diagnostic_handler& dh) -> failure_or<MatchPattern>;

auto is_irrefutable_match_pattern(ast::match_pattern const& pattern) -> bool {
  return pattern.kind->match<bool>(
    [](ast::wildcard_pattern const&) {
      return true;
    },
    [](ast::expression_pattern const&) {
      return false;
    },
    [](ast::range_pattern const&) {
      return false;
    });
}

auto bind_match_pattern(ast::match_pattern& pattern, compile_ctx& ctx)
  -> failure_or<void> {
  return pattern.kind->match<failure_or<void>>(
    [](ast::wildcard_pattern&) -> failure_or<void> {
      return {};
    },
    [&](ast::expression_pattern& expr) -> failure_or<void> {
      return expr.expr.bind(ctx);
    },
    [&](ast::range_pattern& range) -> failure_or<void> {
      TRY(range.lower.bind(ctx));
      TRY(range.upper.bind(ctx));
      return {};
    });
}

auto substitute_match_expression(ast::expression& expr, location source,
                                 substitute_ctx ctx, bool instantiate)
  -> failure_or<void> {
  TRY(auto subst, expr.substitute(ctx));
  if (instantiate and subst == ast::substitute_result::some_remaining) {
    diagnostic::error("match patterns must be constant expressions")
      .primary(source)
      .emit(ctx);
    return failure::promise();
  }
  return {};
}

auto substitute_match_pattern(ast::match_pattern& pattern, substitute_ctx ctx,
                              bool instantiate) -> failure_or<void> {
  return pattern.kind->match<failure_or<void>>(
    [](ast::wildcard_pattern&) -> failure_or<void> {
      return {};
    },
    [&](ast::expression_pattern& expr) -> failure_or<void> {
      return substitute_match_expression(expr.expr, expr.get_location(), ctx,
                                         instantiate);
    },
    [&](ast::range_pattern& range) -> failure_or<void> {
      TRY(substitute_match_expression(range.lower, range.lower.get_location(),
                                      ctx, instantiate));
      TRY(substitute_match_expression(range.upper, range.upper.get_location(),
                                      ctx, instantiate));
      return {};
    });
}

auto compare_range_bounds(data const& lower, data const& upper)
  -> std::partial_ordering {
  return lower.get_data().match<std::partial_ordering>(
    [&](caf::none_t value) {
      return partial_order(data_view3{value}, upper);
    },
    [&](bool value) {
      return partial_order(data_view3{value}, upper);
    },
    [&](int64_t value) {
      return partial_order(data_view3{value}, upper);
    },
    [&](uint64_t value) {
      return partial_order(data_view3{value}, upper);
    },
    [&](double value) {
      return partial_order(data_view3{value}, upper);
    },
    [&](duration value) {
      return partial_order(data_view3{value}, upper);
    },
    [&](time value) {
      return partial_order(data_view3{value}, upper);
    },
    [&](std::string const& value) {
      return partial_order(data_view3{value}, upper);
    },
    [](pattern const&) {
      return std::partial_ordering::unordered;
    },
    [&](ip value) {
      return partial_order(data_view3{value}, upper);
    },
    [&](subnet value) {
      return partial_order(data_view3{value}, upper);
    },
    [&](enumeration value) {
      return partial_order(data_view3{value}, upper);
    },
    [](list const&) {
      return std::partial_ordering::unordered;
    },
    [](map const&) {
      return std::partial_ordering::unordered;
    },
    [](record const&) {
      return std::partial_ordering::unordered;
    },
    [&](blob const& value) {
      return partial_order(data_view3{value}, upper);
    },
    [&](secret const& value) {
      return partial_order(data_view3{value}, upper);
    });
}

auto lower_match_pattern(ast::match_pattern const& pattern,
                         diagnostic_handler& dh) -> failure_or<MatchPattern> {
  return pattern.kind->match<failure_or<MatchPattern>>(
    [](ast::wildcard_pattern const&) -> failure_or<MatchPattern> {
      return MatchPattern{MatchPattern::Wildcard{}};
    },
    [&](ast::expression_pattern const& expr) -> failure_or<MatchPattern> {
      TRY(auto value,
          const_eval_match_expression(expr.expr, expr.get_location(), dh));
      return MatchPattern{MatchPattern::Constant{std::move(value)}};
    },
    [&](ast::range_pattern const& range) -> failure_or<MatchPattern> {
      TRY(auto lower, const_eval_match_expression(
                        range.lower, range.lower.get_location(), dh));
      TRY(auto upper, const_eval_match_expression(
                        range.upper, range.upper.get_location(), dh));
      if (compare_range_bounds(lower, upper)
          == std::partial_ordering::unordered) {
        diagnostic::error("range pattern bounds are not comparable")
          .primary(range.get_location())
          .emit(dh);
        return failure::promise();
      }
      return MatchPattern{
        MatchPattern::Range{std::move(lower), std::move(upper)}};
    });
}

auto matches_pattern(data_view3 value, MatchPattern const& pattern) -> bool {
  return pattern.kind.match(
    [](MatchPattern::Wildcard const&) {
      return true;
    },
    [&](MatchPattern::Constant const& constant) {
      return partial_order(value, constant.value)
             == std::partial_ordering::equivalent;
    },
    [&](MatchPattern::Range const& range) {
      auto lower = partial_order(value, range.lower);
      auto upper = partial_order(value, range.upper);
      return lower == std::partial_ordering::greater
             and upper == std::partial_ordering::less;
    });
}

class MatchIr final : public ir::Operator {
public:
  MatchIr() = default;

  explicit MatchIr(MatchArgs args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "Match";
  }

  auto copy() const -> Box<ir::Operator> override {
    return MatchIr{args_};
  }

  auto move() && -> Box<ir::Operator> override {
    return MatchIr{std::move(args_)};
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    TRY(args_.scrutinee.substitute(ctx));
    for (auto& arm : args_.arms) {
      arm.patterns.clear();
      for (auto& pattern : arm.pattern_exprs) {
        TRY(substitute_match_pattern(pattern, ctx, instantiate));
        if (instantiate) {
          TRY(auto lowered, lower_match_pattern(pattern, ctx));
          arm.patterns.push_back(std::move(lowered));
        }
      }
      if (arm.guard) {
        TRY(arm.guard->substitute(ctx));
      }
      TRY(arm.pipeline.substitute(ctx, instantiate));
    }
    return {};
  }

  auto
  optimize(ir::optimize_filter filter, event_order order,
           const ir::OptimizeCtx& octx) && -> ir::optimize_result override {
    // The planner lowers the arms inline, so this is the only pass that gets to
    // optimize them. Without recursing here, optimizer-only operators such as
    // `unordered` would survive into the plan and panic when spawned.
    //
    // Every row goes to exactly one arm and the arm outputs are merged, so the
    // downstream filter and order requirement can be pushed into each arm that
    // still returns events. An arm that does not inherits neither, as it has no
    // downstream event consumer. The filter is not propagated past `match`,
    // because the arms may rewrite the fields it refers to.
    auto null_dh = null_diagnostic_handler{};
    auto outputs_events = std::vector<bool>{};
    outputs_events.reserve(args_.arms.size());
    auto types_known = true;
    for (const auto& arm : args_.arms) {
      auto ty = arm.pipeline.infer_type(tag_v<table_slice>, null_dh);
      types_known = types_known and static_cast<bool>(ty);
      outputs_events.push_back(ty and ty->is<table_slice>());
    }
    // Pushing into the arms is only complete if we know every arm's output
    // type. Otherwise the filter stays behind `match`.
    auto pushed = ir::optimize_filter{};
    if (types_known) {
      pushed = std::move(filter);
      filter.clear();
    }
    auto result_order = order;
    for (auto i = size_t{0}; i < args_.arms.size(); ++i) {
      auto& arm = args_.arms[i];
      auto events = outputs_events[i];
      auto opt = std::move(arm.pipeline)
                   .optimize(events ? pushed : ir::optimize_filter{},
                             events ? order : event_order::ordered, octx);
      arm.pipeline = std::move(opt.replacement);
      // All arms share `match` as their upstream, so an arm cannot push its
      // residual filter further up.
      arm.pipeline.prepend(std::move(opt.filter));
      result_order = stronger_event_order(result_order, opt.order);
    }
    auto replacement = std::vector<Box<ir::Operator>>{};
    replacement.push_back(std::move(*this).move());
    for (auto& expr : filter) {
      replacement.push_back(make_where_ir(std::move(expr)));
    }
    return {
      {},
      result_order,
      ir::pipeline{{}, std::move(replacement)},
    };
  }

  auto spawn(element_type_tag) const -> AnyOperator override {
    return Box<tenzir::Operator<table_slice, table_slice>>{
      MatchOp{args_}.with_name("match")};
  }

  auto plan(ir::PlanBuilder& builder, ir::PlanPorts input,
            diagnostic_handler& dh) && -> failure_or<ir::PlanPorts> override {
    // `match` is an N-output operator with one output port per arm. It
    // evaluates the scrutinee and guards per row and routes each row to the
    // first arm that claims it. The arm tails are returned so the consumer
    // merges them.
    auto ty = tag_v<table_slice>;
    auto branches = std::vector<ir::pipeline>{};
    branches.reserve(args_.arms.size());
    for (auto& arm : args_.arms) {
      branches.push_back(std::move(arm.pipeline));
    }
    auto node = builder.append_node(std::move(*this).move(), ty, ty);
    builder.add_channels(input, node);
    auto tails = ir::PlanPorts{};
    for (auto port = size_t{0}; port < branches.size(); ++port) {
      TRY(auto tail, builder.lower_subpipeline(
                       node, std::move(branches[port]),
                       ir::PlanPorts{ir::Port{node, port, ty}}, dh));
      tails.insert(tails.end(), tail.begin(), tail.end());
    }
    return tails;
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<element_type_tag> override {
    if (input.is_not<table_slice>()) {
      diagnostic::error("match operator expected events").emit(dh);
      return failure::promise();
    }
    auto result = std::optional<element_type_tag>{};
    auto has_wildcard = false;
    for (auto const& arm : args_.arms) {
      has_wildcard = has_wildcard or arm.wildcard;
      TRY(auto branch_ty, arm.pipeline.infer_type(input, dh));
      if (branch_ty.is<chunk_ptr>()) {
        diagnostic::error("branches must not return bytes")
          .primary(arm.source)
          .emit(dh);
        return failure::promise();
      }
      if (not result) {
        result = branch_ty;
        continue;
      }
      TRY(result,
          combine_branch_types(*result, branch_ty, args_.match_keyword, dh));
    }
    TENZIR_ASSERT(has_wildcard);
    // A match always has a wildcard arm, so at least one branch contributed.
    TENZIR_ASSERT(result);
    return *result;
  }

  friend auto inspect(auto& f, MatchIr& x) -> bool {
    return f.apply(x.args_);
  }

private:
  MatchArgs args_;
};

TENZIR_REGISTER_PLUGIN(inspection_plugin<ir::Operator, MatchIr>)

} // namespace

auto make_match_ir(ast::match_stmt x, compile_ctx& ctx)
  -> failure_or<Box<ir::Operator>> {
  TRY(x.expr.bind(ctx));
  if (x.arms.empty()) {
    diagnostic::error("expected at least one match arm")
      .primary(x.end)
      .emit(ctx);
    return failure::promise();
  }
  auto args = MatchArgs{};
  args.match_keyword = x.begin;
  args.scrutinee = std::move(x.expr);
  for (auto arm_index = size_t{0}; auto& ast_arm : x.arms) {
    auto arm = MatchArgs::Arm{};
    arm.source = ast_arm.patterns.front().get_location();
    arm.wildcard
      = not ast_arm.guard
        and std::ranges::any_of(ast_arm.patterns, [&](auto& pattern) {
              return is_irrefutable_match_pattern(pattern);
            });
    if (arm.wildcard and arm_index + 1 != x.arms.size()) {
      diagnostic::error("irrefutable match arm must be last")
        .primary(arm.source)
        .emit(ctx);
      return failure::promise();
    }
    if (not arm.wildcard) {
      for (auto& pattern : ast_arm.patterns) {
        TRY(bind_match_pattern(pattern, ctx));
        arm.pattern_exprs.push_back(pattern);
      }
    }
    if (ast_arm.guard) {
      TRY(ast_arm.guard->bind(ctx));
      arm.guard = std::move(ast_arm.guard);
    }
    TRY(arm.pipeline, std::move(ast_arm.pipe).compile(ctx));
    args.arms.push_back(std::move(arm));
    ++arm_index;
  }
  if (not std::ranges::any_of(args.arms, &MatchArgs::Arm::wildcard)) {
    diagnostic::error("match arms must be exhaustive")
      .primary(x.begin)
      .hint("add a final `_` arm")
      .emit(ctx);
    return failure::promise();
  }
  return Box<ir::Operator>{MatchIr{std::move(args)}};
}

} // namespace tenzir
