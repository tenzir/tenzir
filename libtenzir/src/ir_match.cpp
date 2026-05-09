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
#include "tenzir/plugin/register.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/view3.hpp"

#include <algorithm>

namespace tenzir {

auto combine_branch_types(std::optional<element_type_tag> lhs,
                          std::optional<element_type_tag> rhs, location primary,
                          diagnostic_handler& dh)
  -> failure_or<std::optional<element_type_tag>>;

namespace {

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

class MatchImpl {
public:
  explicit MatchImpl(MatchArgs args, bool passthrough_unmatched)
    : args_{std::move(args)}, passthrough_unmatched_{passthrough_unmatched} {
  }

  auto start(OpCtx& ctx) -> Task<void> {
    for (auto i = size_t{0}; i < args_.arms.size(); ++i) {
      if (not args_.arms[i].pipeline.operators.empty()
          and ctx.get_sub(static_cast<int64_t>(i)).is_some()) {
        co_return;
      }
    }
    for (auto i = size_t{0}; i < args_.arms.size(); ++i) {
      if (args_.arms[i].pipeline.operators.empty()) {
        continue;
      }
      co_await ctx.spawn_sub<table_slice>(static_cast<int64_t>(i),
                                          args_.arms[i].pipeline);
    }
  }

  auto process(table_slice input, OpCtx& ctx, Push<table_slice>* push = nullptr)
    -> Task<void> {
    auto matched = std::vector<bool>(input.rows(), false);
    auto scrutinee = eval(args_.scrutinee, input, ctx);
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
          auto matches = arm.wildcard;
          for (auto const& pattern : arm.patterns) {
            if (matches_pattern(value, pattern)) {
              matches = true;
              break;
            }
          }
          if (matches) {
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
        for (auto const& predicate : eval(*arm.guard, candidate_input, ctx)) {
          auto const start = std::exchange(end, end + predicate.length());
          auto const typed_predicate = predicate.as<bool_type>();
          if (not typed_predicate) {
            diagnostic::warning("expected `bool`, but got `{}`",
                                predicate.type.kind())
              .primary(*arm.guard)
              .emit(ctx);
            std::fill(guard_mask.begin() + start, guard_mask.begin() + end,
                      false);
            continue;
          }
          if (typed_predicate->array->null_count() > 0) {
            diagnostic::warning("expected `bool`, but got `null`")
              .primary(*arm.guard)
              .emit(ctx);
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
      if (arm_closed_[arm_index]) {
        continue;
      }
      auto filtered = filter(input, *make_boolean_array(arm_mask));
      if (filtered.rows() == 0) {
        continue;
      }
      auto sub = ctx.get_sub(static_cast<int64_t>(arm_index));
      if (not sub) {
        if (args_.arms[arm_index].pipeline.operators.empty()) {
          if (push) {
            co_await (*push)(std::move(filtered));
          }
        } else {
          arm_closed_[arm_index] = true;
        }
        continue;
      }
      auto& handle = as<SubHandle<table_slice>>(*sub);
      arm_closed_[arm_index]
        = (co_await handle.push(std::move(filtered))).is_err();
    }
    if (not has_wildcard() and passthrough_unmatched_) {
      auto unmatched = std::vector<bool>(matched.size(), false);
      for (auto row = size_t{0}; row < matched.size(); ++row) {
        unmatched[row] = not matched[row];
      }
      auto filtered = filter(input, *make_boolean_array(unmatched));
      if (filtered.rows() > 0) {
        co_await (*push)(std::move(filtered));
      }
    }
  }

  auto state() -> OperatorState {
    // Without a wildcard, transform matches pass unmatched rows through even
    // after all explicit arms have closed, so upstream must continue.
    if (passthrough_unmatched_ and not has_wildcard()) {
      return OperatorState::normal;
    }
    if (std::ranges::all_of(arm_closed_, std::identity{})) {
      return OperatorState::done;
    }
    return OperatorState::normal;
  }

private:
  auto has_wildcard() const -> bool {
    return std::ranges::any_of(args_.arms, &MatchArgs::Arm::wildcard);
  }

  MatchArgs args_;
  bool passthrough_unmatched_ = false;
  std::vector<bool> arm_closed_ = std::vector<bool>(args_.arms.size(), false);
};

class Match final : public Operator<table_slice, table_slice> {
public:
  explicit Match(MatchArgs args) : impl_{std::move(args), true} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    return impl_.start(ctx);
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    return impl_.process(std::move(input), ctx, &push);
  }

  auto state() -> OperatorState override {
    return impl_.state();
  }

private:
  MatchImpl impl_;
};

class MatchSink final : public Operator<table_slice, void> {
public:
  explicit MatchSink(MatchArgs args) : impl_{std::move(args), false} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    return impl_.start(ctx);
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    return impl_.process(std::move(input), ctx);
  }

  auto state() -> OperatorState override {
    return impl_.state();
  }

private:
  MatchImpl impl_;
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
  return *value;
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
          .primary(range.dots)
          .emit(dh);
        return failure::promise();
      }
      return MatchPattern{
        MatchPattern::Range{std::move(lower), std::move(upper)}};
    });
}

auto matches_pattern(data_view3 value, MatchPattern const& pattern) -> bool {
  if (std::holds_alternative<MatchPattern::Wildcard>(pattern.kind)) {
    return true;
  }
  if (auto constant = std::get_if<MatchPattern::Constant>(&pattern.kind)) {
    return partial_order(value, constant->value)
           == std::partial_ordering::equivalent;
  }
  if (auto range = std::get_if<MatchPattern::Range>(&pattern.kind)) {
    auto lower = partial_order(value, range->lower);
    auto upper = partial_order(value, range->upper);
    return lower == std::partial_ordering::greater
           and upper == std::partial_ordering::less;
  }
  TENZIR_UNREACHABLE();
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

  auto spawn(element_type_tag input) && -> AnyOperator override {
    TENZIR_ASSERT(input.is<table_slice>());
    auto dh = null_diagnostic_handler{};
    auto output = infer_type(input, dh);
    TENZIR_ASSERT(output and *output);
    if ((**output).is<void>()) {
      return MatchSink{std::move(args_)};
    }
    return Match{std::move(args_)};
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>> override {
    if (input.is_not<table_slice>()) {
      diagnostic::error("match operator expected events").emit(dh);
      return failure::promise();
    }
    auto result = std::optional<element_type_tag>{};
    auto has_wildcard = false;
    for (auto const& arm : args_.arms) {
      has_wildcard = has_wildcard or arm.wildcard;
      TRY(auto branch_ty, arm.pipeline.infer_type(input, dh));
      if (branch_ty and branch_ty->is<chunk_ptr>()) {
        diagnostic::error("branches must not return bytes")
          .primary(arm.source)
          .emit(dh);
        return failure::promise();
      }
      TRY(result,
          combine_branch_types(result, branch_ty, args_.match_keyword, dh));
    }
    TENZIR_ASSERT(has_wildcard);
    return result;
  }

  friend auto inspect(auto& f, MatchIr& x) -> bool {
    return f.apply(x.args_);
  }

private:
  MatchArgs args_;
};

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

auto make_match_ir_inspection_plugin() -> plugin* {
  return new inspection_plugin<ir::Operator, MatchIr>{};
}

} // namespace tenzir
