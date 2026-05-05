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
#include "tenzir/plugin/register.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/resolve.hpp"
#include "tenzir/tql2/set.hpp"
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
  struct Field {
    std::string name;
    Box<MatchPattern> pattern;
  };
  struct Record {
    std::vector<Field> fields;
    bool has_rest = false;
  };
  using kind_type = variant<Wildcard, Constant, Range, Record>;

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

  friend auto inspect(auto& f, Field& x) -> bool {
    return f.object(x).fields(f.field("name", x.name),
                              f.field("pattern", x.pattern));
  }

  friend auto inspect(auto& f, Record& x) -> bool {
    return f.object(x).fields(f.field("fields", x.fields),
                              f.field("has_rest", x.has_rest));
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
    ir::pipeline pipeline;
    bool wildcard = false;

    friend auto inspect(auto& f, Arm& x) -> bool {
      return f.object(x).fields(f.field("source", x.source),
                                f.field("pattern_exprs", x.pattern_exprs),
                                f.field("patterns", x.patterns),
                                f.field("pipeline", x.pipeline),
                                f.field("wildcard", x.wildcard));
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

class MatchImpl {
public:
  explicit MatchImpl(MatchArgs args) : args_{std::move(args)} {
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
    auto arm_masks = std::vector<std::vector<bool>>(args_.arms.size());
    for (auto& mask : arm_masks) {
      mask.resize(input.rows(), false);
    }
    auto matched = std::vector<bool>(input.rows(), false);
    auto scrutinee = eval(args_.scrutinee, input, ctx);
    auto offset = int64_t{0};
    for (auto& part : scrutinee) {
      for (auto value : part.values3()) {
        auto row = offset++;
        for (auto arm_index = size_t{0}; arm_index < args_.arms.size();
             ++arm_index) {
          if (matched[row]) {
            break;
          }
          auto const& arm = args_.arms[arm_index];
          auto matches = arm.wildcard;
          for (auto const& pattern : arm.patterns) {
            if (matches_pattern(value, pattern)) {
              matches = true;
              break;
            }
          }
          if (matches) {
            arm_masks[arm_index][row] = true;
            matched[row] = true;
          }
        }
      }
    }
    TENZIR_ASSERT_EQ(offset, static_cast<int64_t>(input.rows()));
    for (auto arm_index = size_t{0}; arm_index < args_.arms.size();
         ++arm_index) {
      if (arm_closed_[arm_index]) {
        continue;
      }
      auto builder = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
      check(builder.Reserve(input.rows()));
      for (auto value : arm_masks[arm_index]) {
        builder.UnsafeAppend(value);
      }
      auto filtered = filter(input, *finish(builder));
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
    if (not has_wildcard() and push) {
      auto builder = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
      check(builder.Reserve(input.rows()));
      for (auto value : matched) {
        builder.UnsafeAppend(not value);
      }
      auto filtered = filter(input, *finish(builder));
      if (filtered.rows() > 0) {
        co_await (*push)(std::move(filtered));
      }
    }
  }

  auto state() -> OperatorState {
    // Without a wildcard, unmatched rows pass through even after all explicit
    // arms have closed, so upstream must continue.
    if (not has_wildcard()) {
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
  std::vector<bool> arm_closed_ = std::vector<bool>(args_.arms.size(), false);
};

class Match final : public Operator<table_slice, table_slice> {
public:
  explicit Match(MatchArgs args) : impl_{std::move(args)} {
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
  explicit MatchSink(MatchArgs args) : impl_{std::move(args)} {
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
      .hint("use a literal, a constant `let` binding, or `_`")
      .emit(dh);
    return failure::promise();
  }
  return *value;
}

using BindingMap = std::unordered_map<std::string, ast::expression>;
using BindingPath = std::vector<ast::identifier>;

auto is_irrefutable_match_pattern(ast::match_pattern const& pattern,
                                  compile_ctx const& ctx) -> bool;

auto validate_match_pattern_bindings(ast::match_pattern const& pattern,
                                     compile_ctx const& ctx,
                                     std::unordered_set<std::string>& names)
  -> failure_or<void>;

auto bind_match_pattern(ast::match_pattern& pattern, compile_ctx& ctx)
  -> failure_or<void>;

auto collect_match_pattern_bindings(ast::match_pattern const& pattern,
                                    ast::expression const& scrutinee,
                                    BindingPath& path, compile_ctx const& ctx,
                                    BindingMap& replacements)
  -> failure_or<void>;

auto compare_range_bounds(data const& lower, data const& upper)
  -> std::partial_ordering;

auto validate_record_pattern_scrutinee(ast::match_pattern const& pattern,
                                       ast::expression const& scrutinee,
                                       diagnostic_handler& dh)
  -> failure_or<void>;

auto substitute_match_pattern(ast::match_pattern& pattern, substitute_ctx ctx,
                              bool instantiate) -> failure_or<void>;

auto lower_match_pattern(ast::match_pattern const& pattern,
                         diagnostic_handler& dh) -> failure_or<MatchPattern>;

auto is_irrefutable_match_pattern(ast::match_pattern const& pattern,
                                  compile_ctx const& ctx) -> bool {
  return pattern.kind->match<bool>(
    [](ast::wildcard_pattern const&) {
      return true;
    },
    [](ast::expression_pattern const&) {
      return false;
    },
    [&](ast::binding_pattern const& binding) {
      auto name = std::string_view{binding.name.name};
      TENZIR_ASSERT(name.starts_with("$"));
      return not ctx.get(name.substr(1));
    },
    [](ast::range_pattern const&) {
      return false;
    },
    [](ast::record_pattern const&) {
      return false;
    });
}

auto validate_match_pattern_bindings(ast::match_pattern const& pattern,
                                     compile_ctx const& ctx,
                                     std::unordered_set<std::string>& names)
  -> failure_or<void> {
  return pattern.kind->match<failure_or<void>>(
    [](ast::wildcard_pattern const&) -> failure_or<void> {
      return {};
    },
    [](ast::expression_pattern const&) -> failure_or<void> {
      return {};
    },
    [&](ast::binding_pattern const& binding) -> failure_or<void> {
      auto name = std::string_view{binding.name.name};
      TENZIR_ASSERT(name.starts_with("$"));
      if (ctx.get(name.substr(1))) {
        return {};
      }
      if (not names.emplace(binding.name.name).second) {
        diagnostic::error("binding `{}` appears more than once in this match "
                          "arm",
                          binding.name.name)
          .primary(binding)
          .emit(ctx);
        return failure::promise();
      }
      return {};
    },
    [&](ast::range_pattern const&) -> failure_or<void> {
      return {};
    },
    [&](ast::record_pattern const& record) -> failure_or<void> {
      for (auto const& field : record.fields) {
        TRY(validate_match_pattern_bindings(*field.pattern, ctx, names));
      }
      return {};
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
    [&](ast::binding_pattern& binding) -> failure_or<void> {
      auto name = std::string_view{binding.name.name};
      TENZIR_ASSERT(name.starts_with("$"));
      if (ctx.get(name.substr(1))) {
        auto expr = ast::expression{ast::dollar_var{binding.name}};
        TRY(expr.bind(ctx));
        pattern.kind = Box<ast::match_pattern_kind>{
          std::in_place,
          ast::expression_pattern{std::move(expr)},
        };
      }
      return {};
    },
    [&](ast::range_pattern& range) -> failure_or<void> {
      TRY(range.lower.bind(ctx));
      TRY(range.upper.bind(ctx));
      return {};
    },
    [&](ast::record_pattern& record) -> failure_or<void> {
      for (auto& field : record.fields) {
        TRY(bind_match_pattern(*field.pattern, ctx));
      }
      return {};
    });
}

auto is_addressable_scrutinee(ast::expression const& scrutinee) -> bool {
  if (std::holds_alternative<ast::this_>(*scrutinee.kind)) {
    return true;
  }
  auto copy = scrutinee;
  return ast::field_path::try_from(std::move(copy)).has_value();
}

auto make_binding_expression(ast::expression const& scrutinee,
                             BindingPath const& path) -> ast::expression {
  auto result = scrutinee;
  for (auto const& field : path) {
    result = ast::expression{ast::field_access{
      std::move(result),
      field.location,
      false,
      field,
    }};
  }
  return result;
}

auto collect_match_pattern_bindings(ast::match_pattern const& pattern,
                                    ast::expression const& scrutinee,
                                    BindingPath& path, compile_ctx const& ctx,
                                    BindingMap& replacements)
  -> failure_or<void> {
  return pattern.kind->match<failure_or<void>>(
    [](ast::wildcard_pattern const&) -> failure_or<void> {
      return {};
    },
    [](ast::expression_pattern const&) -> failure_or<void> {
      return {};
    },
    [&](ast::binding_pattern const& binding) -> failure_or<void> {
      auto name = std::string_view{binding.name.name};
      TENZIR_ASSERT(name.starts_with("$"));
      if (ctx.get(name.substr(1))) {
        return {};
      }
      auto const addressable_scrutinee = is_addressable_scrutinee(scrutinee);
      if (not path.empty() and not addressable_scrutinee) {
        diagnostic::error("binding `{}` cannot be derived from a "
                          "non-addressable match expression",
                          binding.name.name)
          .primary(binding)
          .emit(ctx);
        return failure::promise();
      }
      if (not addressable_scrutinee
          and not scrutinee.is_deterministic(ctx.reg())) {
        diagnostic::error("binding `{}` depends on a non-deterministic match "
                          "expression",
                          binding.name.name)
          .primary(binding)
          .emit(ctx);
        return failure::promise();
      }
      replacements.emplace(std::string{name.substr(1)},
                           make_binding_expression(scrutinee, path));
      return {};
    },
    [](ast::range_pattern const&) -> failure_or<void> {
      return {};
    },
    [&](ast::record_pattern const& record) -> failure_or<void> {
      for (auto const& field : record.fields) {
        path.push_back(field.name);
        TRY(collect_match_pattern_bindings(*field.pattern, scrutinee, path, ctx,
                                           replacements));
        path.pop_back();
      }
      return {};
    });
}

auto paths_overlap(ast::field_path const& assigned,
                   ast::field_path const& captured) -> bool {
  auto assigned_path = assigned.path();
  auto captured_path = captured.path();
  auto common_size = std::min(assigned_path.size(), captured_path.size());
  for (auto i = size_t{0}; i < common_size; ++i) {
    if (assigned_path[i].id.name != captured_path[i].id.name) {
      return false;
    }
  }
  return true;
}

class match_binding_mutation_validator
  : public ast::visitor<match_binding_mutation_validator> {
public:
  match_binding_mutation_validator(BindingMap const& replacements,
                                   diagnostic_handler& dh)
    : replacements_{replacements}, dh_{dh} {
  }

  void visit(ast::expression& expr) {
    if (auto* var = try_as<ast::dollar_var>(expr)) {
      auto name = std::string{var->name_without_dollar()};
      auto replacement = replacements_.find(name);
      if (replacement == replacements_.end()) {
        return;
      }
      auto captured_expr = replacement->second;
      auto captured = ast::field_path::try_from(std::move(captured_expr));
      if (not captured) {
        return;
      }
      for (auto const& mutated : mutated_paths_) {
        if (not paths_overlap(mutated, *captured)) {
          continue;
        }
        diagnostic::error("match binding `${}` is used after its matched field "
                          "is assigned",
                          name)
          .primary(*var)
          .hint("move the assignment after all uses of the binding")
          .emit(dh_);
        result_ = failure::promise();
        return;
      }
      return;
    }
    enter(expr);
  }

  void visit(ast::assignment& assignment) {
    visit(assignment.right);
    if (auto const* assigned = std::get_if<ast::field_path>(&assignment.left)) {
      mutated_paths_.push_back(*assigned);
    }
  }

  template <class T>
  void visit(T& x) {
    enter(x);
  }

  auto result() -> failure_or<void> {
    return result_;
  }

private:
  BindingMap const& replacements_;
  diagnostic_handler& dh_;
  std::vector<ast::field_path> mutated_paths_;
  failure_or<void> result_;
};

auto validate_match_binding_mutations(ast::pipeline& pipe,
                                      BindingMap const& replacements,
                                      diagnostic_handler& dh)
  -> failure_or<void> {
  auto validator = match_binding_mutation_validator{replacements, dh};
  validator.visit(pipe);
  return validator.result();
}

auto validate_record_pattern_scrutinee(ast::match_pattern const& pattern,
                                       ast::expression const& scrutinee,
                                       diagnostic_handler& dh)
  -> failure_or<void> {
  if (not std::holds_alternative<ast::record_pattern>(*pattern.kind)) {
    return {};
  }
  auto diagnostics = collecting_diagnostic_handler{};
  auto value = const_eval(scrutinee, diagnostics);
  if (value.is_error() or not diagnostics.empty()) {
    return {};
  }
  if (not std::holds_alternative<record>(value->get_data())) {
    diagnostic::error("record pattern cannot match non-record expression")
      .primary(pattern)
      .emit(dh);
    return failure::promise();
  }
  return {};
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
    [](ast::binding_pattern&) -> failure_or<void> {
      return {};
    },
    [&](ast::range_pattern& range) -> failure_or<void> {
      TRY(substitute_match_expression(range.lower, range.lower.get_location(),
                                      ctx, instantiate));
      TRY(substitute_match_expression(range.upper, range.upper.get_location(),
                                      ctx, instantiate));
      return {};
    },
    [&](ast::record_pattern& record) -> failure_or<void> {
      for (auto& field : record.fields) {
        TRY(substitute_match_pattern(*field.pattern, ctx, instantiate));
      }
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

auto lower_match_pattern(ast::record_pattern const& pattern,
                         diagnostic_handler& dh) -> failure_or<MatchPattern> {
  auto fields = std::vector<MatchPattern::Field>{};
  for (auto const& field : pattern.fields) {
    TRY(auto nested, lower_match_pattern(*field.pattern, dh));
    fields.push_back(
      MatchPattern::Field{field.name.name, Box{std::move(nested)}});
  }
  return MatchPattern{
    MatchPattern::Record{std::move(fields), pattern.rest.is_some()}};
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
    [](ast::binding_pattern const&) -> failure_or<MatchPattern> {
      return MatchPattern{MatchPattern::Wildcard{}};
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
    },
    [&](ast::record_pattern const& record) -> failure_or<MatchPattern> {
      return lower_match_pattern(record, dh);
    });
}

auto matches_record(data_view3 value, MatchPattern::Record const& pattern)
  -> bool {
  auto const* record = std::get_if<record_view3>(&value);
  if (not record) {
    return false;
  }
  auto matched_fields = size_t{0};
  for (auto const& field : pattern.fields) {
    auto found = false;
    for (auto const& [name, nested] : *record) {
      if (name == field.name) {
        found = matches_pattern(nested, *field.pattern);
        break;
      }
    }
    if (not found) {
      return false;
    }
    ++matched_fields;
  }
  auto field_count = size_t{0};
  for ([[maybe_unused]] auto const& field : *record) {
    ++field_count;
  }
  return pattern.has_rest or field_count == matched_fields;
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
    return lower != std::partial_ordering::less
           and lower != std::partial_ordering::unordered
           and upper != std::partial_ordering::greater
           and upper != std::partial_ordering::unordered;
  }
  auto const* record = std::get_if<MatchPattern::Record>(&pattern.kind);
  TENZIR_ASSERT(record);
  return matches_record(value, *record);
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
    if (not has_wildcard) {
      TRY(result, combine_branch_types(result, std::optional{input},
                                       args_.match_keyword, dh));
    }
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
    arm.wildcard = std::ranges::any_of(ast_arm.patterns, [&](auto& pattern) {
      return is_irrefutable_match_pattern(pattern, ctx);
    });
    if (arm.wildcard and arm_index + 1 != x.arms.size()) {
      diagnostic::error("irrefutable match arm must be last")
        .primary(arm.source)
        .emit(ctx);
      return failure::promise();
    }
    auto bindings = std::unordered_set<std::string>{};
    for (auto const& pattern : ast_arm.patterns) {
      TRY(validate_match_pattern_bindings(pattern, ctx, bindings));
      TRY(validate_record_pattern_scrutinee(pattern, args.scrutinee, ctx));
    }
    if (not arm.wildcard) {
      for (auto& pattern : ast_arm.patterns) {
        TRY(bind_match_pattern(pattern, ctx));
        arm.pattern_exprs.push_back(pattern);
      }
    }
    auto replacements = BindingMap{};
    auto path = BindingPath{};
    for (auto const& pattern : ast_arm.patterns) {
      TRY(collect_match_pattern_bindings(pattern, args.scrutinee, path, ctx,
                                         replacements));
    }
    if (not replacements.empty()) {
      TRY(validate_match_binding_mutations(ast_arm.pipe, replacements, ctx));
      TRY(ast_arm.pipe, substitute_named_expressions(std::move(ast_arm.pipe),
                                                     replacements, ctx));
    }
    TRY(arm.pipeline, std::move(ast_arm.pipe).compile(ctx));
    args.arms.push_back(std::move(arm));
    ++arm_index;
  }
  return Box<ir::Operator>{MatchIr{std::move(args)}};
}

auto make_match_ir_inspection_plugin() -> plugin* {
  return new inspection_plugin<ir::Operator, MatchIr>{};
}

} // namespace tenzir
