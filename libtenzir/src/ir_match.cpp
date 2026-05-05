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

#include <caf/binary_serializer.hpp>

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
  struct List {
    std::vector<Box<MatchPattern>> elements;
    bool has_rest = false;
  };
  struct Record {
    std::vector<Field> fields;
    bool has_rest = false;
  };
  using kind_type = variant<Wildcard, Constant, Range, List, Record>;

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

  friend auto inspect(auto& f, List& x) -> bool {
    return f.object(x).fields(f.field("elements", x.elements),
                              f.field("has_rest", x.has_rest));
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
    : args_{std::move(args)},
      passthrough_unmatched_{passthrough_unmatched} {
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
    auto candidate_masks = std::vector<std::vector<bool>>(args_.arms.size());
    auto arm_masks = std::vector<std::vector<bool>>(args_.arms.size());
    for (auto& mask : candidate_masks) {
      mask.resize(input.rows(), false);
    }
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
          auto const& arm = args_.arms[arm_index];
          auto matches = arm.wildcard;
          for (auto const& pattern : arm.patterns) {
            if (matches_pattern(value, pattern)) {
              matches = true;
              break;
            }
          }
          if (matches) {
            candidate_masks[arm_index][row] = true;
          }
        }
      }
    }
    TENZIR_ASSERT_EQ(offset, static_cast<int64_t>(input.rows()));
    for (auto arm_index = size_t{0}; arm_index < args_.arms.size();
         ++arm_index) {
      auto const& arm = args_.arms[arm_index];
      auto candidate_rows = std::vector<size_t>{};
      for (auto row = size_t{0}; row < input.rows(); ++row) {
        if (not matched[row] and candidate_masks[arm_index][row]) {
          candidate_rows.push_back(row);
        }
      }
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
      for (auto index = size_t{0}; index < candidate_rows.size(); ++index) {
        if (guard_mask[index]) {
          auto row = candidate_rows[index];
          arm_masks[arm_index][row] = true;
          matched[row] = true;
        }
      }
    }
    for (auto arm_index = size_t{0}; arm_index < args_.arms.size();
         ++arm_index) {
      if (arm_closed_[arm_index]) {
        continue;
      }
      auto filtered = filter(input, *make_boolean_array(arm_masks[arm_index]));
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
      .hint("use a literal, a constant `let` binding, or `_`")
      .emit(dh);
    return failure::promise();
  }
  return *value;
}

using BindingMap = std::unordered_map<std::string, ast::expression>;
struct BindingFieldPathSegment {
  ast::identifier name;
};
struct BindingIndexPathSegment {
  location source;
  int64_t index = 0;
};
using BindingPath
  = std::vector<variant<BindingFieldPathSegment, BindingIndexPathSegment>>;

auto is_irrefutable_match_pattern(ast::match_pattern const& pattern,
                                  ast::expression const& scrutinee,
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
                                  ast::expression const& scrutinee,
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
    [](ast::list_pattern const&) {
      return false;
    },
    [&](ast::record_pattern const& record) {
      return record.fields.empty() and record.rest.is_some()
             and std::holds_alternative<ast::this_>(*scrutinee.kind);
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
    [&](ast::list_pattern const& list) -> failure_or<void> {
      for (auto const& element : list.elements) {
        TRY(validate_match_pattern_bindings(*element, ctx, names));
      }
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
    [&](ast::list_pattern& list) -> failure_or<void> {
      for (auto& element : list.elements) {
        TRY(bind_match_pattern(*element, ctx));
      }
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
  for (auto const& segment : path) {
    segment.match(
      [&](BindingFieldPathSegment const& field) {
        result = ast::expression{ast::field_access{
          std::move(result),
          field.name.location,
          false,
          field.name,
        }};
      },
      [&](BindingIndexPathSegment const& index) {
        result = ast::expression{ast::index_expr{
          std::move(result),
          index.source,
          ast::expression{ast::constant{index.index, index.source}},
          index.source,
          false,
        }};
      });
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
    [&](ast::list_pattern const& list) -> failure_or<void> {
      for (auto index = size_t{0}; index < list.elements.size(); ++index) {
        path.push_back(
          BindingIndexPathSegment{list.begin, static_cast<int64_t>(index)});
        TRY(collect_match_pattern_bindings(*list.elements[index], scrutinee,
                                           path, ctx, replacements));
        path.pop_back();
      }
      return {};
    },
    [&](ast::record_pattern const& record) -> failure_or<void> {
      for (auto const& field : record.fields) {
        path.push_back(BindingFieldPathSegment{field.name});
        TRY(collect_match_pattern_bindings(*field.pattern, scrutinee, path, ctx,
                                           replacements));
        path.pop_back();
      }
      return {};
    });
}

auto serialized_expressions_equal(ast::expression const& lhs,
                                  ast::expression const& rhs) -> bool {
  auto lhs_buffer = caf::byte_buffer{};
  auto lhs_serializer = caf::binary_serializer{lhs_buffer};
  if (not lhs_serializer.apply(lhs)) {
    return false;
  }
  auto rhs_buffer = caf::byte_buffer{};
  auto rhs_serializer = caf::binary_serializer{rhs_buffer};
  if (not rhs_serializer.apply(rhs)) {
    return false;
  }
  return lhs_buffer == rhs_buffer;
}

auto binding_expressions_equal(ast::expression const& lhs,
                               ast::expression const& rhs) -> bool {
  auto lhs_path = ast::field_path::try_from(lhs);
  auto rhs_path = ast::field_path::try_from(rhs);
  if (not lhs_path or not rhs_path) {
    return serialized_expressions_equal(lhs, rhs);
  }
  if (lhs_path->has_this() != rhs_path->has_this()) {
    return false;
  }
  auto lhs_segments = lhs_path->path();
  auto rhs_segments = rhs_path->path();
  if (lhs_segments.size() != rhs_segments.size()) {
    return false;
  }
  for (auto i = size_t{0}; i < lhs_segments.size(); ++i) {
    if (lhs_segments[i].id.name != rhs_segments[i].id.name
        or lhs_segments[i].has_question_mark
             != rhs_segments[i].has_question_mark) {
      return false;
    }
  }
  return true;
}

auto binding_maps_equal(BindingMap const& lhs, BindingMap const& rhs) -> bool {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (auto const& [name, lhs_expr] : lhs) {
    auto rhs_expr = rhs.find(name);
    if (rhs_expr == rhs.end()
        or not binding_expressions_equal(lhs_expr, rhs_expr->second)) {
      return false;
    }
  }
  return true;
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

auto try_get_field_path(ast::expression const& expr)
  -> std::optional<ast::field_path> {
  auto copy = expr;
  return ast::field_path::try_from(std::move(copy));
}

auto is_builtin_operator(ast::invocation const& invocation,
                         std::string_view name) -> bool {
  if (not invocation.op.ref.resolved()) {
    return false;
  }
  if (invocation.op.ref.pkg() != entity_pkg_std
      or invocation.op.ref.ns() != entity_ns::op) {
    return false;
  }
  auto segments = invocation.op.ref.segments();
  return segments.size() == 1 and segments.front() == name;
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
      auto captured = try_get_field_path(replacement->second);
      if (not captured) {
        return;
      }
      for (auto const& mutated : mutated_paths_) {
        if (not paths_overlap(mutated, *captured)) {
          continue;
        }
        diagnostic::error("match binding `${}` is used after its matched field "
                          "is mutated",
                          name)
          .primary(*var)
          .hint("move the mutation after all uses of the binding")
          .emit(dh_);
        result_ = failure::promise();
        return;
      }
      return;
    }
    auto const was_in_expression = std::exchange(in_expression_, true);
    enter(expr);
    in_expression_ = was_in_expression;
  }

  void visit(ast::assignment& assignment) {
    visit(assignment.right);
    if (in_expression_) {
      return;
    }
    auto copy = assignment;
    auto moved_fields = resolve_move_keyword(std::move(copy)).second;
    std::ranges::move(moved_fields, std::back_inserter(mutated_paths_));
    if (auto const* assigned = std::get_if<ast::field_path>(&assignment.left)) {
      mutated_paths_.push_back(*assigned);
    }
  }

  void visit(ast::invocation& invocation) {
    enter(invocation);
    if (is_builtin_operator(invocation, "drop")) {
      for (auto const& arg : invocation.args) {
        if (auto field = try_get_field_path(arg)) {
          mutated_paths_.push_back(std::move(*field));
        }
      }
    } else if (is_builtin_operator(invocation, "move")) {
      for (auto const& arg : invocation.args) {
        auto const* assignment = try_as<ast::assignment>(arg);
        if (not assignment) {
          continue;
        }
        if (auto const* left
            = std::get_if<ast::field_path>(&assignment->left)) {
          mutated_paths_.push_back(*left);
        }
        if (auto right = try_get_field_path(assignment->right)) {
          mutated_paths_.push_back(std::move(*right));
        }
      }
    } else if (is_builtin_operator(invocation, "select")) {
      mutated_paths_.push_back(ast::field_path::try_from(ast::this_{}).value());
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
  bool in_expression_ = false;
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
    [&](ast::list_pattern& list) -> failure_or<void> {
      for (auto& element : list.elements) {
        TRY(substitute_match_pattern(*element, ctx, instantiate));
      }
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

auto lower_match_pattern(ast::list_pattern const& pattern,
                         diagnostic_handler& dh) -> failure_or<MatchPattern> {
  auto elements = std::vector<Box<MatchPattern>>{};
  for (auto const& element : pattern.elements) {
    TRY(auto nested, lower_match_pattern(*element, dh));
    elements.push_back(Box{std::move(nested)});
  }
  return MatchPattern{
    MatchPattern::List{std::move(elements), pattern.rest.is_some()}};
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
    [&](ast::list_pattern const& list) -> failure_or<MatchPattern> {
      return lower_match_pattern(list, dh);
    },
    [&](ast::record_pattern const& record) -> failure_or<MatchPattern> {
      return lower_match_pattern(record, dh);
    });
}

auto matches_list(data_view3 value, MatchPattern::List const& pattern) -> bool {
  auto const* list = std::get_if<list_view3>(&value);
  if (not list) {
    return false;
  }
  if (list->size() < pattern.elements.size()) {
    return false;
  }
  if (not pattern.has_rest and list->size() != pattern.elements.size()) {
    return false;
  }
  for (auto index = size_t{0}; index < pattern.elements.size(); ++index) {
    if (not matches_pattern(list->at(static_cast<int64_t>(index)),
                            *pattern.elements[index])) {
      return false;
    }
  }
  return true;
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
  if (auto list = std::get_if<MatchPattern::List>(&pattern.kind)) {
    return matches_list(value, *list);
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
    arm.wildcard
      = not ast_arm.guard
        and std::ranges::any_of(ast_arm.patterns, [&](auto& pattern) {
              return is_irrefutable_match_pattern(pattern, args.scrutinee, ctx);
            });
    if (arm.wildcard and arm_index + 1 != x.arms.size()) {
      diagnostic::error("irrefutable match arm must be last")
        .primary(arm.source)
        .emit(ctx);
      return failure::promise();
    }
    auto bindings = std::unordered_set<std::string>{};
    if (ast_arm.patterns.size() > 1) {
      auto expected_bindings = Option<std::unordered_set<std::string>>{None{}};
      for (auto const& pattern : ast_arm.patterns) {
        auto pattern_bindings = std::unordered_set<std::string>{};
        TRY(validate_match_pattern_bindings(pattern, ctx, pattern_bindings));
        if (expected_bindings and pattern_bindings != *expected_bindings) {
          diagnostic::error("alternatives must bind the same names")
            .primary(pattern)
            .hint("split this arm into multiple arms")
            .emit(ctx);
          return failure::promise();
        }
        expected_bindings = std::move(pattern_bindings);
        TRY(validate_record_pattern_scrutinee(pattern, args.scrutinee, ctx));
      }
    } else {
      for (auto const& pattern : ast_arm.patterns) {
        TRY(validate_match_pattern_bindings(pattern, ctx, bindings));
        TRY(validate_record_pattern_scrutinee(pattern, args.scrutinee, ctx));
      }
    }
    if (not arm.wildcard) {
      for (auto& pattern : ast_arm.patterns) {
        TRY(bind_match_pattern(pattern, ctx));
        arm.pattern_exprs.push_back(pattern);
      }
    }
    auto replacements = BindingMap{};
    for (auto const& pattern : ast_arm.patterns) {
      auto pattern_replacements = BindingMap{};
      auto path = BindingPath{};
      TRY(collect_match_pattern_bindings(pattern, args.scrutinee, path, ctx,
                                         pattern_replacements));
      if (replacements.empty()) {
        replacements = std::move(pattern_replacements);
      } else if (not binding_maps_equal(replacements, pattern_replacements)) {
        diagnostic::error("alternatives must bind the same names")
          .primary(pattern)
          .hint("split this arm into multiple arms")
          .emit(ctx);
        return failure::promise();
      } else {
        replacements.insert(pattern_replacements.begin(),
                            pattern_replacements.end());
      }
    }
    if (not replacements.empty()) {
      TRY(validate_match_binding_mutations(ast_arm.pipe, replacements, ctx));
      if (ast_arm.guard) {
        TRY(*ast_arm.guard, substitute_named_expressions(
                              std::move(*ast_arm.guard), replacements, ctx));
      }
      TRY(ast_arm.pipe, substitute_named_expressions(std::move(ast_arm.pipe),
                                                     replacements, ctx));
    }
    if (ast_arm.guard) {
      TRY(ast_arm.guard->bind(ctx));
      arm.guard = std::move(ast_arm.guard);
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
