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
#include "tenzir/nova/array.hpp"
#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/eval_ctx.hpp"
#include "tenzir/nova/eval_util.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/type_system.hpp"
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

using PathSegment = variant<ast::field_path::segment, ast::expression>;

struct DynamicPath {
  ast::expression source;
  std::vector<PathSegment> segments;
};

using AssignmentTarget = variant<ast::selector, DynamicPath>;

struct AssignmentTargetParser {
  auto parse(ast::expression const& expression) -> bool {
    return expression.match(
      [&](ast::this_ const&) {
        return true;
      },
      [&](ast::root_field const& field) {
        if (field.has_question_mark) {
          return false;
        }
        segments.emplace_back(ast::field_path::segment{field.id, false});
        return true;
      },
      [&](ast::field_access const& access) {
        if (access.has_question_mark or not parse(access.left)) {
          return false;
        }
        segments.emplace_back(ast::field_path::segment{access.name, false});
        return true;
      },
      [&](ast::index_expr const& index) {
        if (index.has_question_mark or not parse(index.expr)) {
          return false;
        }
        if (auto* constant = try_as<ast::constant>(index.index)) {
          auto* name = try_as<std::string>(constant->value);
          if (not name) {
            return false;
          }
          segments.emplace_back(ast::field_path::segment{
            ast::identifier{*name, constant->source},
            false,
          });
        } else {
          segments.emplace_back(index.index);
        }
        return true;
      },
      [](auto const&) {
        return false;
      });
  }

  std::vector<PathSegment> segments;
};

auto make_assignment_target(ast::expression expression)
  -> Option<AssignmentTarget> {
  if (auto selector = ast::selector::try_from(expression)) {
    return AssignmentTarget{std::move(*selector)};
  }
  auto parser = AssignmentTargetParser{};
  if (not parser.parse(expression)) {
    return None{};
  }
  return AssignmentTarget{DynamicPath{
    .source = std::move(expression),
    .segments = std::move(parser.segments),
  }};
}

struct ResolvedAssignment {
  ast::assignment assignment;
  AssignmentTarget target;
  std::vector<ast::field_path> moved_fields;
};

auto assign_dynamic(DynamicPath const& path, const series& right,
                    std::span<basic_series<string_type> const> indexes,
                    int64_t offset, table_slice const& input,
                    diagnostic_handler& dh) -> std::vector<table_slice> {
  for (auto const& values : indexes) {
    TENZIR_ASSERT(values.length() >= offset + right.length());
  }
  auto same_keys = [&](int64_t lhs, int64_t rhs) {
    return std::ranges::all_of(indexes, [&](auto const& values) {
      return values.at(offset + lhs) == values.at(offset + rhs);
    });
  };
  auto result = std::vector<table_slice>{};
  for (auto begin = int64_t{0}; begin < right.length();) {
    auto end = begin + 1;
    while (end < right.length() and same_keys(begin, end)) {
      ++end;
    }
    auto fields = std::vector<ast::field_path::segment>{};
    fields.reserve(path.segments.size());
    auto valid = true;
    auto index = size_t{0};
    for (auto const& segment : path.segments) {
      if (auto* field = try_as<ast::field_path::segment>(segment)) {
        fields.push_back(*field);
        continue;
      }
      auto const& expression = as<ast::expression>(segment);
      if (auto key = indexes[index].at(offset + begin)) {
        fields.emplace_back(
          ast::identifier{std::string{*key}, expression.get_location()}, false);
      } else {
        valid = false;
      }
      ++index;
    }
    auto part = subslice(input, begin, end);
    if (valid) {
      auto field = ast::field_path::make(path.source, false, std::move(fields));
      result.push_back(assign(field, right.slice(begin, end), part, dh));
    } else {
      result.push_back(std::move(part));
    }
    begin = end;
  }
  return result;
}

using AssignmentIndexes = std::vector<basic_series<string_type>>;

auto evaluate_indexes(AssignmentTarget const& target, table_slice const& input,
                      OpCtx& ctx) -> AssignmentIndexes {
  auto result = AssignmentIndexes{};
  auto* path = try_as<DynamicPath>(target);
  if (not path) {
    return result;
  }
  for (auto const& segment : path->segments) {
    auto* expression = try_as<ast::expression>(segment);
    if (not expression) {
      continue;
    }
    auto evaluated = eval(*expression, input, ctx);
    auto normalized = multi_series{};
    for (auto& part : evaluated) {
      if (part.length() == 0) {
        continue;
      }
      if (auto strings = part.as<string_type>()) {
        if (strings->array->null_count() > 0) {
          diagnostic::warning(
            "assignment index must be a string, but got `null`")
            .primary(*expression, "is null")
            .emit(ctx);
        }
        normalized.append(series{std::move(*strings)});
      } else {
        diagnostic::warning("assignment index must be a string, but got `{}`",
                            part.type.kind())
          .primary(*expression)
          .emit(ctx);
        normalized.append(
          series{basic_series<string_type>::null(part.length())});
      }
    }
    if (evaluated.length() == 0) {
      result.push_back(basic_series<string_type>::null(0));
      continue;
    }
    auto joined = normalized.to_series();
    TENZIR_ASSERT(joined.status == multi_series::to_series_result::status::ok);
    auto strings = joined.series.as<string_type>();
    TENZIR_ASSERT(strings);
    result.push_back(std::move(*strings));
  }
  return result;
}

auto has_valid_indexes(AssignmentIndexes const& indexes, int64_t row) -> bool {
  return std::ranges::all_of(indexes, [row](auto const& values) {
    return values.at(row).has_value();
  });
}

auto prepare_state(std::span<ResolvedAssignment const> assignments,
                   std::span<AssignmentIndexes const> indexes,
                   const table_slice& input, int64_t offset,
                   diagnostic_handler& dh) -> std::vector<table_slice> {
  TENZIR_ASSERT(assignments.size() == indexes.size());
  auto same_validity = [&](int64_t lhs, int64_t rhs) {
    return std::ranges::all_of(indexes, [&](auto const& values) {
      return has_valid_indexes(values, offset + lhs)
             == has_valid_indexes(values, offset + rhs);
    });
  };
  auto result = std::vector<table_slice>{};
  auto rows = detail::narrow<int64_t>(input.rows());
  for (auto begin = int64_t{0}; begin < rows;) {
    auto end = begin + 1;
    while (end < rows and same_validity(begin, end)) {
      ++end;
    }
    auto moved = std::vector<ast::field_path>{};
    for (auto [assignment, values] : std::views::zip(assignments, indexes)) {
      if (has_valid_indexes(values, offset + begin)) {
        std::ranges::copy(assignment.moved_fields, std::back_inserter(moved));
      }
    }
    result.push_back(drop(subslice(input, begin, end), moved, dh, false));
    begin = end;
  }
  return result;
}

auto assign_target(AssignmentTarget const& target, series right,
                   AssignmentIndexes const& indexes, int64_t offset,
                   table_slice const& input, diagnostic_handler& dh)
  -> std::vector<table_slice> {
  return target.match(
    [&](ast::selector const& selector) {
      TENZIR_ASSERT(indexes.empty());
      return assign(selector, std::move(right), input, dh);
    },
    [&](DynamicPath const& path) {
      return assign_dynamic(path, right, indexes, offset, input, dh);
    });
}

class Set final : public Operator<table_slice, table_slice> {
public:
  Set(std::vector<ast::assignment> assignments, EventOrder order)
    : assignments_{std::move(assignments)}, order_{order} {
    if (std::ranges::any_of(assignments_, [](auto const& assignment) {
          return not ast::selector::try_from(assignment.left);
        })) {
      dynamic_assignments_.reserve(assignments_.size());
      for (auto& assignment : assignments_) {
        auto [pruned_assignment, moved_fields]
          = resolve_move_keyword(std::move(assignment));
        auto target = make_assignment_target(pruned_assignment.left);
        TENZIR_ASSERT(target);
        dynamic_assignments_.emplace_back(std::move(pruned_assignment),
                                          std::move(*target),
                                          std::move(moved_fields));
      }
      assignments_.clear();
      return;
    }
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
    -> Task<void> override {
    auto results = std::vector<table_slice>{};
    if (not dynamic_assignments_.empty()) {
      // Right-hand sides and dynamic indexes are evaluated against the original
      // input, so preceding assignments do not affect later values or targets.
      auto values = std::vector<multi_series>{};
      auto indexes = std::vector<AssignmentIndexes>{};
      for (auto const& assignment : dynamic_assignments_) {
        values.push_back(eval(assignment.assignment.right, input, ctx));
        indexes.push_back(evaluate_indexes(assignment.target, input, ctx));
      }
      auto begin = int64_t{0};
      for (auto values_slice : split_multi_series(values)) {
        TENZIR_ASSERT(not values_slice.empty());
        auto end = begin + values_slice[0].length();
        auto state = prepare_state(dynamic_assignments_, indexes,
                                   subslice(input, begin, end), begin, ctx);
        auto new_state = std::vector<table_slice>{};
        for (auto [assignment, assignment_indexes, right] :
             std::views::zip(dynamic_assignments_, indexes, values_slice)) {
          auto offset = int64_t{0};
          for (auto const& entry : state) {
            auto entry_rows = detail::narrow<int64_t>(entry.rows());
            auto assigned
              = assign_target(assignment.target,
                              right.slice(offset, offset + entry_rows),
                              assignment_indexes, begin + offset, entry, ctx);
            offset += entry_rows;
            std::ranges::move(assigned, std::back_inserter(new_state));
          }
          std::swap(state, new_state);
          new_state.clear();
        }
        std::ranges::move(state, std::back_inserter(results));
        begin = end;
      }
    } else {
      // The right-hand side is always evaluated with the original input,
      // because side-effects from preceding assignments shall not be reflected
      // when calculating the value of the left-hand side.
      auto values = std::vector<multi_series>{};
      for (const auto& assignment : assignments_) {
        values.push_back(eval(assignment.right, input, ctx));
      }
      input = drop(input, moved_fields_, ctx, false);
      // After we know all the multi series values on the right, we can split
      // the input table slice and perform the actual assignment.
      auto begin = int64_t{0};
      for (auto values_slice : split_multi_series(values)) {
        TENZIR_ASSERT(not values_slice.empty());
        auto end = begin + values_slice[0].length();
        // We could still perform further splits if metadata is assigned.
        auto state = std::vector<table_slice>{};
        state.push_back(subslice(input, begin, end));
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
    }
    // TODO: Consider adding a property to function plugins that let's them
    // indicate whether they want their outputs to be strictly ordered. If any
    // of the called functions has this requirement, then we should not be
    // making this optimization. This will become relevant in the future once we
    // allow functions to be stateful.
    if (order_ != EventOrder::ordered) {
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
  EventOrder order_{};
  std::vector<ast::field_path> moved_fields_;
  std::vector<ResolvedAssignment> dynamic_assignments_;
};

/// Extracts the `Array<Record>` alternative from `value`, falling back to an
/// empty record of `length` rows if it isn't record-typed.
auto extract_record_or_empty(nova::MaskedArray<nova::Array<nova::Data>> value,
                             nova::storage::Index length, location rhs,
                             diagnostic_handler& dh)
  -> nova::Array<nova::Record> {
  if (auto record = value.data.try_as<nova::Record>()) {
    return std::move(*record);
  }
  auto result = nova::Array<nova::Record>::make_empty(length);
  if (auto* u = try_as<nova::UnionArray>(value.data)) {
    auto alt = u->get_alternative<nova::Record>();
    if (alt) {
      // The alternative spans every row, but only the rows in `present` are
      // actually records; the others must become empty records here.
      result = std::move(alt->data).empty_where(alt->present.make_inverted());
    }
    if (value.present.and_not(u->alternative_mask<nova::Record>())
          .and_not(u->alternative_mask<nova::Null>())
          .any()) {
      diagnostic::warning("expected `record`").primary(rhs).emit(dh);
    }
    return result;
  }
  if (not value.data.try_as<nova::Null>()) {
    diagnostic::warning("expected `record`").primary(rhs).emit(dh);
  }
  return result;
}

/// Implements `set`/`select` for the nova columnar representation.
class SetNova final : public Operator<nova::Events, nova::Events> {
public:
  /// One assignment `SetNova` will apply, in order, to the original input.
  struct Field {
    /// Empty `path()` means a bare `this = expr` assignment.
    ast::field_path target;
    location rhs_location;
    ast::expression rhs;
  };

  explicit SetNova(std::vector<ast::assignment> assignments) {
    fields_.reserve(assignments.size());
    for (auto& assignment : assignments) {
      auto [pruned, moved] = resolve_move_keyword(std::move(assignment));
      auto selector = ast::selector::try_from(pruned.left);
      TENZIR_ASSERT(selector);
      auto* target = try_as<ast::field_path>(&*selector);
      TENZIR_ASSERT(target);
      fields_.push_back(Field{
        .target = *target,
        .rhs_location = pruned.right.get_location(),
        .rhs = std::move(pruned.right),
      });
      std::ranges::move(moved, std::back_inserter(moved_fields_));
    }
    drop_tree_ = nova::DropTree::make(moved_fields_);
  }

  // `drop_tree_` aliases string data owned by `moved_fields_`, so `SetNova`
  // must never be copied (only moved) and `moved_fields_` must not be
  // mutated after construction.
  SetNova(const SetNova&) = delete;
  SetNova(SetNova&&) = default;
  auto operator=(const SetNova&) -> SetNova& = delete;
  auto operator=(SetNova&&) -> SetNova& = default;
  ~SetNova() override = default;

  auto start(OpCtx& ctx) -> Task<void> override {
    evaluators_.reserve(fields_.size());
    for (auto& field : fields_) {
      auto evaluator = nova::Evaluator::make(
        std::move(field.rhs), nova::InstantiateCtx{ctx.dh(), ctx.reg()});
      if (not evaluator) {
        co_return;
      }
      evaluators_.push_back(std::move(*evaluator));
    }
  }

  auto process(nova::Events input, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_ASSERT(evaluators_.size() == fields_.size());
    // Evaluated against the original input, like `Set`: earlier assignments
    // must not affect later right-hand sides.
    auto values = std::vector<nova::MaskedArray<nova::Array<nova::Data>>>{};
    values.reserve(fields_.size());
    for (auto& evaluator : evaluators_) {
      values.push_back(nova::MaskedArray<nova::Array<nova::Data>>{
        evaluator.eval(input, nova::EvalCtx{ctx.dh()}), input.mask});
    }
    auto& data = input.data;
    if (not drop_tree_.empty()) {
      data = drop_tree_.apply(std::move(data), input.mask);
    }
    for (auto [field, value] : std::views::zip(fields_, values)) {
      auto path = field.target.path();
      if (path.empty()) {
        data = extract_record_or_empty(std::move(value), data.length(),
                                       field.rhs_location, ctx.dh());
        continue;
      }
      data = nova::assign_nested_field(std::move(data), path, std::move(value));
    }
    co_await push(std::move(input));
  }

private:
  std::vector<Field> fields_;
  std::vector<nova::Evaluator> evaluators_;
  /// Field paths moved out of via `move` in a right-hand side; owns the
  /// segment strings that `drop_tree_` aliases as `string_view`s.
  std::vector<ast::field_path> moved_fields_;
  nova::DropTree drop_tree_;
};

} // namespace

auto validate_assignment_target(ast::expression const& expression,
                                diagnostic_handler& dh) -> failure_or<void> {
  if (make_assignment_target(expression)) {
    return {};
  }
  diagnostic::error(
    "left side of `=` must be a field path or metadata reference")
    .primary(expression)
    .emit(dh);
  return failure::promise();
}

/// Checks that `assignment`'s left side is a target `SetNova` can handle: a
/// field path of any depth, or `this`. Rejects metadata (`@x`) and
/// dynamic/computed paths (`$var`, `foo[expr]`), and `move this` (there is
/// no field to drop for a `this`-level move).
auto validate_nova_target(ast::assignment const& assignment,
                          diagnostic_handler& dh) -> failure_or<void> {
  auto const& expression = assignment.left;
  auto selector = ast::selector::try_from(expression);
  const auto* path = selector ? try_as<ast::field_path>(&*selector) : nullptr;
  if (path == nullptr) {
    diagnostic::error("set operator does not yet support this assignment "
                      "target with nova events")
      .primary(expression, "expected a field path or `this`, e.g. `x`, "
                           "`x.y`, or `this`")
      .emit(dh);
    return failure::promise();
  }
  auto [resolved, moved] = resolve_move_keyword(assignment);
  TENZIR_UNUSED(resolved);
  for (const auto& field : moved) {
    if (field.path().empty()) {
      diagnostic::error("cannot `move this` with nova events")
        .primary(field)
        .hint("use `this = {{}}` to clear the event instead")
        .emit(dh);
      return failure::promise();
    }
  }
  return {};
}

/// Checks that `expression` is a target `SetNova` (the `nova::Events`
/// implementation of `set`) can handle: a simple, static, top-level field
/// reference, e.g. `x` in `x = expr`. Rejects metadata targets (`@name`),
/// `this`, nested paths (`foo.bar`), and optional-access targets (`foo?`) —
/// `SetNova` has no equivalent to `Set`'s dynamic-path/metadata machinery.
auto validate_simple_nova_target(ast::expression const& expression,
                                 diagnostic_handler& dh) -> failure_or<void> {
  auto selector = ast::selector::try_from(expression);
  const auto* path = selector ? try_as<ast::field_path>(&*selector) : nullptr;
  if (path != nullptr and not path->has_this() and path->path().size() == 1
      and not path->path()[0].has_question_mark) {
    return {};
  }
  diagnostic::error("set operator does not yet support this assignment "
                    "target with nova events")
    .primary(expression, "expected a simple top-level field name, e.g. `x`")
    .emit(dh);
  return failure::promise();
}

ir::SetIr::SetIr() : order_{EventOrder::ordered} {
}

ir::SetIr::SetIr(std::vector<ast::assignment> assignments)
  : assignments_{std::move(assignments)}, order_{EventOrder::ordered} {
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
    // Dynamic indexes may contain `$` variables, so validate the target after
    // substitution.
    TRY(x.left.substitute(ctx));
    TRY(validate_assignment_target(x.left, ctx));
    TRY(x.right.substitute(ctx));
  }
  return {};
}

auto ir::SetIr::spawn(element_type_tag input) const -> AnyOperator {
  if (input.is<table_slice>()) {
    return Set{assignments_, order_}.with_name("set");
  }
  TENZIR_ASSERT(input.is<nova::Events>());
  return SetNova{assignments_}.with_name("set");
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

/// Computes the projection to push upstream of a `set` from the projection
/// that downstream needs. Assignments are walked in reverse: a field that
/// downstream needs and that an assignment writes is replaced by whatever the
/// right-hand side references, and `this = ...` discards everything upstream.
/// This covers `select`, which desugars to `this = {}` followed by `x = x`.
auto projection_for_set(const std::vector<ast::assignment>& assignments,
                        Option<ir::OptimizeProjection> downstream,
                        const ir::OptimizeFilter& filter_self)
  -> Option<ir::OptimizeProjection> {
  // Fields of our output that downstream needs, or `None` for all of them.
  // Predicates kept behind us run on our output, so they count as downstream.
  auto survivors = std::move(downstream);
  for (const auto& expr : filter_self) {
    ir::add_refs_to_projection(survivors, expr);
  }
  // Fields of our input that the relevant right-hand sides reference.
  auto extra = Option<ir::OptimizeProjection>{ir::OptimizeProjection{}};
  for (const auto& assignment : std::views::reverse(assignments)) {
    auto left = ast::selector::try_from(assignment.left);
    if (not left) {
      // Dynamic targets read index expressions and preserve input structure
      // that a static field path cannot describe safely.
      return None{};
    }
    const auto* path = try_as<ast::field_path>(&*left);
    if (path == nullptr) {
      // Meta selectors such as `@name` do not touch the event's fields.
      ir::add_refs_to_projection(extra, assignment.right);
      continue;
    }
    if (path->path().empty()) {
      // `this = ...` replaces the whole event. Nothing of our input survives
      // except what the right-hand side references.
      survivors = ir::OptimizeProjection{};
      ir::add_refs_to_projection(extra, assignment.right);
      continue;
    }
    if (survivors) {
      const auto needed
        = std::ranges::any_of(*survivors, [&](const ast::field_path& survivor) {
            return ir::is_field_path_prefix(*path, survivor)
                   or ir::is_field_path_prefix(survivor, *path);
          });
      if (not needed) {
        continue;
      }
      // Downstream reads the assigned value, not the input. Drop the survivors
      // that the assignment overwrites, but keep the ancestors it writes into.
      std::erase_if(*survivors, [&](const ast::field_path& survivor) {
        return ir::is_field_path_prefix(*path, survivor);
      });
    }
    ir::add_refs_to_projection(extra, assignment.right);
  }
  ir::merge_projection(survivors, extra);
  return survivors;
}

} // namespace

auto ir::SetIr::optimize(ir::OptimizeRequest req,
                         const ir::OptimizeCtx& octx) && -> ir::OptimizeResult {
  TENZIR_UNUSED(octx);
  auto filter = std::move(req.filter);
  order_ = weaker_event_order(order_, req.order);
  auto touched_paths = touched_fields_for_set(assignments_);
  auto split = touched_paths
                 ? ir::split_filter_by_dependents(
                     std::move(filter),
                     ast::ExprRefs{.field_paths = std::move(*touched_paths)})
                 : ir::split_filter_result{{}, std::move(filter)};
  auto [filter_upstream, filter_self] = std::move(split);
  // A carried limit counts events after the whole filter chain. If we keep
  // predicates behind us, upstream can no longer honor it.
  auto limit = filter_self.empty() ? req.limit : Option<uint64_t>{};
  auto projection
    = projection_for_set(assignments_, std::move(req.projection), filter_self);
  auto ops = std::vector<Box<ir::Operator>>{};
  ops.reserve(1 + filter_self.size());
  ops.emplace_back(ir::SetIr{std::move(*this)});
  for (auto& expr : filter_self) {
    ops.push_back(make_where_ir(expr));
  }
  return {
    .filter = std::move(filter_upstream),
    .order = order_,
    .replacement = ir::pipeline{{}, std::move(ops)},
    .limit = limit,
    .projection = std::move(projection),
  };
}

auto ir::SetIr::infer_type(element_type_tag input, diagnostic_handler& dh) const
  -> failure_or<element_type_tag> {
  if (input.is<table_slice>()) {
    return input;
  }
  if (input.is<nova::Events>()) {
    for (const auto& assignment : assignments_) {
      TRY(validate_nova_target(assignment, dh));
    }
    return input;
  }
  diagnostic::error("set operator expected events").emit(dh);
  return failure::promise();
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
