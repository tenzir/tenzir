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
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/detail/heterogeneous_string_hash.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/nova/array.hpp"
#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/censor.hpp"
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
#include "tenzir/tql2/registry.hpp"
#include "tenzir/tql2/set.hpp"

#include <algorithm>
#include <ranges>
#include <string>
#include <unordered_map>

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

/// Returns the type name of the first row in `rows`, for diagnostics.
auto first_kind(nova::Array<nova::Data> const& values,
                nova::storage::BitMap const& rows) -> std::string_view {
  for (auto row : nova::storage::true_bits(rows)) {
    return match(values.get(row),
                 []<class T>(nova::RowView<T> const&) -> std::string_view {
                   return nova::Type<T>::static_name;
                 });
  }
  TENZIR_UNREACHABLE();
}

/// Splits the active `mask` rows of `values` into those of type `Tag`, nulls,
/// and everything else.
template <class Tag>
struct MetaValues {
  static auto make(nova::Array<nova::Data> const& values,
                   nova::storage::BitMap const& mask) -> MetaValues {
    auto alternative = values.get_alternative<Tag>();
    auto nulls = values.get_alternative<nova::Null>();
    auto matching = alternative ? mask & alternative->present
                                : nova::storage::BitMap{mask.length(), false};
    auto null = nulls ? mask & nulls->present
                      : nova::storage::BitMap{mask.length(), false};
    auto other = mask.and_not(matching).and_not(null);
    return MetaValues{
      .alternative = std::move(alternative),
      .matching = std::move(matching),
      .null = std::move(null),
      .other = std::move(other),
    };
  }

  Option<nova::MaskedArray<nova::Array<Tag>>> alternative;
  nova::storage::BitMap matching;
  nova::storage::BitMap null;
  nova::storage::BitMap other;
};

/// Takes the matching values, `fallback` for the other rows in `mask`, and
/// `old` elsewhere. A `None` fallback keeps `old`.
template <class Tag>
auto merge_meta(nova::Array<Tag> const& old, MetaValues<Tag> const& values,
                nova::storage::BitMap const& mask,
                Option<typename nova::Type<Tag>::ViewType> fallback)
  -> nova::Array<Tag> {
  auto const length = old.length();
  if (not values.matching.any() and not(fallback and mask.any())) {
    return old;
  }
  if (values.matching.true_count() == length) {
    return values.alternative->data;
  }
  auto builder = nova::ArrayBuilder<Tag>{};
  for (auto row = nova::storage::Index{0}; row < length; ++row) {
    if (values.matching.get(row)) {
      builder.data(*values.alternative->data.get(row));
    } else if (fallback and mask.get(row)) {
      builder.data(*fallback);
    } else {
      builder.data(*old.get(row));
    }
  }
  return builder.finish();
}

/// Assigns `values` to the metadata `target` of the active rows, with the
/// same diagnostics and fallbacks as the legacy `set`.
auto assign_meta(nova::Events& events, ast::meta const& target,
                 nova::Array<nova::Data> const& values, diagnostic_handler& dh)
  -> void {
  auto const& mask = events.mask;
  switch (target.kind) {
    case ast::meta::name: {
      auto split = MetaValues<nova::String>::make(values, mask);
      if (split.other.any()) {
        diagnostic::warning("expected string but got {}",
                            first_kind(values, split.other))
          .primary(target)
          .emit(dh);
      }
      if (split.null.any()) {
        diagnostic::warning("schema name must not be `null`")
          .primary(target)
          .emit(dh);
      }
      events.meta.name = merge_meta(events.meta.name, split, mask, None{});
      return;
    }
    case ast::meta::import_time: {
      // The import time is not nullable. The default-constructed time marks
      // `null`, and replaces values that are not times.
      auto split = MetaValues<nova::Time>::make(values, mask);
      if (split.other.any()) {
        diagnostic::warning("expected `time` but got `{}`",
                            first_kind(values, split.other))
          .primary(target)
          .emit(dh);
      }
      for (auto row : nova::storage::true_bits(split.matching)) {
        if (*split.alternative->data.get(row) == time{}) {
          diagnostic::warning("import time cannot be `{}`", time{})
            .primary(target)
            .hint("consider using `null` instead")
            .emit(dh);
          break;
        }
      }
      events.meta.import_time
        = merge_meta(events.meta.import_time, split, mask, time{});
      return;
    }
    case ast::meta::internal: {
      auto split = MetaValues<nova::Bool>::make(values, mask);
      if (split.other.any()) {
        diagnostic::warning("expected bool but got {}",
                            first_kind(values, split.other))
          .primary(target)
          .emit(dh);
      }
      if (split.null.any()) {
        diagnostic::warning("cannot set `@internal` to `null`")
          .primary(target)
          .emit(dh);
      }
      events.meta.internal
        = merge_meta(events.meta.internal, split, mask, None{});
      return;
    }
  }
  TENZIR_UNREACHABLE();
}

/// Warns about every type other than string that occurs in the active rows of
/// the `values` of the index `expression`.
auto warn_non_string_index(nova::Array<nova::Data> const& values,
                           nova::storage::BitMap const& mask,
                           ast::expression const& expression,
                           diagnostic_handler& dh) -> void {
  auto warn = [&]<nova::data_type Tag>(nova::Array<Tag> const&,
                                       nova::storage::BitMap const& present) {
    if constexpr (not std::same_as<Tag, nova::String>) {
      if ((mask & present).any()) {
        diagnostic::warning("assignment index must be a string, but got `{}`",
                            nova::Type<Tag>::static_name)
          .primary(expression)
          .emit(dh);
      }
    }
  };
  match(values, [&](auto const& array) {
    if constexpr (std::same_as<std::remove_cvref_t<decltype(array)>,
                               nova::UnionArray>) {
      for (auto const& alternative : array.fields()) {
        match(alternative.data, [&](auto const& typed) {
          warn(typed, alternative.present);
        });
      }
    } else {
      warn(array, mask);
    }
  });
}

/// The rows of a dynamic assignment that share the same computed field path.
struct DynamicGroup {
  std::vector<ast::field_path::segment> path;
  nova::storage::BitMap rows;
};

/// The rows of a dynamic assignment with valid indexes, grouped by the field
/// path that the indexes compute.
struct DynamicRows {
  nova::storage::BitMap valid;
  std::vector<DynamicGroup> groups;
};

/// Evaluates the indexes of `path` for the active rows of `input`. Rows with
/// an index that is not a string are skipped with a warning, like the legacy
/// `set`.
auto group_dynamic(DynamicPath const& path,
                   std::span<nova::Evaluator> evaluators,
                   nova::Events const& input, diagnostic_handler& dh)
  -> DynamicRows {
  auto const& mask = input.mask;
  auto valid = mask;
  auto keys = std::vector<nova::Array<nova::String>>{};
  auto evaluator = evaluators.begin();
  for (auto const& segment : path.segments) {
    auto const* expression = try_as<ast::expression>(segment);
    if (not expression) {
      continue;
    }
    TENZIR_ASSERT(evaluator != evaluators.end());
    auto values = evaluator->eval(input, nova::EvalCtx{dh});
    ++evaluator;
    auto strings = values.get_alternative<nova::String>();
    auto present = strings ? mask & strings->present
                           : nova::storage::BitMap{mask.length(), false};
    warn_non_string_index(values, mask, *expression, dh);
    valid = std::move(valid) & present;
    if (strings) {
      keys.push_back(std::move(strings->data));
    }
  }
  auto result = DynamicRows{.valid = std::move(valid), .groups = {}};
  if (not result.valid.any()) {
    return result;
  }
  // Every index has a string in a valid row, so there is one key per index.
  TENZIR_ASSERT(std::ranges::count_if(path.segments,
                                      [](auto const& segment) {
                                        return is<ast::expression>(segment);
                                      })
                == std::ssize(keys));
  // Rows with the same indexes form one group. With several indexes, the
  // lookup key concatenates the length-prefixed indexes of a row, so that it
  // is unambiguous.
  auto lookup
    = std::unordered_map<std::string, size_t, detail::heterogeneous_string_hash,
                         detail::heterogeneous_string_equal>{};
  auto rows = std::vector<nova::storage::BitMap::Mutable>{};
  auto buffer = std::string{};
  auto make_key = [&](nova::storage::Index row) -> std::string_view {
    if (keys.size() == 1) {
      return *keys.front().get(row);
    }
    buffer.clear();
    for (auto const& key : keys) {
      auto value = *key.get(row);
      auto size = value.size();
      buffer.append(reinterpret_cast<char const*>(&size), sizeof(size));
      buffer.append(value);
    }
    return buffer;
  };
  // Neighboring rows often share their indexes, so the last group is checked
  // before the lookup.
  auto last = Option<decltype(lookup)::iterator>{};
  for (auto row : nova::storage::true_bits(result.valid)) {
    auto key = make_key(row);
    if (last and (*last)->first == key) {
      rows[(*last)->second].set(row, true);
      continue;
    }
    auto it = lookup.find(key);
    if (it == lookup.end()) {
      auto segments = std::vector<ast::field_path::segment>{};
      segments.reserve(path.segments.size());
      auto index = keys.begin();
      for (auto const& segment : path.segments) {
        if (auto const* field = try_as<ast::field_path::segment>(segment)) {
          segments.push_back(*field);
          continue;
        }
        auto const& expression = as<ast::expression>(segment);
        segments.emplace_back(ast::identifier{std::string{*index->get(row)},
                                              expression.get_location()},
                              false);
        ++index;
      }
      result.groups.push_back(DynamicGroup{
        .path = std::move(segments),
        .rows = nova::storage::BitMap{},
      });
      rows.emplace_back(mask.length());
      it = lookup.emplace(key, rows.size() - 1).first;
    }
    rows[it->second].set(row, true);
    last = it;
  }
  for (auto [group, group_rows] : std::views::zip(result.groups, rows)) {
    group.rows = std::move(group_rows).finish();
  }
  return result;
}

/// Returns the records to assign the nested fields of `groups` into for the
/// `rows` of the `existing` field: its records, and empty records where it
/// holds none. Warns when this replaces a value other than `null`, like
/// `assign_nested_field`, naming the segment at `depth` of a group that
/// replaces it.
auto nested_records(nova::MaskedArray<nova::Array<nova::Data>> const& existing,
                    nova::storage::BitMap const& rows,
                    std::span<DynamicGroup const* const> groups, size_t depth,
                    diagnostic_handler& dh) -> nova::Array<nova::Record> {
  auto const active = rows & existing.present;
  auto warn = [&]<nova::data_type Tag>(nova::Array<Tag> const&,
                                       nova::storage::BitMap const& present) {
    if constexpr (not std::same_as<Tag, nova::Record>
                  and not std::same_as<Tag, nova::Null>) {
      auto const replaced = active & present;
      if (replaced.any()) {
        auto const* group = *std::ranges::find_if(groups, [&](auto* group) {
          return (group->rows & replaced).any();
        });
        auto const& next = group->path[depth];
        diagnostic::warning("implicit record for `{}` field overwrites `{}` "
                            "value",
                            next.id.name, nova::Type<Tag>::static_name)
          .primary(next.id)
          .hint("if this is intentional, drop the parent field before")
          .emit(dh);
      }
    }
  };
  match(existing.data, [&](auto const& array) {
    if constexpr (std::same_as<std::remove_cvref_t<decltype(array)>,
                               nova::UnionArray>) {
      for (auto const& alternative : array.fields()) {
        match(alternative.data, [&](auto const& typed) {
          warn(typed, alternative.present);
        });
      }
    } else {
      warn(array, existing.present);
    }
  });
  auto records = existing.data.get_alternative<nova::Record>();
  if (not records) {
    return nova::Array<nova::Record>::make_empty(rows.length());
  }
  auto replaced = rows.and_not(active & records->present);
  if (not replaced.any()) {
    return std::move(records->data);
  }
  return std::move(records->data).empty_where(std::move(replaced));
}

/// Assigns `value` to the path of each of `groups`, on the group's rows. The
/// groups' paths have the same length, and their rows are disjoint and
/// together make up `rows`. All paths share their first `depth` segments.
///
/// This has the effect of `assign_nested_field` for each group in turn, but
/// writes all fields of a level at once. Otherwise, every group would rebuild
/// the records along its path, at a cost that grows with the number of fields
/// that the groups before it added.
auto assign_groups(nova::Array<nova::Record> record,
                   std::span<DynamicGroup const* const> groups,
                   nova::storage::BitMap const& rows, size_t depth,
                   nova::Array<nova::Data> const& value, diagnostic_handler& dh)
  -> nova::Array<nova::Record> {
  using MaskedArray = nova::MaskedArray<nova::Array<nova::Data>>;
  TENZIR_ASSERT(not groups.empty());
  auto const length = record.length();
  auto const leaf = depth + 1 == groups.front()->path.size();
  // The groups by the field that they write at this level, in order of
  // appearance.
  struct Field {
    std::string_view name;
    std::vector<DynamicGroup const*> groups;
    nova::storage::BitMap rows;
  };
  auto fields = std::vector<Field>{};
  auto lookup = std::unordered_map<std::string_view, size_t>{};
  for (auto const* group : groups) {
    TENZIR_ASSERT_EQ(group->path.size(), groups.front()->path.size());
    auto const& name = group->path[depth].id.name;
    auto [it, inserted] = lookup.try_emplace(name, fields.size());
    if (inserted) {
      fields.push_back(Field{.name = name, .groups = {}, .rows = {}});
    }
    fields[it->second].groups.push_back(group);
  }
  for (auto& field : fields) {
    if (fields.size() == 1) {
      field.rows = rows;
      continue;
    }
    field.rows = field.groups.front()->rows;
    for (auto const* group : field.groups | std::views::drop(1)) {
      field.rows = std::move(field.rows) | group->rows;
    }
  }
  auto updates = std::vector<std::pair<std::string_view, MaskedArray>>{};
  updates.reserve(fields.size());
  if (leaf) {
    for (auto& field : fields) {
      updates.emplace_back(field.name,
                           MaskedArray{value, std::move(field.rows)});
    }
    return std::move(record).with_fields(
      nova::Array<nova::Record>::from_fields(updates));
  }
  // The fields that do not exist yet share the records of their nested
  // fields: as the rows of the groups are disjoint, each row of these records
  // only has the nested fields of its own group.
  auto values = std::vector<Option<nova::Array<nova::Data>>>{};
  values.reserve(fields.size());
  auto fresh_groups = std::vector<DynamicGroup const*>{};
  auto fresh_rows = Option<nova::storage::BitMap>{};
  for (auto const& field : fields) {
    auto existing = record.field(field.name);
    if (not existing) {
      std::ranges::copy(field.groups, std::back_inserter(fresh_groups));
      fresh_rows
        = fresh_rows ? std::move(*fresh_rows) | field.rows : field.rows;
      values.emplace_back(None{});
      continue;
    }
    auto nested
      = nested_records(*existing, field.rows, field.groups, depth + 1, dh);
    values.emplace_back(nova::Array<nova::Data>{assign_groups(
      std::move(nested), field.groups, field.rows, depth + 1, value, dh)});
  }
  auto fresh = Option<nova::Array<nova::Data>>{};
  if (fresh_rows) {
    fresh = nova::Array<nova::Data>{
      assign_groups(nova::Array<nova::Record>::make_empty(length), fresh_groups,
                    *fresh_rows, depth + 1, value, dh)};
  }
  for (auto [field, field_value] : std::views::zip(fields, values)) {
    updates.emplace_back(
      field.name, MaskedArray{field_value ? std::move(*field_value) : *fresh,
                              std::move(field.rows)});
  }
  return std::move(record).with_fields(
    nova::Array<nova::Record>::from_fields(updates));
}

/// Implements `set`/`select` for the nova columnar representation.
class SetNova final : public Operator<nova::Events, nova::Events> {
public:
  /// One assignment `SetNova` will apply, in order, to the original input.
  struct Field {
    /// A metadata reference, a field path, or a field path with computed
    /// segments. An empty `path()` means a bare `this = expr` assignment.
    AssignmentTarget target;
    location rhs_location;
    ast::expression rhs;
    /// The fields that a dynamic target moves, which are only dropped for
    /// rows with valid indexes. Owns the segment strings that `drop_tree`
    /// aliases.
    std::vector<ast::field_path> moved_fields;
    nova::DropTree drop_tree;
  };

  explicit SetNova(std::vector<ast::assignment> assignments) {
    fields_.reserve(assignments.size());
    for (auto& assignment : assignments) {
      auto [pruned, moved] = resolve_move_keyword(std::move(assignment));
      auto target = make_assignment_target(pruned.left);
      TENZIR_ASSERT(target);
      auto dynamic = is<DynamicPath>(*target);
      fields_.push_back(Field{
        .target = std::move(*target),
        .rhs_location = pruned.right.get_location(),
        .rhs = std::move(pruned.right),
        .moved_fields = {},
        .drop_tree = {},
      });
      if (dynamic) {
        // `fields_` does not reallocate, so the strings stay in place.
        auto& field = fields_.back();
        field.moved_fields = std::move(moved);
        field.drop_tree = nova::DropTree::make(field.moved_fields);
        continue;
      }
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
    index_evaluators_.reserve(fields_.size());
    for (auto& field : fields_) {
      auto evaluator
        = co_await nova::Evaluator::make(std::move(field.rhs), ctx);
      if (not evaluator) {
        co_return;
      }
      evaluators_.push_back(std::move(*evaluator));
      auto& indexes = index_evaluators_.emplace_back();
      auto* path = try_as<DynamicPath>(field.target);
      if (not path) {
        continue;
      }
      for (auto const& segment : path->segments) {
        auto const* expression = try_as<ast::expression>(segment);
        if (not expression) {
          continue;
        }
        // The target keeps its expressions for diagnostics.
        auto index = co_await nova::Evaluator::make(*expression, ctx);
        if (not index) {
          co_return;
        }
        indexes.push_back(std::move(*index));
      }
    }
  }

  auto process(nova::Events input, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_ASSERT(evaluators_.size() == fields_.size());
    TENZIR_ASSERT(index_evaluators_.size() == fields_.size());
    // Evaluated against the original input, like `Set`: earlier assignments
    // must not affect later right-hand sides or dynamic targets.
    auto values = std::vector<nova::MaskedArray<nova::Array<nova::Data>>>{};
    values.reserve(fields_.size());
    for (auto [field, evaluator] : std::views::zip(fields_, evaluators_)) {
      // Secrets may be used within expressions, but never stored in events.
      auto [value, censored]
        = nova::censor_secrets(evaluator.eval(input, nova::EvalCtx{ctx.dh()}));
      if (censored and input.mask.any()) {
        nova::warn_censored_secrets(field.rhs_location, ctx.dh());
      }
      values.push_back(nova::MaskedArray<nova::Array<nova::Data>>{
        std::move(value), input.mask});
    }
    auto dynamic_rows = std::vector<Option<DynamicRows>>{};
    dynamic_rows.reserve(fields_.size());
    for (auto [field, indexes] : std::views::zip(fields_, index_evaluators_)) {
      auto* path = try_as<DynamicPath>(field.target);
      dynamic_rows.push_back(
        path ? Option{group_dynamic(*path, indexes, input, ctx.dh())} : None{});
    }
    auto& data = input.data;
    if (not drop_tree_.empty()) {
      data = drop_tree_.apply(std::move(data), input.mask);
    }
    for (auto [field, rows] : std::views::zip(fields_, dynamic_rows)) {
      if (rows and not field.drop_tree.empty()) {
        data = field.drop_tree.apply(std::move(data), rows->valid);
      }
    }
    for (auto [field, value, rows] :
         std::views::zip(fields_, values, dynamic_rows)) {
      if (rows) {
        if (not rows->groups.empty()) {
          auto groups = std::vector<DynamicGroup const*>{};
          groups.reserve(rows->groups.size());
          for (auto const& group : rows->groups) {
            groups.push_back(&group);
          }
          data = assign_groups(std::move(data), groups, rows->valid, 0,
                               value.data, ctx.dh());
        }
        continue;
      }
      auto const& selector = as<ast::selector>(field.target);
      if (auto* meta = try_as<ast::meta>(&selector)) {
        assign_meta(input, *meta, value.data, ctx.dh());
        continue;
      }
      auto path = as<ast::field_path>(selector).path();
      if (path.empty()) {
        data = nova::records_or_empty(std::move(value), data.length(),
                                      field.rhs_location, ctx.dh());
        continue;
      }
      data = nova::assign_nested_field(std::move(data), path, std::move(value),
                                       ctx.dh());
    }
    co_await push(std::move(input));
  }

private:
  std::vector<Field> fields_;
  std::vector<nova::Evaluator> evaluators_;
  /// The evaluators for the computed segments of each field's target.
  std::vector<std::vector<nova::Evaluator>> index_evaluators_;
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

/// Checks that `assignment` is one `SetNova` can handle. Rejects `move this`,
/// as there is no field to drop for a `this`-level move.
auto validate_nova_target(ast::assignment const& assignment,
                          diagnostic_handler& dh) -> failure_or<void> {
  TRY(validate_assignment_target(assignment.left, dh));
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

using Segments = std::span<ast::field_path::segment const>;

/// Returns whether `prefix` is a prefix of `path`, comparing segment names.
auto is_prefix(Segments prefix, Segments path) -> bool {
  if (prefix.size() > path.size()) {
    return false;
  }
  auto const name = [](ast::field_path::segment const& segment) {
    return std::string_view{segment.id.name};
  };
  return std::ranges::equal(prefix, path.first(prefix.size()), {}, name, name);
}

auto make_null(location source) -> ast::expression {
  return ast::constant{caf::none, source};
}

/// Appends field accesses for `path` to `base`.
auto append_path(ast::expression base, Segments path) -> ast::expression {
  for (auto const& segment : path) {
    base = ast::field_access{
      std::move(base),
      segment.id.location,
      segment.has_question_mark,
      segment.id,
    };
  }
  return base;
}

auto access_field(ast::expression const& base, Segments path)
  -> Option<ast::expression>;

/// Returns an expression for the field `path` of the value of a record
/// literal, or `None` if the value depends on the evaluator.
auto access_record_field(ast::record const& record, Segments path)
  -> Option<ast::expression> {
  TENZIR_ASSERT(not path.empty());
  auto const& name = path.front().id.name;
  auto field = Option<size_t>{};
  auto spread = Option<size_t>{};
  for (auto [index, item] : detail::enumerate(record.items)) {
    if (auto* x = try_as<ast::record::field>(item)) {
      if (x->name.name == name) {
        field = index;
      }
      continue;
    }
    if (spread) {
      // Several spreads do not define a single field order.
      return None{};
    }
    spread = index;
  }
  if (field) {
    // The legacy evaluator applies the items in order, but the Nova evaluator
    // applies literal fields over the spread. Both agree only if the spread
    // comes first.
    if (spread and *spread > *field) {
      return None{};
    }
    return access_field(as<ast::record::field>(record.items[*field]).expr,
                        path.subspan(1));
  }
  if (spread) {
    return access_field(as<ast::spread>(record.items[*spread]).expr, path);
  }
  // Reading a field that the literal does not define yields `null`.
  return make_null(path.front().id.location);
}

/// Returns an expression for the field `path` of the value of `base`, or
/// `None` if there is no exact equivalent.
auto access_field(ast::expression const& base, Segments path)
  -> Option<ast::expression> {
  if (path.empty()) {
    return base;
  }
  if (auto* record = try_as<ast::record>(base)) {
    return access_record_field(*record, path);
  }
  if (is<ast::this_>(base)) {
    auto const& first = path.front();
    return append_path(ast::root_field{first.id, first.has_question_mark},
                       path.subspan(1));
  }
  if (auto* constant = try_as<ast::constant>(base);
      constant and is<caf::none_t>(constant->value)) {
    return make_null(path.front().id.location);
  }
  return append_path(base, path);
}

/// Detects a `move` that `resolve_move_keyword` left in place, such as one
/// within a lambda.
class MoveFinder final : public ast::visitor<MoveFinder> {
public:
  template <class T>
  auto visit(T& x) -> void {
    enter(x);
  }

  auto visit(ast::unary_expr& x) -> void {
    found = found or x.op == ast::unary_op::move;
    enter(x);
  }

  bool found = false;
};

/// Resolves the fields and metadata of a `set`'s output to expressions over
/// its input.
///
/// Every right-hand side reads the original input. The fields that a `move`
/// reads are dropped before the first assignment, and the assignments then
/// apply in order. So the last assignment whose target covers a field
/// determines the field's value.
class SetRewrite {
public:
  explicit SetRewrite(std::vector<ast::assignment> const& assignments)
    : registry_{global_registry()} {
    for (auto const& assignment : assignments) {
      auto [resolved, moved] = resolve_move_keyword(assignment);
      auto target = make_assignment_target(resolved.left);
      auto entry = Entry{.right = std::move(resolved.right)};
      if (not target) {
        // Substitution validates the targets, so this is merely defensive.
        entry.dynamic = true;
        entry.path.emplace();
      } else if (auto* selector = try_as<ast::selector>(&*target)) {
        if (auto* path = try_as<ast::field_path>(&*selector)) {
          entry.path.emplace(path->path().begin(), path->path().end());
        } else {
          entry.meta = as<ast::meta>(*selector).kind;
        }
      } else {
        // A dynamic target writes a computed field below its static prefix.
        entry.dynamic = true;
        entry.path.emplace();
        for (auto const& segment : as<DynamicPath>(*target).segments) {
          auto* field = try_as<ast::field_path::segment>(segment);
          if (not field) {
            break;
          }
          entry.path->push_back(*field);
        }
      }
      for (auto& field : moved) {
        moved_.push_back(Moved{
          .path = {field.path().begin(), field.path().end()},
          // A dynamic assignment only moves for rows with valid indexes, and
          // `move this` does not drop anything.
          .conditional = entry.dynamic or field.path().empty(),
        });
      }
      entries_.push_back(std::move(entry));
    }
  }

  auto resolve_meta(ast::meta const& meta) const -> Option<ast::expression> {
    for (auto const& entry : std::views::reverse(entries_)) {
      if (entry.meta == meta.kind) {
        return assigned_meta(meta, entry.right);
      }
    }
    return ast::expression{meta};
  }

  auto resolve(ast::field_path const& field) const -> Option<ast::expression> {
    auto path = field.path();
    for (auto const& entry : std::views::reverse(entries_)) {
      if (not entry.path) {
        continue;
      }
      auto target = Segments{*entry.path};
      if (entry.dynamic) {
        if (is_prefix(target, path) or is_prefix(path, target)) {
          return None{};
        }
        continue;
      }
      if (is_prefix(target, path)) {
        // An assignment to `this` turns values other than records into empty
        // records, so only its fields have a direct equivalent.
        if (path.empty()) {
          return None{};
        }
        auto result = access_field(entry.right, path.subspan(target.size()));
        if (not result or not substitutable(*result)) {
          return None{};
        }
        return result;
      }
      if (is_prefix(path, target)) {
        // The assignment changes part of the field.
        return None{};
      }
    }
    auto dropped = std::ranges::any_of(moved_, [&](Moved const& moved) {
      return not moved.conditional and is_prefix(moved.path, path);
    });
    if (dropped) {
      return make_null(field.get_location());
    }
    auto overlaps = std::ranges::any_of(moved_, [&](Moved const& moved) {
      return is_prefix(moved.path, path) or is_prefix(path, moved.path);
    });
    if (overlaps) {
      return None{};
    }
    return field.inner();
  }

private:
  struct Entry {
    /// The target path, or its static prefix for a dynamic target. `None` for
    /// metadata targets.
    Option<std::vector<ast::field_path::segment>> path = None{};
    /// The metadata target, if any.
    Option<ast::meta_kind> meta = None{};
    bool dynamic = false;
    ast::expression right = {};
  };

  /// Returns the value of `meta` after assigning `right` to it, if both
  /// executors agree on it for every event. Only constants qualify, because
  /// the executors keep or reset metadata on values of the wrong type.
  static auto assigned_meta(ast::meta const& meta, ast::expression const& right)
    -> Option<ast::expression> {
    auto const* constant = try_as<ast::constant>(right);
    if (not constant) {
      return None{};
    }
    auto result = *constant;
    result.source = meta.source;
    switch (meta.kind) {
      case ast::meta::name: {
        // A value other than a string keeps the previous name. The executors
        // disagree on an empty name.
        auto const* name = try_as<std::string>(constant->value);
        if (not name or name->empty()) {
          return None{};
        }
        return result;
      }
      case ast::meta::import_time: {
        // A value other than a time resets the import time to `null`, and so
        // does the epoch, which marks `null`.
        auto const* value = try_as<time>(constant->value);
        if (not value or *value == time{}) {
          return make_null(meta.source);
        }
        return result;
      }
      case ast::meta::internal:
        // A value other than a bool keeps the previous flag.
        if (not is<bool>(constant->value)) {
          return None{};
        }
        return result;
    }
    TENZIR_UNREACHABLE();
  }

  struct Moved {
    std::vector<ast::field_path::segment> path;
    bool conditional = false;
  };

  /// Returns whether the predicate may evaluate `expr` in place of the
  /// assignment, which requires the same value and no side effects.
  auto substitutable(ast::expression const& expr) const -> bool {
    if (not expr.is_deterministic(*registry_)) {
      return false;
    }
    auto finder = MoveFinder{};
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
    finder.visit(const_cast<ast::expression&>(expr));
    return not finder.found;
  }

  std::shared_ptr<registry const> registry_;
  std::vector<Entry> entries_;
  std::vector<Moved> moved_;
};

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
  order_ = weaker_event_order(order_, req.order);
  // Predicates that we can rewrite over our input move upstream.
  auto rewrite = SetRewrite{assignments_};
  auto [filter_upstream, filter_self] = ir::split_filter_by_substitution(
    std::move(req.filter),
    [&](ast::field_path const& field) {
      return rewrite.resolve(field);
    },
    [&](ast::meta const& meta) {
      return rewrite.resolve_meta(meta);
    });
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
