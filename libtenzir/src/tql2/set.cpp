//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/set.hpp"

#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/collect.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/rebatch.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/type.hpp"

#include <arrow/compute/api_scalar.h>
#include <caf/detail/is_complete.hpp>

#include <algorithm>
#include <ranges>
#include <type_traits>
#include <utility>

namespace tenzir {

namespace {

auto is_null_constant_expression(const ast::expression& expr) -> bool {
  const auto* constant = try_as<ast::constant>(expr);
  return constant and is<caf::none_t>(constant->value);
}

auto is_timestamp_selector(const ast::selector& selector) -> bool {
  const auto* meta = std::get_if<ast::meta>(&selector);
  return meta and meta->kind == ast::meta::timestamp;
}

auto serialize_timestamp_field(const ast::field_path& path,
                               diagnostic_handler& dh, location primary)
  -> std::optional<std::string> {
  auto result = std::string{};
  auto add_dot = false;
  for (const auto& segment : path.path()) {
    if (segment.has_question_mark) {
      diagnostic::error("`@timestamp` does not support optional field paths")
        .primary(primary)
        .secondary(segment.id)
        .emit(dh);
      return std::nullopt;
    }
    if (segment.id.name.contains('.')) {
      diagnostic::error("`@timestamp` field paths must not contain `.` in a "
                        "segment")
        .primary(primary)
        .secondary(segment.id)
        .emit(dh);
      return std::nullopt;
    }
    if (std::exchange(add_dot, true)) {
      result += '.';
    }
    result += segment.id.name;
  }
  if (result.empty()) {
    diagnostic::error("`@timestamp` requires a field path")
      .primary(primary)
      .emit(dh);
    return std::nullopt;
  }
  return result;
}

auto validate_timestamp_assignment(const ast::assignment& assignment,
                                   diagnostic_handler& dh) -> failure_or<void> {
  if (not is_timestamp_selector(assignment.left)) {
    return {};
  }
  if (is_null_constant_expression(assignment.right)) {
    return {};
  }
  auto copy = ast::expression{assignment.right};
  auto path = ast::field_path::try_from(std::move(copy));
  if (not path) {
    diagnostic::error("`@timestamp` expects a field path or `null`")
      .primary(assignment.right)
      .emit(dh);
    return failure::promise();
  }
  if (not serialize_timestamp_field(*path, dh,
                                    assignment.right.get_location())) {
    return failure::promise();
  }
  return {};
}

} // namespace

auto assign_timestamp(const ast::expression& right, const table_slice& input,
                      diagnostic_handler& dh) -> std::vector<table_slice> {
  if (is_null_constant_expression(right)) {
    return {with_event_timestamp_field(input, std::nullopt)};
  }
  auto copy = ast::expression{right};
  auto path = ast::field_path::try_from(std::move(copy));
  if (not path) {
    diagnostic::warning("`@timestamp` expects a field path or `null`")
      .primary(right)
      .emit(dh);
    return {input};
  }
  auto serialized = serialize_timestamp_field(*path, dh, right.get_location());
  if (not serialized) {
    return {input};
  }
  auto resolved = resolve(*path, input.schema());
  auto* offset = try_as<tenzir::offset>(resolved);
  if (not offset) {
    diagnostic::warning("cannot assign `@timestamp` from missing field")
      .primary(right)
      .emit(dh);
    return {input};
  }
  auto field_type = as<record_type>(input.schema()).field(*offset).type;
  if (field_type.kind().is_not<time_type>()) {
    diagnostic::warning("`@timestamp` requires a `time` field, but got `{}`",
                        field_type.kind())
      .primary(right)
      .emit(dh);
    return {input};
  }
  return {with_event_timestamp_field(input, *serialized)};
}

namespace {

auto parse_timestamp_field(std::string_view dotted_path)
  -> std::optional<ast::field_path> {
  if (dotted_path.empty()) {
    return std::nullopt;
  }
  auto expr = std::optional<ast::expression>{};
  auto remaining = dotted_path;
  while (not remaining.empty()) {
    auto dot = remaining.find('.');
    auto segment = remaining.substr(0, dot);
    if (segment.empty()) {
      return std::nullopt;
    }
    auto id = ast::identifier{segment, location::unknown};
    if (not expr) {
      expr = ast::expression{ast::root_field{std::move(id), false}};
    } else {
      expr = ast::expression{ast::field_access{
        std::move(*expr),
        location::unknown,
        false,
        std::move(id),
      }};
    }
    if (dot == std::string_view::npos) {
      break;
    }
    if (dot + 1 == remaining.size()) {
      return std::nullopt;
    }
    remaining.remove_prefix(dot + 1);
  }
  TENZIR_ASSERT(expr);
  return ast::field_path::try_from(std::move(*expr));
}

auto resolves_to_time(const ast::field_path& path, type schema) -> bool {
  auto resolved = resolve(path, schema);
  auto* offset = try_as<tenzir::offset>(resolved);
  if (not offset) {
    return false;
  }
  return as<record_type>(schema).field(*offset).type.kind().is<time_type>();
}

auto path_has_prefix(std::span<const ast::field_path::segment> path,
                     std::span<const ast::field_path::segment> prefix) -> bool {
  if (prefix.size() > path.size()) {
    return false;
  }
  for (auto i = size_t{0}; i < prefix.size(); ++i) {
    if (path[i].id.name != prefix[i].id.name) {
      return false;
    }
  }
  return true;
}

auto rewrite_path(const ast::field_path& path, const field_remapping& remapping)
  -> std::optional<ast::field_path> {
  if (not path_has_prefix(path.path(), remapping.from.path())) {
    return std::nullopt;
  }
  auto expr = std::optional<ast::expression>{};
  auto append = [&](const ast::identifier& id) {
    auto next_id = ast::identifier{id.name, location::unknown};
    if (not expr) {
      expr = ast::expression{ast::root_field{std::move(next_id), false}};
    } else {
      expr = ast::expression{ast::field_access{
        std::move(*expr),
        location::unknown,
        false,
        std::move(next_id),
      }};
    }
  };
  for (const auto& segment : remapping.to.path()) {
    append(segment.id);
  }
  for (const auto& segment :
       path.path().subspan(remapping.from.path().size())) {
    append(segment.id);
  }
  if (not expr) {
    return ast::field_path::try_from(ast::this_{location::unknown});
  }
  return ast::field_path::try_from(std::move(*expr));
}

auto serialize_path(const ast::field_path& path) -> std::string {
  auto result = std::string{};
  auto add_dot = false;
  for (const auto& segment : path.path()) {
    if (std::exchange(add_dot, true)) {
      result += '.';
    }
    result += segment.id.name;
  }
  return result;
}

} // namespace

auto consume_path(std::span<const ast::field_path::segment> path, series value)
  -> series {
  if (path.empty()) {
    return value;
  }
  value = consume_path(path.subspan(1), std::move(value));
  auto new_type
    = type{record_type{record_type::field_view{path[0].id.name, value.type}}};
  auto new_array = make_struct_array(value.length(), nullptr, {path[0].id.name},
                                     {value.array}, as<record_type>(new_type));
  return series{
    std::move(new_type),
    std::move(new_array),
  };
}

auto assign(std::span<const ast::field_path::segment> left, series right,
            series input, diagnostic_handler& dh, assign_position position)
  -> series {
  TENZIR_ASSERT(right.length() == input.length());
  if (left.empty()) {
    return right;
  }
  if (input.type.kind().is<null_type>()) {
    // We silently upgrade the null type to a record. We could also warn here,
    // but then we should perhaps adjust the code below to check for `null`
    // values assigning to a `record` type.
    return consume_path(left, std::move(right));
  }
  auto* rec_ty = try_as<record_type>(input.type);
  if (not rec_ty) {
    diagnostic::warning("implicit record for `{}` field overwrites `{}` value",
                        left[0].id.name, input.type.kind())
      .primary(left[0].id)
      .hint("if this is intentional, drop the parent field before")
      .emit(dh);
    return consume_path(left, std::move(right));
  }
  auto& array = as<arrow::StructArray>(*input.array);
  auto new_ty_fields = collect(rec_ty->fields());
  // We flatten the input fields here because the input `{foo: null}` of type
  // `{foo: {bar: int64}` should become `{foo: {bar: null, qux: 42}}` for the
  // assignment `foo.qux = 42`. This is consistent with the behavior for when
  // `foo` is of type `null` or does not exist. Also, simply using `.fields()`
  // here would not propagate the `null` values from the parent record.
  auto new_field_arrays = std::invoke([&] {
    if (array.null_count() == 0) {
      return array.fields();
    }
    return check(array.Flatten(tenzir::arrow_memory_pool()));
  });
  auto index = std::optional<size_t>{};
  for (auto [i, field] : detail::enumerate(new_ty_fields)) {
    if (field.name == left[0].id.name) {
      index = i;
      break;
    }
  }
  if (index) {
    auto& field_ty = new_ty_fields[*index].type;
    auto& field_array = new_field_arrays[*index];
    auto new_field = assign(left.subspan(1), std::move(right),
                            series{field_ty, field_array}, dh, position);
    field_ty = std::move(new_field.type);
    field_array = std::move(new_field.array);
  } else {
    auto inner = consume_path(left.subspan(1), std::move(right));
    switch (position) {
      case tenzir::assign_position::front: {
        new_ty_fields.emplace(new_ty_fields.begin(), left[0].id.name,
                              inner.type);
        new_field_arrays.emplace(new_field_arrays.begin(), inner.array);
        break;
      }
      case tenzir::assign_position::back: {
        new_ty_fields.emplace_back(left[0].id.name, inner.type);
        new_field_arrays.push_back(inner.array);
        break;
      }
    }
  }
  auto new_ty_field_names
    = new_ty_fields | std::views::transform(&record_type::field_view::name);
  auto new_type = type{record_type{new_ty_fields}};
  auto new_array
    = make_struct_array(array.length(), nullptr,
                        std::vector<std::string>{new_ty_field_names.begin(),
                                                 new_ty_field_names.end()},
                        new_field_arrays, as<record_type>(new_type));
  // TODO: What to do with metadata on record?
  // new_type.assign_metadata(input.type);
  return series{std::move(new_type), std::move(new_array)};
}

auto assign(const ast::meta& left, const series& right,
            const table_slice& input, diagnostic_handler& diag)
  -> std::vector<table_slice> {
  auto transform = [&input]<std::derived_from<arrow::Array> Array>(
                     const Array& array, auto&& f) {
    using ty = type_from_arrow_t<Array>;
    static_assert(basic_type<ty>);
    TENZIR_ASSERT(array.length() > 0);
    auto get_value = [&](int64_t i) {
      return array.IsValid(i) ? std::optional{value_at(ty{}, array, i)}
                              : std::nullopt;
    };
    auto result = std::vector<table_slice>{};
    auto begin = int64_t{0};
    auto begin_value = get_value(begin);
    for (auto i = int64_t{0}; i < array.length() + 1; ++i) {
      auto emit = i == array.length() || get_value(i) != begin_value;
      if (emit) {
        result.emplace_back(f(subslice(input, begin, i), begin_value));
        begin = i;
        if (i != array.length()) {
          begin_value = get_value(begin);
        }
      }
    }
    return result;
  };
  auto original = [&input] {
    auto result = std::vector<table_slice>{};
    result.push_back(input);
    return result;
  };
  // TODO: The locations used in the warnings below point to the value. Maybe
  // they should point to the metadata selector, or the equal token instead?
  switch (left.kind) {
    case ast::meta::name: {
      // TODO: We considered to make the schema name optional at some point.
      // This would help here, as we could set the name to `null` if we get a
      // non-string type or a null string. Instead, we keep the original schema
      // name in both cases for now.
      auto* array = dynamic_cast<arrow::StringArray*>(right.array.get());
      if (not array) {
        diagnostic::warning("expected string but got {}", right.type.kind())
          .primary(left)
          .emit(diag);
        return original();
      }
      return transform(*array, [&](table_slice slice,
                                   std::optional<std::string_view> value) {
        if (not value) {
          diagnostic::warning("schema name must not be `null`")
            .primary(left)
            .emit(diag);
          return slice;
        }
        auto new_type = type{
          *value,
          slice.schema(),
          collect(slice.schema().attributes()),
        };
        auto new_batch = check(
          to_record_batch(slice)->ReplaceSchema(new_type.to_arrow_schema()));
        return table_slice{new_batch, std::move(new_type)};
      });
    }
    case ast::meta::import_time: {
      // If the right side is not a timestamp, we do the same as if the value is
      // null. On an implementation level, the import time is not nullable, but
      // we use the default-constructed time as a marker for `null`. This is
      // translated back to `null` when it is read by the evaluator.
      auto* array = dynamic_cast<arrow::TimestampArray*>(right.array.get());
      if (not array) {
        if (not right.type.kind().is<null_type>()) {
          diagnostic::warning("expected `time` but got `{}`", right.type.kind())
            .primary(left)
            .emit(diag);
        }
        auto copy = input;
        copy.import_time(time{});
        return {std::move(copy)};
      }
      return transform(*array, [&](table_slice slice,
                                   std::optional<time> value) {
        if (not value) {
          value = time{};
        } else if (value == time{}) {
          // This means that we are trying to set a non-null
          // time which would be interpreted as null later.
          diagnostic::warning("import time cannot be `{}`", time{})
            .primary(left)
            .hint("consider using `null` instead")
            .emit(diag);
        }
        slice.import_time(*value);
        return slice;
      });
    }
    case ast::meta::internal: {
      // TODO: If this is set to null, we keep the original setting for now. We
      // could instead also set it to `false`, as `null` is also treated as
      // `false` in contexts such as `where` or `if`.
      auto* values = dynamic_cast<arrow::BooleanArray*>(right.array.get());
      if (not values) {
        diagnostic::warning("expected bool but got {}", right.type.kind())
          .primary(left)
          .emit(diag);
        return original();
      }
      return transform(*values, [&](table_slice slice,
                                    std::optional<bool> value) {
        if (not value) {
          diagnostic::warning("cannot set `@internal` to `null`")
            .primary(left)
            .emit(diag);
          return slice;
        }
        auto previous = slice.schema().attribute("internal").has_value();
        if (*value == previous) {
          return slice;
        }
        auto new_attributes = std::vector<type::attribute_view>{};
        for (auto [key, value] : input.schema().attributes()) {
          if (key == "internal") {
            continue;
          }
          new_attributes.emplace_back(key, value);
        }
        if (*value) {
          new_attributes.emplace_back("internal", "");
        }
        auto new_type = type{
          input.schema().name(),
          as<record_type>(input.schema()),
          std::move(new_attributes),
        };
        auto new_batch = check(
          to_record_batch(input)->ReplaceSchema(new_type.to_arrow_schema()));
        return table_slice{new_batch, std::move(new_type)};
      });
    }
    case ast::meta::timestamp: {
      diagnostic::warning("assignment to `@timestamp` requires a field path or "
                          "`null`")
        .primary(left)
        .emit(diag);
      return original();
    }
  }
  TENZIR_UNREACHABLE();
}

auto assign(const ast::field_path& left, series right, const table_slice& input,
            diagnostic_handler& dh, assign_position position) -> table_slice {
  auto result
    = assign(left.path(), std::move(right), series{input}, dh, position);
  auto* rec_ty = try_as<record_type>(result.type);
  if (not rec_ty) {
    diagnostic::warning("assignment to `this` requires `record`, but got `{}`",
                        result.type.kind())
      .primary(left)
      .emit(dh);
    result = {record_type{},
              make_struct_array(result.length(), nullptr, {}, record_type{})};
  }
  result.type.assign_metadata(input.schema());
  auto schema = arrow::schema(result.array->type()->fields(),
                              to_record_batch(input)->schema()->metadata());
  auto slice = table_slice{
    arrow::RecordBatch::Make(std::move(schema), result.length(),
                             as<arrow::StructArray>(*result.array).fields()),
    std::move(result.type),
  };
  slice.import_time(input.import_time());
  return slice;
}

auto assign(const ast::selector& left, series right, const table_slice& input,
            diagnostic_handler& dh, assign_position position)
  -> std::vector<table_slice> {
  return left.match(
    [&](const ast::meta& left) {
      return assign(left, right, input, dh);
    },
    [&](const ast::field_path& left) {
      auto result = std::vector<table_slice>{};
      result.push_back(assign(left, std::move(right), input, dh, position));
      return result;
    },
    [&](const ast::dollar_var&) -> std::vector<table_slice> {
      TENZIR_UNREACHABLE();
    });
}

namespace {

struct move_resolver final : ast::visitor<move_resolver> {
  std::vector<ast::field_path> out;

  auto visit(ast::expression& x) -> void {
    if (auto* unary = try_as<ast::unary_expr>(x)) {
      if (unary->op.inner == ast::unary_op::move) {
        if (auto field = ast::field_path::try_from(unary->expr)) {
          out.push_back(std::move(*field));
          x = std::move(unary->expr);
        }
      }
    }
    enter(x);
  }

  auto visit(ast::lambda_expr&) -> void {
    // Expressions inside lambda arguments do not necessarily refer to the
    // top-level event. We cannot resolve the move keyword inside of them.
  }

  auto visit(auto& x) -> void {
    enter(x);
  }
};

} // namespace

auto resolve_move_keyword(ast::assignment assignment)
  -> std::pair<ast::assignment, std::vector<ast::field_path>> {
  auto f = move_resolver{};
  f.visit(assignment);
  return {std::move(assignment), std::move(f.out)};
}

auto validate_set_assignment(const ast::assignment& assignment,
                             diagnostic_handler& dh) -> failure_or<void> {
  return validate_timestamp_assignment(assignment, dh);
}

auto extract_field_remappings(std::span<const ast::assignment> assignments)
  -> std::vector<field_remapping> {
  auto result = std::vector<field_remapping>{};
  for (const auto& assignment : assignments) {
    auto* lhs = std::get_if<ast::field_path>(&assignment.left);
    if (not lhs) {
      continue;
    }
    auto copy = ast::expression{assignment.right};
    auto rhs = ast::field_path::try_from(std::move(copy));
    if (not rhs) {
      continue;
    }
    result.push_back({*rhs, *lhs});
  }
  return result;
}

auto track_event_timestamp_field(const table_slice& original,
                                 table_slice updated,
                                 std::span<const field_remapping> remappings)
  -> table_slice {
  auto field = event_timestamp_field(original);
  if (not field) {
    return updated;
  }
  auto original_path = parse_timestamp_field(*field);
  if (not original_path) {
    return with_event_timestamp_field(updated, std::nullopt);
  }
  if (resolves_to_time(*original_path, updated.schema())) {
    return with_event_timestamp_field(updated, *field);
  }
  auto candidates = std::vector<std::string>{};
  for (const auto& remapping : remappings) {
    auto rewritten = rewrite_path(*original_path, remapping);
    if (not rewritten) {
      continue;
    }
    if (not resolves_to_time(*rewritten, updated.schema())) {
      continue;
    }
    auto serialized = serialize_path(*rewritten);
    if (std::ranges::find(candidates, serialized) == candidates.end()) {
      candidates.push_back(std::move(serialized));
    }
  }
  if (candidates.size() == 1) {
    return with_event_timestamp_field(updated, candidates.front());
  }
  return with_event_timestamp_field(updated, std::nullopt);
}

auto drop(const table_slice& slice, std::span<const ast::field_path> fields,
          diagnostic_handler& dh, bool warn_for_duplicates) -> table_slice {
  constexpr auto drop = [](auto&&...) {
    return indexed_transformation::result_type{};
  };
  auto offsets = std::vector<located<offset>>{};
  for (const auto& field : fields) {
    match(
      resolve(field, slice.schema()),
      [&](const offset& resolved) {
        if (resolved.empty()) {
          diagnostic::warning("cannot drop `this`").primary(field).emit(dh);
          return;
        }
        if (offsets.empty()) {
          offsets.emplace_back(resolved, into_location{field});
          return;
        }
        for (auto& offset : offsets) {
          const auto [l, r] = std::ranges::mismatch(offset.inner, resolved);
          const auto offset_exhausted = l == offset.inner.end();
          const auto resolved_exhausted = r == resolved.end();
          if (offset_exhausted and resolved_exhausted) {
            if (warn_for_duplicates) {
              diagnostic::warning("field `{}` may only be dropped once",
                                  field.path().back().id.name)
                .primary(offset)
                .primary(field)
                .emit(dh);
            }
            return;
          }
          if (offset_exhausted) {
            if (warn_for_duplicates) {
              diagnostic::warning("ignoring dropped field within record")
                .primary(field, "ignoring this field")
                .secondary(offset, "because it is already dropped here")
                .emit(dh);
            }
            return;
          }
          if (resolved_exhausted) {
            if (warn_for_duplicates) {
              diagnostic::warning(
                "ignoring dropped field within dropped record")
                .primary(offset, "ignoring this field")
                .secondary(field, "because it is already dropped here")
                .emit(dh);
            }
            offset = located{resolved, into_location{field}};
            return;
          }
        }
        offsets.emplace_back(resolved, into_location{field});
      },
      [&](const resolve_error& err) {
        match(
          err.reason,
          [&](const resolve_error::field_not_found&) {
            diagnostic::warning("field `{}` not found", err.ident.name)
              .primary(err.ident)
              .hint("append `?` to suppress this warning")
              .emit(dh);
          },
          [&](const resolve_error::field_not_found_no_error&) {},
          [&](const resolve_error::field_of_non_record& reason) {
            diagnostic::warning("type `{}` has no field `{}`",
                                reason.type.kind(), err.ident.name)
              .primary(err.ident)
              .emit(dh);
          });
      });
  }
  auto ts = std::vector<indexed_transformation>{};
  for (const auto& of : offsets) {
    ts.emplace_back(of.inner, drop);
  }
  return transform_columns(slice, ts);
}

auto set_operator::operator()(generator<table_slice> input,
                              operator_control_plane& ctrl) const
  -> generator<table_slice> {
  auto remappings = extract_field_remappings(assignments_);
  auto has_explicit_timestamp_assignment
    = std::ranges::any_of(assignments_, [](const auto& assignment) {
        auto* meta = std::get_if<ast::meta>(&assignment.left);
        return meta and meta->kind == ast::meta::timestamp;
      });
  for (auto&& slice : input) {
    if (slice.rows() == 0) {
      co_yield {};
      continue;
    }
    if (assignments_.empty()) {
      co_yield std::move(slice);
      continue;
    }
    // The right-hand side is always evaluated with the original input, because
    // side-effects from preceding assignments shall not be reflected when
    // calculating the value of the left-hand side.
    auto values = std::vector<multi_series>{};
    for (const auto& assignment : assignments_) {
      if (is_timestamp_selector(assignment.left)) {
        values.push_back(
          series::null(null_type{}, detail::narrow<int64_t>(slice.rows())));
        continue;
      }
      values.push_back(eval(assignment.right, slice, ctrl.diagnostics()));
    }
    slice = drop(slice, moved_fields_, ctrl.diagnostics(), false);
    // After we know all the multi series values on the right, we can split the
    // input table slice and perform the actual assignment.
    auto begin = int64_t{0};
    auto results = std::vector<table_slice>{};
    for (auto values_slice : split_multi_series(values)) {
      TENZIR_ASSERT(not values_slice.empty());
      auto end = begin + values_slice[0].length();
      // We could still perform further splits if metadata is assigned.
      auto state = std::vector<table_slice>{};
      auto original = subslice(slice, begin, end);
      state.push_back(original);
      begin = end;
      auto new_state = std::vector<table_slice>{};
      TENZIR_ASSERT(assignments_.size() == values_slice.size());
      for (auto [assignment, value] :
           std::views::zip(assignments_, values_slice)) {
        auto begin = int64_t{0};
        for (auto& entry : state) {
          auto entry_rows = detail::narrow<int64_t>(entry.rows());
          auto assigned = std::vector<table_slice>{};
          if (is_timestamp_selector(assignment.left)) {
            assigned
              = assign_timestamp(assignment.right, entry, ctrl.diagnostics());
          } else {
            assigned = assign(assignment.left, value.slice(begin, entry_rows),
                              entry, ctrl.diagnostics());
          }
          begin += entry_rows;
          new_state.insert(new_state.end(),
                           std::move_iterator{assigned.begin()},
                           std::move_iterator{assigned.end()});
        }
        std::swap(state, new_state);
        new_state.clear();
      }
      if (has_explicit_timestamp_assignment) {
        std::ranges::move(state, std::back_inserter(results));
      } else {
        auto tracked = std::vector<table_slice>{};
        auto tracked_begin = size_t{0};
        for (auto& entry : state) {
          auto tracked_end = tracked_begin + entry.rows();
          tracked.push_back(track_event_timestamp_field(
            subslice(original, tracked_begin, tracked_end), std::move(entry),
            remappings));
          tracked_begin = tracked_end;
        }
        TENZIR_ASSERT(tracked_begin == original.rows());
        std::ranges::move(tracked, std::back_inserter(results));
      }
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
      co_yield std::move(result);
    }
  }
}

} // namespace tenzir
