//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/set.hpp"

#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/collect.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/expression.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/type.hpp"

#include <arrow/compute/api_scalar.h>
#include <caf/detail/is_complete.hpp>
#include <caf/detail/is_one_of.hpp>

#include <type_traits>

namespace tenzir {

namespace {
/// Creates a record that maps `path` to `value`.
///
/// # Examples
//
/// ["foo", "bar"] -> {"foo": {"bar": value}}
/// [] -> value
auto consume_path(std::span<const ast::identifier> path, series value)
  -> series {
  if (path.empty()) {
    return value;
  }
  value = consume_path(path.subspan(1), std::move(value));
  return series{record_type{record_type::field_view{path[0].name, value.type}},
                make_struct_array(value.length(), nullptr, {path[0].name},
                                  {value.array})};
}

auto assign(std::span<const ast::identifier> left, series right, series input,
            diagnostic_handler& dh, assign_position position) -> series {
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
  auto rec_ty = try_as<record_type>(input.type);
  if (not rec_ty) {
    diagnostic::warning("implicit record for `{}` field overwrites `{}` value",
                        left[0].name, input.type.kind())
      .primary(left[0])
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
  auto new_field_arrays = array.Flatten().ValueOrDie();
  auto index = std::optional<size_t>{};
  for (auto [i, field] : detail::enumerate(new_ty_fields)) {
    if (field.name == left[0].name) {
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
        new_ty_fields.emplace(new_ty_fields.begin(), left[0].name, inner.type);
        new_field_arrays.emplace(new_field_arrays.begin(), inner.array);
        break;
      }
      case tenzir::assign_position::back: {
        new_ty_fields.emplace_back(left[0].name, inner.type);
        new_field_arrays.push_back(inner.array);
        break;
      }
    }
  }
  auto new_ty_field_names
    = new_ty_fields | std::views::transform(&record_type::field_view::name);
  auto new_array
    = make_struct_array(array.length(), nullptr,
                        std::vector<std::string>{new_ty_field_names.begin(),
                                                 new_ty_field_names.end()},
                        new_field_arrays);
  auto new_type = type{record_type{new_ty_fields}};
  // TODO: What to do with metadata on record?
  // new_type.assign_metadata(input.type);
  return series{std::move(new_type), std::move(new_array)};
}

} // namespace

auto assign(const ast::meta& left, series right, const table_slice& input,
            diagnostic_handler& diag) -> table_slice {
  switch (left.kind) {
    case ast::meta::name: {
      auto values = dynamic_cast<arrow::StringArray*>(right.array.get());
      if (not values) {
        // TODO: Inaccurate location.
        diagnostic::warning("expected string but got {}", right.type.kind())
          .primary(left)
          .emit(diag);
        return input;
      }
      // TODO: We actually have to split the batch sometimes.
      auto new_name = values->GetView(0);
      // TODO: Is this correct?
      auto new_type = type{
        new_name,
        input.schema(),
        collect(input.schema().attributes()),
      };
      auto new_batch
        = to_record_batch(input)->ReplaceSchema(new_type.to_arrow_schema());
      TENZIR_ASSERT(new_batch.ok());
      return table_slice{new_batch.MoveValueUnsafe(), new_type};
    }
    case ast::meta::import_time: {
      auto values = dynamic_cast<arrow::TimestampArray*>(right.array.get());
      if (not values) {
        // TODO: Inaccurate location.
        diagnostic::warning("expected time but got {}", right.type.kind())
          .primary(left)
          .emit(diag);
        return input;
      }
      // Have to potentially split, again.
      auto new_time = value_at(time_type{}, *values, 0);
      auto copy = table_slice{input};
      copy.import_time(new_time);
      return copy;
    }
    case ast::meta::internal: {
      auto values = dynamic_cast<arrow::BooleanArray*>(right.array.get());
      if (not values) {
        // TODO: Inaccurate location.
        diagnostic::warning("expected bool but got {}", right.type.kind())
          .primary(left)
          .emit(diag);
        return input;
      }
      // TODO: Have to potentially split, again.
      auto new_value = values->Value(0);
      auto old_value = input.schema().attribute("internal").has_value();
      if (new_value == old_value) {
        return input;
      }
      auto new_attributes = std::vector<type::attribute_view>{};
      for (auto [key, value] : input.schema().attributes()) {
        if (key == "internal") {
          continue;
        }
        new_attributes.emplace_back(key, value);
      }
      if (new_value) {
        new_attributes.emplace_back("internal", "");
      }
      auto new_type = type{
        input.schema().name(),
        as<record_type>(input.schema()),
        std::move(new_attributes),
      };
      auto new_batch
        = to_record_batch(input)->ReplaceSchema(new_type.to_arrow_schema());
      TENZIR_ASSERT(new_batch.ok());
      return table_slice{new_batch.MoveValueUnsafe(), new_type};
    }
  }
  TENZIR_UNREACHABLE();
}

auto assign(const ast::simple_selector& left, series right,
            const table_slice& input, diagnostic_handler& dh,
            assign_position position) -> table_slice {
  auto array = to_record_batch(input)->ToStructArray().ValueOrDie();
  auto result
    = assign(left.path(), std::move(right), series{input}, dh, position);
  auto* rec_ty = try_as<record_type>(result.type);
  if (not rec_ty) {
    diagnostic::warning("assignment to `this` requires `record`, but got "
                        "`{}`",
                        result.type.kind())
      .primary(left)
      .emit(dh);
    result = {record_type{}, make_struct_array(result.length(), nullptr, {})};
  }
  result.type.assign_metadata(input.schema());
  auto slice = table_slice{
    arrow::RecordBatch::Make(result.type.to_arrow_schema(), result.length(),
                             as<arrow::StructArray>(*result.array).fields()),
    result.type,
  };
  slice.import_time(input.import_time());
  return slice;
}

auto assign(const ast::selector& left, series right, const table_slice& input,
            diagnostic_handler& dh, assign_position position) -> table_slice {
  return left.match(
    [&](const ast::meta& left) {
      return assign(left, std::move(right), input, dh);
    },
    [&](const ast::simple_selector& left) {
      return assign(left, std::move(right), input, dh, position);
    });
}

auto set_operator::operator()(generator<table_slice> input,
                              operator_control_plane& ctrl) const
  -> generator<table_slice> {
  for (auto&& slice : input) {
    if (slice.rows() == 0) {
      co_yield {};
      continue;
    }
    // 1. Evaluate every right-hand side with the original input.
    // 2. Evaluate every left-hand side as l-value and then assign.
    // => Left side is evaluated after side effects, in order!
    // set foo={bar: 42}, foo.bar=foo.bar+42
    auto result = slice;
    for (auto& assignment : assignments_) {
      auto right = eval(assignment.right, slice, ctrl.diagnostics());
      result = assign(assignment.left, right, result, ctrl.diagnostics());
    }
    co_yield result;
  }
}

} // namespace tenzir
