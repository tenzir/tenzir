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
#include "tenzir/expression.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/type.hpp"

#include <arrow/compute/api_scalar.h>
#include <caf/detail/is_complete.hpp>
#include <caf/detail/is_one_of.hpp>

#include <type_traits>

namespace tenzir::tql2 {

namespace {

auto assign(const ast::meta& left, series right, const table_slice& input,
            diagnostic_handler& diag) -> table_slice {
  switch (left.kind) {
    case meta_extractor::schema: {
      // TODO: Name.
      auto values = dynamic_cast<arrow::StringArray*>(right.array.get());
      if (not values) {
        // TODO: Inaccurate location.
        diagnostic::warning("expected string but got {}", right.type.kind())
          .primary(left.get_location())
          .emit(diag);
        return input;
      }
      // TODO: We actually have to split the batch sometimes.
      auto new_name = values->GetView(0);
      // TODO: Is this correct?
      return table_slice{to_record_batch(input),
                         type{new_name, input.schema(),
                              collect(input.schema().attributes())}};
    }
    case meta_extractor::schema_id:
      // TODO: This is actually not assignable. Should this even be a selector
      // then? Maybe make it a function.
      TENZIR_TODO();
    case meta_extractor::import_time: {
      auto values = dynamic_cast<arrow::TimestampArray*>(right.array.get());
      if (not values) {
        // TODO: Inaccurate location.
        diagnostic::warning("expected time but got {}", right.type.kind())
          .primary(left.get_location())
          .emit(diag);
        return input;
      }
      // Have to potentially split, again.
      auto new_time = value_at(time_type{}, *values, 0);
      // TODO: Copy is unnecessary.
      auto copy = table_slice{input};
      copy.import_time(new_time);
      return copy;
    }
    case meta_extractor::internal: {
      auto values = dynamic_cast<arrow::BooleanArray*>(right.array.get());
      if (not values) {
        // TODO: Inaccurate location.
        diagnostic::warning("expected bool but got {}", right.type.kind())
          .primary(left.get_location())
          .emit(diag);
        return input;
      }
      // Have to potentially split, again.
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
      // TODO: Is this correct? Probably not!
      return table_slice{to_record_batch(input),
                         type{input.schema().name(), input.schema(),
                              std::move(new_attributes)}};
    }
  }
  TENZIR_UNREACHABLE();
}

auto assign(const ast::simple_selector& left, series right,
            const table_slice& input) -> table_slice {
  auto resolved = resolve(left, input.schema());
  auto off = std::get_if<offset>(&resolved);
  // TODO: Write this without transform columns.
  auto transformation = indexed_transformation{};
  if (off) {
    transformation = indexed_transformation{
      .index = std::move(*off),
      .fun =
        [&](struct record_type::field field,
            std::shared_ptr<arrow::Array> array) {
          TENZIR_UNUSED(array);
          field.type = std::move(right.type);
          return indexed_transformation::result_type{
            std::pair{std::move(field), std::move(right.array)}};
        },
    };
  } else {
    // TODO: Handle the nested case.
    TENZIR_ASSERT(left.path().size() == 1);
    auto num_fields = caf::get<record_type>(input.schema()).num_fields();
    TENZIR_ASSERT(num_fields > 0);
    transformation = indexed_transformation{
      .index = offset{num_fields - 1},
      .fun =
        [&](struct record_type::field field,
            std::shared_ptr<arrow::Array> array) {
          auto result = indexed_transformation::result_type{};
          result.emplace_back(std::move(field), std::move(array));
          result.emplace_back(decltype(field){left.path()[0].name, right.type},
                              std::move(right.array));
          return result;
        },
    };
  }
  // TODO: We can't use `transform_columns` if we assign to `this` (which
  // has an empty offset).
  if (transformation.index.empty()) {
    auto record = caf::get_if<arrow::StructArray>(&*right.array);
    if (right.type.name().empty()) {
      right.type = type{"tenzir.set", right.type};
    }
    TENZIR_ASSERT(record);
    auto fields = record->Flatten().ValueOrDie();
    auto result
      = table_slice{arrow::RecordBatch::Make(right.type.to_arrow_schema(),
                                             right.length(), fields),
                    right.type};
    TENZIR_ASSERT_EXPENSIVE(to_record_batch(result)->Validate().ok());
    return result;
  } else {
    return transform_columns(input, {transformation});
  }
}

auto assign(const ast::selector& left, series right, const table_slice& input,
            diagnostic_handler& diag) -> table_slice {
  return left.match(
    [&](const ast::meta& left) {
      return assign(left, std::move(right), input, diag);
    },
    [&](const ast::simple_selector& left) {
      return assign(left, std::move(right), input);
    });
}

} // namespace

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
      // TODO: We are using `slice` here, not `result`. Okay?
      auto right = eval(assignment.right, slice, ctrl.diagnostics());
      result = assign(assignment.left, right, result, ctrl.diagnostics());
    }
    co_yield result;
  }
}

} // namespace tenzir::tql2
