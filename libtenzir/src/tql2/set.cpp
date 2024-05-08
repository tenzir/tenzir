//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/set.hpp"

#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/type.hpp"

#include <arrow/compute/api_scalar.h>
#include <caf/detail/is_complete.hpp>
#include <caf/detail/is_one_of.hpp>

#include <type_traits>

namespace tenzir::tql2 {

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
      auto resolved = resolve(assignment.left, result.schema());
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
        TENZIR_ASSERT(assignment.left.path.size() == 1);
        auto num_fields = caf::get<record_type>(slice.schema()).num_fields();
        TENZIR_ASSERT(num_fields > 0);
        transformation = indexed_transformation{
          .index = offset{num_fields - 1},
          .fun =
            [&](struct record_type::field field,
                std::shared_ptr<arrow::Array> array) {
              auto result = indexed_transformation::result_type{};
              result.emplace_back(std::move(field), std::move(array));
              result.emplace_back(decltype(field){assignment.left.path[0].name,
                                                  right.type},
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
        result
          = table_slice{arrow::RecordBatch::Make(right.type.to_arrow_schema(),
                                                 right.length(), fields),
                        right.type};
        TENZIR_ASSERT_EXPENSIVE(to_record_batch(result)->Validate().ok());
      } else {
        result = transform_columns(result, {transformation});
      }
    }
    co_yield result;
  }
}

} // namespace tenzir::tql2
