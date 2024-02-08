//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/offset.hpp"
#include "tenzir/type.hpp"

#include <arrow/array.h>
#include <arrow/record_batch.h>

#include <memory>

namespace tenzir {

/// A series represents a contiguous representation of nullable data of the same
/// type, e.g., a column in a table slice.
struct series {
  series() = default;

  series(table_slice slice, offset idx) {
    std::tie(type, array) = idx.get(slice);
  }

  template <type_or_concrete_type Type>
  series(Type type, std::shared_ptr<arrow::Array> array)
    : type{std::move(type)}, array{std::move(array)} {
  }

  auto length() const -> int64_t {
    return array ? array->length() : 0;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, series& x) -> bool {
    if constexpr (Inspector::is_loading) {
      table_slice slice;
      auto callback = [&]() noexcept {
        x.type = caf::get<record_type>(slice.schema()).field(0).type;
        x.array = to_record_batch(slice)->column(0);
        return true;
      };
      return f.object(x)
        .pretty_name("tenzir.series")
        .on_load(callback)
        .fields(f.field("slice", slice));
    } else {
      auto arrays = std::vector{x.array};
      auto tenzir_schema = tenzir::type{record_type{{"x", x.type}}};
      auto arrow_schema = tenzir_schema.to_arrow_schema();
      auto batch
        = arrow::RecordBatch::Make(arrow_schema, x.array->length(), arrays);
      auto slice
        = table_slice{batch, tenzir_schema, table_slice::serialize::yes};
      return f.object(x)
        .pretty_name("tenzir.series")
        .fields(f.field("slice", slice));
    }
  }

  template <type_or_concrete_type Type = type>
  auto values() const {
    if constexpr (concrete_type<Type>) {
      const auto* ct = caf::get_if<Type>(type);
      TENZIR_ASSERT_CHEAP(ct);
      TENZIR_ASSERT_CHEAP(array);
      return tenzir::values(*ct, *array);
    } else {
      TENZIR_ASSERT_CHEAP(array);
      return tenzir::values(type, *array);
    }
  }

  tenzir::type type;
  std::shared_ptr<arrow::Array> array;
};

} // namespace tenzir
