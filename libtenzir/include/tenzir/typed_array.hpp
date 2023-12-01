//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/table_slice.hpp"
#include "tenzir/type.hpp"

#include <arrow/array.h>
#include <arrow/record_batch.h>

#include <memory>

namespace tenzir {

/// A temporary series representation (until we have a proper one).
struct typed_array {
  typed_array() = default;

  template <type_or_concrete_type Type>
  typed_array(Type type, std::shared_ptr<arrow::Array> array)
    : type{std::move(type)}, array{std::move(array)} {
  }

  auto length() const -> int64_t {
    return array ? array->length() : 0;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, typed_array& x) -> bool {
    if constexpr (Inspector::is_loading) {
      table_slice slice;
      auto callback = [&]() noexcept {
        x.type = caf::get<record_type>(slice.schema()).field(0).type;
        x.array = to_record_batch(slice)->column(0);
        return true;
      };
      return f.object(x)
        .pretty_name("tenzir.typed_array")
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
      return f.object(x).fields(f.field("slice", slice));
      return f.object(x)
        .pretty_name("tenzir.typed_array")
        .fields(f.field("slice", slice));
    }
  }

  tenzir::type type;
  std::shared_ptr<arrow::Array> array;
};

} // namespace tenzir
