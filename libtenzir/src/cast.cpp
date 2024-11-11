//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/cast.hpp"

#include "tenzir/table_slice.hpp"

#include <arrow/record_batch.h>

namespace tenzir {

auto cast(table_slice from_slice, const type& to_schema) noexcept
  -> table_slice {
  TENZIR_ASSERT_EXPENSIVE(can_cast(from_slice.schema(), to_schema));
  if (from_slice.schema() == to_schema)
    return from_slice;
  const auto from_batch = to_record_batch(from_slice);
  const auto from_struct_array = from_batch->ToStructArray().ValueOrDie();
  const auto to_struct_array
    = detail::cast_helper<record_type, record_type>::cast(
      as<record_type>(from_slice.schema()), from_struct_array,
      as<record_type>(to_schema));
  const auto to_batch = arrow::RecordBatch::Make(to_schema.to_arrow_schema(),
                                                 to_struct_array->length(),
                                                 to_struct_array->fields());
  auto result = table_slice{to_batch, to_schema};
  result.offset(from_slice.offset());
  result.import_time(from_slice.import_time());
  return result;
}

} // namespace tenzir
