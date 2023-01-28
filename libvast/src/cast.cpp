//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/cast.hpp"

#include "vast/table_slice.hpp"

#include <arrow/record_batch.h>

namespace vast {

auto cast(table_slice from_slice, const type& to_schema) noexcept
  -> table_slice {
  VAST_ASSERT(can_cast(from_slice.schema(), to_schema));
  if (from_slice.schema() == to_schema)
    return from_slice;
  const auto from_batch = to_record_batch(from_slice);
  const auto from_struct_array = from_batch->ToStructArray().ValueOrDie();
  const auto to_struct_array
    = detail::cast_helper<record_type, record_type>::cast(
      caf::get<record_type>(from_slice.schema()), from_struct_array,
      caf::get<record_type>(to_schema));
  const auto to_batch = arrow::RecordBatch::Make(to_schema.to_arrow_schema(),
                                                 to_struct_array->length(),
                                                 to_struct_array->fields());
  return table_slice{to_batch, to_schema};
}

} // namespace vast
