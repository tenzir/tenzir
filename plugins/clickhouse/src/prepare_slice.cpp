// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
#include "clickhouse/prepare_slice.hpp"

#include "tenzir/arrow_utils.hpp"

#include <arrow/record_batch.h>

namespace tenzir::plugins::clickhouse {
auto prepare_slice(const table_slice& slice, const transformer_record& tr,
                   diagnostic_handler& dh, location operator_location)
  -> table_slice {
  auto fields = std::vector<record_type::field_view>{};
  auto arrays = arrow::ArrayVector{};
  auto changed = false;
  for (const auto& column : columns_of(slice)) {
    const auto trafo = tr.transfrom_and_index_for(column.name).trafo;
    if (not trafo) {
      // The column is not a writable target column: drop it (and warn),
      // shrinking the schema so more slices coalesce.
      if (tr.generated_columns.contains(column.name)) {
        diagnostic::warning("column `{}` is a generated ClickHouse column "
                            "and "
                            "cannot be written",
                            column.name)
          .note("the provided value is ignored; ClickHouse computes the "
                "column")
          .primary(operator_location)
          .emit(dh);
      } else {
        diagnostic::warning("column `{}` does not exist in the ClickHouse "
                            "table",
                            column.name)
          .note("column will be dropped")
          .primary(operator_location)
          .emit(dh);
      }
      changed = true;
      continue;
    }
    if (auto replaced
        = prepare_json_fields(column.type, column.array.Slice(0), *trafo)) {
      fields.emplace_back(column.name, replaced->type);
      arrays.push_back(std::move(replaced->array));
      changed = true;
    } else {
      fields.emplace_back(column.name, column.type);
      arrays.push_back(column.array.Slice(0));
    }
  }
  if (not changed) {
    return slice;
  }
  auto new_schema = type{"tenzir.clickhouse-prepared", record_type{fields}};
  auto batch
    = arrow::RecordBatch::Make(new_schema.to_arrow_schema(),
                               detail::narrow_cast<int64_t>(slice.rows()),
                               std::move(arrays));
  auto result = table_slice{batch, std::move(new_schema)};
  result.offset(slice.offset());
  result.import_time(slice.import_time());
  return result;
}

} // namespace tenzir::plugins::clickhouse
