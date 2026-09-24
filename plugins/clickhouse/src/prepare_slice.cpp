//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
#include "clickhouse/prepare_slice.hpp"

#include "tenzir/arrow_utils.hpp"
#include "tenzir/view3.hpp"

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

namespace {

using MappingPath = std::vector<std::string>;

auto select_path(series input, MappingPath const& path) -> Option<series> {
  for (auto const& component : path) {
    auto const record = input.as<record_type>();
    if (not record) {
      return {};
    }
    auto const index = record->array->struct_type()->GetFieldIndex(component);
    if (index < 0) {
      return {};
    }
    // Propagate null parents, including for arrays with nonzero offsets.
    input = series{record->type.field(index).type,
                   check(record->array->GetFlattenedField(index))};
  }
  return input;
}

auto remove_mapped_fields(series input, transformer_record const& tr,
                          MappingPath& path) -> series {
  auto record = input.as<record_type>();
  if (not record) {
    return input;
  }
  auto fields = std::vector<series_field>{};
  auto index = int{0};
  for (auto field : record->type.fields()) {
    auto child = series{field.type, record->array->field(index++)};
    path.emplace_back(field.name);
    auto mapped = std::ranges::any_of(tr.mapping_paths, [&](auto const& entry) {
      return entry.second == path;
    });
    if (not mapped) {
      auto nested = remove_mapped_fields(child, tr, path);
      // Remove a parent emptied by extraction, but retain explicit empty records.
      auto original_record = child.as<record_type>();
      auto emptied = original_record and original_record->type.num_fields() != 0
                     and as<record_type>(nested.type).num_fields() == 0;
      if (not emptied) {
        fields.push_back({field.name, std::move(nested)});
      }
    }
    path.pop_back();
  }
  return make_record_series(fields, *record->array);
}

} // namespace

auto restructure_for_catch_all(table_slice const& slice,
                               transformer_record const& tr) -> table_slice {
  TENZIR_ASSERT(tr.catch_all);
  auto root = series{slice};
  auto fields = std::vector<record_type::field_view>{};
  auto arrays = arrow::ArrayVector{};
  for (auto const& [name, path] : tr.mapping_paths) {
    if (auto input = select_path(root, path)) {
      fields.emplace_back(name, input->type);
      arrays.push_back(input->array);
    }
  }
  auto path = MappingPath{};
  auto remainder = remove_mapped_fields(root, tr, path);
  fields.emplace_back(*tr.catch_all, remainder.type);
  arrays.push_back(std::move(remainder.array));
  auto schema = type{"tenzir.clickhouse-prepared", record_type{fields}};
  auto batch = arrow::RecordBatch::Make(schema.to_arrow_schema(), slice.rows(),
                                        std::move(arrays));
  auto result = table_slice{batch, std::move(schema)};
  result.offset(slice.offset());
  result.import_time(slice.import_time());
  return result;
}

} // namespace tenzir::plugins::clickhouse
