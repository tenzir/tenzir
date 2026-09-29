//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/arrow_export.hpp"

#include "tenzir/arrow_memory_pool.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/nova/arrow_metadata.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/materialize.hpp"
#include "tenzir/series_builder.hpp"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/util/key_value_metadata.h>

#include <algorithm>
#include <iterator>
#include <limits>
#include <set>
#include <utility>

namespace tenzir::nova {

auto to_table_slices(Events const& events) -> std::vector<table_slice> {
  struct Builder {
    ArrowMetadata metadata;
    time import_time;
    series_builder builder;
  };
  auto builders = std::vector<Builder>{};
  for (auto index : storage::true_bits(events.mask)) {
    auto value = materialize_legacy(events.data.get(index));
    auto metadata = ArrowMetadata{std::string{*events.meta.name.get(index)},
                                  *events.meta.internal.get(index)};
    auto import_time = *events.meta.import_time.get(index);
    if (builders.empty() or builders.back().metadata.name != metadata.name
        or builders.back().metadata.internal != metadata.internal
        or builders.back().import_time != import_time) {
      builders.emplace_back(std::move(metadata), import_time, series_builder{});
    }
    builders.back().builder.data(value);
  }
  auto result = std::vector<table_slice>{};
  for (auto& [metadata, import_time, builder] : builders) {
    auto slices = builder.finish_as_table_slice(metadata.name);
    for (auto& slice : slices) {
      auto schema = metadata.apply(slice.schema()).to_arrow_schema();
      slice = table_slice{
        to_record_batch(slice)->ReplaceSchemaMetadata(schema->metadata())};
      slice.import_time(import_time);
    }
    result.insert(result.end(), std::make_move_iterator(slices.begin()),
                  std::make_move_iterator(slices.end()));
  }
  return result;
}

namespace {

auto export_status(arrow::Status const& status) -> Result<void, std::string> {
  if (not status.ok()) {
    return Err{status.ToString()};
  }
  return {};
}

auto finish_array(arrow::ArrayBuilder& builder)
  -> Result<std::shared_ptr<arrow::Array>, std::string> {
  auto result = builder.Finish();
  if (not result.ok()) {
    return Err{result.status().ToString()};
  }
  return std::move(*result);
}

auto export_column(Array<Data> const& array, type const& schema,
                   std::span<storage::Index const> rows,
                   storage::BitMap const& present)
  -> Result<std::shared_ptr<arrow::Array>, std::string> {
  auto nulls = array.get_alternative<Null>();
  auto absent = [&](auto row) {
    return row < 0 or not present.get(row)
           or (nulls and nulls->present.get(row));
  };
  return match(
    schema,
    [&]<class T>(
      T const& target) -> Result<std::shared_ptr<arrow::Array>, std::string> {
      if constexpr (std::same_as<T, record_type>
                    or std::same_as<T, list_type>) {
        using Tag
          = std::conditional_t<std::same_as<T, record_type>, Record, List>;
        auto values = array.get_alternative<Tag>();
        auto selected = std::vector<storage::Index>{};
        selected.reserve(rows.size());
        auto validity = arrow::BooleanBuilder{arrow_memory_pool()};
        TRY(export_status(validity.Reserve(rows.size())));
        auto null_count = int64_t{0};
        for (auto row : rows) {
          if (absent(row)) {
            selected.push_back(-1);
            ++null_count;
            TRY(export_status(validity.Append(false)));
          } else {
            if (not values or not values->present.get(row)) {
              return Err{"concrete value type differs from Arrow schema"};
            }
            selected.push_back(row);
            TRY(export_status(validity.Append(true)));
          }
        }
        if (null_count == static_cast<int64_t>(rows.size())) {
          auto result = arrow::MakeArrayOfNull(
            schema.to_arrow_type(), rows.size(), arrow_memory_pool());
          if (not result.ok()) {
            return Err{result.status().ToString()};
          }
          return std::move(*result);
        }
        TRY(auto bits, finish_array(validity));
        auto bitmap = null_count == 0 ? nullptr : bits->data()->buffers[1];
        if constexpr (std::same_as<T, record_type>) {
          auto primary = values->data.to_primary();
          auto const& records = *as<storage::RecordStorage>(primary.storage());
          auto validated = std::set<ShapeTable::ShapeId>{};
          for (auto row : selected) {
            if (row < 0) {
              continue;
            }
            auto shape = records.shape_indices.get(row);
            if (shape < 0) {
              return Err{"selected row has no record shape"};
            }
            if (not validated.insert(shape).second) {
              continue;
            }
            auto fields = records.shape_table.fields(shape);
            if (fields.size() != target.num_fields()) {
              return Err{"record shape differs from Arrow schema"};
            }
            for (auto i = size_t{0}; i < fields.size(); ++i) {
              if (records.names_by_index[fields[i]] != target.field(i).name) {
                return Err{"record field order differs from Arrow schema"};
              }
            }
          }
          auto columns = arrow::ArrayVector{};
          for (auto const& field : target.fields()) {
            auto index = records.names.find(field.name)->second;
            auto const& source = records.arrays[index];
            TRY(auto column, export_column(source.data, field.type, selected,
                                           source.present));
            columns.push_back(std::move(column));
          }
          return std::shared_ptr<arrow::Array>{
            std::make_shared<arrow::StructArray>(
              schema.to_arrow_type(), rows.size(), std::move(columns),
              std::move(bitmap), null_count)};
        } else {
          // Expand a constant list once, not once for every selected row.
          auto constant = try_as<storage::ConstantStorage<List, RowView<List>>>(
            values->data.storage());
          auto primary
            = constant
                ? Array<List>{storage::ConstantStorage<List, RowView<List>>{
                                1, constant->value()}}
                    .to_primary()
                : values->data;
          auto const& lists = as<storage::ListStorage>(primary.storage());
          auto children = std::vector<storage::Index>{};
          auto offsets = arrow::Int32Builder{arrow_memory_pool()};
          TRY(export_status(offsets.Reserve(rows.size() + 1)));
          TRY(export_status(offsets.Append(0)));
          for (auto row : selected) {
            if (row >= 0) {
              auto span = lists.spans()[constant ? 0 : row];
              if (children.size() + static_cast<size_t>(span.end - span.begin)
                  > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
                return Err{"Arrow list child count exceeds the offset limit"};
              }
              for (auto i = span.begin; i < span.end; ++i) {
                children.push_back(i);
              }
            }
            TRY(export_status(
              offsets.Append(static_cast<int32_t>(children.size()))));
          }
          TRY(auto child,
              export_column(lists.values(), target.value_type(), children,
                            storage::BitMap{lists.values().length(), true}));
          TRY(auto positions, finish_array(offsets));
          return std::shared_ptr<arrow::Array>{
            std::make_shared<arrow::ListArray>(
              schema.to_arrow_type(), rows.size(),
              positions->data()->buffers[1], std::move(child),
              std::move(bitmap), null_count)};
        }
      } else if constexpr (std::same_as<T, null_type>) {
        for (auto row : rows) {
          if (not absent(row)) {
            return Err{"concrete value type differs from Arrow schema"};
          }
        }
        return std::shared_ptr<arrow::Array>{
          std::make_shared<arrow::NullArray>(rows.size())};
      } else if constexpr (fundamental_type<type_to_data_t<T>>) {
        auto values = array.get_alternative<type_to_data_t<T>>();
        auto builder = target.make_arrow_builder(arrow_memory_pool());
        TRY(export_status(builder->Reserve(rows.size())));
        auto& typed = *builder;
        for (auto row : rows) {
          if (absent(row)) {
            TRY(export_status(typed.AppendNull()));
          } else {
            if (not values or not values->present.get(row)) {
              return Err{"concrete value type differs from Arrow schema"};
            }
            TRY(export_status(
              append_builder(target, typed, *values->data.get(row))));
          }
        }
        return finish_array(*builder);
      } else {
        return Err{"unsupported Arrow export type"};
      }
    });
}

// Deliberately narrower than Arrow's schema unification/casting: no numeric
// promotions, field insertion, field removal, or field reordering are allowed.
auto refine(std::shared_ptr<arrow::DataType> lhs,
            std::shared_ptr<arrow::DataType> rhs, bool fixed)
  -> Result<std::shared_ptr<arrow::DataType>, std::string> {
  if (lhs->Equals(rhs)) {
    return lhs;
  }
  if (rhs->id() == arrow::Type::NA) {
    return lhs;
  }
  if (not fixed and lhs->id() == arrow::Type::NA) {
    return rhs;
  }
  if (lhs->id() == rhs->id()
      and (lhs->id() == arrow::Type::STRUCT or lhs->id() == arrow::Type::LIST)
      and lhs->num_fields() == rhs->num_fields()) {
    auto fields = lhs->fields();
    for (auto i = 0; i < lhs->num_fields(); ++i) {
      auto const& other = rhs->field(i);
      if (not fields[i]->WithType(other->type())->Equals(other, true)) {
        return Err{std::string{"record fields or metadata changed"}};
      }
      TRY(auto type, refine(fields[i]->type(), other->type(), fixed));
      fields[i] = fields[i]->WithType(std::move(type));
    }
    if (lhs->id() == arrow::Type::LIST) {
      return arrow::list(fields[0]);
    }
    return arrow::struct_(std::move(fields));
  }
  return Err{fmt::format("incompatible types `{}` and `{}`", lhs->ToString(),
                         rhs->ToString())};
}

auto unresolved(arrow::DataType const& type) -> bool {
  if (type.id() == arrow::Type::NA) {
    return true;
  }
  return std::ranges::any_of(type.fields(), [](auto const& field) {
    return unresolved(*field->type());
  });
}

auto estimated_metadata_bytes(
  std::shared_ptr<arrow::KeyValueMetadata const> const& metadata) -> size_t {
  if (not metadata) {
    return 0;
  }
  auto result = sizeof(arrow::KeyValueMetadata);
  for (auto i = int64_t{0}; i < metadata->size(); ++i) {
    result += 2 * sizeof(std::string) + metadata->key(i).size()
              + metadata->value(i).size();
  }
  return result;
}

auto estimated_field_bytes(arrow::Field const& field) -> size_t {
  auto result = sizeof(arrow::Field) + field.name().size()
                + estimated_metadata_bytes(field.metadata())
                + sizeof(arrow::DataType);
  for (auto const& child : field.type()->fields()) {
    result += estimated_field_bytes(*child);
  }
  return result;
}

auto estimated_bytes(arrow::ArrayData const& data) -> size_t {
  auto result = sizeof(arrow::ArrayData);
  for (auto const& buffer : data.buffers) {
    if (buffer) {
      result += buffer->size();
    }
  }
  for (auto const& child : data.child_data) {
    result += estimated_bytes(*child);
  }
  return result;
}

// Reuse buffers only after refine() has checked the complete structure. In
// particular, a struct cast must never silently discard newly arrived fields.
auto normalize(std::shared_ptr<arrow::ArrayData> const& source,
               std::shared_ptr<arrow::DataType> const& target)
  -> arrow::Result<std::shared_ptr<arrow::ArrayData>> {
  if (source->type->Equals(target)) {
    return source;
  }
  if (source->type->id() == arrow::Type::NA) {
    ARROW_ASSIGN_OR_RAISE(auto array,
                          arrow::MakeArrayOfNull(target, source->length,
                                                 arrow_memory_pool()));
    return array->data();
  }
  auto result = source->Copy();
  result->type = target;
  TENZIR_ASSERT(result->child_data.size() == target->fields().size());
  for (auto i = size_t{0}; i < result->child_data.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(result->child_data[i],
                          normalize(result->child_data[i],
                                    target->field(i)->type()));
  }
  return result;
}

} // namespace

auto to_arrow_record_batch(Array<Record> const& records, type const& schema,
                           std::span<storage::Index const> rows)
  -> Result<std::shared_ptr<arrow::RecordBatch>, std::string> {
  if (not is<record_type>(schema)) {
    return Err{"Arrow export schema must be a record"};
  }
  for (auto row : rows) {
    if (row < 0 or row >= records.length()) {
      return Err{"Arrow export row is out of bounds"};
    }
  }
  TRY(auto array, export_column(Array<Data>{records}, schema, rows,
                                storage::BitMap{records.length(), true}));
  auto const& structure = as<arrow::StructArray>(*array);
  auto batch = arrow::RecordBatch::Make(schema.to_arrow_schema(), rows.size(),
                                        structure.fields());
  TRY(export_status(batch->ValidateFull()));
  return batch;
}

ArrowExportBuilder::ArrowExportBuilder() : ArrowExportBuilder{Limits{}} {
}

ArrowExportBuilder::ArrowExportBuilder(Limits limits) : limits_{limits} {
}

auto ArrowExportBuilder::flush(std::vector<table_slice>& output,
                               diagnostic_handler& dh, location loc)
  -> failure_or<void> {
  fixed_ = true;
  auto normalized_slices = std::vector<table_slice>{};
  normalized_slices.reserve(pending_.size());
  for (auto const& slice : pending_) {
    auto batch = to_record_batch(slice);
    auto columns = arrow::ArrayVector{};
    for (auto i = 0; i < batch->num_columns(); ++i) {
      auto normalized
        = normalize(batch->column(i)->data(), schema_->field(i)->type());
      if (not normalized.ok()) {
        diagnostic::error("failed to normalize Arrow export")
          .primary(loc)
          .note("{}", normalized.status().ToString())
          .emit(dh);
        return failure::promise();
      }
      columns.push_back(arrow::MakeArray(*normalized));
    }
    normalized_slices.emplace_back(
      arrow::RecordBatch::Make(schema_, batch->num_rows(), std::move(columns)));
  }
  if (not normalized_slices.empty()) {
    output.push_back(concatenate(std::move(normalized_slices)));
  }
  pending_.clear();
  rows_ = 0;
  bytes_ = 0;
  return {};
}

auto ArrowExportBuilder::accept(table_slice slice,
                                std::vector<table_slice>& output,
                                diagnostic_handler& dh, location loc)
  -> failure_or<void> {
  // A table_slice may carry a schema override that is not reflected in its
  // underlying RecordBatch (notably the internal-event attribute).
  auto batch = to_record_batch(slice)->ReplaceSchemaMetadata(
    slice.schema().to_arrow_schema()->metadata());
  if (schema_) {
    auto candidate = refine(arrow::struct_(schema_->fields()),
                            arrow::struct_(batch->schema()->fields()), fixed_);
    auto old_metadata = arrow::schema({}, schema_->metadata());
    auto new_metadata = arrow::schema({}, batch->schema()->metadata());
    if (not candidate or not old_metadata->Equals(new_metadata, true)) {
      auto diagnostic
        = diagnostic::error("input schema changed while writing Arrow data")
            .primary(loc)
            .note("{}", candidate ? "schema metadata changed"
                                  : candidate.unwrap_err())
            .note("first schema: {}", schema_->ToString())
            .note("current schema: {}", batch->schema()->ToString());
      if (limit_committed_ and not candidate
          and old_metadata->Equals(new_metadata, true)
          and refine(arrow::struct_(schema_->fields()),
                     arrow::struct_(batch->schema()->fields()), false)) {
        diagnostic = std::move(diagnostic)
                       .note("the schema inference window ended at the row or "
                             "byte limit; "
                             "null types can no longer be refined");
      }
      std::move(diagnostic).emit(dh);
      return failure::promise();
    }
    schema_ = arrow::schema(candidate.unwrap()->fields(), schema_->metadata());
  } else {
    schema_ = batch->schema();
  }
  rows_ += slice.rows();
  for (auto const& column : batch->columns()) {
    bytes_ += estimated_bytes(*column->data());
  }
  // Each pending slice owns schema information even when its values are all
  // null. Charge names and metadata recursively per slice, without deduplicating
  // shared types. Include both Arrow's schema and the table_slice schema copy;
  // this is a conservative estimate, not an exact allocator accounting.
  auto schema_bytes = sizeof(arrow::Schema)
                      + estimated_metadata_bytes(batch->schema()->metadata());
  for (auto const& field : batch->schema()->fields()) {
    schema_bytes += estimated_field_bytes(*field);
  }
  bytes_ += sizeof(table_slice) + sizeof(arrow::RecordBatch) + 2 * schema_bytes;
  pending_.push_back(std::move(slice));
  if (fixed_) {
    // Validate rows individually, but bound the temporary slice count and
    // concatenate them instead of exposing one output batch per input row.
    if (rows_ >= 1024 or bytes_ >= Limits{}.bytes) {
      TRY(flush(output, dh, loc));
    }
  } else {
    auto incomplete = unresolved(*arrow::struct_(schema_->fields()));
    auto at_limit = rows_ >= limits_.rows or bytes_ >= limits_.bytes;
    if (at_limit or not incomplete) {
      limit_committed_ = at_limit and incomplete;
      TRY(flush(output, dh, loc));
    }
  }
  return {};
}

auto ArrowExportBuilder::add(Events const& events, diagnostic_handler& dh,
                             location loc)
  -> failure_or<std::vector<table_slice>> {
  auto result = std::vector<table_slice>{};
  auto builder = series_builder{};
  auto rows = std::vector<storage::Index>{};
  for (auto row : storage::true_bits(events.mask)) {
    rows.push_back(row);
  }
  auto direct = true;
  for (auto offset = size_t{0}; offset < rows.size();) {
    if (direct and fixed_ and schema_ and pending_.empty()) {
      auto count = std::min(size_t{1024}, rows.size() - offset);
      auto selection = std::span{rows}.subspan(offset, count);
      auto metadata = ArrowMetadata::from_arrow(*schema_);
      auto same_metadata = std::ranges::all_of(selection, [&](auto row) {
        return *events.meta.name.get(row) == metadata.name
               and *events.meta.internal.get(row) == metadata.internal;
      });
      if (same_metadata) {
        auto batch = to_arrow_record_batch(
          events.data, type::from_arrow(*schema_), selection);
        if (batch) {
          TRY(accept(table_slice{std::move(batch).unwrap()}, result, dh, loc));
          offset += count;
          continue;
        }
        direct = false;
      }
      // Preserve existing coercions and diagnostics for inputs that cannot be
      // represented directly, notably heterogeneous lists.
    }
    auto index = rows[offset++];
    auto metadata = ArrowMetadata{std::string{*events.meta.name.get(index)},
                                  *events.meta.internal.get(index)};
    builder.data(materialize_legacy(events.data.get(index)));
    // A batch builder unions record fields and loses each original row's
    // shape/order. Convert each row separately before validation, retaining
    // the builder's heterogeneous-list coercion within that row.
    for (auto& slice : builder.finish_as_table_slice(metadata.name)) {
      auto schema = metadata.apply(slice.schema()).to_arrow_schema();
      slice = table_slice{
        to_record_batch(slice)->ReplaceSchemaMetadata(schema->metadata())};
      TRY(accept(std::move(slice), result, dh, loc));
    }
  }
  if (fixed_) {
    TRY(flush(result, dh, loc));
  }
  return result;
}

auto ArrowExportBuilder::finish(diagnostic_handler& dh, location loc)
  -> failure_or<std::vector<table_slice>> {
  auto result = std::vector<table_slice>{};
  TRY(flush(result, dh, loc));
  return result;
}

} // namespace tenzir::nova
