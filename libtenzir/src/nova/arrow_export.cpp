//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/arrow_export.hpp"

#include "tenzir/arrow_memory_pool.hpp"
#include "tenzir/nova/arrow_metadata.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/materialize.hpp"
#include "tenzir/series_builder.hpp"

#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/util/key_value_metadata.h>

#include <algorithm>
#include <iterator>
#include <utility>

namespace tenzir::nova {

auto to_table_slices(Events const& events) -> std::vector<table_slice> {
  struct Builder {
    ArrowMetadata metadata;
    series_builder builder;
  };
  auto builders = std::vector<Builder>{};
  for (auto index : storage::true_bits(events.mask)) {
    auto value = materialize_legacy(events.data.get(index));
    auto metadata = ArrowMetadata{std::string{*events.meta.name.get(index)},
                                  *events.meta.internal.get(index)};
    auto it = std::ranges::find_if(builders, [&](auto const& entry) {
      return entry.metadata.name == metadata.name
             and entry.metadata.internal == metadata.internal;
    });
    if (it == builders.end()) {
      it = builders.emplace(builders.end(), std::move(metadata),
                            series_builder{});
    }
    it->builder.data(value);
  }
  auto result = std::vector<table_slice>{};
  for (auto& [metadata, builder] : builders) {
    auto slices = builder.finish_as_table_slice(metadata.name);
    for (auto& slice : slices) {
      slice
        = table_slice{to_record_batch(slice), metadata.apply(slice.schema())};
    }
    result.insert(result.end(), std::make_move_iterator(slices.begin()),
                  std::make_move_iterator(slices.end()));
  }
  return result;
}

namespace {

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
  // underlying RecordBatch (notably Nova's internal-event attribute).
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
  for (auto index : storage::true_bits(events.mask)) {
    auto metadata = ArrowMetadata{std::string{*events.meta.name.get(index)},
                                  *events.meta.internal.get(index)};
    builder.data(materialize_legacy(events.data.get(index)));
    // A batch builder unions record fields and loses each original row's
    // shape/order. Convert each row separately before validation, retaining
    // the builder's heterogeneous-list coercion within that row.
    for (auto& slice : builder.finish_as_table_slice(metadata.name)) {
      slice
        = table_slice{to_record_batch(slice), metadata.apply(slice.schema())};
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
