//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "parquet/constant_columns.hpp"

#include "parquet/statistics_values.hpp"

#include <tenzir/arrow_memory_pool.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/nova/arrow_import.hpp>
#include <tenzir/option.hpp>
#include <tenzir/type.hpp>

#include <arrow/array.h>
#include <arrow/array/util.h>
#include <arrow/buffer.h>
#include <arrow/extension_type.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <parquet/arrow/schema.h>
#include <parquet/exception.h>
#include <parquet/metadata.h>
#include <parquet/schema.h>
#include <parquet/statistics.h>

#include <algorithm>
#include <cstring>
#include <string>

namespace tenzir::plugins::parquet {

namespace {

using ::parquet::arrow::SchemaField;

/// What the statistics of a leaf column say about its chunks in all row
/// groups to read.
struct Chunks {
  enum class Kind : uint8_t { varies, constant, null };

  Kind kind = Kind::varies;
  /// Whether the column is valid in every row.
  bool valid = false;
  /// Statistics that hold the value of a constant.
  std::shared_ptr<::parquet::Statistics> stats;
};

} // namespace

class ConstantColumns::Planner {
public:
  Planner(::parquet::FileMetaData const& metadata,
          std::span<int const> row_groups, std::span<int const> columns,
          FormatArray const& format)
    : schema_{*metadata.schema()},
      format_{format},
      selected_(metadata.num_columns(), false),
      chunks_(metadata.num_columns()) {
    for (auto group : row_groups) {
      auto row_group = metadata.RowGroup(group);
      // Row groups without rows add nothing to a batch.
      if (row_group->num_rows() > 0) {
        groups_.push_back(std::move(row_group));
      }
    }
    for (auto column : columns) {
      selected_[column] = true;
    }
  }

  auto empty() const -> bool {
    return groups_.empty();
  }

  /// Plans a field if a leaf below it is selected.
  auto plan(SchemaField const& field) const -> Option<Node> {
    if (not selected(field)) {
      return None{};
    }
    auto node = Node{};
    node.field = field.field;
    auto const& type = field.field->type();
    // The restored type decides, not the Parquet schema: a subnet, for
    // example, is stored like a record.
    if (type->id() == arrow::Type::STRUCT) {
      node.kind = Node::Kind::record;
      for (auto const& child : field.children) {
        if (auto planned = plan(child)) {
          node.children.push_back(std::move(*planned));
        }
      }
      update(node);
      // Without decoded fields, the reader cannot tell where a record is
      // null. It is valid in every row if it cannot be null, or if a leaf
      // below it is valid in every row, which includes all constants.
      // Otherwise, all its selected leaves are null throughout, and one of
      // them still needs decoding.
      if (not node.decoded and field.field->nullable() and not present(field)) {
        decode_first(node);
      }
      return node;
    }
    if (field.is_leaf()) {
      leaf(field, node);
    } else if (dynamic_cast<subnet_type::arrow_type const*>(type.get())) {
      subnet(field, node);
    }
    // Lists and maps hold several values of their leaves per row, and other
    // extension types do not import.
    return node;
  }

private:
  auto selected(SchemaField const& field) const -> bool {
    if (field.is_leaf()) {
      return selected_[field.column_index];
    }
    return std::ranges::any_of(field.children, [&](SchemaField const& child) {
      return selected(child);
    });
  }

  static auto update(Node& node) -> void {
    node.decoded = std::ranges::any_of(node.children, &Node::decoded);
    node.constant = std::ranges::any_of(node.children, &Node::constant);
  }

  /// Decodes the first leaf of a record whose leaves are all null.
  static auto decode_first(Node& node) -> void {
    if (node.kind == Node::Kind::record) {
      TENZIR_ASSERT(not node.children.empty());
      decode_first(node.children.front());
      update(node);
      return;
    }
    TENZIR_ASSERT(node.kind == Node::Kind::null);
    auto decoded = Node{};
    decoded.field = std::move(node.field);
    node = std::move(decoded);
  }

  /// Whether a leaf below `field` is valid in every row, which makes all its
  /// parents valid, too.
  auto present(SchemaField const& field) const -> bool {
    if (not field.is_leaf()) {
      return std::ranges::any_of(field.children, [&](SchemaField const& child) {
        return present(child);
      });
    }
    return chunks(field.column_index).valid;
  }

  /// What the statistics say about a leaf column.
  auto chunks(int column) const -> Chunks const& {
    auto& chunks = chunks_[column];
    if (not chunks) {
      chunks = classify(column);
    }
    return *chunks;
  }

  auto classify(int column) const -> Chunks {
    // The null count of a leaf inside a list counts its values, not rows.
    if (schema_.Column(column)->max_repetition_level() != 0) {
      return {};
    }
    auto result = Chunks{};
    result.valid = true;
    auto value = std::string{};
    for (auto const& group : groups_) {
      auto chunk = group->ColumnChunk(column);
      // A leaf outside of lists holds one value per row. A chunk that says
      // otherwise is left to the reader.
      if (chunk->num_values() != group->num_rows()
          or not chunk->is_stats_set()) {
        return {};
      }
      auto stats = chunk->statistics();
      if (not stats or not stats->HasNullCount()) {
        return {};
      }
      result.valid &= stats->null_count() == 0;
      auto kind = Chunks::Kind::varies;
      if (stats->null_count() == chunk->num_values()) {
        kind = Chunks::Kind::null;
      } else if (stats->null_count() == 0
                 and stats->HasMinMax()
                 // Writers may truncate long values and then flag them as
                 // inexact. Files that predate the flag leave it unset, and
                 // their bounds are exact.
                 and stats->is_min_value_exact().value_or(true)
                 and stats->is_max_value_exact().value_or(true)) {
        // Equal bounds imply a single value under any ordering, so the
        // column order does not matter here.
        auto min = stats->EncodeMin();
        if (min == stats->EncodeMax() and (not result.stats or min == value)) {
          kind = Chunks::Kind::constant;
          value = std::move(min);
        }
      }
      if (not result.stats) {
        result.kind = kind;
        result.stats = std::move(stats);
      } else if (kind != result.kind) {
        result.kind = Chunks::Kind::varies;
      }
    }
    return result;
  }

  /// The formatted array if its import succeeds.
  auto imports(std::shared_ptr<arrow::Array> array) const
    -> Option<std::shared_ptr<arrow::Array>> {
    auto formatted = format_(std::move(array));
    if (not formatted.ok() or nova::import_arrow_array(**formatted).is_err()) {
      return None{};
    }
    return formatted.MoveValueUnsafe();
  }

  /// The value of a constant leaf, if it imports.
  auto value(SchemaField const& field, Chunks const& chunks) const
    -> Option<std::shared_ptr<arrow::Array>> {
    auto const& column = *schema_.Column(field.column_index);
    auto value = statistics_bound(*chunks.stats, false);
    if (not value) {
      return None{};
    }
    return decode_value(column, field.field->type(), *value);
  }

  auto leaf(SchemaField const& field, Node& node) const -> void {
    auto const& column = *schema_.Column(field.column_index);
    auto const& chunks = this->chunks(field.column_index);
    switch (chunks.kind) {
      case Chunks::Kind::varies:
        return;
      case Chunks::Kind::null: {
        // The import of an all-null column fails only for types that it
        // rejects, and otherwise yields nulls, whatever the type.
        auto nulls
          = arrow::MakeArrayOfNull(field.field->type(), 1, arrow_memory_pool());
        if (nulls.ok() and imports(nulls.MoveValueUnsafe())) {
          node.kind = Node::Kind::null;
          node.columns = {field.column_index};
          node.decoded = false;
          node.constant = true;
        }
        return;
      }
      case Chunks::Kind::constant: {
        // Statistics leave out NaNs, and -0 and +0 compare equal, so equal
        // bounds do not make a float constant.
        if (column.physical_type() == ::parquet::Type::FLOAT
            or column.physical_type() == ::parquet::Type::DOUBLE) {
          return;
        }
        if (auto decoded = value(field, chunks)) {
          constant(node, std::move(*decoded), {field.column_index});
        }
        return;
      }
    }
    TENZIR_UNREACHABLE();
  }

  /// A subnet is constant if both its address and its length are.
  auto subnet(SchemaField const& field, Node& node) const -> void {
    auto arrays = arrow::ArrayVector{};
    auto columns = std::vector<int>{};
    for (auto const& child : field.children) {
      if (not child.is_leaf()) {
        return;
      }
      columns.push_back(child.column_index);
      auto const& chunks = this->chunks(child.column_index);
      if (chunks.kind != Chunks::Kind::constant) {
        return;
      }
      auto decoded = value(child, chunks);
      if (not decoded) {
        return;
      }
      arrays.push_back(std::move(*decoded));
    }
    auto const& type = field.field->type();
    auto const& storage
      = static_cast<arrow::ExtensionType const&>(*type).storage_type();
    // Arrow restores a subnet only if its fields have the types of the
    // storage, which the import relies on.
    if (static_cast<int>(arrays.size()) != storage->num_fields()) {
      return;
    }
    for (auto i = 0; i < storage->num_fields(); ++i) {
      if (not arrays[i]->type()->Equals(*storage->field(i)->type())) {
        return;
      }
    }
    auto records = arrow::StructArray::Make(arrays, storage->fields());
    if (not records.ok()) {
      return;
    }
    constant(node, arrow::ExtensionType::WrapArray(type, *records),
             std::move(columns));
  }

  auto constant(Node& node, std::shared_ptr<arrow::Array> value,
                std::vector<int> columns) const -> void {
    auto imported = imports(std::move(value));
    if (not imported) {
      return;
    }
    node.kind = Node::Kind::constant;
    node.value = (*imported)->data();
    node.columns = std::move(columns);
    node.decoded = false;
    node.constant = true;
  }

  ::parquet::SchemaDescriptor const& schema_;
  FormatArray const& format_;
  std::vector<std::unique_ptr<::parquet::RowGroupMetaData>> groups_;
  std::vector<bool> selected_;
  mutable std::vector<Option<Chunks>> chunks_;
};

auto ConstantColumns::make(::parquet::FileMetaData const& metadata,
                           ::parquet::arrow::SchemaManifest const* manifest,
                           std::span<int const> row_groups,
                           std::span<int const> columns,
                           FormatArray const& format) -> ConstantColumns {
  auto result = ConstantColumns{};
  result.decoded_.assign(columns.begin(), columns.end());
  if (not manifest) {
    return result;
  }
  // The leaves of the fields that the statistics provide, which the reader
  // then skips.
  auto skipped = std::vector<int>{};
  auto skip = [&](this auto const& self, Node const& node) -> void {
    skipped.insert(skipped.end(), node.columns.begin(), node.columns.end());
    for (auto const& child : node.children) {
      self(child);
    }
  };
  try {
    auto planner = Planner{metadata, row_groups, columns, format};
    if (planner.empty()) {
      return result;
    }
    for (auto const& field : manifest->schema_fields) {
      if (auto node = planner.plan(field)) {
        skip(*node);
        result.fields_.push_back(std::move(*node));
      }
    }
  } catch (::parquet::ParquetException const&) {
    // Statistics that do not decode decide nothing, and the reader never
    // looks at them.
    result.fields_.clear();
    return result;
  }
  if (skipped.empty()) {
    result.fields_.clear();
    return result;
  }
  result.any_ = true;
  std::ranges::sort(skipped);
  auto [first, last] = std::ranges::remove_if(result.decoded_, [&](int column) {
    return std::ranges::binary_search(skipped, column);
  });
  result.decoded_.erase(first, last);
  return result;
}

auto ConstantColumns::complete(std::shared_ptr<arrow::RecordBatch> batch)
  -> arrow::Result<std::shared_ptr<arrow::RecordBatch>> {
  if (not any_) {
    return batch;
  }
  auto rows = batch->num_rows();
  auto schema = batch->schema();
  auto columns = batch->column_data();
  // Keep the decoded arrays the only owners of their buffers, so that the
  // import can adopt them.
  batch.reset();
  auto fields = arrow::FieldVector{};
  auto arrays = arrow::ArrayDataVector{};
  auto next = size_t{0};
  for (auto& node : fields_) {
    if (not node.decoded) {
      ARROW_ASSIGN_OR_RAISE(auto data, repeat(node, rows));
      fields.push_back(node.field->WithType(data->type));
      arrays.push_back(std::move(data));
      continue;
    }
    if (next == columns.size()) {
      return arrow::Status::Invalid("parquet reader returned fewer columns "
                                    "than expected");
    }
    auto field = schema->field(static_cast<int>(next));
    auto data = std::move(columns[next]);
    ++next;
    if (node.constant) {
      ARROW_ASSIGN_OR_RAISE(data, rebuild(node, std::move(data)));
      field = field->WithType(data->type);
    }
    fields.push_back(std::move(field));
    arrays.push_back(std::move(data));
  }
  if (next != columns.size()) {
    return arrow::Status::Invalid("parquet reader returned more columns than "
                                  "expected");
  }
  return arrow::RecordBatch::Make(
    arrow::schema(std::move(fields), schema->endianness(), schema->metadata()),
    rows, std::move(arrays));
}

auto ConstantColumns::decoded() const -> std::vector<int> const& {
  return decoded_;
}

auto ConstantColumns::repeat(Node& node, int64_t length)
  -> arrow::Result<std::shared_ptr<arrow::ArrayData>> {
  // Arrays of the last length serve all batches of that length, as the import
  // never adopts the buffers of arrays that are shared.
  if (node.cache and node.cache->length == length) {
    return node.cache;
  }
  auto result = std::shared_ptr<arrow::ArrayData>{};
  switch (node.kind) {
    case Node::Kind::constant: {
      // A dictionary whose indices are all zero repeats its only value, which
      // the import recognizes without looking at the indices.
      if (not zeros_ or zeros_->size() < length) {
        ARROW_ASSIGN_OR_RAISE(
          auto zeros, arrow::AllocateBuffer(length, arrow_memory_pool()));
        std::memset(zeros->mutable_data(), 0,
                    static_cast<size_t>(zeros->size()));
        zeros_ = std::move(zeros);
      }
      result = arrow::ArrayData::Make(
        arrow::dictionary(arrow::int8(), node.value->type), length,
        {nullptr, zeros_}, {}, node.value, 0);
      break;
    }
    case Node::Kind::null:
      result = arrow::ArrayData::Make(arrow::null(), length, {nullptr}, length);
      break;
    case Node::Kind::record: {
      auto fields = arrow::FieldVector{};
      auto children = arrow::ArrayDataVector{};
      for (auto& child : node.children) {
        ARROW_ASSIGN_OR_RAISE(auto data, repeat(child, length));
        fields.push_back(child.field->WithType(data->type));
        children.push_back(std::move(data));
      }
      // The planner made sure that records without decoded fields are valid
      // in every row.
      result = arrow::ArrayData::Make(arrow::struct_(std::move(fields)), length,
                                      {nullptr}, std::move(children), 0);
      break;
    }
    case Node::Kind::decoded:
      TENZIR_UNREACHABLE();
  }
  node.cache = result;
  return result;
}

auto ConstantColumns::rebuild(Node& node,
                              std::shared_ptr<arrow::ArrayData> data)
  -> arrow::Result<std::shared_ptr<arrow::ArrayData>> {
  if (data->type->id() != arrow::Type::STRUCT) {
    return arrow::Status::Invalid("parquet reader returned `",
                                  data->type->ToString(),
                                  "` instead of a record");
  }
  // The fields of a record line up with its rows including its offset.
  auto length = data->offset + data->length;
  auto const& type = *data->type;
  auto fields = arrow::FieldVector{};
  auto children = arrow::ArrayDataVector{};
  auto next = size_t{0};
  for (auto& child : node.children) {
    if (not child.decoded) {
      ARROW_ASSIGN_OR_RAISE(auto repeated, repeat(child, length));
      fields.push_back(child.field->WithType(repeated->type));
      children.push_back(std::move(repeated));
      continue;
    }
    if (next == data->child_data.size()) {
      return arrow::Status::Invalid("parquet reader returned fewer fields "
                                    "than expected");
    }
    auto field = type.field(static_cast<int>(next));
    auto child_data = data->child_data[next];
    ++next;
    if (child.constant) {
      ARROW_ASSIGN_OR_RAISE(child_data, rebuild(child, std::move(child_data)));
      field = field->WithType(child_data->type);
    }
    fields.push_back(std::move(field));
    children.push_back(std::move(child_data));
  }
  if (next != data->child_data.size()) {
    return arrow::Status::Invalid("parquet reader returned more fields than "
                                  "expected");
  }
  auto validity = data->buffers.empty() ? nullptr : data->buffers[0];
  return arrow::ArrayData::Make(arrow::struct_(std::move(fields)), data->length,
                                {std::move(validity)}, std::move(children),
                                data->null_count.load(), data->offset);
}

} // namespace tenzir::plugins::parquet
