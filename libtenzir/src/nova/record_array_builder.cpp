#include "tenzir/nova/record_array_builder.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/nova/data_array_builder.hpp"
#include "tenzir/nova/masked_array_builder.hpp"
#include "tenzir/nova/shape_table.hpp"

#include <string>
#include <string_view>
#include <utility>

namespace tenzir::nova {

struct ArrayBuilder<Record>::Storage {
  using Slot = MaskedArrayBuilder<ArrayBuilder<Data>>;

  storage::DataOwner<storage::Index[]>::Builder shape_index_builder;
  ShapeTable shape_table;
  Array<Record>::Names names;
  /// Field names by index, viewing the keys of `names`.
  storage::Vector<std::string_view> field_names;
  storage::Vector<storage::Index> current_row_indices;
  /// The names of `current_row_indices`, viewing the keys of `names`, which
  /// are stable because the map is node-based.
  storage::Vector<std::string_view> current_row_names;
  /// The previous row's fields, in order. Consecutive events usually share
  /// their key order, so `field()` first checks whether the next key matches
  /// the previous row's key at the same position and skips hashing if so, and
  /// `finish_last_row()` reuses the previous shape when the rows agree.
  storage::Vector<storage::Index> previous_row_indices;
  storage::Vector<std::string_view> previous_row_names;
  ShapeTable::ShapeId previous_shape = -1;
  /// Field builders are padded lazily: a field only catches up to the number
  /// of finished rows right before it receives a value, and every field
  /// catches up in `finish()`. Rows without a given field therefore cost
  /// nothing per field until that field is next written, when the absent rows
  /// are appended in bulk.
  storage::Vector<Slot> field_builders;
  bool row_open = false;
};

ArrayBuilder<Record>::RecordBuilder::RecordBuilder(ArrayBuilder* parent)
  : parent_{parent} {
}

ArrayBuilder<Record>::ArrayBuilder() : storage_{std::make_unique<Storage>()} {
}
ArrayBuilder<Record>::ArrayBuilder(ArrayBuilder&&) noexcept = default;
auto ArrayBuilder<Record>::operator=(ArrayBuilder&&) noexcept
  -> ArrayBuilder& = default;
ArrayBuilder<Record>::~ArrayBuilder() = default;

auto ArrayBuilder<Record>::record() -> RecordBuilder {
  finish_last_row();
  storage_->row_open = true;
  return RecordBuilder{this};
}

auto ArrayBuilder<Record>::RecordBuilder::field(std::string_view name)
  -> FieldBuilder {
  auto& storage = *parent_->storage_;
  const auto position = storage.current_row_indices.size();
  auto field_index = storage::Index{-1};
  auto stored_name = std::string_view{};
  if (position < storage.previous_row_names.size()
      and storage.previous_row_names[position] == name) {
    // Same key at the same position as in the previous row: no hashing.
    field_index = storage.previous_row_indices[position];
    stored_name = storage.previous_row_names[position];
  } else if (const auto it = storage.names.find(name);
             it != storage.names.end()) {
    field_index = static_cast<storage::Index>(it->second);
    stored_name = it->first;
  } else {
    // Only a genuinely new field pays for the owning key string.
    const auto [inserted, did_insert] = storage.names.try_emplace(
      storage::String<>{name}, storage.field_builders.size());
    TENZIR_ASSERT(did_insert);
    field_index = static_cast<storage::Index>(inserted->second);
    stored_name = inserted->first;
    storage.field_builders.emplace_back();
    storage.field_names.push_back(stored_name);
  }
  auto& builder = storage.field_builders[static_cast<std::size_t>(field_index)];
  const auto finished_rows = storage.shape_index_builder.size();
  if (builder.size() > finished_rows) {
    // The open row already holds this field.
    return FieldBuilder{&builder, true};
  }
  if (const auto missing = finished_rows - builder.size(); missing > 0) {
    builder.skip_n(missing);
  }
  storage.current_row_indices.push_back(field_index);
  storage.current_row_names.push_back(stored_name);
  return FieldBuilder{&builder, false};
}

auto ArrayBuilder<Record>::finish_last_row() -> void {
  auto& storage = *storage_;
  if (not storage.row_open) {
    return;
  }
  auto shape_index = storage.previous_shape;
  if (shape_index < 0
      or storage.current_row_indices != storage.previous_row_indices) {
    shape_index = ShapeTable::empty_shape;
    for (auto const& field : storage.current_row_indices) {
      shape_index = storage.shape_table.with_field(shape_index, field);
    }
  }
  storage.shape_index_builder.emplace_back(shape_index);
  storage.previous_shape = shape_index;
  std::swap(storage.current_row_indices, storage.previous_row_indices);
  std::swap(storage.current_row_names, storage.previous_row_names);
  storage.current_row_indices.clear();
  storage.current_row_names.clear();
  storage.row_open = false;
}

auto ArrayBuilder<Record>::skip() -> void {
  finish_last_row();
  storage_->shape_index_builder.emplace_back(-1);
}

auto ArrayBuilder<Record>::skip_n(storage::Index count) -> void {
  finish_last_row();
  storage_->shape_index_builder.append_n(count, -1);
}

auto ArrayBuilder<Record>::length() const -> storage::Index {
  return storage_->shape_index_builder.size() + (storage_->row_open ? 1 : 0);
}

auto ArrayBuilder<Record>::take_last() -> Data {
  finish_last_row();
  auto& storage = *storage_;
  TENZIR_ASSERT_GT(storage.shape_index_builder.size(), 0);
  const auto shape = storage.shape_index_builder.back();
  TENZIR_ASSERT_LEQ(0, shape);
  storage.shape_index_builder.pop_back();
  auto result = Record{};
  for (const auto field : storage.shape_table.fields(shape)) {
    const auto index = static_cast<std::size_t>(field);
    result.emplace(std::string{storage.field_names[index]},
                   storage.field_builders[index].take_last());
  }
  // The row cache described the removed row.
  storage.previous_shape = -1;
  storage.previous_row_indices.clear();
  storage.previous_row_names.clear();
  return Data{std::move(result)};
}

auto ArrayBuilder<Record>::finish() -> Array<Record> {
  finish_last_row();
  const auto rows = storage_->shape_index_builder.size();
  auto arrays = Array<Record>::MaskedArrays{};
  arrays.reserve(storage_->field_builders.size());
  for (auto& field : storage_->field_builders) {
    if (const auto missing = rows - field.size(); missing > 0) {
      field.skip_n(missing);
    }
    arrays.push_back(field.finish());
  }
  return Array<Record>{storage::SparseStorage<storage::Index>{
                         storage_->shape_index_builder.finish()},
                       std::move(storage_->shape_table),
                       std::move(storage_->names), std::move(arrays)};
}

} // namespace tenzir::nova
