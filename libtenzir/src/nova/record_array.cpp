#include "tenzir/nova/record_array.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/nova/array_merge.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/data_array_builder.hpp"
#include "tenzir/nova/shape_table.hpp"
#include "tenzir/nova/storage.hpp"
#include "tenzir/nova/union_array.hpp"

#include <algorithm>
#include <memory>
#include <ranges>
#include <utility>

namespace tenzir::nova {

namespace storage {

RecordStorage::Storage::Storage(IndicesStorage indices, ShapeTable table,
                                Names field_names, MaskedArrays fields)
  : shape_indices{std::move(indices)},
    shape_table{std::move(table)},
    names{std::move(field_names)},
    arrays{std::move(fields)} {
  names_by_index.resize(arrays.size());
  for (auto const& [name, index] : names) {
    names_by_index[index] = std::string_view{name};
  }
}

RecordStorage::Storage::~Storage() = default;
RecordStorage::Storage::Storage(Storage const& other)
  : Storage{other.shape_indices, other.shape_table, other.names, other.arrays} {
}
RecordStorage::Storage::Storage(Storage&&) noexcept = default;

RecordStorage::RecordStorage(IndicesStorage indices, ShapeTable table,
                             Names names, MaskedArrays arrays)
  : storage_{StructureOwner<Storage>::make(std::move(indices), std::move(table),
                                           std::move(names),
                                           std::move(arrays))} {
}

RecordStorage::RecordStorage(StructureOwner<Storage> storage)
  : storage_{std::move(storage)} {
}
RecordStorage::~RecordStorage() = default;
RecordStorage::RecordStorage(RecordStorage const&) = default;
RecordStorage::RecordStorage(RecordStorage&&) noexcept = default;
auto RecordStorage::operator=(RecordStorage const&) -> RecordStorage& = default;
auto RecordStorage::operator=(RecordStorage&&) noexcept
  -> RecordStorage& = default;

auto RecordStorage::as_unique() const& -> RecordStorage {
  return RecordStorage{storage_.as_unique()};
}

auto RecordStorage::as_unique() && -> RecordStorage {
  return RecordStorage{std::move(storage_).as_unique()};
}

auto RecordStorage::operator*() const -> Storage const& {
  return *storage_;
}

auto RecordStorage::length() const noexcept -> Index {
  return storage_->shape_indices.length();
}

auto RecordStorage::get(Index i) const -> ViewType {
  TENZIR_ASSERT_LEQ_EXPENSIVE(0, i);
  TENZIR_ASSERT_LT_EXPENSIVE(i, length());
  TENZIR_ASSERT_LEQ_EXPENSIVE(0, storage_->shape_indices.get(i));
  return RowView<Record>{*storage_, i};
}

static_assert(storage<RecordStorage>);
static_assert(storage<ConstantStorage<Record, RowView<Record>>>);
} // namespace storage

Array<Record>::Array(IndicesStorage indices, ShapeTable&& table, Names names,
                     MaskedArrays arrays)
  : Array{storage::RecordStorage{std::move(indices), std::move(table),
                                 std::move(names), std::move(arrays)}} {
}

Array<Record>::Array(storage::RecordStorage storage)
  : storage_{std::move(storage)} {
}

Array<Record>::Array(storage::ConstantStorage<Record, RowView<Record>> storage)
  : storage_{std::move(storage)} {
}

auto Array<Record>::storage() const& -> PhysicalStorage const& {
  return storage_;
}

auto Array<Record>::storage() && -> PhysicalStorage&& {
  return std::move(storage_);
}

auto Array<Record>::primary() -> storage::RecordStorage::Storage& {
  auto& physical = as<storage::RecordStorage>(storage_);
  physical = std::move(physical).as_unique();
  return *physical.storage_;
}

auto Array<Record>::primary() const -> storage::RecordStorage::Storage const& {
  return *as<storage::RecordStorage>(storage());
}

auto Array<Record>::as_unique() const& -> Array {
  return match(storage(), [](auto const& physical) -> Array {
    return Array{physical.as_unique()};
  });
}

auto Array<Record>::as_unique() && -> Array {
  return match(std::move(storage_), [](auto&& physical) -> Array {
    return Array{std::move(physical).as_unique()};
  });
}

auto Array<Record>::length() const noexcept -> storage::Index {
  return match(storage(), [](auto const& physical) {
    return physical.length();
  });
}

auto Array<Record>::get(storage::Index i) const -> RowView<Record> {
  return match(storage(), [i](auto const& physical) {
    return physical.get(i);
  });
}

auto Array<Record>::to_primary() const -> Array {
  if (is<storage::RecordStorage>(storage())) {
    return *this;
  }
  auto builder = ArrayBuilder<Record>{};
  for (auto i = storage::Index{0}; i < length(); ++i) {
    auto row = builder.record();
    for (auto [name, value] : get(i)) {
      append_row(row.field(name), value);
    }
  }
  return builder.finish();
}

auto Array<Record>::field(std::string_view name) const -> Option<MaskedArray> {
  return match(
    storage(),
    [&](storage::RecordStorage const& physical) -> Option<MaskedArray> {
      auto const& storage = *physical;
      auto it = storage.names.find(name);
      if (it == storage.names.end()) {
        return None{};
      }
      return storage.arrays[it->second];
    },
    [&](storage::ConstantStorage<Record, RowView<Record>> const& physical)
      -> Option<MaskedArray> {
      auto const& value = physical.value();
      auto it = value.find(name);
      if (it == value.end()) {
        return None{};
      }
      return MaskedArray{repeat(it->second, length()),
                         storage::BitMap{length(), true}};
    });
}

RowView<Record>::RowView(Record const& value)
  : representation_{Constant{value}} {
}

RowView<Record>::RowView(storage::RecordStorage::Storage const& storage,
                         storage::Index row)
  : representation_{Primary{storage, row}} {
}

RowView<Record>::RowView(Array<Record> const& array, storage::Index row)
  : RowView{array.get(row)} {
}

RowView<Record>::iterator::iterator(Representation representation,
                                    std::size_t index)
  : representation_{std::move(representation)}, index_{index} {
}

auto RowView<Record>::iterator::operator*() const -> value_type {
  return match(
    representation_,
    [this](Primary const& primary) -> value_type {
      auto const shape_index = primary.storage->shape_indices.get(primary.row);
      auto const shape = primary.storage->shape_table.fields(shape_index);
      auto const flat_index = static_cast<std::size_t>(shape[index_]);
      auto const name = primary.storage->names_by_index[flat_index];
      auto const& field = primary.storage->arrays[flat_index];
      if (not field.present.get(primary.row)) {
        return {name, RowView<Data>{RowView<Null>{Null{}}}};
      }
      return {name, field.data.get(primary.row)};
    },
    [this](Constant const& constant) -> value_type {
      auto const& [name, value] = *(constant.value->begin() + index_);
      return {name, RowView<Data>{value}};
    });
}

auto RowView<Record>::iterator::operator++() -> iterator& {
  ++index_;
  return *this;
}

auto operator==(const RowView<Record>::iterator&,
                const RowView<Record>::iterator&) -> bool
  = default;

auto RowView<Record>::begin() const -> iterator {
  return {representation_, 0};
}

auto RowView<Record>::end() const -> iterator {
  auto count = match(
    representation_,
    [](Primary const& primary) {
      auto const shape_index = primary.storage->shape_indices.get(primary.row);
      return primary.storage->shape_table.fields(shape_index).size();
    },
    [](Constant const& constant) {
      return constant.value->size();
    });
  return {representation_, count};
}

namespace {

template <class Transition>
auto rewrite_shape_indices(Array<Record>::IndicesStorage shape_indices,
                           ShapeTable& shape_table, const storage::BitMap& mask,
                           Transition&& transition)
  -> Array<Record>::IndicesStorage {
  if (mask.as_constant() == false) {
    return shape_indices;
  }
  TENZIR_ASSERT_EQ(shape_indices.length(), mask.length());
  auto mutable_indices
    = Array<Record>::IndicesStorage::Mutable{std::move(shape_indices)};
  auto* const indices = mutable_indices.data();
  constexpr auto unresolved = ShapeTable::ShapeId{-1};
  // Shape ids that `transition` creates are never read from `indices`, so
  // the table only needs to cover the ids that exist before the loop.
  auto resolved
    = storage::Vector<ShapeTable::ShapeId>(shape_table.size(), unresolved);
  storage::for_each_true(mask, [&](storage::Index row) {
    const auto old_id = indices[row];
    if (old_id < 0) {
      return;
    }
    TENZIR_ASSERT_LT_EXPENSIVE(static_cast<std::size_t>(old_id),
                               resolved.size());
    auto& new_id = resolved[static_cast<std::size_t>(old_id)];
    if (new_id == unresolved) {
      new_id = transition(shape_table, old_id);
    }
    indices[row] = new_id;
  });
  return std::move(mutable_indices).finish();
}

} // namespace

auto Array<Record>::dangerously_extract_field(
  std::string_view name) && -> Option<MaskedArray> {
  if (not is<storage::RecordStorage>(storage())) {
    return to_primary().dangerously_extract_field(name);
  }
  *this = std::move(*this).as_unique();
  auto const it = primary().names.find(name);
  if (it == primary().names.end()) {
    return None{};
  }
  return std::move(primary().arrays[it->second]);
}

auto Array<Record>::with_field_overwrite(std::string_view name,
                                         MaskedArray value) const& -> Array {
  return as_unique().with_field_overwrite(name, std::move(value));
}

auto Array<Record>::with_field_overwrite(std::string_view name,
                                         MaskedArray value) && -> Array {
  if (not is<storage::RecordStorage>(storage())) {
    return to_primary().with_field_overwrite(name, std::move(value));
  }
  *this = std::move(*this).as_unique();
  const auto it = primary().names.find(name);
  TENZIR_ASSERT_EQ(length(), value.data.length());
  TENZIR_ASSERT_EQ(length(), value.present.length());
  if (it != primary().names.end()) {
    const auto existing_index = static_cast<storage::Index>(it->second);
    primary().shape_indices = rewrite_shape_indices(
      std::move(primary().shape_indices), primary().shape_table, value.present,
      [existing_index](ShapeTable& table, ShapeTable::ShapeId id) {
        return table.with_field(id, existing_index);
      });
    primary().arrays[it->second] = std::move(value);
    return std::move(*this);
  }
  const auto new_index = static_cast<storage::Index>(primary().arrays.size());
  auto mask = value.present;
  primary().arrays.push_back(std::move(value));
  auto const [inserted, did_insert] = primary().names.try_emplace(
    storage::String<>{name}, primary().arrays.size() - 1);
  TENZIR_ASSERT(did_insert);
  primary().names_by_index.push_back(inserted->first);
  primary().shape_indices = rewrite_shape_indices(
    std::move(primary().shape_indices), primary().shape_table, mask,
    [new_index](ShapeTable& table, ShapeTable::ShapeId id) {
      return table.with_field(id, new_index);
    });
  return std::move(*this);
}

auto Array<Record>::with_fields(
  std::vector<std::pair<std::string_view, MaskedArray>> fields) const& -> Array {
  if (fields.empty()) {
    return *this;
  }
  return as_unique().with_fields(std::move(fields));
}

auto Array<Record>::with_fields(
  std::vector<std::pair<std::string_view, MaskedArray>> fields) && -> Array {
  if (fields.empty()) {
    return std::move(*this);
  }
  if (not is<storage::RecordStorage>(storage())) {
    return to_primary().with_fields(std::move(fields));
  }
  *this = std::move(*this).as_unique();
  auto new_fields
    = storage::Vector<std::pair<storage::Index, storage::BitMap>>{};
  for (auto& [name, value] : fields) {
    const auto it = primary().names.find(name);
    if (it != primary().names.end()) {
      new_fields.emplace_back(static_cast<storage::Index>(it->second),
                              value.present);
      auto& existing = primary().arrays[it->second];
      auto merged = with_merged(existing, value);
      existing.present = existing.present | value.present;
      existing.data = std::move(merged);
      continue;
    }
    const auto new_index = static_cast<storage::Index>(primary().arrays.size());
    auto mask = value.present;
    primary().arrays.push_back(std::move(value));
    auto const [inserted, did_insert] = primary().names.try_emplace(
      storage::String<>{name}, primary().arrays.size() - 1);
    TENZIR_ASSERT(did_insert);
    primary().names_by_index.push_back(inserted->first);
    new_fields.emplace_back(new_index, std::move(mask));
  }
  // Applying the fields one after another, each over the rows its mask
  // selects, yields the same per-row transition sequence as visiting every
  // row and applying the fields in order -- but it only touches the selected
  // rows and memoizes the shape transition per source shape.
  for (const auto& [field_index, mask] : new_fields) {
    primary().shape_indices = rewrite_shape_indices(
      std::move(primary().shape_indices), primary().shape_table, mask,
      [field_index](ShapeTable& table, ShapeTable::ShapeId id) {
        return table.with_field(id, field_index);
      });
  }
  return std::move(*this);
}

auto Array<Record>::without_fields(std::span<const std::string_view> names,
                                   storage::BitMap mask) const& -> Array {
  if (not is<storage::RecordStorage>(storage())) {
    return to_primary().without_fields(names, std::move(mask));
  }
  auto const has_field = std::ranges::any_of(names, [this](auto const name) {
    return std::as_const(*this).primary().names.contains(name);
  });
  if (not has_field) {
    return *this;
  }
  return as_unique().without_fields(names, std::move(mask));
}

auto Array<Record>::without_fields(std::span<const std::string_view> names,
                                   storage::BitMap mask) && -> Array {
  if (not is<storage::RecordStorage>(storage())) {
    return to_primary().without_fields(names, std::move(mask));
  }
  auto const has_field = std::ranges::any_of(names, [this](auto const name) {
    return std::as_const(*this).primary().names.contains(name);
  });
  if (not has_field) {
    return std::move(*this);
  }
  *this = std::move(*this).as_unique();
  auto removed = storage::Vector<storage::Index>{};
  for (const auto& name : names) {
    const auto it = primary().names.find(name);
    if (it == primary().names.end()) {
      continue;
    }
    removed.push_back(static_cast<storage::Index>(it->second));
    auto& field = primary().arrays[it->second];
    field.present = std::move(field.present).and_not(mask);
    // Unselected rows may still refer to this name through their shape.
    if (mask.as_constant() == true) {
      primary().names_by_index[it->second] = {};
      primary().names.erase(it);
    }
  }
  if (removed.empty()) {
    return std::move(*this);
  }
  primary().shape_indices = rewrite_shape_indices(
    std::move(primary().shape_indices), primary().shape_table, mask,
    [&removed](ShapeTable& table, ShapeTable::ShapeId id) {
      return table.without_fields(id, removed);
    });
  return std::move(*this);
}

auto Array<Record>::empty_where(storage::BitMap mask) const& -> Array {
  if (not is<storage::RecordStorage>(storage())) {
    return to_primary().empty_where(std::move(mask));
  }
  if (mask.as_constant() == false) {
    return *this;
  }
  return as_unique().empty_where(std::move(mask));
}

auto Array<Record>::empty_where(storage::BitMap mask) && -> Array {
  if (not is<storage::RecordStorage>(storage())) {
    return to_primary().empty_where(std::move(mask));
  }
  TENZIR_ASSERT_EQ(length(), mask.length());
  if (mask.as_constant() == false) {
    return std::move(*this);
  }
  if (mask.as_constant() == true) {
    return make_empty(length());
  }
  *this = std::move(*this).as_unique();
  for (auto& field : primary().arrays) {
    field.present = std::move(field.present).and_not(mask);
  }
  // Emptied rows no longer reach any field through their shape, so the names
  // stay registered for the untouched rows -- the same invariant that
  // `without_fields` relies on for its partial-mask case.
  auto indices = IndicesStorage::Mutable{std::move(primary().shape_indices)};
  auto* const data = indices.data();
  storage::for_each_true(mask, [&](storage::Index row) {
    data[row] = ShapeTable::empty_shape;
  });
  primary().shape_indices = std::move(indices).finish();
  return std::move(*this);
}

auto Array<Record>::make_empty(storage::Index length) -> Array {
  auto shape_indices
    = storage::SparseStorage<storage::Index>::Mutable{length}.finish();
  return Array{std::move(shape_indices), ShapeTable{}, Names{}, MaskedArrays{}};
}

} // namespace tenzir::nova
