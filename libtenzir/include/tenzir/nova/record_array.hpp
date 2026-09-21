//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/heterogeneous_string_hash.hpp"
#include "tenzir/nova/allocator.hpp"
#include "tenzir/nova/array_base.hpp"
#include "tenzir/nova/structured_storage.hpp"
#include "tenzir/option.hpp"
#include "tenzir/ref.hpp"

#include <memory>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace tenzir::nova {

class ShapeTable;

template <>
class RowView<Record>;

template <>
class Array<Record> {
public:
  using PhysicalStorage = Type<Record>::PhysicalStorage::apply<variant>;
  using MaskedArray = storage::RecordStorage::MaskedArray;
  using IndicesStorage = storage::RecordStorage::IndicesStorage;
  using Names = storage::RecordStorage::Names;
  using MaskedArrays = storage::RecordStorage::MaskedArrays;

  Array(storage::RecordStorage storage);
  Array(storage::ConstantStorage<Record, RowView<Record>> storage);
  Array(IndicesStorage shape_indices, ShapeTable&& shape_table, Names names,
        MaskedArrays arrays);

  /// Borrows the physical representation.
  auto storage() const& -> PhysicalStorage const&;
  /// Exposes the physical representation for moving.
  auto storage() && -> PhysicalStorage&&;
  /// Expands constants to columnar storage; primary arrays retain sharing.
  auto to_primary() const -> Array;

  auto as_unique() const& -> Array;
  auto as_unique() && -> Array;
  auto length() const noexcept -> storage::Index;
  auto get(storage::Index i) const -> RowView<Record>;
  auto field(std::string_view name) const -> Option<MaskedArray>;

  /// Moves a field out of this record array, leaving its field storage invalid.
  /// Afterward, either do not use the array again or replace the extracted
  /// field with `with_field_overwrite` before any other use.
  auto
  dangerously_extract_field(std::string_view name) && -> Option<MaskedArray>;

  [[nodiscard]] auto
  with_field_overwrite(std::string_view name, MaskedArray value,
                       FieldPosition position
                       = FieldPosition::back) const& -> Array;
  [[nodiscard]] auto
  with_field_overwrite(std::string_view name, MaskedArray value,
                       FieldPosition position
                       = FieldPosition::back) && -> Array;
  [[nodiscard]] auto
  with_fields(std::vector<std::pair<std::string_view, MaskedArray>> fields)
    const& -> Array;
  [[nodiscard]] auto with_fields(
    std::vector<std::pair<std::string_view, MaskedArray>> fields) && -> Array;
  [[nodiscard]] auto without_fields(std::span<const std::string_view> names,
                                    storage::BitMap mask) const& -> Array;
  [[nodiscard]] auto without_fields(std::span<const std::string_view> names,
                                    storage::BitMap mask) && -> Array;

  /// Replaces the rows selected by `mask` with the empty record, regardless of
  /// their previous shape -- including rows that have no shape at all. Rows
  /// outside `mask` are untouched.
  [[nodiscard]] auto empty_where(storage::BitMap mask) const& -> Array;
  [[nodiscard]] auto empty_where(storage::BitMap mask) && -> Array;

  [[nodiscard]] static auto make_empty(storage::Index length) -> Array;

private:
  auto primary() -> storage::RecordStorage::Storage&;
  auto primary() const -> storage::RecordStorage::Storage const&;
  PhysicalStorage storage_;
};

static_assert(storage::unique_ownership<Array<Record>>);

template <>
class RowView<Record> {
public:
  using value_type = std::pair<std::string_view, RowView<Data>>;

  RowView(Record const& value);
  RowView(Array<Record> const& array, storage::Index row);

  class iterator;
  auto begin() const -> iterator;
  auto end() const -> iterator;

private:
  friend class storage::RecordStorage;
  RowView(storage::RecordStorage::Storage const& storage, storage::Index row);

  struct Primary {
    Ref<storage::RecordStorage::Storage const> storage;
    storage::Index row;

    auto operator==(Primary const& other) const -> bool {
      return &storage.get() == &other.storage.get() and row == other.row;
    }
  };

  struct Constant {
    Ref<Record const> value;

    auto operator==(Constant const& other) const -> bool {
      return &value.get() == &other.value.get();
    }
  };

  using Representation = variant<Primary, Constant>;
  Representation representation_;
};

class RowView<Record>::iterator {
public:
  using value_type = RowView<Record>::value_type;
  using difference_type = std::ptrdiff_t;

  auto operator*() const -> value_type;
  auto operator++() -> iterator&;
  friend auto operator==(iterator const&, iterator const&) -> bool;

private:
  friend class RowView<Record>;
  iterator(Representation representation, std::size_t index);

  Representation representation_;
  std::size_t index_;
};

static_assert(fully_implemented_type<Type<Record>>);

} // namespace tenzir::nova
