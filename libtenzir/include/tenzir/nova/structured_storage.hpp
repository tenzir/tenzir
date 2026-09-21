//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/heterogeneous_string_hash.hpp"
#include "tenzir/nova/array_base.hpp"
#include "tenzir/nova/shape_table.hpp"
#include "tenzir/nova/shared_owner.hpp"

namespace tenzir::nova::storage {

class ListStorage {
public:
  using ViewType = RowView<List>;
  struct Storage;
  ListStorage(DataOwner<Span[]> spans, Array<Data> values);
  ~ListStorage();
  ListStorage(ListStorage const&);
  ListStorage(ListStorage&&) noexcept;
  auto operator=(ListStorage const&) -> ListStorage&;
  auto operator=(ListStorage&&) noexcept -> ListStorage&;
  auto as_unique() const& -> ListStorage;
  auto as_unique() && -> ListStorage;
  auto length() const noexcept -> Index;
  auto get(Index i) const -> ViewType;
  auto values() const& -> Array<Data> const&;
  auto values() && -> Array<Data>&&;
  auto spans() const& -> DataOwner<Span[]> const&;
  auto spans() && -> DataOwner<Span[]>&&;
  auto operator*() const -> Storage const&;

private:
  explicit ListStorage(StructureOwner<Storage> storage);
  StructureOwner<Storage> storage_;
};

class RecordStorage {
public:
  using ViewType = RowView<Record>;
  using MaskedArray = nova::MaskedArray<Array<Data>>;
  using IndicesStorage = SparseStorage<Index>;
  using Names = UnorderedMap<String<>, std::size_t,
                             ::tenzir::detail::heterogeneous_string_hash,
                             ::tenzir::detail::heterogeneous_string_equal>;
  using MaskedArrays = Vector<MaskedArray>;
  RecordStorage(IndicesStorage indices, ShapeTable table, Names names,
                MaskedArrays arrays);
  ~RecordStorage();
  RecordStorage(RecordStorage const&);
  RecordStorage(RecordStorage&&) noexcept;
  auto operator=(RecordStorage const&) -> RecordStorage&;
  auto operator=(RecordStorage&&) noexcept -> RecordStorage&;
  auto as_unique() const& -> RecordStorage;
  auto as_unique() && -> RecordStorage;
  auto length() const noexcept -> Index;
  auto get(Index i) const -> ViewType;

  struct Storage {
    Storage(IndicesStorage indices, ShapeTable table, Names names,
            MaskedArrays arrays);
    ~Storage();
    Storage(Storage const&);
    Storage(Storage&&) noexcept;
    auto operator=(Storage const&) -> Storage& = delete;
    auto operator=(Storage&&) -> Storage& = delete;

    IndicesStorage shape_indices;
    ShapeTable shape_table;
    Names names;
    MaskedArrays arrays;
    Vector<std::string_view> names_by_index;
  };
  auto operator*() const -> Storage const&;

private:
  friend class Array<Record>;
  explicit RecordStorage(StructureOwner<Storage> storage);
  StructureOwner<Storage> storage_;
};

} // namespace tenzir::nova::storage
