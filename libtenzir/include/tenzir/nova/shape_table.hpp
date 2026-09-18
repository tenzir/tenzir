//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/nova/allocator.hpp"
#include "tenzir/nova/storage.hpp"

#include <boost/container/small_vector.hpp>

#include <cstdint>
#include <span>

namespace tenzir::nova {

class ShapeTable {
public:
  using ShapeId = storage::Index;
  using ShapeRemap = storage::UnorderedMap<ShapeId, ShapeId>;
  static constexpr auto empty_shape = ShapeId{0};

  ShapeTable();

  auto with_field(ShapeId id, storage::Index field) -> ShapeId;
  auto without_field(ShapeId id, storage::Index field) -> ShapeId;
  auto without_fields(ShapeId id, std::span<const storage::Index> fields)
    -> ShapeId;
  auto size() const noexcept -> std::size_t;
  auto fields(ShapeId id) const -> std::span<const storage::Index>;
  auto import_shapes(ShapeTable const& source,
                     std::span<const storage::Index> field_remap,
                     std::span<const ShapeId> source_shapes) -> ShapeRemap;

private:
  struct ShapeEdge {
    storage::Index field = -1;
    ShapeId shape = -1;
  };

  struct EdgeCache {
    auto find(storage::Index field) const -> ShapeId;
    auto insert(storage::Index field, ShapeId shape) -> void;

    ShapeEdge first;
    storage::Vector<ShapeEdge> overflow;
  };

  using FieldsType = boost::container::small_vector<
    storage::Index, 16, storage::CppStructureAllocator<storage::Index>>;
  struct ShapeNode {
    FieldsType fields;
    EdgeCache add_edges;
    EdgeCache remove_edges;
  };

  struct ShapeIds {
    ShapeId first;
    storage::Vector<ShapeId> overflow;
  };

  auto find_or_add(FieldsType candidate) -> ShapeId;

  storage::Vector<ShapeNode> nodes_;
  storage::UnorderedFlatMap<std::uint64_t, ShapeIds> ids_by_hash_;
};

} // namespace tenzir::nova
