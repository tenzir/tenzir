#include "tenzir/nova/shape_table.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/hash/hash.hpp"

#include <algorithm>
#include <utility>

namespace tenzir::nova {

auto ShapeTable::EdgeCache::find(storage::Index field) const -> ShapeId {
  if (first.field == field) {
    return first.shape;
  }
  if (auto const it = std::ranges::find(overflow, field, &ShapeEdge::field);
      it != overflow.end()) {
    return it->shape;
  }
  return -1;
}

auto ShapeTable::EdgeCache::insert(storage::Index field, ShapeId shape)
  -> void {
  if (first.field == field
      or std::ranges::find(overflow, field, &ShapeEdge::field)
           != overflow.end()) {
    return;
  }
  if (first.field < 0) {
    first = ShapeEdge{field, shape};
    return;
  }
  overflow.push_back(ShapeEdge{field, shape});
}

ShapeTable::ShapeTable() {
  nodes_.push_back(ShapeNode{});
  ids_by_hash_.try_emplace(tenzir::hash(std::span<const storage::Index>{}),
                           ShapeIds{empty_shape, {}});
}

auto ShapeTable::with_field(ShapeId id, storage::Index field,
                            FieldPosition position) -> ShapeId {
  TENZIR_ASSERT_LEQ_EXPENSIVE(0, id);
  TENZIR_ASSERT_LT_EXPENSIVE(id, static_cast<ShapeId>(nodes_.size()));
  auto& node = nodes_[static_cast<std::size_t>(id)];
  auto& edges
    = position == FieldPosition::front ? node.prepend_edges : node.add_edges;
  if (auto const cached = edges.find(field); cached >= 0) {
    return cached;
  }
  if (std::ranges::find(node.fields, field) != node.fields.end()) {
    edges.insert(field, id);
    return id;
  }
  auto candidate = node.fields;
  if (position == FieldPosition::front) {
    candidate.insert(candidate.begin(), field);
  } else {
    candidate.push_back(field);
  }
  const auto new_id = find_or_add(std::move(candidate));
  auto& updated = nodes_[static_cast<std::size_t>(id)];
  auto& updated_edges = position == FieldPosition::front ? updated.prepend_edges
                                                         : updated.add_edges;
  updated_edges.insert(field, new_id);
  nodes_[static_cast<std::size_t>(new_id)].remove_edges.insert(field, id);
  return new_id;
}

auto ShapeTable::without_field(ShapeId id, storage::Index field) -> ShapeId {
  TENZIR_ASSERT_LEQ_EXPENSIVE(0, id);
  TENZIR_ASSERT_LT_EXPENSIVE(id, static_cast<ShapeId>(nodes_.size()));
  auto& node = nodes_[static_cast<std::size_t>(id)];
  if (auto const cached = node.remove_edges.find(field); cached >= 0) {
    return cached;
  }
  if (std::ranges::find(node.fields, field) == node.fields.end()) {
    node.remove_edges.insert(field, id);
    return id;
  }
  auto candidate = FieldsType{};
  candidate.reserve(node.fields.size() - 1);
  for (const auto& candidate_field : node.fields) {
    if (candidate_field != field) {
      candidate.push_back(candidate_field);
    }
  }
  const auto new_id = find_or_add(std::move(candidate));
  nodes_[static_cast<std::size_t>(id)].remove_edges.insert(field, new_id);
  // Removing an arbitrary field is not the inverse of appending or prepending.
  // Let insertion compute the transition for its requested position.
  return new_id;
}

auto ShapeTable::without_fields(ShapeId id,
                                std::span<const storage::Index> fields)
  -> ShapeId {
  auto current = id;
  for (const auto& field : fields) {
    current = without_field(current, field);
  }
  return current;
}

auto ShapeTable::size() const noexcept -> std::size_t {
  return nodes_.size();
}

auto ShapeTable::fields(ShapeId id) const -> std::span<const storage::Index> {
  TENZIR_ASSERT_LEQ_EXPENSIVE(0, id);
  TENZIR_ASSERT_LT_EXPENSIVE(id, static_cast<ShapeId>(nodes_.size()));
  return nodes_[static_cast<std::size_t>(id)].fields;
}

auto ShapeTable::import_shapes(ShapeTable const& source,
                               std::span<const storage::Index> field_remap,
                               std::span<const ShapeId> source_shapes)
  -> ShapeRemap {
  TENZIR_ASSERT(this != &source);
  auto result = ShapeRemap{};
  result.reserve(source_shapes.size());
  nodes_.reserve(nodes_.size() + source_shapes.size());
  ids_by_hash_.reserve(ids_by_hash_.size() + source_shapes.size());
  for (auto const source_id : source_shapes) {
    TENZIR_ASSERT_LEQ(0, source_id);
    TENZIR_ASSERT_LT(source_id, static_cast<ShapeId>(source.nodes_.size()));
    if (result.contains(source_id)) {
      continue;
    }
    auto const& source_node
      = source.nodes_[static_cast<std::size_t>(source_id)];
    auto candidate = FieldsType{};
    candidate.reserve(source_node.fields.size());
    for (auto const source_field : source_node.fields) {
      TENZIR_ASSERT_LEQ(0, source_field);
      TENZIR_ASSERT_LT(source_field,
                       static_cast<storage::Index>(field_remap.size()));
      auto const destination_field
        = field_remap[static_cast<std::size_t>(source_field)];
      TENZIR_ASSERT_LEQ(0, destination_field);
      candidate.push_back(destination_field);
    }
    result.try_emplace(source_id, find_or_add(std::move(candidate)));
  }
  return result;
}

auto ShapeTable::find_or_add(FieldsType candidate) -> ShapeId {
  auto const digest = tenzir::hash(std::span<const storage::Index>{candidate});
  if (auto const it = ids_by_hash_.find(digest); it != ids_by_hash_.end()) {
    if (nodes_[static_cast<std::size_t>(it->second.first)].fields
        == candidate) {
      return it->second.first;
    }
    for (auto const id : it->second.overflow) {
      if (nodes_[static_cast<std::size_t>(id)].fields == candidate) {
        return id;
      }
    }
  }
  const auto new_id = static_cast<ShapeId>(nodes_.size());
  nodes_.push_back(ShapeNode{.fields = std::move(candidate),
                             .add_edges = {},
                             .prepend_edges = {},
                             .remove_edges = {}});
  auto [it, inserted] = ids_by_hash_.try_emplace(digest, ShapeIds{new_id, {}});
  if (not inserted) {
    it->second.overflow.push_back(new_id);
  }
  return new_id;
}

} // namespace tenzir::nova
