#include "tenzir/nova/flatten.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/array_merge.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/shape_table.hpp"
#include "tenzir/nova/storage.hpp"

#include <fmt/format.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace tenzir::nova {

namespace {

using FieldArray = MaskedArray<Array<Data>>;
using RecordStorage = storage::RecordStorage::Storage;
using storage::BitMap;
using storage::Index;

auto record_storage(Array<Record> const& record) -> RecordStorage const& {
  return *as<storage::RecordStorage>(record.storage());
}

/// The rows of one partition group, together with what each of them maps to.
/// Every row of a group produces the same output structure, so it is computed
/// once per group instead of once per row.
template <class T>
struct Group {
  BitMap rows;
  std::vector<T> items;
};

template <class T>
using Partition = std::vector<Group<T>>;

/// Appends the items of every part to the groups it overlaps, splitting the
/// groups where needed. The parts must cover all rows of `groups`.
template <class T>
auto refine(Partition<T> groups, Partition<T> const& parts) -> Partition<T> {
  if (parts.size() == 1) {
    for (auto& group : groups) {
      group.items.insert(group.items.end(), parts.front().items.begin(),
                         parts.front().items.end());
    }
    return groups;
  }
  auto result = Partition<T>{};
  for (auto& group : groups) {
    for (auto const& part : parts) {
      auto rows = group.rows & part.rows;
      if (not rows.any()) {
        continue;
      }
      auto items = group.items;
      items.insert(items.end(), part.items.begin(), part.items.end());
      result.push_back({std::move(rows), std::move(items)});
    }
  }
  return result;
}

/// Partitions the records at `rows` by shape and then by the parts that
/// `field(index, rows)` returns for each field, concatenating the items of
/// a group's fields in its field order.
template <class T, class F>
auto partition_fields(RecordStorage const& storage, BitMap const& rows,
                      F&& field) -> Partition<T> {
  auto shapes = std::vector<std::pair<ShapeTable::ShapeId, BitMap>>{};
  auto const* indices = storage.shape_indices.data();
  auto uniform = Option<ShapeTable::ShapeId>{};
  auto mixed = false;
  storage::for_each_true(rows, [&](Index row) {
    TENZIR_ASSERT_GEQ_EXPENSIVE(indices[row], 0);
    if (not uniform) {
      uniform = indices[row];
    } else if (*uniform != indices[row]) {
      mixed = true;
    }
  });
  if (uniform and not mixed) {
    shapes.emplace_back(*uniform, rows);
  } else if (uniform) {
    auto shape_rows
      = std::vector<Option<BitMap::Mutable>>(storage.shape_table.size());
    storage::for_each_true(rows, [&](Index row) {
      auto& bits = shape_rows[static_cast<std::size_t>(indices[row])];
      if (not bits) {
        bits.emplace(rows.length());
      }
      bits->set(row, true);
    });
    for (auto shape = std::size_t{0}; shape < shape_rows.size(); ++shape) {
      if (shape_rows[shape]) {
        shapes.emplace_back(static_cast<ShapeTable::ShapeId>(shape),
                            std::move(*shape_rows[shape]).finish());
      }
    }
  }
  auto members = std::vector<Option<BitMap>>(storage.arrays.size());
  for (auto const& [shape, shape_rows] : shapes) {
    for (auto field_index : storage.shape_table.fields(shape)) {
      auto& member = members[static_cast<std::size_t>(field_index)];
      member = member ? *member | shape_rows : shape_rows;
    }
  }
  auto parts = std::vector<Partition<T>>(storage.arrays.size());
  for (auto index = std::size_t{0}; index < members.size(); ++index) {
    if (members[index]) {
      parts[index] = field(index, *members[index]);
    }
  }
  auto result = Partition<T>{};
  for (auto& [shape, shape_rows] : shapes) {
    auto groups = Partition<T>{{std::move(shape_rows), {}}};
    for (auto field_index : storage.shape_table.fields(shape)) {
      groups = refine(std::move(groups),
                      parts[static_cast<std::size_t>(field_index)]);
    }
    std::ranges::move(groups, std::back_inserter(result));
  }
  return result;
}

/// Builds shape indices that hold `shape` on the rows of its entry.
auto make_shapes(Index length,
                 std::span<std::pair<BitMap, ShapeTable::ShapeId> const> shapes)
  -> storage::SparseStorage<Index> {
  if (shapes.size() == 1) {
    // Rows outside the group are not part of the result, so they may share
    // its shape.
    return storage::DataOwner<Index[]>::make_value(length,
                                                   shapes.front().second);
  }
  auto result = storage::SparseStorage<Index>::Mutable{length};
  auto* data = result.data();
  for (auto const& [rows, shape] : shapes) {
    storage::for_each_true(rows, [&](Index row) {
      data[row] = shape;
    });
  }
  return std::move(result).finish();
}

auto make_record(storage::SparseStorage<Index> shapes, ShapeTable table,
                 std::span<std::string_view const> names,
                 storage::RecordStorage::MaskedArrays arrays) -> Array<Record> {
  auto field_names = Array<Record>::Names{};
  for (auto index = std::size_t{0}; index < names.size(); ++index) {
    auto const inserted
      = field_names.try_emplace(storage::String<>{names[index]}, index).second;
    TENZIR_ASSERT(inserted);
  }
  return Array<Record>{std::move(shapes), std::move(table),
                       std::move(field_names), std::move(arrays)};
}

/// Folds `parts`, whose `present` masks are disjoint, into one field.
auto merge_parts(std::vector<FieldArray> parts) -> FieldArray {
  TENZIR_ASSERT(not parts.empty());
  auto result = std::move(parts.front());
  for (auto i = std::size_t{1}; i < parts.size(); ++i) {
    auto data = with_merged(result, parts[i]);
    result.present = std::move(result.present) | parts[i].present;
    result.data = std::move(data);
  }
  return result;
}

// -- flatten ------------------------------------------------------------------

class Flattener {
public:
  Flattener(Index length, std::string_view separator)
    : length_{length}, separator_{separator} {
  }

  auto run(Array<Record> record, BitMap const& rows) -> FlattenResult {
    auto groups = flatten_record(record, rows, "");
    if (not restructured_) {
      // Without nested records or lists, the names are unchanged and unique.
      return {std::move(record), {}};
    }
    return assemble(groups);
  }

private:
  /// A leaf field, which exists on `rows` and holds a value on the rows of
  /// `value.present`.
  struct Leaf {
    std::string name;
    FieldArray value;
    BitMap rows;
  };

  auto add_leaf(std::string name, FieldArray value, BitMap rows) -> Index {
    leaves_.push_back({std::move(name), std::move(value), std::move(rows)});
    return static_cast<Index>(leaves_.size() - 1);
  }

  auto flatten_record(Array<Record> const& record, BitMap const& rows,
                      std::string const& prefix) -> Partition<Index> {
    auto const primary = record.to_primary();
    auto const& storage = record_storage(primary);
    return partition_fields<Index>(
      storage, rows, [&](std::size_t index, BitMap const& member) {
        return flatten_value(storage.arrays[index], member,
                             fmt::format("{}{}", prefix,
                                         storage.names_by_index[index]));
      });
  }

  /// Flattens `value` at `rows`, where it is named `name`.
  auto flatten_value(FieldArray const& value, BitMap const& rows,
                     std::string const& name) -> Partition<Index> {
    auto parts = Partition<Index>{};
    auto scalar = rows;
    auto const values = rows & value.present;
    if (auto records = value.data.get_alternative<Record>()) {
      auto nested = values & records->present;
      if (nested.any()) {
        restructured_ = true;
        scalar = std::move(scalar).and_not(nested);
        std::ranges::move(flatten_record(records->data, nested,
                                         fmt::format("{}{}", name, separator_)),
                          std::back_inserter(parts));
      }
    }
    if (auto lists = value.data.get_alternative<List>()) {
      auto candidates = values & lists->present;
      if (candidates.any()) {
        for (auto& group : flatten_list(lists->data, candidates, name)) {
          restructured_ = true;
          scalar = std::move(scalar).and_not(group.rows);
          parts.push_back(std::move(group));
        }
      }
    }
    if (scalar.any()) {
      auto present = value.present & scalar;
      auto leaf
        = add_leaf(name, FieldArray{value.data, std::move(present)}, scalar);
      parts.push_back({std::move(scalar), {leaf}});
    }
    return parts;
  }

  /// Splits the lists at `rows` that hold records or lists into one list per
  /// leaf of their elements. Returns the rows of the split lists, partitioned
  /// by the leaves they have.
  auto flatten_list(Array<List> const& lists, BitMap const& rows,
                    std::string const& name) -> Partition<Index> {
    auto const primary = lists.to_primary();
    auto const& storage = as<storage::ListStorage>(primary.storage());
    auto const& values = storage.values();
    auto const element_count = values.length();
    auto structured = BitMap{element_count, false};
    if (auto records = values.get_alternative<Record>()) {
      structured = std::move(structured) | records->present;
    }
    if (auto nested = values.get_alternative<List>()) {
      structured = std::move(structured) | nested->present;
    }
    if (not structured.any()) {
      return {};
    }
    auto const* spans = storage.spans().begin();
    auto split = BitMap::Mutable{length_};
    auto elements = BitMap::Mutable{element_count};
    storage::for_each_true(rows, [&](Index row) {
      auto const span = spans[row];
      auto needed = false;
      for (auto i = span.begin; i < span.end and not needed; ++i) {
        needed = structured.get(i);
      }
      if (not needed) {
        return;
      }
      split.set(row, true);
      for (auto i = span.begin; i < span.end; ++i) {
        elements.set(i, true);
      }
    });
    auto split_rows = std::move(split).finish();
    if (not split_rows.any()) {
      return {};
    }
    // The elements form a column of their own, which keeps the name of the
    // list. A `null` element has no leaves, so it contributes `null` to all.
    auto element_rows = std::move(elements).finish();
    if (auto nulls = values.get_alternative<Null>()) {
      element_rows = std::move(element_rows).and_not(nulls->present);
    }
    auto inner = Flattener{element_count, separator_};
    inner.flatten_value(FieldArray{values, BitMap{element_count, true}},
                        element_rows, name);
    auto groups = Partition<Index>{{split_rows, {}}};
    for (auto& leaf : inner.leaves_) {
      auto const id = lift(leaf, storage, split_rows);
      auto const& member = leaves_[static_cast<std::size_t>(id)].rows;
      auto next = Partition<Index>{};
      for (auto& group : groups) {
        auto without = group.rows.and_not(member);
        auto with = std::move(group.rows) & member;
        if (without.any()) {
          next.push_back({std::move(without), group.items});
        }
        if (with.any()) {
          group.items.push_back(id);
          next.push_back({std::move(with), std::move(group.items)});
        }
      }
      groups = std::move(next);
    }
    return groups;
  }

  /// Turns a leaf of the elements of `lists` into a leaf of the lists at
  /// `rows`, which holds the leaf's values of each list's elements. If all
  /// values are lists, they are spliced into one; otherwise, an element
  /// without a value contributes `null`.
  auto lift(Leaf const& leaf, storage::ListStorage const& lists,
            BitMap const& rows) -> Index {
    auto const* spans = lists.spans().begin();
    auto member = BitMap::Mutable{length_};
    storage::for_each_true(rows, [&](Index row) {
      for (auto i = spans[row].begin; i < spans[row].end; ++i) {
        if (leaf.rows.get(i)) {
          member.set(row, true);
          return;
        }
      }
    });
    auto member_rows = std::move(member).finish();
    auto const& present = leaf.value.present;
    auto const inner = leaf.value.data.get_alternative<List>();
    auto const splice
      = inner and present.any() and not present.and_not(inner->present).any();
    auto data
      = splice
          ? Array<Data>{splice_lists(inner->data, present, lists, member_rows)}
          : Array<Data>{Array<List>{
              lists.spans(),
              leaf.value.data.null_where(
                BitMap{present.length(), true}.and_not(present)),
            }};
    return add_leaf(leaf.name, FieldArray{std::move(data), member_rows},
                    member_rows);
  }

  /// Concatenates the lists in `inner` at `present` along the spans of
  /// `outer` for the rows at `rows`.
  auto splice_lists(Array<List> const& inner, BitMap const& present,
                    storage::ListStorage const& outer, BitMap const& rows) const
    -> Array<List> {
    auto const primary = inner.to_primary();
    auto const& storage = as<storage::ListStorage>(primary.storage());
    auto const* inner_spans = storage.spans().begin();
    auto const* outer_spans = outer.spans().begin();
    // Builders lay out consecutive lists back to back, so the concatenation is
    // usually a single span over the existing values.
    auto spans = storage::DataOwner<storage::Span[]>::make_value(
      length_, storage::Span{0, 0});
    auto* out = spans.begin();
    auto contiguous = true;
    storage::for_each_true(rows, [&](Index row) {
      auto result = Option<storage::Span>{};
      for (auto i = outer_spans[row].begin; i < outer_spans[row].end; ++i) {
        auto const span = inner_spans[i];
        if (not present.get(i) or span.begin == span.end) {
          continue;
        }
        if (not result) {
          result = span;
        } else if (result->end == span.begin) {
          result->end = span.end;
        } else {
          contiguous = false;
        }
      }
      out[row] = result.unwrap_or(storage::Span{0, 0});
    });
    if (contiguous) {
      return Array<List>{std::move(spans), storage.values()};
    }
    auto builder = ArrayBuilder<List>{};
    storage::for_each_true(rows, [&](Index row) {
      builder.skip_n(row - builder.length());
      auto list = builder.list();
      for (auto i = outer_spans[row].begin; i < outer_spans[row].end; ++i) {
        if (present.get(i)) {
          for (auto value : storage.get(i)) {
            append_row(list, value);
          }
        }
      }
    });
    builder.skip_n(length_ - builder.length());
    return builder.finish();
  }

  /// Builds the result from the leaves and the leaf sequence of each group.
  /// Leaves that share a name share a field as long as they never meet in a
  /// row. Otherwise, the later leaf gets the first free name `<name>_<n>`.
  auto assemble(Partition<Index> const& groups) -> FlattenResult {
    struct Output {
      std::string name;
      std::vector<Index> leaves;
      BitMap rows;
    };
    auto taken = std::unordered_set<std::string>{};
    for (auto const& leaf : leaves_) {
      taken.insert(leaf.name);
    }
    auto outputs = std::vector<Output>{};
    auto by_name = std::unordered_map<std::string, std::vector<Index>>{};
    auto suffixes = std::unordered_map<std::string, std::size_t>{};
    auto output_of = std::vector<Index>(leaves_.size());
    auto renamed = std::vector<std::string>{};
    for (auto index = std::size_t{0}; index < leaves_.size(); ++index) {
      auto const& leaf = leaves_[index];
      auto& candidates = by_name[leaf.name];
      auto const it = std::ranges::find_if(candidates, [&](Index output) {
        return not(outputs[static_cast<std::size_t>(output)].rows & leaf.rows)
                    .any();
      });
      if (it != candidates.end()) {
        auto& output = outputs[static_cast<std::size_t>(*it)];
        output.leaves.push_back(static_cast<Index>(index));
        output.rows = std::move(output.rows) | leaf.rows;
        output_of[index] = *it;
        continue;
      }
      auto name = leaf.name;
      if (not candidates.empty()) {
        auto& suffix = suffixes[leaf.name];
        do {
          name = fmt::format("{}_{}", leaf.name, ++suffix);
        } while (taken.contains(name));
        taken.insert(name);
        renamed.push_back(fmt::format("{} -> {}", leaf.name, name));
      }
      output_of[index] = static_cast<Index>(outputs.size());
      candidates.push_back(output_of[index]);
      outputs.push_back(
        {std::move(name), {static_cast<Index>(index)}, leaf.rows});
    }
    auto table = ShapeTable{};
    auto shapes = std::vector<std::pair<BitMap, ShapeTable::ShapeId>>{};
    auto fields = std::vector<Index>{};
    for (auto const& group : groups) {
      fields.clear();
      for (auto leaf : group.items) {
        fields.push_back(output_of[static_cast<std::size_t>(leaf)]);
      }
      shapes.emplace_back(group.rows, table.with_fields(fields));
    }
    auto names = std::vector<std::string_view>{};
    auto arrays = storage::RecordStorage::MaskedArrays{};
    for (auto const& output : outputs) {
      names.push_back(output.name);
      auto parts = std::vector<FieldArray>{};
      for (auto leaf : output.leaves) {
        parts.push_back(leaves_[static_cast<std::size_t>(leaf)].value);
      }
      arrays.push_back(merge_parts(std::move(parts)));
    }
    return {
      make_record(make_shapes(length_, shapes), std::move(table), names,
                  std::move(arrays)),
      std::move(renamed),
    };
  }

  Index length_;
  std::string_view separator_;
  std::vector<Leaf> leaves_;
  bool restructured_ = false;
};

// -- unflatten ----------------------------------------------------------------

auto split_name(std::string_view name, std::string_view separator)
  -> std::vector<std::string_view> {
  auto result = std::vector<std::string_view>{};
  while (true) {
    auto const next = name.find(separator);
    if (next == std::string_view::npos) {
      result.push_back(name);
      return result;
    }
    result.push_back(name.substr(0, next));
    name.remove_prefix(next + separator.size());
  }
}

/// Whether any record in `data`, including nested ones, has a field name that
/// contains `separator`.
auto contains_separator(Array<Data> const& data, std::string_view separator)
  -> bool {
  if (auto records = data.get_alternative<Record>()) {
    auto const primary = std::move(records->data).to_primary();
    auto const& storage = record_storage(primary);
    for (auto const& [name, _] : storage.names) {
      if (std::string_view{name}.contains(separator)) {
        return true;
      }
    }
    for (auto const& field : storage.arrays) {
      if (contains_separator(field.data, separator)) {
        return true;
      }
    }
  }
  if (auto lists = data.get_alternative<List>()) {
    auto const primary = lists->data.to_primary();
    auto const& storage = as<storage::ListStorage>(primary.storage());
    if (contains_separator(storage.values(), separator)) {
      return true;
    }
  }
  return false;
}

auto unflatten_lists(Array<List> const& lists, BitMap const& rows,
                     std::string_view separator) -> Array<List> {
  auto const primary = lists.to_primary();
  auto const& storage = as<storage::ListStorage>(primary.storage());
  auto elements = BitMap::Mutable{storage.values().length()};
  auto const* spans = storage.spans().begin();
  storage::for_each_true(rows, [&](Index row) {
    for (auto i = spans[row].begin; i < spans[row].end; ++i) {
      elements.set(i, true);
    }
  });
  return Array<List>{
    storage.spans(),
    unflatten(storage.values(), std::move(elements).finish(), separator),
  };
}

/// One field of a record: a value from `source` at `node`, or a record at
/// `node` if `source` is negative.
struct Placement {
  Index node;
  Index source;
};

/// Unflattens records by placing every field into a tree of paths. The rows
/// are partitioned by the sequence of placements their fields make. Each
/// group replays its sequence once on the tree, which determines the nested
/// shapes and which value ends up at each path for all of its rows.
class Unflattener {
public:
  Unflattener(Index length, std::string_view separator)
    : length_{length}, separator_{separator} {
  }

  auto run(Array<Record> const& record, BitMap const& rows) -> Array<Record> {
    nodes_.emplace_back();
    nodes_.front().record = true;
    auto groups = analyze(record, rows, 0);
    for (auto& node : nodes_) {
      if (merged(node)) {
        node.record_rows = BitMap{length_, false};
      }
    }
    for (auto& source : sources_) {
      if (merged(nodes_[static_cast<std::size_t>(source.node)])) {
        source.rows = BitMap{length_, false};
      }
    }
    for (auto index = std::size_t{0}; index < groups.size(); ++index) {
      auto const group = static_cast<Index>(index);
      auto& root = nodes_.front();
      root.group = group;
      root.generation = ++generation_;
      root.children.clear();
      for (auto const& placement : groups[index].items) {
        place(placement, group);
      }
      emit(0, groups[index].rows);
    }
    return build(0);
  }

private:
  struct Node {
    std::string name;
    /// The nodes from below the root down to this one.
    std::vector<Index> path;
    std::unordered_map<std::string, Index> children_by_name;
    /// The children in the order of their creation, which is also the field
    /// order of this node's record.
    std::vector<Index> fields;
    /// This node's field index in its parent's record.
    Index slot = -1;
    /// The values that may end up at this node.
    std::vector<Index> sources;
    /// Whether this node holds a record in some rows.
    bool record = false;
    // The state while replaying a group.
    Index group = -1;
    std::uint64_t attached_to = 0;
    std::uint64_t generation = 0;
    bool is_record = false;
    Index source = -1;
    std::vector<Index> children;
    // The output of a node that holds a record.
    ShapeTable table;
    std::vector<std::pair<BitMap, ShapeTable::ShapeId>> shapes;
    Option<BitMap> record_rows;
  };

  struct Source {
    Index node;
    FieldArray value;
    /// The rows where this value ends up, if `node` has more than one origin.
    Option<BitMap> rows;
  };

  /// Whether the field of `node` combines more than one origin.
  static auto merged(Node const& node) -> bool {
    return node.sources.size() + (node.record ? 1 : 0) > 1;
  }

  auto child(Index parent, std::string_view name) -> Index {
    auto& node = nodes_[static_cast<std::size_t>(parent)];
    if (auto it = node.children_by_name.find(std::string{name});
        it != node.children_by_name.end()) {
      return it->second;
    }
    auto const id = static_cast<Index>(nodes_.size());
    node.children_by_name.emplace(std::string{name}, id);
    node.fields.push_back(id);
    auto slot = static_cast<Index>(node.fields.size() - 1);
    auto path = node.path;
    path.push_back(id);
    auto& result = nodes_.emplace_back();
    result.name = std::string{name};
    result.path = std::move(path);
    result.slot = slot;
    return id;
  }

  auto analyze(Array<Record> const& record, BitMap const& rows, Index parent)
    -> Partition<Placement> {
    auto const primary = record.to_primary();
    auto const& storage = record_storage(primary);
    return partition_fields<Placement>(
      storage, rows, [&](std::size_t index, BitMap const& member) {
        auto const& field = storage.arrays[index];
        auto node = parent;
        for (auto segment :
             split_name(storage.names_by_index[index], separator_)) {
          nodes_[static_cast<std::size_t>(node)].record = true;
          node = child(node, segment);
        }
        auto parts = Partition<Placement>{};
        auto scalar = member;
        if (auto records = field.data.get_alternative<Record>()) {
          auto nested = member & field.present & records->present;
          if (nested.any()) {
            scalar = std::move(scalar).and_not(nested);
            nodes_[static_cast<std::size_t>(node)].record = true;
            for (auto& group : analyze(records->data, nested, node)) {
              group.items.insert(group.items.begin(), Placement{node, -1});
              parts.push_back(std::move(group));
            }
          }
        }
        if (scalar.any()) {
          auto const source = static_cast<Index>(sources_.size());
          nodes_[static_cast<std::size_t>(node)].sources.push_back(source);
          auto value
            = unflatten(field.data, scalar & field.present, separator_);
          sources_.push_back(
            {node, FieldArray{std::move(value), field.present}, None{}});
          parts.push_back({std::move(scalar), {Placement{node, source}}});
        }
        return parts;
      });
  }

  auto is_attached(Node const& node, Node const& parent, Index group) const
    -> bool {
    return node.group == group and node.attached_to == parent.generation;
  }

  auto attach(Node& node, Node& parent, Index group, Index id) -> void {
    node.group = group;
    node.attached_to = parent.generation;
    parent.children.push_back(id);
  }

  /// Makes `id` a record, keeping its fields if it already is one.
  auto ensure_record(Index parent_id, Index id, Index group) -> void {
    auto& parent = nodes_[static_cast<std::size_t>(parent_id)];
    auto& node = nodes_[static_cast<std::size_t>(id)];
    if (not is_attached(node, parent, group)) {
      attach(node, parent, group, id);
    } else if (node.is_record) {
      return;
    }
    node.is_record = true;
    node.generation = ++generation_;
    node.children.clear();
  }

  auto place(Placement const& placement, Index group) -> void {
    auto const& path = nodes_[static_cast<std::size_t>(placement.node)].path;
    auto parent = Index{0};
    for (auto i = std::size_t{0}; i + 1 < path.size(); ++i) {
      ensure_record(parent, path[i], group);
      parent = path[i];
    }
    if (placement.source < 0) {
      ensure_record(parent, placement.node, group);
      return;
    }
    auto& parent_node = nodes_[static_cast<std::size_t>(parent)];
    auto& node = nodes_[static_cast<std::size_t>(placement.node)];
    if (not is_attached(node, parent_node, group)) {
      attach(node, parent_node, group, placement.node);
    }
    node.is_record = false;
    node.source = placement.source;
  }

  /// Records the outcome of the replayed group at `rows` for the record at
  /// `id`.
  auto emit(Index id, BitMap const& rows) -> void {
    auto& node = nodes_[static_cast<std::size_t>(id)];
    auto slots = std::vector<Index>{};
    slots.reserve(node.children.size());
    for (auto child_id : node.children) {
      slots.push_back(nodes_[static_cast<std::size_t>(child_id)].slot);
    }
    node.shapes.emplace_back(rows, node.table.with_fields(slots));
    if (node.record_rows) {
      *node.record_rows = std::move(*node.record_rows) | rows;
    }
    for (auto child_id : node.children) {
      auto const& child = nodes_[static_cast<std::size_t>(child_id)];
      if (child.is_record) {
        emit(child_id, rows);
      } else if (auto& source
                 = sources_[static_cast<std::size_t>(child.source)];
                 source.rows) {
        *source.rows = std::move(*source.rows) | rows;
      }
    }
  }

  auto build(Index id) -> Array<Record> {
    auto& node = nodes_[static_cast<std::size_t>(id)];
    auto names = std::vector<std::string_view>{};
    auto arrays = storage::RecordStorage::MaskedArrays{};
    for (auto child_id : node.fields) {
      auto& child = nodes_[static_cast<std::size_t>(child_id)];
      auto const combine = merged(child);
      auto parts = std::vector<FieldArray>{};
      for (auto source_id : child.sources) {
        auto& source = sources_[static_cast<std::size_t>(source_id)];
        if (combine) {
          parts.push_back(
            FieldArray{source.value.data, source.value.present & *source.rows});
        } else {
          parts.push_back(std::move(source.value));
        }
      }
      if (child.record) {
        auto present
          = combine ? std::move(*child.record_rows) : BitMap{length_, true};
        parts.push_back(
          FieldArray{Array<Data>{build(child_id)}, std::move(present)});
      }
      names.push_back(child.name);
      arrays.push_back(merge_parts(std::move(parts)));
    }
    return make_record(make_shapes(length_, node.shapes), std::move(node.table),
                       names, std::move(arrays));
  }

  Index length_;
  std::string_view separator_;
  std::vector<Node> nodes_;
  std::vector<Source> sources_;
  std::uint64_t generation_ = 0;
};

auto unflatten_records(Array<Record> const& record, BitMap const& rows,
                       std::string_view separator) -> Array<Record> {
  auto const primary = record.to_primary();
  auto const& storage = record_storage(primary);
  auto const split = std::ranges::any_of(storage.names, [&](auto const& entry) {
    return std::string_view{entry.first}.contains(separator);
  });
  if (split) {
    return Unflattener{primary.length(), separator}.run(primary, rows);
  }
  // The names stay as they are, so only nested values can change.
  auto arrays = storage.arrays;
  for (auto& field : arrays) {
    field.data
      = unflatten(std::move(field.data), rows & field.present, separator);
  }
  return Array<Record>{storage.shape_indices, ShapeTable{storage.shape_table},
                       storage.names, std::move(arrays)};
}

} // namespace

auto flatten(Array<Record> record, storage::BitMap const& rows,
             std::string_view separator) -> FlattenResult {
  auto const length = record.length();
  return Flattener{length, separator}.run(std::move(record), rows);
}

auto unflatten(Array<Data> data, storage::BitMap const& rows,
               std::string_view separator) -> Array<Data> {
  TENZIR_ASSERT(not separator.empty());
  if (not rows.any() or not contains_separator(data, separator)) {
    return data;
  }
  data = std::move(data).map_alternative<Record>(
    [&](MaskedArray<Array<Record>> records) {
      auto active = rows & records.present;
      if (not active.any()) {
        return std::move(records.data);
      }
      return unflatten_records(records.data, active, separator);
    });
  return std::move(data).map_alternative<List>(
    [&](MaskedArray<Array<List>> lists) {
      auto active = rows & lists.present;
      if (not active.any()) {
        return std::move(lists.data);
      }
      return unflatten_lists(lists.data, active, separator);
    });
}

} // namespace tenzir::nova
