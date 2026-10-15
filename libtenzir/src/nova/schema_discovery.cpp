//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/schema_discovery.hpp"

#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/try.hpp"

#include <limits>
#include <utility>

namespace tenzir::nova {

auto refine_null_types(type const& lhs, type const& rhs)
  -> Result<type, std::string> {
  if (lhs == rhs or is<null_type>(rhs)) {
    return lhs;
  }
  if (is<null_type>(lhs)) {
    return rhs;
  }
  if (auto left = try_as<list_type>(lhs)) {
    if (auto right = try_as<list_type>(rhs)) {
      TRY(auto element,
          refine_null_types(left->value_type(), right->value_type()));
      return type{list_type{element}};
    }
  }
  if (auto left = try_as<record_type>(lhs)) {
    if (auto right = try_as<record_type>(rhs)) {
      if (left->num_fields() != right->num_fields()) {
        return Err{"record field sets differ"};
      }
      auto fields = std::vector<struct record_type::field>{};
      for (auto i = size_t{0}; i < left->num_fields(); ++i) {
        auto a = left->field(i);
        auto b = right->field(i);
        if (a.name != b.name) {
          return Err{"record field names or order differ"};
        }
        TRY(auto field_type, refine_null_types(a.type, b.type));
        fields.emplace_back(std::string{a.name}, std::move(field_type));
      }
      return type{record_type{fields}};
    }
  }
  return Err{"concrete value types differ"};
}

SchemaDiscovery::SchemaDiscovery() {
  std::ignore = intern(type{null_type{}});
}

auto SchemaDiscovery::types() const -> std::vector<type> const& {
  return types_;
}

auto SchemaDiscovery::discover(Array<Data> const& array,
                               storage::BitMap const& selected)
  -> Result<std::vector<size_t>, std::string> {
  auto result = std::vector<size_t>(array.length(), 0);
  if (not selected.any()) {
    return result;
  }
  TRY(match(
    array,
    [&](UnionArray const& union_) -> Result<void, std::string> {
      for (auto const& alternative : union_.fields()) {
        auto mask = selected & alternative.present;
        if (not mask.any()) {
          continue;
        }
        TRY(auto ids, discover(Array<Data>{alternative.data}, mask));
        storage::for_each_true(mask, [&](auto row) {
          result[row] = ids[row];
        });
      }
      return {};
    },
    [&]<class T>(Array<T> const& values) -> Result<void, std::string> {
      if constexpr (std::same_as<T, Record>) {
        TRY(result, discover_record(values, selected));
      } else if constexpr (std::same_as<T, List>) {
        TRY(result, discover_list(values, selected));
      } else if constexpr (std::same_as<T, Secret>) {
        return Err{"secrets cannot be stored without redaction"};
      } else {
        auto id = size_t{0};
        if constexpr (not std::same_as<T, Null>) {
          id = intern(type{data_to_type_t<T>{}});
        }
        storage::for_each_true(selected, [&](auto row) {
          result[row] = id;
        });
      }
      return {};
    }));
  return result;
}

auto SchemaDiscovery::intern(type value) -> size_t {
  auto [it, inserted] = ids_.try_emplace(value, types_.size());
  if (inserted) {
    types_.push_back(std::move(value));
  }
  return it->second;
}

auto SchemaDiscovery::discover_record(Array<Record> const& array,
                                      storage::BitMap const& selected)
  -> Result<std::vector<size_t>, std::string> {
  auto primary = array.to_primary();
  auto const& records = *as<storage::RecordStorage>(primary.storage());
  auto masks = std::vector<storage::BitMap::Mutable>{};
  for (auto i = size_t{0}; i < records.arrays.size(); ++i) {
    masks.emplace_back(array.length());
  }
  for (auto row : storage::true_bits(selected)) {
    auto shape = records.shape_indices.get(row);
    if (shape < 0) {
      return Err{"selected row has no record shape"};
    }
    for (auto field : records.shape_table.fields(shape)) {
      masks[field].set(row, true);
    }
  }
  auto columns = std::vector<std::vector<size_t>>{};
  for (auto i = size_t{0}; i < records.arrays.size(); ++i) {
    auto mask = std::move(masks[i]).finish() & records.arrays[i].present;
    TRY(auto ids, discover(records.arrays[i].data, mask));
    columns.push_back(std::move(ids));
  }
  auto result = std::vector<size_t>(array.length(), 0);
  auto schemas = std::map<std::vector<size_t>, size_t>{};
  auto signature = std::vector<size_t>{};
  for (auto row : storage::true_bits(selected)) {
    auto shape = records.shape_indices.get(row);
    auto fields = records.shape_table.fields(shape);
    signature.clear();
    signature.push_back(static_cast<size_t>(shape));
    for (auto field : fields) {
      signature.push_back(columns[field][row]);
    }
    auto it = schemas.find(signature);
    if (it == schemas.end()) {
      auto schema = std::vector<struct record_type::field>{};
      for (auto field : fields) {
        schema.emplace_back(std::string{records.names_by_index[field]},
                            types_[columns[field][row]]);
      }
      it = schemas.emplace(signature, intern(type{record_type{schema}})).first;
    }
    result[row] = it->second;
  }
  return result;
}

auto SchemaDiscovery::discover_list(Array<List> const& array,
                                    storage::BitMap const& selected)
  -> Result<std::vector<size_t>, std::string> {
  if (auto constant = try_as<storage::ConstantStorage<List, RowView<List>>>(
        array.storage())) {
    auto single = Array<List>{
      storage::ConstantStorage<List, RowView<List>>{1, constant->value()}};
    TRY(auto ids, discover_list(single.to_primary(), storage::BitMap{1, true}));
    auto result = std::vector<size_t>(array.length(), 0);
    storage::for_each_true(selected, [&](auto row) {
      result[row] = ids[0];
    });
    return result;
  }
  auto const& lists = as<storage::ListStorage>(array.storage());
  if (lists.values().length() == 0) {
    auto id = intern(type{list_type{type{null_type{}}}});
    auto result = std::vector<size_t>(array.length(), 0);
    storage::for_each_true(selected, [&](auto row) {
      result[row] = id;
    });
    return result;
  }
  auto children = storage::BitMap::Mutable{lists.values().length()};
  for (auto row : storage::true_bits(selected)) {
    auto span = lists.spans()[row];
    for (auto i = span.begin; i < span.end; ++i) {
      children.set(i, true);
    }
  }
  TRY(auto ids, discover(lists.values(), std::move(children).finish()));
  auto result = std::vector<size_t>(array.length(), 0);
  auto merged = std::map<std::pair<size_t, size_t>, size_t>{};
  auto list_types = std::map<size_t, size_t>{};
  for (auto row : storage::true_bits(selected)) {
    auto element = size_t{0};
    auto span = lists.spans()[row];
    for (auto i = span.begin; i < span.end; ++i) {
      auto next = ids[i];
      if (element == next or next == 0) {
        continue;
      }
      if (element == 0) {
        element = next;
        continue;
      }
      auto key = std::pair{element, next};
      auto it = merged.find(key);
      if (it == merged.end()) {
        auto refined = refine_null_types(types_[element], types_[next]);
        if (not refined) {
          return Err{"list contains incompatible element types or record "
                     "shapes"};
        }
        it = merged.emplace(key, intern(std::move(refined).unwrap())).first;
      }
      element = it->second;
    }
    auto it = list_types.find(element);
    if (it == list_types.end()) {
      it = list_types.emplace(element, intern(type{list_type{types_[element]}}))
             .first;
    }
    result[row] = it->second;
  }
  return result;
}

auto group_by_schema(Events const& events, SchemaGroupingOptions options)
  -> Result<std::vector<SchemaGroup>, std::string> {
  auto discovery = SchemaDiscovery{};
  TRY(auto ids, discovery.discover(Array<Data>{events.data}, events.mask));
  auto const& types = discovery.types();
  auto const unassigned = std::numeric_limits<size_t>::max();
  auto groups = std::vector<SchemaGroup>{};
  if (not options.refine_nulls) {
    // Exact grouping only needs to map each discovered type to its group.
    auto destinations = std::vector<size_t>(types.size(), unassigned);
    for (auto row : storage::true_bits(events.mask)) {
      auto& destination = destinations[ids[row]];
      if (destination == unassigned) {
        destination = groups.size();
        groups.push_back(SchemaGroup{types[ids[row]], {}});
      }
      groups[destination].rows.push_back(row);
    }
    return groups;
  }
  // Refinement changes first-fit decisions, so cached assignments are only
  // valid until a group schema changes or a new group is appended.
  auto epoch = size_t{1};
  auto destinations
    = std::vector<std::pair<size_t, size_t>>(types.size(), {0, unassigned});
  for (auto row : storage::true_bits(events.mask)) {
    auto& cached = destinations[ids[row]];
    if (cached.first != epoch) {
      auto const& incoming = types[ids[row]];
      auto destination = unassigned;
      for (auto i = size_t{0}; i < groups.size(); ++i) {
        if (groups[i].schema == incoming) {
          destination = i;
          break;
        }
      }
      if (destination == unassigned) {
        for (auto i = size_t{0}; i < groups.size(); ++i) {
          auto merged = refine_null_types(groups[i].schema, incoming);
          if (merged) {
            auto schema = std::move(merged).unwrap();
            if (schema != groups[i].schema) {
              groups[i].schema = std::move(schema);
              ++epoch;
            }
            destination = i;
            break;
          }
        }
      }
      if (destination == unassigned) {
        destination = groups.size();
        groups.push_back(SchemaGroup{incoming, {}});
        ++epoch;
      }
      cached = {epoch, destination};
    }
    groups[cached.second].rows.push_back(row);
  }
  return groups;
}

} // namespace tenzir::nova
