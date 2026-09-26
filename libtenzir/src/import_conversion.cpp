//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
//

#include "tenzir/import_conversion.hpp"

#include "tenzir/arrow_utils.hpp"
#include "tenzir/nova/arrow_export.hpp"
#include "tenzir/nova/arrow_import.hpp"
#include "tenzir/nova/arrow_metadata.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"

#include <arrow/record_batch.h>

#include <limits>
#include <map>
#include <utility>

namespace tenzir {

namespace {

auto refine(type const& lhs, type const& rhs) -> Result<type, std::string> {
  if (lhs == rhs or is<null_type>(rhs)) {
    return lhs;
  }
  if (is<null_type>(lhs)) {
    return rhs;
  }
  if (auto left = try_as<list_type>(lhs)) {
    if (auto right = try_as<list_type>(rhs)) {
      TRY(auto element, refine(left->value_type(), right->value_type()));
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
        TRY(auto field_type, refine(a.type, b.type));
        fields.emplace_back(std::string{a.name}, std::move(field_type));
      }
      return type{record_type{fields}};
    }
  }
  return Err{"concrete value types differ"};
}

// Discover types from columns, constructing each distinct schema once. Rows
// only carry schema IDs; scalar values never participate in discovery.
class SchemaDiscovery {
public:
  SchemaDiscovery() {
    std::ignore = intern(type{null_type{}});
  }

  auto types() const -> std::vector<type> const& {
    return types_;
  }

  auto discover(nova::Array<nova::Data> const& array,
                nova::storage::BitMap const& selected)
    -> Result<std::vector<size_t>, std::string> {
    auto result = std::vector<size_t>(array.length(), 0);
    if (not selected.any()) {
      return result;
    }
    TRY(match(
      array,
      [&](nova::UnionArray const& union_) -> Result<void, std::string> {
        for (auto const& alternative : union_.fields()) {
          auto mask = selected & alternative.present;
          if (not mask.any()) {
            continue;
          }
          TRY(auto ids,
              discover(nova::Array<nova::Data>{alternative.data}, mask));
          nova::storage::for_each_true(mask, [&](auto row) {
            result[row] = ids[row];
          });
        }
        return {};
      },
      [&]<class T>(nova::Array<T> const& values) -> Result<void, std::string> {
        if constexpr (std::same_as<T, nova::Record>) {
          TRY(result, discover_record(values, selected));
        } else if constexpr (std::same_as<T, nova::List>) {
          TRY(result, discover_list(values, selected));
        } else if constexpr (std::same_as<T, nova::Secret>) {
          return Err{"secrets cannot be stored without redaction"};
        } else {
          auto id = size_t{0};
          if constexpr (not std::same_as<T, nova::Null>) {
            id = intern(type{data_to_type_t<T>{}});
          }
          nova::storage::for_each_true(selected, [&](auto row) {
            result[row] = id;
          });
        }
        return {};
      }));
    return result;
  }

private:
  auto intern(type value) -> size_t {
    auto [it, inserted] = ids_.try_emplace(value, types_.size());
    if (inserted) {
      types_.push_back(std::move(value));
    }
    return it->second;
  }

  auto discover_record(nova::Array<nova::Record> const& array,
                       nova::storage::BitMap const& selected)
    -> Result<std::vector<size_t>, std::string> {
    auto primary = array.to_primary();
    auto const& records = *as<nova::storage::RecordStorage>(primary.storage());
    auto masks = std::vector<nova::storage::BitMap::Mutable>{};
    for (auto i = size_t{0}; i < records.arrays.size(); ++i) {
      masks.emplace_back(array.length());
    }
    for (auto row : nova::storage::true_bits(selected)) {
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
    for (auto row : nova::storage::true_bits(selected)) {
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
        it
          = schemas.emplace(signature, intern(type{record_type{schema}})).first;
      }
      result[row] = it->second;
    }
    return result;
  }

  auto discover_list(nova::Array<nova::List> const& array,
                     nova::storage::BitMap const& selected)
    -> Result<std::vector<size_t>, std::string> {
    if (auto constant = try_as<
          nova::storage::ConstantStorage<nova::List, nova::RowView<nova::List>>>(
          array.storage())) {
      auto single = nova::Array<nova::List>{
        nova::storage::ConstantStorage<nova::List, nova::RowView<nova::List>>{
          1, constant->value()}};
      TRY(auto ids,
          discover_list(single.to_primary(), nova::storage::BitMap{1, true}));
      auto result = std::vector<size_t>(array.length(), 0);
      nova::storage::for_each_true(selected, [&](auto row) {
        result[row] = ids[0];
      });
      return result;
    }
    auto const& lists = as<nova::storage::ListStorage>(array.storage());
    if (lists.values().length() == 0) {
      auto id = intern(type{list_type{type{null_type{}}}});
      auto result = std::vector<size_t>(array.length(), 0);
      nova::storage::for_each_true(selected, [&](auto row) {
        result[row] = id;
      });
      return result;
    }
    auto children = nova::storage::BitMap::Mutable{lists.values().length()};
    for (auto row : nova::storage::true_bits(selected)) {
      auto span = lists.spans()[row];
      for (auto i = span.begin; i < span.end; ++i) {
        children.set(i, true);
      }
    }
    TRY(auto ids, discover(lists.values(), std::move(children).finish()));
    auto result = std::vector<size_t>(array.length(), 0);
    auto merged = std::map<std::pair<size_t, size_t>, size_t>{};
    auto list_types = std::map<size_t, size_t>{};
    for (auto row : nova::storage::true_bits(selected)) {
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
          auto refined = refine(types_[element], types_[next]);
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
        it = list_types
               .emplace(element, intern(type{list_type{types_[element]}}))
               .first;
      }
      result[row] = it->second;
    }
    return result;
  }

  std::vector<type> types_;
  std::map<type, size_t> ids_;
};

auto make_slice(nova::Events const& events, record_type const& schema,
                std::vector<nova::storage::Index> const& rows,
                std::string const& name, bool internal, time import_time)
  -> Result<table_slice, std::string> {
  auto output_schema
    = nova::ArrowMetadata{name, internal}.apply(type{name, schema});
  TRY(auto batch,
      nova::to_arrow_record_batch(events.data, output_schema, rows));
  auto slice = table_slice{std::move(batch)};
  slice.import_time(import_time);
  return slice;
}

} // namespace

ImportConversionBuffer::ImportConversionBuffer(std::string name, bool internal)
  : name_{std::move(name)}, internal_{internal} {
}

auto ImportConversionBuffer::add(nova::Events events,
                                 nova::storage::BitMap selection)
  -> Result<void, std::string> {
  if (selection.length() != events.length()
      or events.mask.length() != events.length()
      or events.meta.name.length() != events.length()
      or events.meta.import_time.length() != events.length()
      or events.meta.internal.length() != events.length()) {
    return Err{"event selection has the wrong length"};
  }
  auto staged_schemas = std::vector<record_type>{};
  staged_schemas.reserve(candidates_.size());
  for (auto const& candidate : candidates_) {
    staged_schemas.push_back(candidate.schema);
  }
  auto const unassigned = std::numeric_limits<size_t>::max();
  auto assignments = std::vector<size_t>(events.length(), unassigned);
  for (auto index : nova::storage::true_bits(selection)) {
    if (not events.mask.get(index)) {
      return Err{"event selection includes an inactive row"};
    }
    if (*events.meta.name.get(index) != name_
        or *events.meta.internal.get(index) != internal_) {
      return Err{"event metadata does not match the conversion buffer"};
    }
  }
  auto discovery = SchemaDiscovery{};
  TRY(auto schemas,
      discovery.discover(nova::Array<nova::Data>{events.data}, selection));
  // Refinement changes first-fit decisions, so cached assignments are only
  // valid until a candidate schema changes or a new candidate is appended.
  auto epoch = size_t{1};
  auto destinations = std::vector<std::pair<size_t, size_t>>(
    discovery.types().size(), {0, unassigned});
  for (auto index : nova::storage::true_bits(selection)) {
    auto& cached = destinations[schemas[index]];
    if (cached.first == epoch) {
      assignments[index] = cached.second;
      continue;
    }
    auto incoming = as<record_type>(discovery.types()[schemas[index]]);
    auto destination = unassigned;
    for (auto i = size_t{0}; i < staged_schemas.size(); ++i) {
      if (staged_schemas[i] == incoming) {
        destination = i;
        break;
      }
    }
    if (destination == unassigned) {
      for (auto i = size_t{0}; i < staged_schemas.size(); ++i) {
        auto merged = refine(type{staged_schemas[i]}, type{incoming});
        if (merged) {
          auto schema = as<record_type>(std::move(merged).unwrap());
          if (schema != staged_schemas[i]) {
            staged_schemas[i] = std::move(schema);
            ++epoch;
          }
          destination = i;
          break;
        }
      }
    }
    if (destination == unassigned) {
      destination = staged_schemas.size();
      staged_schemas.push_back(std::move(incoming));
      ++epoch;
    }
    cached = {epoch, destination};
    assignments[index] = destination;
  }
  auto const batch_index = batches_.size();
  auto selected_rows = size_t{0};
  for (auto i = size_t{0}; i < staged_schemas.size(); ++i) {
    auto mask = nova::storage::BitMap::Mutable{events.length()};
    for (auto row = nova::storage::Index{0}; row < events.length(); ++row) {
      if (assignments[row] == i) {
        mask.set(row, true);
        ++selected_rows;
      }
    }
    if (i == candidates_.size()) {
      candidates_.push_back(Candidate{std::move(staged_schemas[i]), {}});
    } else {
      candidates_[i].schema = std::move(staged_schemas[i]);
    }
    if (mask.true_count() > 0) {
      candidates_[i].selections.push_back(
        Selection{batch_index, std::move(mask).finish()});
    }
  }
  if (selected_rows > 0) {
    batches_.push_back(std::move(events));
    rows_ += selected_rows;
  }
  return {};
}

auto ImportConversionBuffer::snapshot() const
  -> Result<std::vector<table_slice>, std::string> {
  auto output = std::vector<table_slice>{};
  for (auto const& candidate : candidates_) {
    for (auto const& selection : candidate.selections) {
      auto const& events = batches_[selection.batch];
      auto rows = std::vector<nova::storage::Index>{};
      auto timestamp = time{};
      for (auto index : nova::storage::true_bits(selection.mask)) {
        auto current = *events.meta.import_time.get(index);
        if (not rows.empty() and current != timestamp) {
          TRY(auto slice, make_slice(events, candidate.schema, rows, name_,
                                     internal_, timestamp));
          output.push_back(std::move(slice));
          rows.clear();
        }
        timestamp = current;
        rows.push_back(index);
      }
      if (not rows.empty()) {
        TRY(auto slice, make_slice(events, candidate.schema, rows, name_,
                                   internal_, timestamp));
        output.push_back(std::move(slice));
      }
    }
  }
  return output;
}

auto ImportConversionBuffer::rows() const -> size_t {
  return rows_;
}

auto ImportConversionBuffer::selected_events() const
  -> std::vector<std::pair<type, nova::Events>> {
  auto result = std::vector<std::pair<type, nova::Events>>{};
  for (auto const& candidate : candidates_) {
    auto schema = nova::ArrowMetadata{name_, internal_}.apply(
      type{name_, candidate.schema});
    for (auto const& selection : candidate.selections) {
      auto events = batches_[selection.batch];
      events.mask = selection.mask;
      result.emplace_back(schema, std::move(events));
    }
  }
  return result;
}

auto import_table_slice(table_slice const& slice)
  -> Result<nova::Events, std::string> {
  auto batch = to_record_batch(slice);
  auto array = batch->ToStructArray();
  if (not array.ok()) {
    return Err{array.status().ToStringWithoutContextLines()};
  }
  TRY(auto imported, nova::import_arrow_array(array.MoveValueUnsafe()));
  auto records = std::move(imported).try_as<nova::Record>();
  if (not records) {
    return Err{"imported Arrow batch is not a record"};
  }
  auto const length = records->length();
  auto meta
    = nova::ArrowMetadata::from_arrow(*batch->schema(), slice.schema().name())
        .to_meta(length);
  meta.import_time = nova::Array<nova::Time>{
    nova::storage::ConstantStorage<nova::Time>{length, slice.import_time()}};
  return nova::Events{std::move(*records), nova::storage::BitMap{length, true},
                      std::move(meta)};
}

} // namespace tenzir
