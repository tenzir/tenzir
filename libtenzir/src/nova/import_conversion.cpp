//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
//

#include "tenzir/nova/import_conversion.hpp"

#include "tenzir/arrow_memory_pool.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/nova/arrow_import.hpp"
#include "tenzir/nova/arrow_metadata.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/materialize.hpp"
#include "tenzir/view.hpp"

#include <arrow/record_batch.h>

#include <limits>
#include <map>
#include <utility>

namespace tenzir::nova {

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

  auto discover(Array<Data> const& array, storage::BitMap const& selected)
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

private:
  auto intern(type value) -> size_t {
    auto [it, inserted] = ids_.try_emplace(value, types_.size());
    if (inserted) {
      types_.push_back(std::move(value));
    }
    return it->second;
  }

  auto
  discover_record(Array<Record> const& array, storage::BitMap const& selected)
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
        it
          = schemas.emplace(signature, intern(type{record_type{schema}})).first;
      }
      result[row] = it->second;
    }
    return result;
  }

  auto discover_list(Array<List> const& array, storage::BitMap const& selected)
    -> Result<std::vector<size_t>, std::string> {
    if (auto constant = try_as<storage::ConstantStorage<List, RowView<List>>>(
          array.storage())) {
      auto single = Array<List>{
        storage::ConstantStorage<List, RowView<List>>{1, constant->value()}};
      TRY(auto ids,
          discover_list(single.to_primary(), storage::BitMap{1, true}));
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

auto make_slice(Events const& events, record_type const& schema,
                std::vector<storage::Index> const& rows,
                std::string const& name, bool internal, time import_time)
  -> Result<table_slice, std::string> {
  auto builders = std::vector<std::shared_ptr<arrow::ArrayBuilder>>{};
  builders.reserve(schema.num_fields());
  for (auto const& field : schema.fields()) {
    builders.push_back(field.type.make_arrow_builder(arrow_memory_pool()));
  }
  for (auto index : rows) {
    auto record = events.data.get(index);
    auto field = record.begin();
    for (auto i = size_t{0}; i < builders.size(); ++i, ++field) {
      auto [field_name, value] = *field;
      if (field_name != schema.field(i).name) {
        return Err{"record shape changed during conversion"};
      }
      auto materialized = materialize(value);
      auto status = append_builder(schema.field(i).type, *builders[i],
                                   make_view(materialized));
      if (not status.ok()) {
        return Err{status.ToString()};
      }
    }
    if (field != record.end()) {
      return Err{"record shape changed during conversion"};
    }
  }
  auto arrays = arrow::ArrayVector{};
  arrays.reserve(builders.size());
  for (auto& builder : builders) {
    auto array = builder->Finish();
    if (not array.ok()) {
      return Err{array.status().ToString()};
    }
    arrays.push_back(std::move(*array));
  }
  auto output_schema
    = ArrowMetadata{name, internal}.apply(type{name, schema}).to_arrow_schema();
  auto batch = arrow::RecordBatch::Make(std::move(output_schema), rows.size(),
                                        std::move(arrays));
  if (auto status = batch->ValidateFull(); not status.ok()) {
    return Err{status.ToString()};
  }
  auto slice = table_slice{std::move(batch)};
  slice.import_time(import_time);
  return slice;
}

} // namespace

ImportConversionBuffer::ImportConversionBuffer(std::string name, bool internal)
  : name_{std::move(name)}, internal_{internal} {
}

auto ImportConversionBuffer::add(Events events, storage::BitMap selection)
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
  for (auto index : storage::true_bits(selection)) {
    if (not events.mask.get(index)) {
      return Err{"event selection includes an inactive row"};
    }
    if (*events.meta.name.get(index) != name_
        or *events.meta.internal.get(index) != internal_) {
      return Err{"event metadata does not match the conversion buffer"};
    }
  }
  auto discovery = SchemaDiscovery{};
  TRY(auto schemas, discovery.discover(Array<Data>{events.data}, selection));
  // Refinement changes first-fit decisions, so cached assignments are only
  // valid until a candidate schema changes or a new candidate is appended.
  auto epoch = size_t{1};
  auto destinations = std::vector<std::pair<size_t, size_t>>(
    discovery.types().size(), {0, unassigned});
  for (auto index : storage::true_bits(selection)) {
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
    auto mask = storage::BitMap::Mutable{events.length()};
    for (auto row = storage::Index{0}; row < events.length(); ++row) {
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
      auto rows = std::vector<storage::Index>{};
      auto timestamp = time{};
      for (auto index : storage::true_bits(selection.mask)) {
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
  -> std::vector<std::pair<type, Events>> {
  auto result = std::vector<std::pair<type, Events>>{};
  for (auto const& candidate : candidates_) {
    auto schema
      = ArrowMetadata{name_, internal_}.apply(type{name_, candidate.schema});
    for (auto const& selection : candidate.selections) {
      auto events = batches_[selection.batch];
      events.mask = selection.mask;
      result.emplace_back(schema, std::move(events));
    }
  }
  return result;
}

auto import_table_slice(table_slice const& slice)
  -> Result<Events, std::string> {
  auto batch = to_record_batch(slice);
  auto array = batch->ToStructArray();
  if (not array.ok()) {
    return Err{array.status().ToStringWithoutContextLines()};
  }
  TRY(auto imported, import_arrow_array(array.MoveValueUnsafe()));
  auto records = std::move(imported).try_as<Record>();
  if (not records) {
    return Err{"imported Arrow batch is not a record"};
  }
  auto const length = records->length();
  auto meta = ArrowMetadata::from_arrow(*batch->schema(), slice.schema().name())
                .to_meta(length);
  meta.import_time
    = Array<Time>{storage::ConstantStorage<Time>{length, slice.import_time()}};
  return Events{std::move(*records), storage::BitMap{length, true},
                std::move(meta)};
}

} // namespace tenzir::nova
