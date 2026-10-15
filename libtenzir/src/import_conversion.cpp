//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
//

#include "tenzir/import_conversion.hpp"

#include "tenzir/arrow_utils.hpp"
#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/arrow_export.hpp"
#include "tenzir/nova/arrow_import.hpp"
#include "tenzir/nova/arrow_metadata.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/censor.hpp"
#include "tenzir/nova/schema_discovery.hpp"
#include "tenzir/nova/stringify.hpp"
#include "tenzir/nova_json_printer.hpp"
#include "tenzir/option.hpp"

#include <arrow/record_batch.h>
#include <arrow/util/utf8.h>

#include <limits>
#include <utility>

namespace tenzir {

namespace {

auto stringify_heterogeneous_lists(nova::Array<nova::Data> const& values)
  -> Result<Option<nova::Array<nova::Data>>, std::string>;

/// Renders a non-null element of a heterogeneous list as a string. Prefers the
/// TQL representation, but falls back to JSON, which encodes blobs as Base64,
/// if the TQL representation would not be valid UTF-8. This happens for blobs
/// that hold arbitrary bytes, which must survive the conversion losslessly.
auto stringify_element(nova::RowView<nova::Data> const& element)
  -> std::string {
  if (auto string = nova::stringify(element)) {
    if (arrow::util::ValidateUTF8(
          reinterpret_cast<uint8_t const*>(string->data()), string->size())) {
      return std::move(*string);
    }
  }
  auto printer = nova::json_printer{{
    .style = no_style(),
    .oneline = true,
    .trailing_commas = false,
  }};
  printer.print(element);
  auto const bytes = printer.bytes();
  return std::string{reinterpret_cast<char const*>(bytes.data()), bytes.size()};
}

/// Rewrites the lists of `array` whose elements have no common type, such as
/// `[1, "two"]`, so that each non-null element becomes its string
/// representation. Returns `None` if no list changed.
auto stringify_heterogeneous_lists(nova::Array<nova::List> const& array)
  -> Result<Option<nova::Array<nova::Data>>, std::string> {
  auto primary = array.to_primary();
  auto const& lists = as<nova::storage::ListStorage>(primary.storage());
  // Rewrite nested lists first, so that the check below sees their final
  // types.
  TRY(auto nested, stringify_heterogeneous_lists(lists.values()));
  auto rewritten = nested ? nova::Array<nova::List>{lists.spans(), *nested}
                          : std::move(primary);
  auto const& current = as<nova::storage::ListStorage>(rewritten.storage());
  auto const& elements = current.values();
  auto discovery = nova::SchemaDiscovery{};
  TRY(auto ids, discovery.discover(
                  elements, nova::storage::BitMap{elements.length(), true}));
  auto const& types = discovery.types();
  auto heterogeneous = std::vector<bool>(rewritten.length(), false);
  auto any = false;
  for (auto row = nova::storage::Index{0}; row < rewritten.length(); ++row) {
    auto span = current.spans()[row];
    auto element = type{null_type{}};
    for (auto i = span.begin; i < span.end; ++i) {
      auto refined = nova::refine_null_types(element, types[ids[i]]);
      if (not refined) {
        heterogeneous[row] = true;
        any = true;
        break;
      }
      element = std::move(refined).unwrap();
    }
  }
  if (not any) {
    if (nested) {
      return Option{nova::Array<nova::Data>{std::move(rewritten)}};
    }
    return Option<nova::Array<nova::Data>>{None{}};
  }
  auto builder = nova::ArrayBuilder<nova::List>{};
  for (auto row = nova::storage::Index{0}; row < rewritten.length(); ++row) {
    auto list = builder.list();
    for (auto element : rewritten.get(row)) {
      if (not heterogeneous[row] or is<nova::RowView<nova::Null>>(element)) {
        append_row(list, element);
      } else {
        list.data(std::string_view{stringify_element(element)});
      }
    }
  }
  return Option{nova::Array<nova::Data>{builder.finish()}};
}

auto stringify_heterogeneous_lists(nova::Array<nova::Data> const& values)
  -> Result<Option<nova::Array<nova::Data>>, std::string> {
  return match(
    values,
    [](nova::UnionArray const& union_)
      -> Result<Option<nova::Array<nova::Data>>, std::string> {
      auto columns = std::vector<nova::Array<nova::Data>>{};
      auto changed = false;
      for (auto const& field : union_.fields()) {
        auto column = match(field.data, [](auto const& concrete) {
          return nova::Array<nova::Data>{concrete};
        });
        TRY(auto replacement, stringify_heterogeneous_lists(column));
        if (replacement) {
          column = std::move(*replacement);
          changed = true;
        }
        columns.push_back(std::move(column));
      }
      if (not changed) {
        return Option<nova::Array<nova::Data>>{None{}};
      }
      auto builder = nova::ArrayBuilder<nova::Data>{};
      for (auto row = nova::storage::Index{0}; row < union_.length(); ++row) {
        auto index = union_.alternative_index_at(row);
        if (index < 0) {
          builder.null();
        } else {
          append_row(builder, columns[index].get(row));
        }
      }
      return Option{builder.finish()};
    },
    []<class T>(nova::Array<T> const& array)
      -> Result<Option<nova::Array<nova::Data>>, std::string> {
      if constexpr (std::same_as<T, nova::List>) {
        return stringify_heterogeneous_lists(array);
      } else if constexpr (std::same_as<T, nova::Record>) {
        auto primary = array.to_primary();
        auto const& source
          = *as<nova::storage::RecordStorage>(primary.storage());
        auto fields = Option<nova::storage::RecordStorage::MaskedArrays>{};
        for (auto i = size_t{0}; i < source.arrays.size(); ++i) {
          TRY(auto replacement,
              stringify_heterogeneous_lists(source.arrays[i].data));
          if (replacement) {
            if (not fields) {
              fields.emplace(source.arrays);
            }
            (*fields)[i].data = std::move(*replacement);
          }
        }
        if (fields) {
          return Option{nova::Array<nova::Data>{nova::Array<nova::Record>{
            source.shape_indices, nova::ShapeTable{source.shape_table},
            source.names, std::move(*fields)}}};
        }
      }
      return Option<nova::Array<nova::Data>>{None{}};
    });
}

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

auto redact_import_secrets(nova::Events events)
  -> std::pair<bool, nova::Events> {
  auto [data, censored]
    = nova::censor_secrets(nova::Array<nova::Data>{events.data});
  if (not censored) {
    return {false, std::move(events)};
  }
  auto record = data.try_as<nova::Record>();
  TENZIR_ASSERT(record);
  events.data = std::move(*record);
  return {true, std::move(events)};
}

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
  auto discovery = nova::SchemaDiscovery{};
  auto discovered
    = discovery.discover(nova::Array<nova::Data>{events.data}, selection);
  if (not discovered) {
    // Arrow requires one element type per list, so the elements of a list
    // without a common type are stored as strings, like legacy parsers did.
    // Checking only on failure keeps the common path free of extra passes.
    TRY(auto rewritten,
        stringify_heterogeneous_lists(nova::Array<nova::Data>{events.data}));
    if (rewritten) {
      auto record = rewritten->try_as<nova::Record>();
      TENZIR_ASSERT(record);
      events.data = std::move(*record);
      discovery = nova::SchemaDiscovery{};
      discovered
        = discovery.discover(nova::Array<nova::Data>{events.data}, selection);
    }
  }
  TRY(auto schemas, std::move(discovered));
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
        auto merged
          = nova::refine_null_types(type{staged_schemas[i]}, type{incoming});
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
