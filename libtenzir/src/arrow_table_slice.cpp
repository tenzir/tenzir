//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_table_slice.hpp"

#include "tenzir/config.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/detail/zip_iterator.hpp"
#include "tenzir/error.hpp"
#include "tenzir/fbs/table_slice.hpp"
#include "tenzir/fbs/utils.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/table_slice_builder.hpp"
#include "tenzir/value_index.hpp"

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/status.h>

#include <type_traits>
#include <utility>

namespace tenzir {

// -- utility for converting Buffer to RecordBatch -----------------------------

namespace {

template <class Callback>
class record_batch_listener final : public arrow::ipc::Listener {
public:
  record_batch_listener(Callback&& callback) noexcept
    : callback_{std::forward<Callback>(callback)} {
    // nop
  }

  ~record_batch_listener() noexcept override = default;

private:
  arrow::Status OnRecordBatchDecoded(
    std::shared_ptr<arrow::RecordBatch> record_batch) override {
    std::invoke(callback_, std::move(record_batch));
    return arrow::Status::OK();
  }

  Callback callback_;
};

template <class Callback>
auto make_record_batch_listener(Callback&& callback) {
  return std::make_shared<record_batch_listener<Callback>>(
    std::forward<Callback>(callback));
}

class record_batch_decoder final {
public:
  record_batch_decoder() noexcept
    : decoder_{make_record_batch_listener(
      [&](std::shared_ptr<arrow::RecordBatch> record_batch) {
        record_batch_ = std::move(record_batch);
      })} {
    // nop
  }

  std::shared_ptr<arrow::RecordBatch>
  decode(const std::shared_ptr<arrow::Buffer>& flat_record_batch) noexcept {
    TENZIR_ASSERT(!record_batch_);
    if (auto status = decoder_.Consume(flat_record_batch); !status.ok()) {
      TENZIR_ERROR("{} failed to decode Arrow Record Batch: {}", __func__,
                   status.ToString());
      return {};
    }
    TENZIR_ASSERT(record_batch_);
    return std::exchange(record_batch_, {});
  }

private:
  arrow::ipc::StreamDecoder decoder_;
  std::shared_ptr<arrow::RecordBatch> record_batch_ = nullptr;
};

/// Compute position for each array by traversing the schema tree breadth-first.
void index_column_arrays(const std::shared_ptr<arrow::Array>& arr,
                         arrow::ArrayVector& out) {
  auto f = detail::overload{
    [&](const auto&) {
      out.push_back(arr);
    },
    [&](const arrow::StructArray& s) {
      for (const auto& child : s.fields())
        index_column_arrays(child, out);
    },
  };
  return caf::visit(f, *arr);
}

arrow::ArrayVector
index_column_arrays(const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  arrow::ArrayVector result{};
  for (const auto& arr : record_batch->columns())
    index_column_arrays(arr, result);
  return result;
}

} // namespace

// -- constructors, destructors, and assignment operators ----------------------

template <class FlatBuffer>
arrow_table_slice<FlatBuffer>::arrow_table_slice(
  const FlatBuffer& slice, [[maybe_unused]] const chunk_ptr& parent,
  const std::shared_ptr<arrow::RecordBatch>& batch, type schema) noexcept
  : slice_{slice}, state_{} {
  if constexpr (std::is_same_v<FlatBuffer, fbs::table_slice::arrow::v2>) {
    // We decouple the sliced type from the schema intentionally. This is an
    // absolute must because we store the state in the deletion step of the
    // table slice's chunk, and storing a sliced chunk in there would cause a
    // cyclic reference. In the future, we should just not store the sliced
    // chunk at all, but rather create it on the fly only.
    if (batch) {
      state_.record_batch = batch;
      // Technically we could infer an outer buffer here as Arrow Buffer
      // instances remember which parent buffer they were sliced from, so we if
      // know that the schema, the dictionary, and then all columns in order
      // concatenated are exactly the parent-most buffer we could get back to
      // it. This is in practice not a bottleneck, as we only create from a
      // record batch directly if we do not have the IPC backing already, so we
      // chose not to implement it and always treat the IPC backing as not yet
      // created.
      state_.is_serialized = false;
    } else {
      auto decoder = record_batch_decoder{};
      state_.record_batch = decoder.decode(
        as_arrow_buffer(parent->slice(as_bytes(*slice.arrow_ipc()))));
      state_.is_serialized = true;
    }
    if (schema) {
      state_.schema = std::move(schema);
      TENZIR_ASSERT_EXPENSIVE(
        state_.schema == type::from_arrow(*state_.record_batch->schema()));
    } else {
      state_.schema = type::from_arrow(*state_.record_batch->schema());
    }
    TENZIR_ASSERT(caf::holds_alternative<record_type>(state_.schema));
    state_.flat_columns = index_column_arrays(state_.record_batch);
    TENZIR_ASSERT_EXPENSIVE(
      state_.flat_columns.size()
      == caf::get<record_type>(state_.schema).num_leaves());
  } else {
    static_assert(detail::always_false_v<FlatBuffer>, "unhandled arrow table "
                                                      "slice version");
  }
#if TENZIR_ENABLE_ASSERTIONS
  auto validate_status = state_.record_batch->Validate();
  TENZIR_ASSERT_EXPENSIVE(validate_status.ok(),
                          validate_status.ToString().c_str());
#endif // TENZIR_ENABLE_ASSERTIONS
}

template <class FlatBuffer>
arrow_table_slice<FlatBuffer>::~arrow_table_slice() noexcept {
  // nop
}

// -- properties -------------------------------------------------------------

template <class FlatBuffer>
const type& arrow_table_slice<FlatBuffer>::schema() const noexcept {
  return state_.schema;
}

template <class FlatBuffer>
table_slice::size_type arrow_table_slice<FlatBuffer>::rows() const noexcept {
  if (auto&& batch = record_batch())
    return batch->num_rows();
  return 0;
}

template <class FlatBuffer>
table_slice::size_type arrow_table_slice<FlatBuffer>::columns() const noexcept {
  if constexpr (std::is_same_v<FlatBuffer, fbs::table_slice::arrow::v2>) {
    if (auto&& batch = record_batch())
      return state_.flat_columns.size();
  } else {
    static_assert(detail::always_false_v<FlatBuffer>, "unhandled arrow table "
                                                      "slice version");
  }
  return 0;
}

template <class FlatBuffer>
bool arrow_table_slice<FlatBuffer>::is_serialized() const noexcept {
  if constexpr (std::is_same_v<FlatBuffer, fbs::table_slice::arrow::v2>) {
    return state_.is_serialized;
  } else {
    static_assert(detail::always_false_v<FlatBuffer>, "unhandled arrow table "
                                                      "slice version");
  }
}

// -- data access ------------------------------------------------------------

template <class FlatBuffer>
void arrow_table_slice<FlatBuffer>::append_column_to_index(
  id offset, table_slice::size_type column, value_index& index) const {
  if constexpr (std::is_same_v<FlatBuffer, fbs::table_slice::arrow::v2>) {
    if (auto&& batch = record_batch()) {
      auto&& array = state_.flat_columns[column];
      const auto& schema = caf::get<record_type>(this->schema());
      auto type = schema.field(schema.resolve_flat_index(column)).type;
      for (size_t row = 0; auto&& value : values(type, *array)) {
        if (!caf::holds_alternative<view<caf::none_t>>(value))
          index.append(value, offset + row);
        ++row;
      }
    }
  } else {
    static_assert(detail::always_false_v<FlatBuffer>, "unhandled arrow table "
                                                      "slice version");
  }
}

template <class FlatBuffer>
data_view
arrow_table_slice<FlatBuffer>::at(table_slice::size_type row,
                                  table_slice::size_type column) const {
  if constexpr (std::is_same_v<FlatBuffer, fbs::table_slice::arrow::v2>) {
    auto&& array = state_.flat_columns[column];
    const auto& schema = caf::get<record_type>(this->schema());
    auto offset = schema.resolve_flat_index(column);
    return value_at(schema.field(offset).type, *array, row);
  } else {
    static_assert(detail::always_false_v<FlatBuffer>, "unhandled arrow table "
                                                      "slice version");
  }
}

template <class FlatBuffer>
data_view arrow_table_slice<FlatBuffer>::at(table_slice::size_type row,
                                            table_slice::size_type column,
                                            const type& t) const {
  if constexpr (std::is_same_v<FlatBuffer, fbs::table_slice::arrow::v2>) {
    TENZIR_ASSERT_EXPENSIVE(congruent(
      caf::get<record_type>(this->schema())
        .field(caf::get<record_type>(this->schema()).resolve_flat_index(column))
        .type,
      t));
    auto&& array = state_.flat_columns[column];
    return value_at(t, *array, row);
  } else {
    static_assert(detail::always_false_v<FlatBuffer>, "unhandled arrow table "
                                                      "slice version");
  }
}

template <class FlatBuffer>
time arrow_table_slice<FlatBuffer>::import_time() const noexcept {
  if constexpr (std::is_same_v<FlatBuffer, fbs::table_slice::arrow::v2>) {
    return time{} + duration{slice_.import_time()};
  } else {
    static_assert(detail::always_false_v<FlatBuffer>, "unhandled table slice "
                                                      "encoding");
  }
}

template <class FlatBuffer>
void arrow_table_slice<FlatBuffer>::import_time(
  [[maybe_unused]] time import_time) noexcept {
  if constexpr (std::is_same_v<FlatBuffer, fbs::table_slice::arrow::v2>) {
    auto result = const_cast<FlatBuffer&>(slice_).mutate_import_time(
      import_time.time_since_epoch().count());
    TENZIR_ASSERT(result, "failed to mutate import time");
  } else {
    static_assert(detail::always_false_v<FlatBuffer>, "unhandled table slice "
                                                      "encoding");
  }
}

template <class FlatBuffer>
std::shared_ptr<arrow::RecordBatch>
arrow_table_slice<FlatBuffer>::record_batch() const noexcept {
  return state_.record_batch;
}

// -- utility functions -------------------------------------------------------

std::pair<type, std::shared_ptr<arrow::StructArray>>
transform_columns(type schema,
                  const std::shared_ptr<arrow::StructArray>& struct_array,
                  const std::vector<indexed_transformation>& transformations) {
  if (struct_array->num_fields() == 0) {
    return {schema, struct_array};
  }
  TENZIR_ASSERT_EXPENSIVE(std::is_sorted(transformations.begin(),
                                         transformations.end()),
                          "transformations must be sorted by index");
  TENZIR_ASSERT_EXPENSIVE(
    transformations.end()
      == std::adjacent_find(transformations.begin(), transformations.end(),
                            [](const auto& lhs, const auto& rhs) {
                              const auto [lhs_mismatch, rhs_mismatch]
                                = std::mismatch(lhs.index.begin(),
                                                lhs.index.end(),
                                                rhs.index.begin(),
                                                rhs.index.end());
                              return lhs_mismatch == lhs.index.end();
                            }),
    "transformation indices must not be a subset of the following "
    "transformation's index");
  // The current unpacked layer of the transformation, i.e., the pieces required
  // to re-assemble the current layer of both the record type and the record
  // batch.
  struct unpacked_layer {
    std::vector<struct record_type::field> fields;
    arrow::ArrayVector arrays;
  };
  const auto impl = [](const auto& impl, unpacked_layer layer, offset index,
                       auto& current, const auto sentinel) -> unpacked_layer {
    TENZIR_ASSERT(!index.empty());
    auto result = unpacked_layer{};
    // Iterate over the current layer. For every entry in the current layer, we
    // need to do one of three things:
    // 1. Apply the transformation if the index matches the transformation
    //    index.
    // 2. Recurse to the next layer if the index is a prefix of the
    //    transformation index.
    // 3. Leave the elements untouched.
    for (; index.back() < layer.fields.size(); ++index.back()) {
      const auto [is_prefix_match, is_exact_match]
        = [&]() -> std::pair<bool, bool> {
        if (current == sentinel)
          return {false, false};
        const auto [index_mismatch, current_index_mismatch]
          = std::mismatch(index.begin(), index.end(), current->index.begin(),
                          current->index.end());
        const auto is_prefix_match = index_mismatch == index.end();
        const auto is_exact_match
          = is_prefix_match && current_index_mismatch == current->index.end();
        return {is_prefix_match, is_exact_match};
      }();
      if (is_exact_match) {
        TENZIR_ASSERT(current != sentinel);
        for (auto&& [field, array] : std::invoke(
               std::move(current->fun), std::move(layer.fields[index.back()]),
               std::move(layer.arrays[index.back()]))) {
          result.fields.push_back(std::move(field));
          result.arrays.push_back(std::move(array));
        }
        ++current;
      } else if (is_prefix_match) {
        auto& nested_array
          = caf::get<arrow::StructArray>(*layer.arrays[index.back()]);
        auto nested_layer = unpacked_layer{
          .fields = {},
          .arrays = nested_array.Flatten().ValueOrDie(),
        };
        nested_layer.fields.reserve(nested_layer.arrays.size());
        for (auto&& [name, type] :
             caf::get<record_type>(layer.fields[index.back()].type).fields())
          nested_layer.fields.push_back({std::string{name}, type});
        auto nested_index = index;
        nested_index.push_back(0);
        nested_layer = impl(impl, std::move(nested_layer),
                            std::move(nested_index), current, sentinel);
        if (!nested_layer.fields.empty()) {
          auto nested_schema = type{record_type{nested_layer.fields}};
          nested_schema.assign_metadata(layer.fields[index.back()].type);
          result.fields.emplace_back(layer.fields[index.back()].name,
                                     nested_schema);
          auto nested_arrow_fields = arrow::FieldVector{};
          nested_arrow_fields.reserve(nested_layer.fields.size());
          for (const auto& nested_field : nested_layer.fields)
            nested_arrow_fields.push_back(
              nested_field.type.to_arrow_field(nested_field.name));
          result.arrays.push_back(
            make_struct_array(nested_array.length(), nested_array.null_bitmap(),
                              nested_arrow_fields, nested_layer.arrays));
        }
      } else {
        result.fields.push_back(std::move(layer.fields[index.back()]));
        result.arrays.push_back(std::move(layer.arrays[index.back()]));
      }
    }
    return result;
  };
  if (transformations.empty())
    return {schema, struct_array};
  auto current = transformations.begin();
  const auto sentinel = transformations.end();
  auto layer = unpacked_layer{
    .fields = {},
    .arrays = struct_array->Flatten().ValueOrDie(),
  };
  const auto num_columns
    = detail::narrow_cast<size_t>(struct_array->num_fields());
  layer.fields.reserve(num_columns);
  for (auto&& [name, type] : caf::get<record_type>(schema).fields())
    layer.fields.push_back({std::string{name}, type});
  // Run the possibly recursive implementation.
  layer = impl(impl, std::move(layer), {0}, current, sentinel);
  TENZIR_ASSERT(current == sentinel, "index out of bounds");
  // Re-assemble the record batch after the transformation.
  TENZIR_ASSERT(layer.fields.size() == layer.arrays.size());
  TENZIR_ASSERT_EXPENSIVE(
    std::ranges::all_of(layer.arrays, [](const auto& x) -> bool {
      return x != nullptr;
    }));
  auto new_schema = type{record_type{layer.fields}};
  new_schema.assign_metadata(schema);
  auto arrow_fields = arrow::FieldVector{};
  arrow_fields.reserve(layer.fields.size());
  for (const auto& field : layer.fields)
    arrow_fields.push_back(field.type.to_arrow_field(field.name));
  auto new_struct_array = std::shared_ptr<arrow::StructArray>{};
  // TODO: Does it make sense to add `struct_array->offset()` here?
  if (layer.arrays.empty()
      || struct_array->length() == layer.arrays[0]->length()) {
    new_struct_array = std::make_shared<arrow::StructArray>(
      std::make_shared<arrow::StructType>(arrow_fields), struct_array->length(),
      layer.arrays, struct_array->null_bitmap(), struct_array->null_count());
  } else {
    // FIXME: Callers should not rely on this hack. The signature of this
    // function does not really allow this, as it can change the behavior for
    // nulls.
    new_struct_array
      = arrow::StructArray::Make(layer.arrays, arrow_fields).ValueOrDie();
  }
#if TENZIR_ENABLE_ASSERTIONS
  auto validate_status = new_struct_array->Validate();
  TENZIR_ASSERT_EXPENSIVE(validate_status.ok(),
                          validate_status.ToString().c_str());
#endif // TENZIR_ENABLE_ASSERTIONS
  return {
    std::move(new_schema),
    std::move(new_struct_array),
  };
}

table_slice
transform_columns(const table_slice& slice,
                  const std::vector<indexed_transformation>& transformations) {
  if (slice.rows() == 0) {
    return {};
  }
  if (caf::get<record_type>(slice.schema()).num_fields() == 0) {
    return slice;
  }
  auto input_batch = to_record_batch(slice);
  auto input_struct_array = input_batch->ToStructArray().ValueOrDie();
  auto [output_schema, output_struct_array]
    = transform_columns(slice.schema(), input_struct_array, transformations);
  if (!output_schema)
    return {};
  auto output_batch = arrow::RecordBatch::Make(output_schema.to_arrow_schema(),
                                               output_struct_array->length(),
                                               output_struct_array->fields());
  auto result = table_slice{output_batch, std::move(output_schema)};
  result.offset(slice.offset());
  result.import_time(slice.import_time());
  return result;
}

std::pair<type, std::shared_ptr<arrow::RecordBatch>>
select_columns(type schema, const std::shared_ptr<arrow::RecordBatch>& batch,
               const std::vector<offset>& indices) {
  TENZIR_ASSERT_EXPENSIVE(batch->schema()->Equals(schema.to_arrow_schema()),
                          "Tenzir schema and Arrow schema must match");
  TENZIR_ASSERT_EXPENSIVE(std::is_sorted(indices.begin(), indices.end()),
                          "indices must be "
                          "sorted");
  TENZIR_ASSERT_EXPENSIVE(
    indices.end()
      == std::adjacent_find(indices.begin(), indices.end(),
                            [](const auto& lhs, const auto& rhs) {
                              const auto [lhs_mismatch, rhs_mismatch]
                                = std::mismatch(lhs.begin(), lhs.end(),
                                                rhs.begin(), rhs.end());
                              return lhs_mismatch == lhs.end();
                            }),
    "indices must not be a subset of the following index");
  // The current unpacked layer of the transformation, i.e., the pieces required
  // to re-assemble the current layer of both the record type and the record
  // batch.
  struct unpacked_layer {
    std::vector<struct record_type::field> fields;
    arrow::ArrayVector arrays;
  };
  const auto impl = [](const auto& impl, unpacked_layer layer, offset index,
                       auto& current, const auto sentinel) -> unpacked_layer {
    TENZIR_ASSERT(!index.empty());
    auto result = unpacked_layer{};
    // Iterate over the current layer, backwards. For every entry in the current
    // layer, we need to do one of two things:
    // 1. If the indices match, keep the entry unchanged.
    // 2. Recurse to the next layer if the current index is a prefix of the
    //    selected index.
    for (; index.back() < layer.fields.size(); ++index.back()) {
      const auto [is_prefix_match, is_exact_match]
        = [&]() -> std::pair<bool, bool> {
        if (current == sentinel)
          return {false, false};
        const auto [index_mismatch, current_index_mismatch] = std::mismatch(
          index.begin(), index.end(), current->begin(), current->end());
        const auto is_prefix_match = index_mismatch == index.end();
        const auto is_exact_match
          = is_prefix_match && current_index_mismatch == current->end();
        return {is_prefix_match, is_exact_match};
      }();
      if (is_exact_match) {
        TENZIR_ASSERT(current != sentinel);
        result.fields.push_back(std::move(layer.fields[index.back()]));
        result.arrays.push_back(std::move(layer.arrays[index.back()]));
        ++current;
      } else if (is_prefix_match) {
        auto& nested_array
          = caf::get<arrow::StructArray>(*layer.arrays[index.back()]);
        auto nested_layer = unpacked_layer{
          .fields = {},
          .arrays = nested_array.Flatten().ValueOrDie(),
        };
        nested_layer.fields.reserve(nested_layer.arrays.size());
        for (auto&& [name, type] :
             caf::get<record_type>(layer.fields[index.back()].type).fields())
          nested_layer.fields.push_back({std::string{name}, type});
        auto nested_index = index;
        nested_index.push_back(0);
        nested_layer = impl(impl, std::move(nested_layer),
                            std::move(nested_index), current, sentinel);
        auto nested_schema = type{record_type{nested_layer.fields}};
        nested_schema.assign_metadata(layer.fields[index.back()].type);
        result.fields.emplace_back(layer.fields[index.back()].name,
                                   nested_schema);
        auto nested_arrow_fields = arrow::FieldVector{};
        nested_arrow_fields.reserve(nested_layer.fields.size());
        for (const auto& nested_field : nested_layer.fields)
          nested_arrow_fields.push_back(
            nested_field.type.to_arrow_field(nested_field.name));
        result.arrays.push_back(
          make_struct_array(nested_array.length(), nested_array.null_bitmap(),
                            nested_arrow_fields, nested_layer.arrays));
      }
    }
    return result;
  };
  if (indices.empty())
    return {};
  auto current = indices.begin();
  const auto sentinel = indices.end();
  auto layer = unpacked_layer{
    .fields = {},
    .arrays = batch->columns(),
  };
  const auto num_columns = detail::narrow_cast<size_t>(batch->num_columns());
  layer.fields.reserve(num_columns);
  for (auto&& [name, type] : caf::get<record_type>(schema).fields())
    layer.fields.push_back({std::string{name}, type});
  // Run the possibly recursive implementation, starting at the last field.
  layer = impl(impl, std::move(layer), {0}, current, sentinel);
  TENZIR_ASSERT(current == sentinel, "index out of bounds");
  // Re-assemble the record batch after the transformation.
  TENZIR_ASSERT(layer.fields.size() == layer.arrays.size());
  if (layer.fields.empty())
    return {};
  auto new_schema = type{record_type{layer.fields}};
  new_schema.assign_metadata(schema);
  auto arrow_schema = new_schema.to_arrow_schema();
  const auto num_rows = layer.arrays[0]->length();
  return {
    std::move(new_schema),
    arrow::RecordBatch::Make(std::move(arrow_schema), num_rows,
                             std::move(layer.arrays)),
  };
}

table_slice
select_columns(const table_slice& slice, const std::vector<offset>& indices) {
  auto [schema, batch]
    = select_columns(slice.schema(), to_record_batch(slice), indices);
  if (!schema)
    return {};
  auto result = table_slice{batch, std::move(schema)};
  result.offset(slice.offset());
  result.import_time(slice.import_time());
  return result;
}

auto make_struct_array(int64_t length,
                       std::shared_ptr<arrow::Buffer> null_bitmap,
                       const arrow::FieldVector& field_types,
                       const arrow::ArrayVector& field_arrays)
  -> std::shared_ptr<arrow::StructArray> {
  auto type = std::make_shared<arrow::StructType>(field_types);
  return std::make_shared<arrow::StructArray>(
    std::move(type), length, field_arrays, std::move(null_bitmap));
}

auto make_struct_array(int64_t length,
                       std::shared_ptr<arrow::Buffer> null_bitmap,
                       std::vector<std::string> field_names,
                       const arrow::ArrayVector& field_arrays)
  -> std::shared_ptr<arrow::StructArray> {
  auto field_types = arrow::FieldVector{};
  for (auto [name, array] : detail::zip(field_names, field_arrays)) {
    field_types.push_back(
      std::make_shared<arrow::Field>(std::move(name), array->type()));
  }
  return make_struct_array(length, std::move(null_bitmap), field_types,
                           field_arrays);
}

auto make_struct_array(
  int64_t length, std::shared_ptr<arrow::Buffer> null_bitmap,
  std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>> fields)
  -> std::shared_ptr<arrow::StructArray> {
  auto field_types = arrow::FieldVector{};
  field_types.reserve(fields.size());
  auto field_arrays = std::vector<std::shared_ptr<arrow::Array>>{};
  field_arrays.reserve(fields.size());
  for (auto& field : fields) {
    field_types.push_back(
      std::make_shared<arrow::Field>(field.first, field.second->type()));
    field_arrays.push_back(std::move(field.second));
  }
  return make_struct_array(length, std::move(null_bitmap), field_types,
                           field_arrays);
}

// -- template machinery -------------------------------------------------------

/// Explicit template instantiations for all Arrow encoding versions.
template class arrow_table_slice<fbs::table_slice::arrow::v2>;

} // namespace tenzir
