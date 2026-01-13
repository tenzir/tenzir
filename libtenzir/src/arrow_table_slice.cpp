//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_table_slice.hpp"

#include "tenzir/arrow_utils.hpp"
#include "tenzir/collect.hpp"
#include "tenzir/config.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/error.hpp"
#include "tenzir/fbs/table_slice.hpp"
#include "tenzir/fbs/utils.hpp"
#include "tenzir/logger.hpp"

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/status.h>

#include <memory>
#include <ranges>
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
                 }),
               arrow_ipc_read_options()} {
    // nop
  }

  std::shared_ptr<arrow::RecordBatch>
  decode(const std::shared_ptr<arrow::Buffer>& flat_record_batch) noexcept {
    TENZIR_ASSERT(! record_batch_);
    if (auto status = decoder_.Consume(flat_record_batch); ! status.ok()) {
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
  return match(
    *arr,
    [&](const auto&) {
      out.push_back(arr);
    },
    [&](const arrow::StructArray& s) {
      for (const auto& child : s.fields()) {
        index_column_arrays(child, out);
      }
    });
}

arrow::ArrayVector
index_column_arrays(const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  arrow::ArrayVector result{};
  for (const auto& arr : record_batch->columns()) {
    index_column_arrays(arr, result);
  }
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
#if TENZIR_ENABLE_ASSERTIONS
      auto from_arrow = type::from_arrow(*state_.record_batch->schema());
      TENZIR_ASSERT_EXPENSIVE(state_.schema == from_arrow, "{} == {}",
                              state_.schema, from_arrow);
#endif
    } else {
      state_.schema = type::from_arrow(*state_.record_batch->schema());
    }
    TENZIR_ASSERT(is<record_type>(state_.schema));
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
  if (auto&& batch = record_batch()) {
    return batch->num_rows();
  }
  return 0;
}

template <class FlatBuffer>
table_slice::size_type arrow_table_slice<FlatBuffer>::columns() const noexcept {
  if constexpr (std::is_same_v<FlatBuffer, fbs::table_slice::arrow::v2>) {
    if (auto&& batch = record_batch()) {
      return state_.get_flat_columns().size();
    }
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
data_view
arrow_table_slice<FlatBuffer>::at(table_slice::size_type row,
                                  table_slice::size_type column) const {
  if constexpr (std::is_same_v<FlatBuffer, fbs::table_slice::arrow::v2>) {
    auto&& array = state_.get_flat_columns()[column];
    const auto& schema = as<record_type>(this->schema());
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
      as<record_type>(this->schema())
        .field(as<record_type>(this->schema()).resolve_flat_index(column))
        .type,
      t));
    auto&& array = state_.get_flat_columns()[column];
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
                  std::vector<indexed_transformation> transformations) {
  if (struct_array->num_fields() == 0 or transformations.empty()) {
    return {schema, struct_array};
  }
  std::ranges::sort(transformations);
  TENZIR_ASSERT(transformations.end()
                  == std::ranges::adjacent_find(
                    transformations,
                    [](const auto& lhs, const auto& rhs) {
                      const auto [lhs_mismatch, rhs_mismatch]
                        = std::mismatch(lhs.index.begin(), lhs.index.end(),
                                        rhs.index.begin(), rhs.index.end());
                      return lhs_mismatch == lhs.index.end();
                    }),
                "transformation indices must not be a subset of the following "
                "transformation's index");
  if (transformations.front().index.empty()) {
    TENZIR_ASSERT(transformations.size() == 1);
    auto result = transformations.front().fun({{}, schema}, struct_array);
    TENZIR_ASSERT(result.size() == 1);
    TENZIR_ASSERT(result.front().first.name.empty());
    TENZIR_ASSERT(is<record_type>(result.front().first.type));
    TENZIR_ASSERT(is<arrow::StructArray>(*result.front().second));
    return {
      std::move(result.front().first.type),
      std::static_pointer_cast<arrow::StructArray>(
        std::move(result.front().second)),
    };
  }
  // The current unpacked layer of the transformation, i.e., the pieces required
  // to re-assemble the current layer of both the record type and the record
  // batch.
  struct unpacked_layer {
    std::vector<struct record_type::field> fields;
    arrow::ArrayVector arrays;
  };
  const auto impl = [](const auto& impl, unpacked_layer layer, offset index,
                       auto& current, const auto sentinel) -> unpacked_layer {
    TENZIR_ASSERT(! index.empty());
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
        if (current == sentinel) {
          return {false, false};
        }
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
          = as<arrow::StructArray>(*layer.arrays[index.back()]);
        auto nested_layer = unpacked_layer{
          .fields = {},
          .arrays = check(nested_array.Flatten(tenzir::arrow_memory_pool())),
        };
        nested_layer.fields.reserve(nested_layer.arrays.size());
        for (auto&& [name, type] :
             as<record_type>(layer.fields[index.back()].type).fields()) {
          nested_layer.fields.push_back({std::string{name}, type});
        }
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
        for (const auto& nested_field : nested_layer.fields) {
          nested_arrow_fields.push_back(
            nested_field.type.to_arrow_field(nested_field.name));
        }
        result.arrays.push_back(
          make_struct_array(nested_array.length(), nested_array.null_bitmap(),
                            nested_arrow_fields, nested_layer.arrays));
      } else {
        result.fields.push_back(std::move(layer.fields[index.back()]));
        result.arrays.push_back(std::move(layer.arrays[index.back()]));
      }
    }
    return result;
  };
  if (transformations.empty()) {
    return {schema, struct_array};
  }
  auto current = transformations.begin();
  const auto sentinel = transformations.end();
  auto layer = unpacked_layer{
    .fields = {},
    .arrays = check(struct_array->Flatten(tenzir::arrow_memory_pool())),
  };
  const auto num_columns
    = detail::narrow_cast<size_t>(struct_array->num_fields());
  layer.fields.reserve(num_columns);
  for (auto&& [name, type] : as<record_type>(schema).fields()) {
    layer.fields.push_back({std::string{name}, type});
  }
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
  for (const auto& field : layer.fields) {
    arrow_fields.push_back(field.type.to_arrow_field(field.name));
  }
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
      = check(arrow::StructArray::Make(layer.arrays, arrow_fields));
  }
#if TENZIR_ENABLE_ASSERTIONS
  auto validate_status = new_struct_array->Validate();
  TENZIR_ASSERT_EXPENSIVE(validate_status.ok(), validate_status.ToString());
#endif // TENZIR_ENABLE_ASSERTIONS
  return {
    std::move(new_schema),
    std::move(new_struct_array),
  };
}

table_slice
transform_columns(const table_slice& slice,
                  std::vector<indexed_transformation> transformations) {
  if (transformations.empty()) {
    return slice;
  }
  if (slice.rows() == 0) {
    return {};
  }
  if (as<record_type>(slice.schema()).num_fields() == 0) {
    return slice;
  }
  auto input_batch = to_record_batch(slice);
  auto input_struct_array = check(input_batch->ToStructArray());
  auto [output_schema, output_struct_array] = transform_columns(
    slice.schema(), input_struct_array, std::move(transformations));
  if (! output_schema) {
    return {};
  }
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
               std::vector<offset> indices) {
  TENZIR_ASSERT_EXPENSIVE(batch->schema()->Equals(schema.to_arrow_schema()),
                          "Tenzir schema and Arrow schema must match");
  std::ranges::sort(indices);
  TENZIR_ASSERT(
    indices.end()
      == std::ranges::adjacent_find(indices,
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
    TENZIR_ASSERT(! index.empty());
    auto result = unpacked_layer{};
    // Iterate over the current layer, backwards. For every entry in the current
    // layer, we need to do one of two things:
    // 1. If the indices match, keep the entry unchanged.
    // 2. Recurse to the next layer if the current index is a prefix of the
    //    selected index.
    for (; index.back() < layer.fields.size(); ++index.back()) {
      const auto [is_prefix_match, is_exact_match]
        = [&]() -> std::pair<bool, bool> {
        if (current == sentinel) {
          return {false, false};
        }
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
          = as<arrow::StructArray>(*layer.arrays[index.back()]);
        auto nested_layer = unpacked_layer{
          .fields = {},
          .arrays = check(nested_array.Flatten(tenzir::arrow_memory_pool())),
        };
        nested_layer.fields.reserve(nested_layer.arrays.size());
        for (auto&& [name, type] :
             as<record_type>(layer.fields[index.back()].type).fields()) {
          nested_layer.fields.push_back({std::string{name}, type});
        }
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
        for (const auto& nested_field : nested_layer.fields) {
          nested_arrow_fields.push_back(
            nested_field.type.to_arrow_field(nested_field.name));
        }
        result.arrays.push_back(
          make_struct_array(nested_array.length(), nested_array.null_bitmap(),
                            nested_arrow_fields, nested_layer.arrays));
      }
    }
    return result;
  };
  if (indices.empty()) {
    return {};
  }
  auto current = indices.begin();
  const auto sentinel = indices.end();
  auto layer = unpacked_layer{
    .fields = {},
    .arrays = batch->columns(),
  };
  const auto num_columns = detail::narrow_cast<size_t>(batch->num_columns());
  layer.fields.reserve(num_columns);
  for (auto&& [name, type] : as<record_type>(schema).fields()) {
    layer.fields.push_back({std::string{name}, type});
  }
  // Run the possibly recursive implementation, starting at the last field.
  layer = impl(impl, std::move(layer), {0}, current, sentinel);
  TENZIR_ASSERT(current == sentinel, "index out of bounds");
  // Re-assemble the record batch after the transformation.
  TENZIR_ASSERT(layer.fields.size() == layer.arrays.size());
  if (layer.fields.empty()) {
    return {};
  }
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
select_columns(const table_slice& slice, std::vector<offset> indices) {
  auto [schema, batch] = select_columns(slice.schema(), to_record_batch(slice),
                                        std::move(indices));
  if (! schema) {
    return {};
  }
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
                       const arrow::ArrayVector& field_arrays,
                       const record_type& rt)
  -> std::shared_ptr<arrow::StructArray> {
  auto field_types = arrow::FieldVector{};
  const auto rt_fields = collect(rt.fields());
  for (const auto& [name, array, rt_field] :
       std::views::zip(field_names, field_arrays, rt_fields)) {
    field_types.push_back(
      std::make_shared<arrow::Field>(std::move(name), array->type(), true,
                                     rt_field.type.make_arrow_metadata()));
  }
  return make_struct_array(length, std::move(null_bitmap), field_types,
                           field_arrays);
}

auto make_struct_array(
  int64_t length, std::shared_ptr<arrow::Buffer> null_bitmap,
  std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>> fields,
  const record_type& rt) -> std::shared_ptr<arrow::StructArray> {
  auto field_types = arrow::FieldVector{};
  field_types.reserve(fields.size());
  auto field_arrays = std::vector<std::shared_ptr<arrow::Array>>{};
  field_arrays.reserve(fields.size());
  auto rt_fields = collect(rt.fields());
  for (const auto& [field, rt_field] : std::views::zip(fields, rt_fields)) {
    field_types.push_back(
      std::make_shared<arrow::Field>(field.first, field.second->type(), true,
                                     rt_field.type.make_arrow_metadata()));
    field_arrays.push_back(std::move(field.second));
  }
  return make_struct_array(length, std::move(null_bitmap), field_types,
                           field_arrays);
}

namespace {

namespace arrow_ext {

using namespace arrow;

// This is copied and adapted from Arrow.
struct GetByteRangesArray {
  GetByteRangesArray(const ArrayData& input, int64_t offset, int64_t length)
    : input{input}, offset{offset}, length{length} {
  }

  const ArrayData& input;
  int64_t offset;
  int64_t length;

  uint64_t total_length = 0;

  Status VisitBitmap(const std::shared_ptr<Buffer>& buffer) {
    if (buffer) {
      total_length += bit_util::CoveringBytes(offset, length);
    }
    return Status::OK();
  }

  Status
  VisitFixedWidthArray(const Buffer& buffer, const FixedWidthType& type) {
    (void)buffer;
    uint64_t offset_bits = offset * type.bit_width();
    uint64_t offset_bytes
      = bit_util::RoundDown(static_cast<int64_t>(offset_bits), 8) / 8;
    uint64_t end_byte
      = bit_util::RoundUp(
          static_cast<int64_t>(offset_bits + (length * type.bit_width())), 8)
        / 8;
    uint64_t length_bytes = (end_byte - offset_bytes);
    total_length += length_bytes;
    return Status::OK();
  }

  Status Visit(const FixedWidthType& type) {
    static_assert(sizeof(uint8_t*) <= sizeof(uint64_t),
                  "Undefined behavior if pointer larger than uint64_t");
    RETURN_NOT_OK(VisitBitmap(input.buffers[0]));
    RETURN_NOT_OK(VisitFixedWidthArray(*input.buffers[1], type));
    if (input.dictionary) {
      // This is slightly imprecise because we always assume the entire
      // dictionary is referenced.  If this array has an offset it may only be
      // referencing a portion of the dictionary
      GetByteRangesArray dict_visitor{
        *input.dictionary,
        input.dictionary->offset,
        input.dictionary->length,
      };
      RETURN_NOT_OK(VisitTypeInline(*input.dictionary->type, &dict_visitor));
      total_length += dict_visitor.total_length;
    }
    return Status::OK();
  }

  Status Visit(const NullType& type) const {
    (void)type;
    return Status::OK();
  }

  template <typename BaseBinaryType>
  Status VisitBaseBinary(const BaseBinaryType& type) {
    (void)type;
    using offset_type = typename BaseBinaryType::offset_type;
    RETURN_NOT_OK(VisitBitmap(input.buffers[0]));

    total_length += sizeof(offset_type) * length;

    const offset_type* offsets = input.GetValues<offset_type>(1, offset);
    offset_type start = offsets[0];
    offset_type end = offsets[length];
    total_length += static_cast<uint64_t>(end - start);
    return Status::OK();
  }

  Status Visit(const BinaryType& type) {
    return VisitBaseBinary(type);
  }

  Status Visit(const LargeBinaryType& type) {
    return VisitBaseBinary(type);
  }

  template <typename BaseListType>
  Status VisitBaseList(const BaseListType& type) {
    using offset_type = typename BaseListType::offset_type;
    RETURN_NOT_OK(VisitBitmap(input.buffers[0]));

    total_length += sizeof(offset_type) * length;

    const offset_type* offsets = input.GetValues<offset_type>(1, offset);
    int64_t start = static_cast<int64_t>(offsets[0]);
    int64_t end = static_cast<int64_t>(offsets[length]);
    GetByteRangesArray child{
      *input.child_data[0],
      start,
      end - start,
    };
    RETURN_NOT_OK(VisitTypeInline(*type.value_type(), &child));
    total_length += child.total_length;
    return Status::OK();
  }

  Status Visit(const ListType& type) {
    return VisitBaseList(type);
  }

  Status Visit(const LargeListType& type) {
    return VisitBaseList(type);
  }

  Status Visit(const FixedSizeListType& type) {
    RETURN_NOT_OK(VisitBitmap(input.buffers[0]));
    GetByteRangesArray child{
      *input.child_data[0],
      offset * type.list_size(),
      length * type.list_size(),
    };
    RETURN_NOT_OK(VisitTypeInline(*type.value_type(), &child));
    total_length += child.total_length;
    return Status::OK();
  }

  Status Visit(const StructType& type) {
    for (int i = 0; i < type.num_fields(); i++) {
      GetByteRangesArray child{
        *input.child_data[i],
        offset + input.child_data[i]->offset,
        length,
      };
      RETURN_NOT_OK(VisitTypeInline(*type.field(i)->type(), &child));
      total_length += child.total_length;
    }
    return Status::OK();
  }

  Status Visit(const DenseUnionType& type) {
    // Skip validity map for DenseUnionType
    // Types buffer is always int8
    RETURN_NOT_OK(VisitFixedWidthArray(
      *input.buffers[1], *std::dynamic_pointer_cast<FixedWidthType>(int8())));
    // Offsets buffer is always int32
    RETURN_NOT_OK(VisitFixedWidthArray(
      *input.buffers[2], *std::dynamic_pointer_cast<FixedWidthType>(int32())));

    // We have to loop through the types buffer to figure out the correct
    // offset / length being referenced in the child arrays
    std::vector<int64_t> lengths_per_type(type.type_codes().size());
    std::vector<int64_t> offsets_per_type(type.type_codes().size());
    const int8_t* type_codes = input.GetValues<int8_t>(1, 0);
    for (const int8_t* it = type_codes; it != type_codes + offset; it++) {
      TENZIR_ASSERT(type.child_ids()[static_cast<std::size_t>(*it)]
                    != UnionType::kInvalidChildId);
      offsets_per_type[type.child_ids()[static_cast<std::size_t>(*it)]]++;
    }
    for (const int8_t* it = type_codes + offset;
         it != type_codes + offset + length; it++) {
      TENZIR_ASSERT(type.child_ids()[static_cast<std::size_t>(*it)]
                    != UnionType::kInvalidChildId);
      lengths_per_type[type.child_ids()[static_cast<std::size_t>(*it)]]++;
    }

    for (int i = 0; i < type.num_fields(); i++) {
      GetByteRangesArray child{
        *input.child_data[i], offsets_per_type[i] + input.child_data[i]->offset,
        lengths_per_type[i]};
      RETURN_NOT_OK(VisitTypeInline(*type.field(i)->type(), &child));
      total_length += child.total_length;
    }

    return Status::OK();
  }

  Status Visit(const SparseUnionType& type) {
    // Skip validity map for SparseUnionType
    // Types buffer is always int8
    RETURN_NOT_OK(VisitFixedWidthArray(
      *input.buffers[1], *std::dynamic_pointer_cast<FixedWidthType>(int8())));

    for (int i = 0; i < type.num_fields(); i++) {
      GetByteRangesArray child{
        *input.child_data[i],
        offset + input.child_data[i]->offset,
        length,
      };
      RETURN_NOT_OK(VisitTypeInline(*type.field(i)->type(), &child));
      total_length += child.total_length;
    }

    return Status::OK();
  }

  Status Visit(const RunEndEncodedType& type) const {
    (void)type;
    TENZIR_UNREACHABLE();
  }

  Status Visit(const ExtensionType& extension_type) {
    GetByteRangesArray storage{
      input,
      offset,
      length,
    };
    RETURN_NOT_OK(VisitTypeInline(*extension_type.storage_type(), &storage));
    total_length += storage.total_length;
    return Status::OK();
  }

  Status Visit(const DataType& type) const {
    return Status::TypeError("Extracting byte ranges not supported for type ",
                             type.ToString());
  }

  static std::shared_ptr<DataType> RangesType() {
    return struct_({field("start", uint64()), field("offset", uint64()),
                    field("length", uint64())});
  }

  static Result<uint64_t> Exec(const ArrayData& input) {
    GetByteRangesArray self{
      input,
      input.offset,
      input.length,
    };
    RETURN_NOT_OK(VisitTypeInline(*input.type, &self));
    return self.total_length;
  }
};

} // namespace arrow_ext

} // namespace

template <class Flatbuffer>
auto arrow_table_slice<Flatbuffer>::approx_bytes() const -> uint64_t {
  if (state_.approx_bytes_ == std::numeric_limits<uint64_t>::max()) {
    state_.approx_bytes_ = std::invoke([&]() -> uint64_t {
      if (state_.record_batch->num_rows() == 0) {
        return 0;
      }
      auto total_size = uint64_t{0};
      for (auto& array : state_.record_batch->column_data()) {
        total_size += check(arrow_ext::GetByteRangesArray::Exec(*array));
      }
      return total_size;
    });
  }
  return state_.approx_bytes_;
}

// -- template machinery -------------------------------------------------------

/// Explicit template instantiations for all Arrow encoding versions.
template class arrow_table_slice<fbs::table_slice::arrow::v2>;

auto arrow_table_slice_state<fbs::table_slice::arrow::v2>::get_flat_columns()
  const -> const arrow::ArrayVector& {
  auto guard = std::unique_lock{flat_columns_mutex};
  if (not flat_columns) {
    flat_columns = index_column_arrays(record_batch);
    TENZIR_ASSERT_EXPENSIVE(flat_columns->size()
                            == as<record_type>(schema).num_leaves());
  }
  return *flat_columns;
}

} // namespace tenzir
