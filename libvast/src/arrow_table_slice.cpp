//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/arrow_table_slice.hpp"

#include "vast/arrow_table_slice_builder.hpp"
#include "vast/config.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/detail/overload.hpp"
#include "vast/detail/passthrough.hpp"
#include "vast/die.hpp"
#include "vast/error.hpp"
#include "vast/fbs/table_slice.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/legacy_type.hpp"
#include "vast/logger.hpp"
#include "vast/value_index.hpp"

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/status.h>

#include <type_traits>
#include <utility>

namespace vast {

auto values(const type& type,
            const std::same_as<arrow::Array> auto& array) noexcept
  -> detail::generator<data_view> {
  const auto f = []<concrete_type Type>(
                   const Type& type,
                   const arrow::Array& array) -> detail::generator<data_view> {
    for (auto&& result :
         values(type, caf::get<type_to_arrow_array_t<Type>>(array))) {
      if (!result)
        co_yield {};
      else
        co_yield std::move(*result);
    }
  };
  return caf::visit(f, type, detail::passthrough(array));
}

template auto values(const type& type, const arrow::Array& array) noexcept
  -> detail::generator<data_view>;

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
    VAST_ASSERT(!record_batch_);
    if (auto status = decoder_.Consume(flat_record_batch); !status.ok()) {
      VAST_ERROR("{} failed to decode Arrow Record Batch: {}", __func__,
                 status.ToString());
      return {};
    }
    VAST_ASSERT(record_batch_);
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
    // We decouple the sliced type from the layout intentionally. This is an
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
      state_.layout = std::move(schema);
      VAST_ASSERT(state_.layout
                  == type::from_arrow(*state_.record_batch->schema()));
    } else {
      state_.layout = type::from_arrow(*state_.record_batch->schema());
    }
    VAST_ASSERT(caf::holds_alternative<record_type>(state_.layout));
    state_.flat_columns = index_column_arrays(state_.record_batch);
    VAST_ASSERT(state_.flat_columns.size()
                == caf::get<record_type>(state_.layout).num_leaves());
  } else {
    static_assert(detail::always_false_v<FlatBuffer>, "unhandled arrow table "
                                                      "slice version");
  }
#if VAST_ENABLE_ASSERTIONS
  auto validate_status = state_.record_batch->Validate();
  VAST_ASSERT(validate_status.ok(), validate_status.ToString().c_str());
#endif // VAST_ENABLE_ASSERTIONS
}

template <class FlatBuffer>
arrow_table_slice<FlatBuffer>::~arrow_table_slice() noexcept {
  // nop
}

// -- properties -------------------------------------------------------------

template <class FlatBuffer>
const type& arrow_table_slice<FlatBuffer>::layout() const noexcept {
  return state_.layout;
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
      const auto& layout = caf::get<record_type>(this->layout());
      auto type = layout.field(layout.resolve_flat_index(column)).type;
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
    const auto& layout = caf::get<record_type>(this->layout());
    auto offset = layout.resolve_flat_index(column);
    return value_at(layout.field(offset).type, *array, row);
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
    VAST_ASSERT(congruent(
      caf::get<record_type>(this->layout())
        .field(caf::get<record_type>(this->layout()).resolve_flat_index(column))
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
    VAST_ASSERT(result, "failed to mutate import time");
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

std::pair<type, std::shared_ptr<arrow::RecordBatch>> transform_columns(
  type layout, const std::shared_ptr<arrow::RecordBatch>& batch,
  const std::vector<indexed_transformation>& transformations) noexcept {
  VAST_ASSERT(batch->schema()->Equals(layout.to_arrow_schema()),
              "VAST layout and Arrow schema must match");
  VAST_ASSERT(std::is_sorted(transformations.begin(), transformations.end()),
              "transformations must be sorted by index");
  VAST_ASSERT(transformations.end()
                == std::adjacent_find(
                  transformations.begin(), transformations.end(),
                  [](const auto& lhs, const auto& rhs) noexcept {
                    const auto [lhs_mismatch, rhs_mismatch]
                      = std::mismatch(lhs.index.begin(), lhs.index.end(),
                                      rhs.index.begin(), rhs.index.end());
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
  const auto impl
    = [](const auto& impl, unpacked_layer layer, offset index, auto& current,
         const auto sentinel) noexcept -> unpacked_layer {
    VAST_ASSERT(!index.empty());
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
        = [&]() noexcept -> std::pair<bool, bool> {
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
        VAST_ASSERT(current != sentinel);
        for (auto&& [field, array] : std::invoke(
               std::move(current->fun), std::move(layer.fields[index.back()]),
               std::move(layer.arrays[index.back()]))) {
          result.fields.push_back(std::move(field));
          result.arrays.push_back(std::move(array));
        }
        ++current;
      } else if (is_prefix_match) {
        auto nested_layer = unpacked_layer{
          .fields = {},
          .arrays = caf::get<type_to_arrow_array_t<record_type>>(
                      *layer.arrays[index.back()])
                      .fields(),
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
          auto nested_layout = type{record_type{nested_layer.fields}};
          nested_layout.assign_metadata(layer.fields[index.back()].type);
          result.fields.emplace_back(layer.fields[index.back()].name,
                                     nested_layout);
          auto nested_arrow_fields = arrow::FieldVector{};
          nested_arrow_fields.reserve(nested_layer.fields.size());
          for (const auto& nested_field : nested_layer.fields)
            nested_arrow_fields.push_back(
              nested_field.type.to_arrow_field(nested_field.name));
          result.arrays.push_back(
            arrow::StructArray::Make(nested_layer.arrays, nested_arrow_fields)
              .ValueOrDie());
        }
      } else {
        result.fields.push_back(std::move(layer.fields[index.back()]));
        result.arrays.push_back(std::move(layer.arrays[index.back()]));
      }
    }
    return result;
  };
  if (transformations.empty())
    return {layout, batch};
  auto current = transformations.begin();
  const auto sentinel = transformations.end();
  auto layer = unpacked_layer{
    .fields = {},
    .arrays = batch->columns(),
  };
  const auto num_columns = detail::narrow_cast<size_t>(batch->num_columns());
  layer.fields.reserve(num_columns);
  for (auto&& [name, type] : caf::get<record_type>(layout).fields())
    layer.fields.push_back({std::string{name}, type});
  // Run the possibly recursive implementation.
  layer = impl(impl, std::move(layer), {0}, current, sentinel);
  VAST_ASSERT(current == sentinel, "index out of bounds");
  // Re-assemble the record batch after the transformation.
  VAST_ASSERT(layer.fields.size() == layer.arrays.size());
  if (layer.fields.empty())
    return {};
  auto new_layout = type{record_type{layer.fields}};
  new_layout.assign_metadata(layout);
  auto arrow_schema = new_layout.to_arrow_schema();
  const auto num_rows = layer.arrays[0]->length();
  return {
    std::move(new_layout),
    arrow::RecordBatch::Make(std::move(arrow_schema), num_rows,
                             std::move(layer.arrays)),
  };
}

std::pair<type, std::shared_ptr<arrow::RecordBatch>>
select_columns(type layout, const std::shared_ptr<arrow::RecordBatch>& batch,
               const std::vector<offset>& indices) noexcept {
  VAST_ASSERT(batch->schema()->Equals(layout.to_arrow_schema()),
              "VAST layout and Arrow schema must match");
  VAST_ASSERT(std::is_sorted(indices.begin(), indices.end()), "indices must be "
                                                              "sorted");
  VAST_ASSERT(
    indices.end()
      == std::adjacent_find(indices.begin(), indices.end(),
                            [](const auto& lhs, const auto& rhs) noexcept {
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
  const auto impl
    = [](const auto& impl, unpacked_layer layer, offset index, auto& current,
         const auto sentinel) noexcept -> unpacked_layer {
    VAST_ASSERT(!index.empty());
    auto result = unpacked_layer{};
    // Iterate over the current layer, backwards. For every entry in the current
    // layer, we need to do one of two things:
    // 1. If the indices match, keep the entry unchanged.
    // 2. Recurse to the next layer if the current index is a prefix of the
    //    selected index.
    for (; index.back() < layer.fields.size(); ++index.back()) {
      const auto [is_prefix_match, is_exact_match]
        = [&]() noexcept -> std::pair<bool, bool> {
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
        VAST_ASSERT(current != sentinel);
        result.fields.push_back(std::move(layer.fields[index.back()]));
        result.arrays.push_back(std::move(layer.arrays[index.back()]));
        ++current;
      } else if (is_prefix_match) {
        auto nested_layer = unpacked_layer{
          .fields = {},
          .arrays = caf::get<type_to_arrow_array_t<record_type>>(
                      *layer.arrays[index.back()])
                      .fields(),
        };
        nested_layer.fields.reserve(nested_layer.arrays.size());
        for (auto&& [name, type] :
             caf::get<record_type>(layer.fields[index.back()].type).fields())
          nested_layer.fields.push_back({std::string{name}, type});
        auto nested_index = index;
        nested_index.push_back(0);
        nested_layer = impl(impl, std::move(nested_layer),
                            std::move(nested_index), current, sentinel);
        auto nested_layout = type{record_type{nested_layer.fields}};
        nested_layout.assign_metadata(layer.fields[index.back()].type);
        result.fields.emplace_back(layer.fields[index.back()].name,
                                   nested_layout);
        auto nested_arrow_fields = arrow::FieldVector{};
        nested_arrow_fields.reserve(nested_layer.fields.size());
        for (const auto& nested_field : nested_layer.fields)
          nested_arrow_fields.push_back(
            nested_field.type.to_arrow_field(nested_field.name));
        result.arrays.push_back(
          arrow::StructArray::Make(nested_layer.arrays, nested_arrow_fields)
            .ValueOrDie());
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
  for (auto&& [name, type] : caf::get<record_type>(layout).fields())
    layer.fields.push_back({std::string{name}, type});
  // Run the possibly recursive implementation, starting at the last field.
  layer = impl(impl, std::move(layer), {0}, current, sentinel);
  VAST_ASSERT(current == sentinel, "index out of bounds");
  // Re-assemble the record batch after the transformation.
  VAST_ASSERT(layer.fields.size() == layer.arrays.size());
  if (layer.fields.empty())
    return {};
  auto new_layout = type{record_type{layer.fields}};
  new_layout.assign_metadata(layout);
  auto arrow_schema = new_layout.to_arrow_schema();
  const auto num_rows = layer.arrays[0]->length();
  return {
    std::move(new_layout),
    arrow::RecordBatch::Make(std::move(arrow_schema), num_rows,
                             std::move(layer.arrays)),
  };
}

// -- template machinery -------------------------------------------------------

/// Explicit template instantiations for all Arrow encoding versions.
template class arrow_table_slice<fbs::table_slice::arrow::v2>;

} // namespace vast
