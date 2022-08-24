//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/detail/narrow.hpp"
#include "vast/table_slice.hpp"

#include <arrow/array.h>
#include <arrow/type_fwd.h>
#include <caf/meta/type_name.hpp>

#include <memory>

namespace vast {

/// Additional state needed for the implementation of Arrow-encoded table slices
/// that cannot easily be accessed from the underlying FlatBuffers table
/// directly.
template <class FlatBuffer>
struct arrow_table_slice_state;

template <>
struct arrow_table_slice_state<fbs::table_slice::arrow::v0> {
  /// The deserialized table layout.
  type layout;

  /// The deserialized Arrow Record Batch.
  std::shared_ptr<arrow::RecordBatch> record_batch;
};

template <>
struct arrow_table_slice_state<fbs::table_slice::arrow::v1> {
  /// The deserialized table layout.
  type layout;

  /// The deserialized Arrow Record Batch.
  std::shared_ptr<arrow::RecordBatch> record_batch;
};

template <>
struct arrow_table_slice_state<fbs::table_slice::arrow::v2> {
  /// The deserialized table layout.
  type layout;

  /// The deserialized Arrow Record Batch.
  std::shared_ptr<arrow::RecordBatch> record_batch;

  /// Mapping from column offset to nested Arrow array
  arrow::ArrayVector flat_columns;

  /// Whether the record batch points to outside data.
  bool is_serialized;
};

/// A table slice that stores elements encoded in the [Arrow](https://arrow.org)
/// format. The implementation stores data in column-major order.
template <class FlatBuffer>
class arrow_table_slice final {
public:
  // -- constructors, destructors, and assignment operators --------------------

  /// Constructs an Arrow-encoded table slice from a FlatBuffers table.
  /// @param slice The encoding-specific FlatBuffers table.
  /// @param parent The surrounding chunk.
  /// @param batch A pre-existing record batch.
  /// @param schema VAST schema matching the record batch schema.
  arrow_table_slice(const FlatBuffer& slice, const chunk_ptr& parent,
                    const std::shared_ptr<arrow::RecordBatch>& batch,
                    type schema) noexcept;

  /// Destroys a Arrow-encoded table slice.
  ~arrow_table_slice() noexcept;

  // -- properties -------------------------------------------------------------

  /// Whether the most recent version of the encoding is used.
  inline static constexpr bool is_latest_version
    = std::is_same_v<FlatBuffer, fbs::table_slice::arrow::v2>;

  /// The encoding of the slice.
  inline static constexpr enum table_slice_encoding encoding
    = table_slice_encoding::arrow;

  /// @returns The table layout.
  [[nodiscard]] const type& layout() const noexcept;

  /// @returns The number of rows in the slice.
  [[nodiscard]] table_slice::size_type rows() const noexcept;

  /// @returns The number of columns in the slice.
  [[nodiscard]] table_slice::size_type columns() const noexcept;

  /// @returns Whether the underlying buffer is serialized.
  [[nodiscard]] bool is_serialized() const noexcept;

  // -- data access ------------------------------------------------------------

  /// Appends all values in column `column` to `index`.
  /// @param `offset` The offset of the table slice in its ID space.
  /// @param `column` The index of the column to append.
  /// @param `index` the value index to append to.
  void append_column_to_index(id offset, table_slice::size_type column,
                              value_index& index) const;

  /// Retrieves data by specifying 2D-coordinates via row and column.
  /// @param row The row offset.
  /// @param column The column offset.
  /// @pre `row < rows() && column < columns()`
  [[nodiscard]] data_view
  at(table_slice::size_type row, table_slice::size_type column) const;

  /// Retrieves data by specifying 2D-coordinates via row and column.
  /// @param row The row offset.
  /// @param column The column offset.
  /// @param t The type of the value to be retrieved.
  /// @pre `row < rows() && column < columns()`
  [[nodiscard]] data_view
  at(table_slice::size_type row, table_slice::size_type column,
     const type& t) const;

  /// @returns The import timestamp.
  [[nodiscard]] time import_time() const noexcept;

  /// Sets the import timestamp.
  void import_time(time import_time) noexcept;

  /// @returns A shared pointer to the underlying Arrow Record Batch.
  [[nodiscard]] std::shared_ptr<arrow::RecordBatch>
  record_batch() const noexcept;

private:
  // -- implementation details -------------------------------------------------

  /// A const-reference to the underlying FlatBuffers table.
  const FlatBuffer& slice_;

  /// Additional state needed for the implementation.
  arrow_table_slice_state<FlatBuffer> state_;
};

// -- utility functions -------------------------------------------------------

/// Access a VAST data view for a given row in an Arrow Array.
data_view value_at(const type& type, const std::same_as<arrow::Array> auto& arr,
                   int64_t row) noexcept;

template <concrete_type Type>
view<type_to_data_t<Type>>
value_at(const Type& type, const std::same_as<arrow::Array> auto& arr,
         int64_t row) noexcept;

template <concrete_type Type>
view<type_to_data_t<Type>>
value_at([[maybe_unused]] const Type& type,
         const type_to_arrow_array_storage_t<Type>& arr, int64_t row) noexcept {
  VAST_ASSERT(!arr.IsNull(row));
  if constexpr (detail::is_any_v<Type, bool_type, count_type, real_type>) {
    return arr.GetView(row);
  } else if constexpr (std::is_same_v<Type, integer_type>) {
    return integer{arr.GetView(row)};
  } else if constexpr (std::is_same_v<Type, duration_type>) {
    VAST_ASSERT(
      caf::get<type_to_arrow_type_t<duration_type>>(*arr.type()).unit()
      == arrow::TimeUnit::NANO);
    return duration{arr.GetView(row)};
  } else if constexpr (std::is_same_v<Type, time_type>) {
    VAST_ASSERT(caf::get<type_to_arrow_type_t<time_type>>(*arr.type()).unit()
                == arrow::TimeUnit::NANO);
    return time{} + duration{arr.GetView(row)};
  } else if constexpr (std::is_same_v<Type, string_type>) {
    const auto str = arr.GetView(row);
    return {str.data(), str.size()};
  } else if constexpr (std::is_same_v<Type, pattern_type>) {
    return view<type_to_data_t<pattern_type>>{
      value_at(string_type{}, arr, row)};
  } else if constexpr (std::is_same_v<Type, address_type>) {
    VAST_ASSERT(arr.byte_width() == 16);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    const auto* bytes = arr.raw_values() + (row * 16);
    return address::v6(std::span<const uint8_t, 16>{bytes, 16});
  } else if constexpr (std::is_same_v<Type, subnet_type>) {
    VAST_ASSERT(arr.num_fields() == 2);
    auto network = value_at(
      address_type{},
      *caf::get<type_to_arrow_array_t<address_type>>(*arr.field(0)).storage(),
      row);
    auto length
      = static_cast<const arrow::UInt8Array&>(*arr.field(1)).GetView(row);
    return {network, length};
  } else if constexpr (std::is_same_v<Type, enumeration_type>) {
    return detail::narrow_cast<view<type_to_data_t<enumeration_type>>>(
      arr.GetValueIndex(row));
  } else if constexpr (std::is_same_v<Type, list_type>) {
    auto f = [&]<concrete_type ValueType>(
               const ValueType& value_type) -> list_view_handle {
      struct list_view final : list_view_handle::view_type {
        list_view(ValueType value_type,
                  std::shared_ptr<arrow::Array> value_slice) noexcept
          : value_type{std::move(value_type)},
            value_slice{std::move(value_slice)} {
          // nop
        }
        value_type at(size_type i) const override {
          const auto row = detail::narrow_cast<int64_t>(i);
          if (value_slice->IsNull(row))
            return caf::none;
          return value_at(value_type, *value_slice, row);
        };
        size_type size() const noexcept override {
          return value_slice->length();
        };
        ValueType value_type;
        std::shared_ptr<arrow::Array> value_slice;
      };
      return list_view_handle{list_view_ptr{
        caf::make_counted<list_view>(value_type, arr.value_slice(row))}};
    };
    return caf::visit(f, type.value_type());
  } else if constexpr (std::is_same_v<Type, map_type>) {
    auto f = [&]<concrete_type KeyType, concrete_type ItemType>(
               const KeyType& key_type,
               const ItemType& item_type) -> map_view_handle {
      struct map_view final : map_view_handle::view_type {
        map_view(KeyType key_type, ItemType item_type,
                 std::shared_ptr<arrow::Array> key_array,
                 std::shared_ptr<arrow::Array> item_array, int64_t value_offset,
                 int64_t value_length)
          : key_type{std::move(key_type)},
            item_type{std::move(item_type)},
            key_array{std::move(key_array)},
            item_array{std::move(item_array)},
            value_offset{value_offset},
            value_length{value_length} {
          // nop
        }
        value_type at(size_type i) const override {
          VAST_ASSERT(!key_array->IsNull(value_offset + i));
          if (item_array->IsNull(value_offset + i))
            return {value_at(key_type, *key_array, value_offset + i), {}};
          return {
            value_at(key_type, *key_array, value_offset + i),
            value_at(item_type, *item_array, value_offset + i),
          };
        };
        size_type size() const noexcept override {
          return detail::narrow_cast<size_type>(value_length);
        };
        KeyType key_type;
        ItemType item_type;
        std::shared_ptr<arrow::Array> key_array;
        std::shared_ptr<arrow::Array> item_array;
        int64_t value_offset;
        int64_t value_length;
      };
      // Note that there's no `value_slice(...)` and `item_slice(...)` functions
      // for map arrays in Arrow similar to the `value_slice(...)` function for
      // list arrays, so we need to manually work with offsets and lengths here.
      return map_view_handle{map_view_ptr{caf::make_counted<map_view>(
        key_type, item_type, arr.keys(), arr.items(), arr.value_offset(row),
        arr.value_length(row))}};
    };
    return caf::visit(f, type.key_type(), type.value_type());
  } else if constexpr (std::is_same_v<Type, record_type>) {
    struct record_view final : record_view_handle::view_type {
      record_view(record_type type, arrow::ArrayVector fields, int64_t row)
        : type{std::move(type)}, fields{std::move(fields)}, row{row} {
        // nop
      }
      value_type at(size_type i) const override {
        const auto& field = type.field(i);
        return {
          field.name,
          value_at(field.type, *fields[i], row),
        };
      };
      size_type size() const noexcept override {
        return type.num_fields();
      };
      record_type type;
      arrow::ArrayVector fields;
      int64_t row;
    };
    return record_view_handle{
      record_view_ptr{caf::make_counted<record_view>(type, arr.fields(), row)}};
  } else {
    static_assert(detail::always_false_v<Type>, "unhandled type");
  }
}

template <concrete_type Type>
view<type_to_data_t<Type>>
value_at(const Type& type, const std::same_as<arrow::Array> auto& arr,
         int64_t row) noexcept {
  VAST_ASSERT(type.to_arrow_type()->id() == arr.type_id());
  VAST_ASSERT(!arr.IsNull(row));
  if constexpr (arrow::is_extension_type<type_to_arrow_type_t<Type>>::value)
    return value_at(type, *caf::get<type_to_arrow_array_t<Type>>(arr).storage(),
                    row);
  else
    return value_at(type, caf::get<type_to_arrow_array_t<Type>>(arr), row);
}

data_view value_at(const type& type, const std::same_as<arrow::Array> auto& arr,
                   int64_t row) noexcept {
  VAST_ASSERT(type.to_arrow_type()->id() == arr.type_id());
  if (arr.IsNull(row))
    return caf::none;
  auto f = [&]<concrete_type Type>(const Type& type) noexcept -> data_view {
    return value_at(type, arr, row);
  };
  return caf::visit(f, type);
}

/// Access VAST data views for all elements of an Arrow Array.
auto values(const type& type,
            const std::same_as<arrow::Array> auto& array) noexcept
  -> detail::generator<data_view>;

struct indexed_transformation {
  using function_type = std::function<std::vector<
    std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>>(
    struct record_type::field, std::shared_ptr<arrow::Array>)>;

  offset index;      ///< The index of the field to transform.
  function_type fun; ///< The transformation function to apply.

  friend auto operator==(const indexed_transformation& lhs,
                         const indexed_transformation& rhs) noexcept {
    return lhs.index == rhs.index;
  }

  friend auto operator<=>(const indexed_transformation& lhs,
                          const indexed_transformation& rhs) noexcept {
    return lhs.index <=> rhs.index;
  }
};

/// Applies a list of transformations to both a VAST layout and an Arrow record
/// batch.
/// @pre VAST layout and Arrow schema must match.
/// @pre Transformations must be sorted by index.
/// @pre Transformation indices must not be a subset of the following
/// transformation's index.
std::pair<type, std::shared_ptr<arrow::RecordBatch>> transform_columns(
  type layout, const std::shared_ptr<arrow::RecordBatch>& batch,
  const std::vector<indexed_transformation>& transformations) noexcept;

/// Removed all unspecified columns from both a VAST layout and an Arrow record
/// batch.
/// @pre VAST layout and Arrow schema must match.
/// @pre Indices must be sorted.
/// @pre Indices must not be a subset of the following index.
std::pair<type, std::shared_ptr<arrow::RecordBatch>>
select_columns(type layout, const std::shared_ptr<arrow::RecordBatch>& batch,
               const std::vector<offset>& indices) noexcept;

// -- template machinery -------------------------------------------------------

/// Explicit deduction guide (not needed as of C++20).
template <class FlatBuffer>
arrow_table_slice(const FlatBuffer&) -> arrow_table_slice<FlatBuffer>;

/// Extern template declarations for all Arrow encoding versions.
extern template class arrow_table_slice<fbs::table_slice::arrow::v0>;
extern template class arrow_table_slice<fbs::table_slice::arrow::v1>;
extern template class arrow_table_slice<fbs::table_slice::arrow::v2>;

// -- utility functions --------------------------------------------------------
/// Converts a legacy (v0/v1) record batch to the new representation, re-using
/// existing Arrow arrays to make this a cheap operation.
/// @param legacy record batch structured according to v0 or v1 implementation.
/// @param t layout of the type represented by this record batch.
/// @note subnet structure changed in a way that array is built from scratch.
std::shared_ptr<arrow::RecordBatch>
convert_record_batch(const std::shared_ptr<arrow::RecordBatch>& legacy,
                     const type& t);

} // namespace vast
