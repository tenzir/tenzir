//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/passthrough.hpp"
#include "tenzir/table_slice.hpp"

#include <arrow/array.h>
#include <arrow/type_fwd.h>

#include <memory>
#include <mutex>

namespace tenzir {

/// Additional state needed for the implementation of Arrow-encoded table slices
/// that cannot easily be accessed from the underlying FlatBuffers table
/// directly.
template <class FlatBuffer>
struct arrow_table_slice_state;

template <>
struct arrow_table_slice_state<fbs::table_slice::arrow::v2> {
  /// The deserialized table schema.
  type schema;

  /// The deserialized Arrow Record Batch.
  std::shared_ptr<arrow::RecordBatch> record_batch;

  /// Mapping from column offset to nested Arrow array
  mutable std::optional<arrow::ArrayVector> flat_columns;
  mutable std::mutex flat_columns_mutex;

  auto get_flat_columns() const -> const arrow::ArrayVector&;

  mutable std::atomic<uint64_t> approx_bytes_
    = std::numeric_limits<uint64_t>::max();

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
  /// @param schema Tenzir schema matching the record batch schema. Parameter
  ///     is optional and derived from the record batch if set to none.
  arrow_table_slice(const FlatBuffer& slice, const chunk_ptr& parent,
                    const std::shared_ptr<arrow::RecordBatch>& batch,
                    type schema) noexcept;

  /// Destroys a Arrow-encoded table slice.
  ~arrow_table_slice() noexcept;

  // -- properties -------------------------------------------------------------

  /// Whether the most recent version of the encoding is used.
  inline static constexpr bool is_latest_version
    = std::is_same_v<FlatBuffer, fbs::table_slice::arrow::v2>;

  /// @returns The table schema.
  [[nodiscard]] const type& schema() const noexcept;

  /// @returns The number of rows in the slice.
  [[nodiscard]] table_slice::size_type rows() const noexcept;

  /// @returns The number of columns in the slice.
  [[nodiscard]] table_slice::size_type columns() const noexcept;

  /// @returns Whether the underlying buffer is serialized.
  [[nodiscard]] bool is_serialized() const noexcept;

  // -- data access ------------------------------------------------------------

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

  auto approx_bytes() const -> uint64_t;

private:
  // -- implementation details -------------------------------------------------

  /// A const-reference to the underlying FlatBuffers table.
  const FlatBuffer& slice_;

  /// Additional state needed for the implementation.
  arrow_table_slice_state<FlatBuffer> state_;
};

/// Access Tenzir data views for all elements of an Arrow Array.
auto values(const type& type, const arrow::Array& array) noexcept
  -> generator<data_view>;

template <concrete_type Type>
auto values([[maybe_unused]] const Type& type,
            const type_to_arrow_array_t<Type>& arr) noexcept
  -> generator<std::optional<view3<type_to_data_t<Type>>>> {
  for (auto i = int64_t{0}; i < arr.length(); ++i) {
    co_yield view_at(arr, i);
  }
}

struct indexed_transformation {
  using result_type = std::vector<
    std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>>;
  using function_type = std::function<result_type(
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

/// Applies a list of transformations to both a Tenzir schema and an Arrow
/// struct array. Transformations are sorted internally by index.
///
/// Each transformation receives a field and its corresponding child array as
/// they are stored in the struct, without the parent struct's null bitmap
/// merged in. The transformation returns zero or more replacement (field,
/// array) pairs. Returning an empty result removes the column; returning
/// multiple results expands it (e.g., for flattening).
///
/// The returned struct array preserves the input's null bitmap. Child arrays
/// follow standard Arrow semantics: their validity must be interpreted in
/// context of the parent struct's null bitmap. Callers that extract children
/// from the result for use outside the struct (e.g., to dissolve a record
/// level) must merge the struct's null bitmap into the children themselves,
/// for example via `arrow::StructArray::Flatten()`.
///
/// Transformations must not change the length of the returned arrays. Doing so
/// leads to unspecified behavior regarding the null bitmap of the result.
///
/// @pre Transformation indices must not be a subset of the following
/// transformation's index.
std::pair<type, std::shared_ptr<arrow::StructArray>>
transform_columns(type schema,
                  const std::shared_ptr<arrow::StructArray>& struct_array,
                  std::vector<indexed_transformation> transformations);

/// Applies a list of transformations to a table slice. Transformations are
/// sorted internally by index.
/// @pre Transformation indices must not be a subset of the following
/// transformation's index.
table_slice
transform_columns(const table_slice& slice,
                  std::vector<indexed_transformation> transformations);

/// Create a record batch from a struct array.
///
/// If the struct array has row-level nulls, they are flattened into the child
/// arrays so the resulting record batch preserves the same semantics.
auto record_batch_from_struct_array(std::shared_ptr<arrow::Schema> schema,
                                    const arrow::StructArray& array)
  -> std::shared_ptr<arrow::RecordBatch>;

/// Remove all unspecified columns from both a Tenzir schema and an Arrow record
/// batch.
/// @pre Tenzir schema and Arrow schema must match.
std::pair<type, std::shared_ptr<arrow::RecordBatch>>
select_columns(type schema, const std::shared_ptr<arrow::RecordBatch>& batch,
               std::vector<offset> indices);

/// Remove all unspecified columns from a table slice.
table_slice
select_columns(const table_slice& slice, std::vector<offset> indices);

/// Create a new `arrow::StructArray`.
///
/// Unlike `arrow::StructArray::Make()`, this properly works with empty records,
/// and unlike `arrow::StructArray{...}`, it handles most of the boilerplate.
auto make_struct_array(int64_t length,
                       std::shared_ptr<arrow::Buffer> null_bitmap,
                       const arrow::FieldVector& field_types,
                       const arrow::ArrayVector& field_arrays)
  -> std::shared_ptr<arrow::StructArray>;
auto make_struct_array(int64_t length,
                       std::shared_ptr<arrow::Buffer> null_bitmap,
                       std::vector<std::string> field_names,
                       const arrow::ArrayVector& field_arrays,
                       const record_type& rt)
  -> std::shared_ptr<arrow::StructArray>;
auto make_struct_array(
  int64_t length, std::shared_ptr<arrow::Buffer> null_bitmap,
  std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>> fields,
  const record_type& rt) -> std::shared_ptr<arrow::StructArray>;

// -- template machinery -------------------------------------------------------

/// Explicit deduction guide (not needed as of C++20).
template <class FlatBuffer>
arrow_table_slice(const FlatBuffer&) -> arrow_table_slice<FlatBuffer>;

/// Extern template declarations for all Arrow encoding versions.
extern template class arrow_table_slice<fbs::table_slice::arrow::v2>;

} // namespace tenzir
