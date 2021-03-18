// SPDX-FileCopyrightText: (c) 2018 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/chunk.hpp"
#include "vast/table_slice_encoding.hpp"
#include "vast/type.hpp"
#include "vast/view.hpp"

#include <caf/meta/load_callback.hpp>
#include <caf/meta/type_name.hpp>

#include <cstddef>
#include <vector>

namespace vast {

/// A horizontal partition of a table. A slice defines a tabular interface for
/// accessing homogenous data independent of the concrete carrier format.
class table_slice final {
public:
  // -- member types -----------------------------------------------------------

  /// Platform-independent unsigned integer type used for sizes.
  using size_type = uint64_t;

  /// Controls whether the underlying FlatBuffers table should be verified.
  enum class verify : uint8_t {
    no,  ///< Disable FlatBuffers table verification.
    yes, ///< Enable FlatBuffers table verification.
  };

  // -- constructors, destructors, and assignment operators --------------------

  /// Default-constructs an empty table slice.
  table_slice() noexcept;

  /// Construct a table slice from a chunk of data, which contains a
  /// `vast.fbs.TableSlice` FlatBuffers table.
  /// @param chunk A `vast.fbs.TableSlice` FlatBuffers table in a chunk.
  /// @param verify Controls whether the table should be verified.
  /// @note Constructs an invalid table slice if the verification of the
  /// FlatBuffers table fails.
  explicit table_slice(chunk_ptr&& chunk, enum verify verify) noexcept;

  /// Construct a table slice from a chunk of data, which contains a
  /// `vast.fbs.TableSlice` FlatBuffers table, and a known layout.
  /// @param chunk A `vast.fbs.TableSlice` FlatBuffers table in a chunk.
  /// @param verify Controls whether the table should be verified.
  /// @param layout The known table layout.
  /// @note Constructs an invalid table slice if the verification of the
  /// FlatBuffers table fails.
  explicit table_slice(chunk_ptr&& chunk, enum verify verify,
                       record_type layout) noexcept;

  /// Construct a table slice from a flattened table slice embedded in a chunk,
  /// and shares the chunk's lifetime.
  /// @param flat_slice The `vast.fbs.FlatTableSlice` object.
  /// @param parent_chunk A chunk that must contain the `flat_slice` object.
  /// @param verify Controls whether the table should be verified.
  /// @pre `flat_slice.data()->begin() >= parent_chunk->begin()`
  /// @pre `flat_slice.data()->end() <= parent_chunk->end()`
  /// @note Constructs an invalid table slice if the verification of the
  /// FlatBuffers table fails.
  table_slice(const fbs::FlatTableSlice& flat_slice,
              const chunk_ptr& parent_chunk, enum verify verify) noexcept;

  /// Copy-construct a table slice.
  /// @param other The copied-from slice.
  table_slice(const table_slice& other) noexcept;

  /// Copy-assigns a table slice.
  /// @param rhs The copied-from slice.
  table_slice& operator=(const table_slice& rhs) noexcept;

  /// Move-constructs a table slice.
  /// @param other The moved-from slice.
  table_slice(table_slice&& other) noexcept;

  /// Move-assigns a table slice.
  /// @param rhs The moved-from slice.
  table_slice& operator=(table_slice&& rhs) noexcept;

  /// Destroys a table slice.
  ~table_slice() noexcept;

  // -- opeerators -------------------------------------------------------------

  /// Compare two table slices for equality.
  friend bool
  operator==(const table_slice& lhs, const table_slice& rhs) noexcept;

  /// Compare two table slices for inequality.
  friend bool
  operator!=(const table_slice& lhs, const table_slice& rhs) noexcept;

  // -- properties -------------------------------------------------------------

  /// @returns The encoding of the slice.
  [[nodiscard]] enum table_slice_encoding encoding() const noexcept;

  /// @returns The table layout.
  [[nodiscard]] const record_type& layout() const noexcept;

  /// @returns The number of rows in the slice.
  [[nodiscard]] size_type rows() const noexcept;

  /// @returns The number of columns in the slice.
  [[nodiscard]] size_type columns() const noexcept;

  /// @returns The offset in the ID space.
  [[nodiscard]] id offset() const noexcept;

  /// Sets the offset in the ID space.
  void offset(id offset) noexcept;

  /// @returns The number of in-memory table slices.
  static int instances() noexcept;

  // -- data access ------------------------------------------------------------

  /// Appends all values in column `column` to `index`.
  /// @param `column` The index of the column to append.
  /// @param `index` the value index to append to.
  /// @pre `offset() != invalid_id`
  void append_column_to_index(size_type column, value_index& index) const;

  /// Retrieves data by specifying 2D-coordinates via row and column.
  /// @param row The row offset.
  /// @param column The column offset.
  /// @pre `row < rows() && column < columns()`
  [[nodiscard]] data_view at(size_type row, size_type column) const;

  /// Retrieves data by specifying 2D-coordinates via row and column. This
  /// overload provides an optimized access path in case the type of the
  /// element is already known.
  /// @param row The row offset.
  /// @param column The column offset.
  /// @param t The type of the value to be retrieved.
  /// @pre `row < rows() && column < columns()`
  /// @pre `t == *layout().at(*layout:).offset_from_index(column)) == t`
  [[nodiscard]] data_view
  at(size_type row, size_type column, const type& t) const;

#if VAST_ENABLE_ARROW

  /// Converts a table slice to an Apache Arrow Record Batch.
  /// @returns The pointer to the Record Batch.
  /// @param slice The table slice to convert.
  friend std::shared_ptr<arrow::RecordBatch>
  as_record_batch(const table_slice& slice);

#endif // VAST_ENABLE_ARROW

  // -- concepts ---------------------------------------------------------------

  /// Returns an immutable view on the underlying binary representation of a
  /// table slice.
  /// @param slice The table slice to view.
  friend span<const std::byte> as_bytes(const table_slice& slice) noexcept;

  /// Opt-in to CAF's type inspection API.
  template <class Inspector>
  friend auto inspect(Inspector& f, table_slice& x) ->
    typename Inspector::result_type {
    auto chunk = x.chunk_;
    return f(caf::meta::type_name("vast.table_slice"), chunk, x.offset_,
             caf::meta::load_callback([&]() noexcept -> caf::error {
               // When VAST allows for external tools to hook directly into the
               // table slice streams, this should be switched to verify if the
               // chunk is unique.
               x = table_slice{std::move(chunk), table_slice::verify::no};
               return caf::none;
             }));
  }

  // -- operations -------------------------------------------------------------

  /// Rebuilds a table slice with a given encoding if necessary.
  /// @param slice The slice to rebuild.
  /// @param encoding The encoding to convert to.
  /// @note This function only rebuilds if necessary, i.e., the new encoding
  /// is different from the existing one.
  friend table_slice
  rebuild(table_slice slice, enum table_slice_encoding encoding) noexcept;

  /// Selects all rows in `slice` with event IDs in `selection` and appends
  /// produced table slices to `result`. Cuts `slice` into multiple slices if
  /// `selection` produces gaps.
  /// @param result The container for appending generated table slices.
  /// @param slice The input table slice.
  /// @param selection ID set for selecting events from `slice`.
  /// @pre `slice.encoding() != table_slice_encoding::none`
  friend void select(std::vector<table_slice>& result, const table_slice& slice,
                     const ids& selection);

private:
  // -- implementation details -------------------------------------------------

  /// A pointer to the underlying chunk, which contains a `vast.fbs.TableSlice`
  /// FlatBuffers table.
  /// @note On construction and destruction, the ref-count of `chunk_` is used
  /// to determine whether the `num_instances_` counter should be increased or
  /// decreased. This implies that the chunk must *never* be exposed outside of
  /// `table_slice`.
  chunk_ptr chunk_ = {};

  /// The offset of the table slice within its ID space.
  /// @note Assigned by the importer on import and the archive on export and as
  /// such not part of the FlatBuffers table. Binary representations of a table
  /// slice do not contain the offset.
  id offset_ = invalid_id;

  /// A pointer to the table slice state. As long as the layout cannot be
  /// represented from a FlatBuffers table directly, it is prohibitively
  /// expensive to deserialize the layout.
  /// TODO: Revisit the need for this hack after converting the type system to
  /// use FlatBuffers.
  union {
    const void* none = {};
    const msgpack_table_slice<fbs::table_slice::msgpack::v0>* msgpack_v0;
    const arrow_table_slice<fbs::table_slice::arrow::v0>* arrow_v0;
  } state_;

  /// The number of in-memory table slices.
  inline static std::atomic<size_t> num_instances_ = {};
};

// -- operations ---------------------------------------------------------------

/// Selects all rows in `slice` with event IDs in `selection`. Cuts `slice` into
/// multiple slices if `selection` produces gaps.
/// @param slice The input table slice.
/// @param selection ID set for selecting events from `slice`.
/// @returns new table slices of the same implementation type as `slice` from
///          `selection`.
/// @pre `slice.encoding() != table_slice_encoding::none`
std::vector<table_slice> select(const table_slice& slice, const ids& selection);

/// Selects the first `num_rows` rows of `slice`.
/// @param slice The input table slice.
/// @param num_rows The number of rows to keep.
/// @returns `slice` if `slice.rows() <= num_rows`, otherwise creates a new
///          table slice of the first `num_rows` rows from `slice`.
/// @pre `slice.encoding() != table_slice_encoding::none`
/// @pre `num_rows > 0`
table_slice truncate(const table_slice& slice, size_t num_rows);

/// Splits a table slice into two slices such that the first slice contains the
/// rows `[0, partition_point)` and the second slice contains the rows
/// `[partition_point, n)`, where `n = slice.rows()`.
/// @param slice The input table slice.
/// @param partition_point The index of the first row for the second slice.
/// @returns two new table slices if `0 < partition_point < slice.rows()`,
///          otherwise returns `slice` and an invalid tbale slice.
/// @pre `slice.encoding() != table_slice_encoding::none`
std::pair<table_slice, table_slice>
split(const table_slice& slice, size_t partition_point);

/// Counts the number of total rows of multiple table slices.
/// @param slices The table slices to count.
/// @returns The sum of rows across *slices*.
uint64_t rows(const std::vector<table_slice>& slices);

/// Evaluates an expression over a table slice by applying it row-wise.
/// @param expr The expression to evaluate.
/// @param slice The table slice to apply *expr* on.
/// @returns The set of row IDs in *slice* for which *expr* yields true.
ids evaluate(const expression& expr, const table_slice& slice);

} // namespace vast
