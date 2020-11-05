/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include "vast/fbs/table_slice.hpp"
#include "vast/fwd.hpp"
#include "vast/table_slice_header.hpp"
#include "vast/type.hpp"
#include "vast/view.hpp"

#include <caf/fwd.hpp>
#include <caf/optional.hpp>
#include <caf/ref_counted.hpp>

#include <cstddef>
#include <vector>

namespace vast {

/// A horizontal partition of a table. A slice defines a tabular interface for
/// accessing homogenous data independent of the concrete carrier format.
class legacy_table_slice : public caf::ref_counted {
public:
  // -- member types -----------------------------------------------------------

  using size_type = uint64_t;

  // -- constructors, destructors, and assignment operators --------------------

  /// Default-constructs an empty table slice.
  legacy_table_slice() noexcept;

  // Copy-construct a table slice.
  legacy_table_slice(const legacy_table_slice& other) noexcept;

  // Copy-assigns a table slice.
  legacy_table_slice& operator=(const legacy_table_slice& rhs) noexcept;

  // Move-constructs a table slice.
  legacy_table_slice(legacy_table_slice&& other) noexcept;

  // Move-assigns a table slice.
  legacy_table_slice& operator=(legacy_table_slice&& rhs) noexcept;

  /// Constructs a table slice from a header.
  /// @param header The header of the table slice.
  explicit legacy_table_slice(table_slice_header header = {}) noexcept;

  /// Destroy a table slice.
  virtual ~legacy_table_slice() noexcept override;

  /// Makes a copy of this slice.
  virtual legacy_table_slice* copy() const = 0;

  // -- persistence ------------------------------------------------------------

  /// Saves the contents (excluding the layout!) of this slice to `sink`.
  virtual caf::error serialize(caf::serializer& sink) const = 0;

  /// Loads the contents for this slice from `source`.
  virtual caf::error deserialize(caf::deserializer& source) = 0;

  /// Loads a table slice from a chunk. Note that the beginning of the chunk
  /// data must point to the table slice data right after the implementation
  /// ID. The default implementation dispatches to `deserialize` with a
  /// `caf::binary_deserializer`.
  /// @param chunk The chunk to convert into a table slice.
  /// @returns An error if the operation fails and `none` otherwise.
  /// @pre `chunk != nullptr`
  virtual caf::error load(chunk_ptr chunk);

  // -- visitation -------------------------------------------------------------

  /// Appends all values in column `col` to `idx`.
  /// @param `col` The index of the column to append.
  /// @param `idx` the value index to append to.
  virtual void append_column_to_index(size_type col, value_index& idx) const;

  // -- properties -------------------------------------------------------------

  /// @returns The table layout.
  record_type layout() const noexcept;

  /// @returns An identifier for the implementing class.
  virtual caf::atom_value implementation_id() const noexcept = 0;

  /// @returns The number of rows in the slice.
  size_type rows() const noexcept;

  /// @returns The number of rows in the slice.
  size_type columns() const noexcept;

  /// @returns The offset in the ID space.
  id offset() const noexcept;

  /// Sets the offset in the ID space.
  void offset(id offset) noexcept;

  /// Retrieves data by specifying 2D-coordinates via row and column.
  /// @param row The row offset.
  /// @param col The column offset.
  /// @pre `row < rows() && col < columns()`
  virtual data_view at(size_type row, size_type col) const = 0;

  /// @returns The number of in-memory table slices.
  static int instances();

  // -- comparison operators ---------------------------------------------------

  /// @relates legacy_table_slice
  friend bool
  operator==(const legacy_table_slice& x, const legacy_table_slice& y);

  /// @relates legacy_table_slice
  friend bool
  operator!=(const legacy_table_slice& x, const legacy_table_slice& y);

  // -- concepts ---------------------------------------------------------------

  /// @relates legacy_table_slice
  friend caf::error inspect(caf::serializer& sink, table_slice_ptr& ptr);

  /// @relates legacy_table_slice
  friend caf::error inspect(caf::deserializer& source, table_slice_ptr& ptr);

  /// Packs a table slice into a flatbuffer.
  /// @param builder The builder to pack *x* into.
  /// @param x The table slice to pack.
  /// @returns The flatbuffer offset in *builder*.
  friend caf::expected<flatbuffers::Offset<fbs::FlatTableSlice>>
  pack(flatbuffers::FlatBufferBuilder& builder, table_slice_ptr x);

  /// Unpacks a table slice from a flatbuffer.
  /// @param x The flatbuffer to unpack.
  /// @param y The target to unpack *x* into.
  /// @returns An error iff the operation fails.
  friend caf::error
  unpack(const fbs::table_slice::legacy::v0& x, table_slice_ptr& y);

protected:
  // -- member variables -------------------------------------------------------

  table_slice_header header_ = {};

  // -- implementation details -------------------------------------------------
private:
  inline static std::atomic<size_t> instance_count_ = 0;
};

// -- intrusive_ptr facade -----------------------------------------------------

/// @relates legacy_table_slice
void intrusive_ptr_add_ref(const legacy_table_slice* ptr);

/// @relates legacy_table_slice
void intrusive_ptr_release(const legacy_table_slice* ptr);

/// @relates legacy_table_slice
legacy_table_slice* intrusive_cow_ptr_unshare(legacy_table_slice*&);

// -- operations ---------------------------------------------------------------

/// Selects all rows in `xs` with event IDs in `selection` and appends produced
/// table slices to `result`. Cuts `xs` into multiple slices if `selection`
/// produces gaps.
/// @param result The container for appending generated table slices.
/// @param xs The input table slice.
/// @param selection ID set for selecting events from `xs`.
/// @pre `xs != nullptr`
void select(std::vector<table_slice_ptr>& result, const table_slice_ptr& xs,
            const ids& selection);

/// Selects all rows in `xs` with event IDs in `selection`. Cuts `xs` into
/// multiple slices if `selection` produces gaps.
/// @param xs The input table slice.
/// @param selection ID set for selecting events from `xs`.
/// @returns new table slices of the same implementation type as `xs` from
///          `selection`.
/// @pre `xs != nullptr`
std::vector<table_slice_ptr> select(const table_slice_ptr& xs,
                                    const ids& selection);

/// Selects the first `num_rows` rows of `slice`.
/// @param slice The input table slice.
/// @param num_rows The number of rows to keep.
/// @returns `slice` if `slice->rows() <= num_rows`, otherwise creates a new
///          table slice of the first `num_rows` rows from `slice`.
/// @pre `slice != nullptr`
/// @pre `num_rows > 0`
table_slice_ptr truncate(const table_slice_ptr& slice, size_t num_rows);

/// Splits a table slice into two slices such that the first slice contains the
/// rows `[0, partition_point)` and the second slice contains the rows
/// `[partition_point, n)`, where `n = slice->rows()`.
/// @param slice The input table slice.
/// @param partition_point The index of the first row for the second slice.
/// @returns two new table slices if `0 < partition_point < slice->rows()`,
///          otherwise returns `slice` and a `nullptr`.
/// @pre `slice != nullptr`
std::pair<table_slice_ptr, table_slice_ptr> split(const table_slice_ptr& slice,
                                                  size_t partition_point);

/// Counts the number of total rows of multiple table slices.
/// @param slices The table slices to count.
/// @returns The sum of rows across *slices*.
uint64_t rows(const std::vector<table_slice_ptr>& slices);

/// Evaluates an expression over a table slice by applying it row-wise.
/// @param expr The expression to evaluate.
/// @param slice The table slice to apply *expr* on.
/// @returns The set of row IDs in *slice* for which *expr* yields true.
ids evaluate(const expression& expr, const table_slice_ptr& slice);

} // namespace vast
