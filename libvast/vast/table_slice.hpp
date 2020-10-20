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

#include "vast/chunk.hpp"
#include "vast/fwd.hpp"
#include "vast/table_slice_encoding.hpp"
#include "vast/type.hpp"
#include "vast/view.hpp"

#include <caf/meta/load_callback.hpp>
#include <caf/meta/type_name.hpp>
#include <caf/optional.hpp>

#include <atomic>
#include <cstddef>
#include <limits>
#include <string_view>
#include <utility>
#include <vector>

namespace vast {

// -- forward-declarations -----------------------------------------------------

namespace fbs {

struct TableSliceBuffer;

} // namespace fbs

class table_slice final {
  friend class table_slice_builder;

public:
  // -- types and constants ----------------------------------------------------

  using size_type = uint64_t;

  // -- constructors, destructors, and assignment operators --------------------

  // Default-construct an invalid table slice.
  table_slice() noexcept;

  /// Destroys a table slice.
  ~table_slice() noexcept;

  /// Copy-constructs a table slice.
  /// @param other The table slice to copy.
  table_slice(const table_slice& other) noexcept;
  table_slice& operator=(const table_slice& rhs) noexcept;

  /// Move-constructs a table slice.
  /// @param other The table slice to move from.
  table_slice(table_slice&& other) noexcept;
  table_slice& operator=(table_slice&& rhs) noexcept;

  // -- comparison operators ---------------------------------------------------

  /// Compares two table slices for equality.
  friend bool operator==(const table_slice& lhs, const table_slice& rhs);

  // -- properties: offset -----------------------------------------------------

  /// @returns The offset in the ID space.
  id offset() const noexcept;

  /// Sets the offset in the ID space.
  /// @param offset The new offset in the ID space.
  /// @pre `chunk() && chunk()->unique()`
  void offset(id offset) noexcept;

  // -- properties: encoding ---------------------------------------------------

  /// @returns The encoding of the table slice.
  table_slice_encoding encoding() const noexcept;

  // -- properties: rows -------------------------------------------------------

  /// @returns The number of rows in the slice.
  size_type rows() const noexcept;

  // -- properties: columns ----------------------------------------------------

  /// @returns The number of rows in the slice.
  size_type columns() const noexcept;

  // -- properties: layout -----------------------------------------------------

  /// @returns The table layout.
  const record_type& layout() const noexcept;

  // -- properties: data access ------------------------------------------------

  /// Retrieves data by specifying 2D-coordinates via row and column.
  /// @param row The row offset.
  /// @param column The column offset.
  /// @returns The data view at the requested position.
  /// @pre `row < rows()`
  /// @pre `column < columns()`
  data_view at(size_type row, size_type column) const;

  /// Appends all values in column `column` to `idx`.
  /// @param column A view on a table slice column.
  /// @param idx The value index to append `column` to.
  /// @pre `column < columns()`
  /// @relates table_slice
  void append_column_to_index(size_type column, value_index& idx) const;

  // -- type introspection -----------------------------------------------------

  /// @returns The underlying chunk.
  const chunk_ptr& chunk() const noexcept;

  /// @returns Number of in-memory table slices.
  static size_t instances() noexcept;

  /// Visitor function for the CAF Type Inspection API; do not call directly.
  /// @param f The visitor used for inspection.
  /// @param slice The inspected table slice.
  /// @returns An implementation-defined return value from the CAF Type
  /// Inspection API.
  template <class Inspector>
  friend auto inspect(Inspector& f, table_slice& slice) {
    auto chunk = slice.chunk_;
    auto offset = slice.offset_;
    auto load = caf::meta::load_callback([&]() -> caf::error {
      slice = table_slice{std::move(chunk)};
      slice.offset(offset);
      return caf::none;
    });
    return f(caf::meta::type_name("table_slice"), chunk, offset, load);
  }

  /// Unpack a table slice from a nested FlatBuffer.
  /// @param source FlatBuffer to unpack from
  /// @param dest The table slice to unpack into
  /// @returns An error on falure, or none.
  friend caf::error
  unpack(const fbs::TableSliceBuffer& source, table_slice& dest);

private:
  // -- implementation details -------------------------------------------------

  /// Constructs a table slice from a chunk.
  /// @param chunk The chunk containing a blob of binary data.
  /// @pre `chunk`
  /// @pre `chunk->unique()`
  /// @post `chunk()`
  /// @relates table_slice_builder
  explicit table_slice(chunk_ptr&& chunk) noexcept;

  /// The raw data of the table slice.
  chunk_ptr chunk_ = nullptr;

  /// The offset in the id space.
  id offset_ = invalid_id;

  /// A pointer to the actual table slice implementation. The lifetime of the
  /// implementation-specifics is tied to the lifetime of the chunk.
  union {
    void* invalid = nullptr;
    msgpack_table_slice* msgpack;
    arrow_table_slice* arrow;
  } pimpl_;

  /// The number of in-memory table slices.
  inline static std::atomic<size_t> num_instances_ = 0;
};

// -- row operations -----------------------------------------------------------

/// Selects all rows in `slices` with event IDs in `selection` and appends
/// produced table slices to `result`. Cuts `slices` into multiple slices if
/// `selection` produces gaps.
/// @param result The container for appending generated table slices.
/// @param slice The input table slice.
/// @param selection ID set for selecting events from `slices`.
/// @relates table_slice
void select(std::vector<table_slice>& result, table_slice slice,
            const ids& selection);

/// Selects all rows in `slice` with event IDs in `selection`. Cuts `slice` into
/// multiple slices if `selection` produces gaps.
/// @param slice The input table slice.
/// @param selection ID set for selecting events from `slice`.
/// @returns new table slices of the same implementation type as `slice` from
///          `selection`.
/// @relates table_slice
std::vector<table_slice> select(table_slice slice, const ids& selection);

/// Selects the first `num_rows` rows of `slice`.
/// @param slice The input table slice.
/// @param num_rows The number of rows to keep.
/// @returns `slice` if `slice.rows() <= num_rows`, otherwise creates a new
///          table slice of the first `num_rows` rows from `slice`.
/// @relates table_slice
table_slice truncate(table_slice slice, table_slice::size_type num_rows);

/// Splits a table slice into two slices such that the first slice contains
/// the rows `[0, partition_point)` and the second slice contains the rows
/// `[partition_point, slice.rows())`.
/// @param slice The input table slice.
/// @param partition_point The index of the first row for the second slice.
/// @returns Two new table slices if `0 < partition_point < slice.rows()`,
///          otherwise returns `slice` and an invalid table slice.
/// @relates table_slice
std::pair<table_slice, table_slice>
split(table_slice slice, table_slice::size_type partition_point);

/// Counts the number of total rows of multiple table slices.
/// @param slices The table slices to count.
/// @returns The sum of rows across *slices*.
/// @relates table_slice
table_slice::size_type rows(const std::vector<table_slice>& slices);

/// Evaluates an expression over a table slice by applying it row-wise.
/// @param expr The expression to evaluate.
/// @param slice The table slice to apply *expr* on.
/// @returns The set of row IDs in *slice* for which *expr* yields true.
/// @relates table_slice
ids evaluate(const expression& expr, const table_slice& slice);

} // namespace vast
