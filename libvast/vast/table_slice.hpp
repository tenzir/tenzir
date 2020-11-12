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
#include "vast/fbs/table_slice.hpp"
#include "vast/fwd.hpp"
#include "vast/table_slice_header.hpp"
#include "vast/type.hpp"
#include "vast/view.hpp"

#include <caf/fwd.hpp>
#include <caf/meta/load_callback.hpp>
#include <caf/meta/type_name.hpp>
#include <caf/optional.hpp>
#include <caf/ref_counted.hpp>

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

  /// The possible encodings of a table slice.
  /// @note This encoding is unversioned. Newly created table slices are
  /// guaranteed to use the newest vesion of the encoding, while deserialized
  /// table slices may use an older version.
  enum class encoding : uint8_t {
    none,    ///< No data is encoded; the table slice is empty or invalid.
    arrow,   ///< The table slice is encoded using the Apache Arrow format.
    msgpack, ///< The table slice is encoded using the MessagePack format.
  };

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

  // FIXME: Remove this when removing legacy table slices.
  explicit table_slice(legacy_table_slice_ptr&& slice) noexcept;

  /// Copy-construct a table slice.
  /// @param other The copied-from slice.
  table_slice(const table_slice& other) noexcept;

  /// Copy-construct a table slice with a given encoding, possibly re-encoding.
  /// @param other The copied-from slice.
  /// @param encoding The encoding to convert to.
  /// @param verify_table Controls whether the table should be verified.
  /// @note This function only re-encodes if necessary, i.e., the new encoding
  /// is different from the existing one.
  table_slice(const table_slice& other, enum encoding encoding,
              enum verify verify) noexcept;

  /// Copy-assigns a table slice.
  /// @param rhs The copied-from slice.
  table_slice& operator=(const table_slice& rhs) noexcept;

  /// Move-constructs a table slice.
  /// @param other The moved-from slice.
  table_slice(table_slice&& other) noexcept;

  /// Move-construct a table slice with a given encoding, possibly re-encoding.
  /// @param other The moved-from slice.
  /// @param encoding The encoding to convert to.
  /// @param verify Controls whether the table should be verified.
  /// @note This function only re-encodes if necessary, i.e., the new encoding
  /// is different from the existing one.
  table_slice(table_slice&& other, enum encoding encoding,
              enum verify verify) noexcept;

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
  enum encoding encoding() const noexcept;

  /// @returns The table layout.
  const record_type& layout() const noexcept;

  /// @returns The number of rows in the slice.
  size_type rows() const noexcept;

  /// @returns The number of columns in the slice.
  size_type columns() const noexcept;

  /// @returns The offset in the ID space.
  id offset() const noexcept;

  /// Sets the offset in the ID space.
  /// @pre `encoding() != encoding::none`
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
  data_view at(size_type row, size_type column) const;

#if VAST_HAVE_ARROW

  // FIXME: Introduce `as_arrow_table_slice` and `as_msgpack_table_slice`
  // functions after removing `legacy_table_slice_ptr`, and remove this
  // function.
  /// Converts a table slice to an Apache Arrow Record Batch.
  /// @returns The pointer to the Record Batch on success, or `nullptr`.
  /// @param x The table slice to convert.
  /// @note Returns `nullptr` if `x.encoding() != table_slice::encoding::arrow`.
  friend std::shared_ptr<arrow::RecordBatch>
  as_record_batch(const table_slice& x);

#endif // VAST_HAVE_ARROW

  // -- concepts ---------------------------------------------------------------

  /// Returns an immutable view on the underlying binary representation of a
  /// table slice.
  /// @param slice The table slice to view.
  friend span<const byte> as_bytes(const table_slice& slice) noexcept;

  /// Opt-in to CAF's type inspection API.
  template <class Inspector>
  friend auto inspect(Inspector& f, table_slice& x) ->
    typename Inspector::result_type {
    auto chunk = x.chunk_;
    return f(caf::meta::type_name("vast.table_slice"), chunk, x.offset_,
             caf::meta::load_callback([&]() noexcept -> caf::error {
               x = table_slice{std::move(chunk), table_slice::verify::no};
               return caf::none;
             }));
  }

private:
  // -- implementation details -------------------------------------------------

  /// A pointer to the underlying chunk, which contains a `vast.fbs.TableSlice`
  /// FlatBuffers table.
  /// @note On construction and destruction, the ref-count of `chunk_` is used
  /// to determine whether the `num_instances_` counter should be increased or
  /// decreased. This implies that the chunk must *never* be exposed outside of
  /// `table_slice`.
  chunk_ptr chunk_ = {};

  // FIXME: Remove when removing legacy table slices.
  legacy_table_slice_ptr legacy_ = {};

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
  } state_;

  /// The number of in-memory table slices.
  inline static std::atomic<size_t> num_instances_ = {};
};

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
  const record_type& layout() const noexcept;

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
  friend caf::error inspect(caf::serializer& sink, legacy_table_slice_ptr& ptr);

  /// @relates legacy_table_slice
  friend caf::error
  inspect(caf::deserializer& source, legacy_table_slice_ptr& ptr);

  /// Packs a table slice into a flatbuffer.
  /// @param builder The builder to pack *x* into.
  /// @param x The table slice to pack.
  /// @returns The flatbuffer offset in *builder*.
  friend caf::expected<flatbuffers::Offset<fbs::FlatTableSlice>>
  pack(flatbuffers::FlatBufferBuilder& builder, legacy_table_slice_ptr x);

  /// Unpacks a table slice from a flatbuffer.
  /// @param x The flatbuffer to unpack.
  /// @param y The target to unpack *x* into.
  /// @returns An error iff the operation fails.
  friend caf::error
  unpack(const fbs::table_slice::legacy::v0& x, legacy_table_slice_ptr& y);

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
void select(std::vector<table_slice>& result, const table_slice& xs,
            const ids& selection);

/// Selects all rows in `xs` with event IDs in `selection`. Cuts `xs` into
/// multiple slices if `selection` produces gaps.
/// @param xs The input table slice.
/// @param selection ID set for selecting events from `xs`.
/// @returns new table slices of the same implementation type as `xs` from
///          `selection`.
/// @pre `xs != nullptr`
std::vector<table_slice> select(const table_slice& xs, const ids& selection);

/// Selects the first `num_rows` rows of `slice`.
/// @param slice The input table slice.
/// @param num_rows The number of rows to keep.
/// @returns `slice` if `slice.rows() <= num_rows`, otherwise creates a new
///          table slice of the first `num_rows` rows from `slice`.
/// @pre `slice != nullptr`
/// @pre `num_rows > 0`
table_slice truncate(const table_slice& slice, size_t num_rows);

/// Splits a table slice into two slices such that the first slice contains the
/// rows `[0, partition_point)` and the second slice contains the rows
/// `[partition_point, n)`, where `n = slice.rows()`.
/// @param slice The input table slice.
/// @param partition_point The index of the first row for the second slice.
/// @returns two new table slices if `0 < partition_point < slice.rows()`,
///          otherwise returns `slice` and a `nullptr`.
/// @pre `slice != nullptr`
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
