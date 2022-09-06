//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/chunk.hpp"
#include "vast/concept/printable/print.hpp"
#include "vast/table_slice_encoding.hpp"
#include "vast/type.hpp"
#include "vast/view.hpp"

#include <caf/meta/load_callback.hpp>
#include <caf/meta/type_name.hpp>

#include <cstddef>
#include <span>
#include <vector>

namespace vast {

/// A horizontal partition of a table. A slice defines a tabular interface for
/// accessing homogenous data independent of the concrete carrier format.
class table_slice final {
public:
  // -- member types and friends ----------------------------------------------

  /// Platform-independent unsigned integer type used for sizes.
  using size_type = uint64_t;

  /// Controls whether the underlying FlatBuffers table should be verified.
  enum class verify : uint8_t {
    no,  ///< Disable FlatBuffers table verification.
    yes, ///< Enable FlatBuffers table verification.
  };

  /// Controls whether the underlying FlatBuffers table should be created when
  /// constructing a table slice from an existing Arrow Record Batch.
  enum class serialize : uint8_t {
    no,  ///< Skip serialization into the Arrow IPC backing if possible.
    yes, ///< Always serialize into an Arrow IPC backing.
  };

  /// A typed view on a given set of columns of a table slice.
  template <class... Types>
  friend class projection;

  // -- constructors, destructors, and assignment operators --------------------

  /// Default-constructs an empty table slice.
  table_slice() noexcept;

  /// Construct a table slice from a chunk of data, which contains a
  /// `vast.fbs.TableSlice` FlatBuffers table.
  /// @param chunk A `vast.fbs.TableSlice` FlatBuffers table in a chunk.
  /// @param verify Controls whether the table should be verified.
  /// @param batch An optional pre-existing record batch to use over the IPC
  /// buffer in the chunk.
  /// @pre !chunk || chunk->unique()
  /// @note Constructs an invalid table slice if the verification of the
  /// FlatBuffers table fails.
  explicit table_slice(chunk_ptr&& chunk, enum verify verify,
                       const std::shared_ptr<arrow::RecordBatch>& batch
                       = nullptr,
                       type schema = {}) noexcept;

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

  /// Construct an Arrow-encoded table slice from an existing record batch.
  /// @param record_batch The record batch containing the table slice data.
  /// @param VAST type for the provided record batch.
  /// @param serialize Whether to store IPC format as a backing.
  explicit table_slice(const std::shared_ptr<arrow::RecordBatch>& record_batch,
                       type schema = {},
                       enum serialize serialize = serialize::no);

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

  /// Creates a new table slice whose underlying chunk is unique.
  [[nodiscard]] table_slice unshare() const noexcept;

  // -- operators -------------------------------------------------------------

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
  [[nodiscard]] const type& layout() const noexcept;

  /// @returns The number of rows in the slice.
  [[nodiscard]] size_type rows() const noexcept;

  /// @returns The number of columns in the slice.
  [[nodiscard]] size_type columns() const noexcept;

  /// @returns The offset in the ID space.
  [[nodiscard]] id offset() const noexcept;

  /// Sets the offset in the ID space.
  void offset(id offset) noexcept;

  /// @returns The import timestamp.
  [[nodiscard]] time import_time() const noexcept;

  /// Sets the import timestamp.
  /// @pre The underlying chunk must be unique.
  void import_time(time import_time) noexcept;

  /// @returns Whether the slice is already serialized.
  [[nodiscard]] bool is_serialized() const noexcept;

  /// @returns The number of in-memory table slices.
  static size_t instances() noexcept;

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
  /// @pre The type has to match the type at the given column.
  [[nodiscard]] data_view
  at(size_type row, size_type column, const type& t) const;

  template <concrete_type T>
  [[nodiscard]] std::optional<view<type_to_data_t<T>>>
  at(size_type row, size_type column, const T& t) const {
    auto result = at(row, column, type{t});
    if (caf::holds_alternative<caf::none_t>(result)) {
#if defined(__GNUC__) && __GNUC__ <= 10
      // gcc-10 issues a bogus maybe-uninitialized warning for the return value
      // here. See also: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=80635
      VAST_DIAGNOSTIC_PUSH
      VAST_DIAGNOSTIC_IGNORE_MAYBE_UNINITIALIZED
      return {};
      VAST_DIAGNOSTIC_POP
#else
      return {};
#endif
    }
    VAST_ASSERT(caf::holds_alternative<view<type_to_data_t<T>>>(result));
    return caf::get<view<type_to_data_t<T>>>(result);
  }

  /// Converts a table slice to an Apache Arrow Record Batch.
  /// @returns The pointer to the Record Batch.
  /// @param slice The table slice to convert.
  friend std::shared_ptr<arrow::RecordBatch>
  to_record_batch(const table_slice& slice);

  /// Creates a typed view on a given set of columns of a table slice.
  /// @note This function is defined and documented in 'vast/project.hpp'.
  template <class... Hints>
  friend auto project(table_slice slice, Hints&&... hints);

  // -- concepts ---------------------------------------------------------------

  /// Returns an immutable view on the underlying binary representation of a
  /// table slice.
  /// @param slice The table slice to view.
  /// @pre slice.is_serialized()
  friend std::span<const std::byte> as_bytes(const table_slice& slice) noexcept;

  /// Opt-in to CAF's type inspection API.
  template <class Inspector>
  friend auto inspect(Inspector& f, table_slice& x) ->
    typename Inspector::result_type {
    auto chunk = x.chunk_;
    return f(caf::meta::type_name("vast.table_slice"),
             caf::meta::save_callback([&]() noexcept -> caf::error {
               if (!x.is_serialized()) {
                 auto serialized_x = table_slice{to_record_batch(x), x.layout(),
                                                 serialize::yes};
                 serialized_x.import_time(x.import_time());
                 chunk = serialized_x.chunk_;
                 x = std::move(serialized_x);
               }
               return caf::none;
             }),
             chunk, caf::meta::load_callback([&]() noexcept -> caf::error {
               // When VAST allows for external tools to hook directly into the
               // table slice streams, this should be switched to verify if the
               // chunk is unique.
               x = table_slice{std::move(chunk), table_slice::verify::no};
               VAST_ASSERT(x.is_serialized());
               return caf::none;
             }),
             x.offset_);
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

  /// Produces a new table slice consisting only of events addressed in `hints`
  /// that match the given expression. Does not preserve ids; use `select`
  /// instead if the id mapping must be maintained.
  /// @param slice The input table slice.
  /// @param expr The expression to evaluate.
  /// @param hints An ID set for pruning the events that need to be considered.
  /// @returns a new table slice consisting only of events matching the given
  ///          expression.
  /// @pre `slice.encoding() != table_slice_encoding::none`
  friend std::optional<table_slice>
  filter(const table_slice& slice, expression expr, const ids& hints);

  /// Counts the rows that match an expression.
  /// @param slice The input table slice.
  /// @param expr The expression to evaluate.
  /// @param hints An ID set for pruning the events that need to be considered.
  /// @returns the number of rows that are included in `hints` and match `expr`.
  /// @pre `slice.encoding() != table_slice_encoding::none`
  friend uint64_t count_matching(const table_slice& slice,
                                 const expression& expr, const ids& hints);

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
  union {
    const void* none = {};
    const arrow_table_slice<fbs::table_slice::arrow::v0>* arrow_v0;
    const arrow_table_slice<fbs::table_slice::arrow::v1>* arrow_v1;
    const arrow_table_slice<fbs::table_slice::arrow::v2>* arrow_v2;
    const msgpack_table_slice<fbs::table_slice::msgpack::v0>* msgpack_v0;
    const msgpack_table_slice<fbs::table_slice::msgpack::v1>* msgpack_v1;
  } state_;

  /// The number of in-memory table slices.
  inline static std::atomic<size_t> num_instances_ = {};
};

// -- operations ---------------------------------------------------------------

/// Selects all rows in `slice` with event IDs in `selection`. Cuts `slice`
/// into multiple slices if `selection` produces gaps.
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
table_slice truncate(table_slice slice, size_t num_rows);

/// Splits a table slice into two slices such that the first slice contains
/// the rows `[0, partition_point)` and the second slice contains the rows
/// `[partition_point, n)`, where `n = slice.rows()`.
/// @param slice The input table slice.
/// @param partition_point The index of the first row for the second slice.
/// @returns two new table slices if `0 < partition_point < slice.rows()`,
///          otherwise returns `slice` and an invalid tbale slice.
/// @pre `slice.encoding() != table_slice_encoding::none`
std::pair<table_slice, table_slice>
split(table_slice slice, size_t partition_point);

/// Counts the number of total rows of multiple table slices.
/// @param slices The table slices to count.
/// @returns The sum of rows across *slices*.
uint64_t rows(const std::vector<table_slice>& slices);

/// Evaluates an expression over a table slice by applying it row-wise.
/// @param expr The expression to evaluate.
/// @param slice The table slice to apply *expr* on.
/// @param hints An optional pre-selection of rows to look at.
/// @returns The set of row IDs in *slice* for which *expr* yields true.
ids evaluate(const expression& expr, const table_slice& slice,
             const ids& hints);

// Attribute-specifier-seqs are not allowed in friend function declarations, so
// we re-declare the filter functions with nodiscard here.
[[nodiscard]] std::optional<table_slice>
filter(const table_slice& slice, expression expr, const ids& hints);

/// Produces a new table slice consisting only of events that match the given
/// expression. Does not preserve ids, use `select`instead if the id mapping
/// must be maintained.
/// @param slice The input table slice.
/// @param expr The expression to evaluate.
/// @returns a new table slice consisting only of events matching the given
///          expression.
/// @pre `slice.encoding() != table_slice_encoding::none`
[[nodiscard]] std::optional<table_slice>
filter(const table_slice& slice, const expression& expr);

/// Produces a new table slice consisting only of events addressed in `hints`.
/// Does not preserve ids, use `select`instead if the id mapping must be
/// maintained.
/// @param slice The input table slice.
/// @param hints The set of IDs to select the events to include in the output
///              slice.
/// @returns a new table slice consisting only of events matching the given
///          expression.
/// @pre `slice.encoding() != table_slice_encoding::none`
[[nodiscard]] std::optional<table_slice>
filter(const table_slice& slice, const ids& hints);

// Attribute-specifier-seqs are not allowed in friend function declarations, so
// we re-declare the count_matching function with nodiscard here.
[[nodiscard]] uint64_t count_matching(const table_slice& slice,
                                      const expression& expr, const ids& hints);

} // namespace vast

#include "vast/concept/printable/vast/table_slice.hpp"

namespace fmt {

template <>
struct formatter<vast::table_slice> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::table_slice& value, FormatContext& ctx) const {
    auto out = ctx.out();
    vast::print(out, value);
    return out;
  }
};

} // namespace fmt
