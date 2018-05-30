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

#include <cstdint>
#include <string_view>
#include <utility>
#include <vector>

#include <caf/expected.hpp>

#include "vast/filesystem.hpp"
#include "vast/system/column_index.hpp"
#include "vast/system/fwd.hpp"
#include "vast/type.hpp"

#include "vast/detail/range.hpp"

namespace vast::system {

// -- free functions -----------------------------------------------------------

/// Creates a column layout for the given type.
/// @relates table_index
caf::expected<table_index> make_table_index(path filename, type event_type);

// -- class definition ---------------------------------------------------------

/// Wraps multiple `column_index` into a single column layout.
class table_index {
public:
  // -- friend declarations ----------------------------------------------------

  friend caf::expected<table_index> make_table_index(path filename,
                                                         type event_type);

  // -- member and nested types ------------------------------------------------

  /// Stores column indexes.
  using columns_vector = std::vector<column_index_ptr>;

  /// Identifies a subset of columns.
  using columns_range = detail::iterator_range<columns_vector::iterator>;

  // -- constants --------------------------------------------------------------

  /// Number of columns holding meta information.
  static constexpr std::ptrdiff_t meta_column_count = 2;

  // -- constructors, destructors, and assignment operators --------------------

  table_index() = default;

  table_index(table_index&&) = default;

  table_index& operator=(table_index&&) = default;

  ~table_index() noexcept;

  // -- persistency ------------------------------------------------------------

  /// Persists all indexes to disk.
  caf::error flush_to_disk();

  /// -- properties ------------------------------------------------------------

  /// Returns the colums for storing meta information.
  inline columns_range meta_columns() {
    auto first = columns_.begin();
    return {first, first + meta_column_count};
  }

  /// Returns the columns for storing data.
  inline columns_range data_columns() {
    return {columns_.begin() + meta_column_count, columns_.end()};
  }

  /// Returns the number of columns.
  inline size_t num_columns() const {
    return columns_.size();
  }

  /// Returns the number of columns for storing meta information.
  inline size_t num_meta_columns() const {
    return static_cast<size_t>(meta_column_count);
  }

  /// Returns the number of columns for storing data.
  inline size_t num_data_columns() const {
    return num_columns() - num_meta_columns();
  }

  /// Returns the column at given index.
  /// @pre `column_size < num_columns()`
  column_index& at(size_t column_index);

  /// Returns the column at given index and creates it lazily from the factory
  /// in case it doesn't yet exist.
  /// @pre `column_size < num_columns()`
  template <class Factory, class Continuation>
  auto with_column(size_t column_index, Factory& factory, Continuation& f) {
    VAST_ASSERT(column_index < columns_.size());
    auto& col = columns_[column_index];
    using result_type = decltype(f(*col));
    if (col == nullptr) {
      auto fac_res = factory();
      if (!fac_res)
        return result_type{std::move(fac_res.error())};
      col = std::move(*fac_res);
    }
    return f(*col);
  }

  /// Returns the column for meta information at given index and creates it
  /// lazily from the factory in case it doesn't yet exist.
  /// @pre `column_size < num_columns()`
  template <class Factory, class Continuation>
  auto with_meta_column(size_t column_index, Factory factory, Continuation f) {
    VAST_ASSERT(column_index < meta_column_count);
    return with_column(column_index, factory, f);
  }

  /// Returns the column for data at given index and creates it lazily from the
  /// factory in case it doesn't yet exist. @pre `column_size < num_columns()`
  template <class Factory, class Continuation>
  auto with_data_column(size_t column_index, Factory factory, Continuation f) {
    VAST_ASSERT(column_index < num_data_columns());
    return with_column(column_index + num_meta_columns(), factory, f);
  }

  /// Returns a pointer the column with given name or `nullptr` if no such
  /// column exists.
  column_index* by_name(std::string_view column_name);

  /// Returns the base directory for all stored column indexes.
  inline const path& base_dir() const {
    return base_dir_;
  }

  /// Returns the type defining this table's layout.
  inline const type& layout() const {
    return event_type_;
  }

  /// Returns the base directory for meta column indexes.
  path meta_dir() const;

  /// Returns the base directory for data column indexes.
  path data_dir() const;

  /// Indexes an event for all columns.
  /// @param x Event for ingestion.
  caf::error add(const event& x);

  /// Queries event IDs that fulfill the given predicate on any column.
  /// @pre `init()` was called previously.
  caf::expected<bitmap> lookup(const predicate& pred);

private:
  // -- dispatch functions -----------------------------------------------------

  caf::expected<bitmap> lookup(const expression& expr);

  caf::expected<bitmap> lookup(const predicate& pred,
                               const attribute_extractor& ex, const data& x);

  caf::expected<bitmap> lookup(const predicate& pred, const data_extractor& dx,
                               const data& x);

  // -- constructors, destructors, and assignment operators --------------------

  table_index(type event_type, path base_dir);

  // -- member variables -------------------------------------------------------

  /// Stores the indexed type whose fields form our columns.
  type event_type_;

  /// Columns of our type-dependant layout. Lazily filled for columns data to
  /// delay file I/O until a column is accessed by the user.
  columns_vector columns_;

  /// Base directory for all children column indexes.
  path base_dir_;

  /// Allows a shortcut in `add` if all columns are initialized.
  bool fully_initialized_;
};

// -- free functions -----------------------------------------------------------


} // namespace vast::system
