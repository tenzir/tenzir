//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/qualified_record_field.hpp"
#include "vast/table_slice.hpp"
#include "vast/view.hpp"

#include <cstdint>
#include <optional>
#include <string_view>

namespace vast {

/// Convenience helper for traversing a column of a table slice.
/// @relates table_slice
class table_slice_column {
public:
  /// Defaulted constructors, destructors, and assignment operators.
  table_slice_column() noexcept;
  ~table_slice_column() noexcept;
  table_slice_column(const table_slice_column&);
  table_slice_column& operator=(const table_slice_column&);
  table_slice_column(table_slice_column&&) noexcept;
  table_slice_column& operator=(table_slice_column&&) noexcept;

  /// Construct a view on a column of a table slice.
  /// @param slice The slice to view.
  /// @param column The viewed column's index.
  /// @pre `column < slice.columns()`
  table_slice_column(table_slice slice, size_t column) noexcept;

  /// @returns the data at given row.
  /// @pre `row < size()`
  data_view operator[](size_t row) const;

  /// @returns the number of rows in the column.
  [[nodiscard]] size_t size() const noexcept;

  /// @returns the viewed table slice.
  [[nodiscard]] const table_slice& slice() const noexcept;

  /// @returns the viewed column's index.
  [[nodiscard]] size_t index() const noexcept;

  /// @returns the viewed column's qualified record field.
  [[nodiscard]] const qualified_record_field& field() const noexcept;

  template <class Inspector>
  friend auto inspect(Inspector& f, table_slice_column& x) {
    return f.object(x)
      .pretty_name("vast.table_slice_column")
      .fields(f.field("slice", x.slice_), f.field("column", x.column_),
              f.field("field", x.field_));
  }

private:
  table_slice slice_ = {};
  size_t column_ = 0;
  qualified_record_field field_ = {};
};

} // namespace vast
