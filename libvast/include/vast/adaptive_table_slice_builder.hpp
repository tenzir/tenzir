//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/adaptive_table_slice_builder_guards.hpp"
#include "vast/detail/series_builders.hpp"
#include "vast/table_slice.hpp"

#include <variant>

namespace vast {

class adaptive_table_slice_builder {
public:
  adaptive_table_slice_builder() = default;
  explicit adaptive_table_slice_builder(type starting_schema,
                                        bool allow_fields_discovery = false);
  struct row_guard {
  public:
    explicit row_guard(adaptive_table_slice_builder& builder);
    /// @brief removes the values and fields added within the scope of this
    /// row_guard.
    auto cancel() -> void;

    /// @brief Adds a field to a row.
    /// @param field_name Field name.
    /// @return Object that allows the caller to add new values to a given
    /// field. The row_guard object must outlive the returned object.
    auto push_field(std::string_view field_name) -> detail::field_guard;
    ~row_guard() noexcept;

  private:
    adaptive_table_slice_builder& builder_;
    detail::arrow_length_type starting_rows_count_ = 0;
  };

  /// @brief Inserts a row to the output table slice.
  /// @return An object used to manipulate fields of an inserted row. The
  /// returned object must be destroyed beforore calling this method again.
  auto push_row() -> row_guard;

  /// @brief Combines all the pushed rows into a table slice. This can be safely
  /// called multipled times only when constructed with a fixed schema (no
  /// fields discovery)
  /// @param slice_schema_name The schema name of the output table slice. The
  /// schema name will be a type::fingerprint() if empty.
  /// @return Finalized table slice.
  auto finish(std::string_view slice_schema_name = {}) -> table_slice;

  /// @brief Calculates the currently occupied rows.
  /// @return count of currently occupied rows.
  auto rows() const -> detail::arrow_length_type;

private:
  auto get_schema(std::string_view slice_schema_name) const -> type;
  auto finish_impl() -> std::shared_ptr<arrow::Array>;

  std::variant<detail::concrete_series_builder<record_type>,
               detail::fixed_fields_record_builder>
    root_builder_;
};

} // namespace vast
