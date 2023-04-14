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

#include <arrow/record_batch.h>

namespace vast {

class adaptive_table_slice_builder {
public:
  struct row_guard {
  public:
    explicit row_guard(detail::concrete_series_builder<record_type>& builder)
      : builder_{builder}, starting_fields_length_{builder_.length()} {
    }

    auto cancel() -> void {
      auto current_rows = builder_.length();
      if (auto row_added = current_rows > starting_fields_length_; row_added) {
        builder_.remove_last_row();
      }
    }

    auto push_field(std::string_view field_name) -> detail::field_guard {
      return {builder_.get_field_builder_provider(field_name,
                                                  starting_fields_length_),
              starting_fields_length_};
    }

    ~row_guard() noexcept {
      builder_.fill_nulls();
    }

  private:
    detail::concrete_series_builder<record_type>& builder_;
    detail::arrow_length_type starting_fields_length_ = 0;
  };

  /// @brief Inserts a row to the output table slice.
  /// @return An object used to manipulate fields of an inserted row. The
  /// returned object must be destroyed beforore calling this method again.
  auto push_row() -> row_guard {
    return row_guard{root_builder_};
  }

  /// @brief Combines all the pushed rows into a table slice.
  /// @return Finalized table slice.
  auto finish() && -> table_slice {
    const auto final_array = std::move(root_builder_).finish();
    if (not final_array)
      return table_slice{};
    auto schema = root_builder_.type();
    auto schema_name = schema.make_fingerprint();
    auto slice_schema = vast::type{std::move(schema_name), std::move(schema)};
    const auto& struct_array
      = static_cast<const arrow::StructArray&>(*final_array);
    const auto batch
      = arrow::RecordBatch::Make(slice_schema.to_arrow_schema(),
                                 struct_array.length(), struct_array.fields());
    VAST_ASSERT(batch);
    return table_slice{batch, std::move(slice_schema)};
  }

  /// @brief Calculates the currently occupied rows.
  /// @return count of currently occupied rows.
  auto rows() const -> detail::arrow_length_type {
    return root_builder_.length();
  }

private:
  detail::concrete_series_builder<record_type> root_builder_;
};

} // namespace vast
