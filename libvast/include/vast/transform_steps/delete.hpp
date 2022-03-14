//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/transform.hpp"

namespace vast {

/// The configuration of a project transform step.
struct delete_step_configuration {
  /// The key suffixes of the fields to delete.
  std::vector<std::string> fields = {};

  /// Support type inspection for easy parsing with convertible.
  template <class Inspector>
  friend auto inspect(Inspector& f, delete_step_configuration& x) {
    return f(x.fields);
  }

  /// Enable parsing from a record via convertible.
  static inline const record_type& layout() noexcept {
    static auto result = record_type{
      {"fields", list_type{string_type{}}},
    };
    return result;
  }
};

/// Deletes the specifed fields from the input
class delete_step : public transform_step {
public:
  explicit delete_step(delete_step_configuration configuration);

  /// Deletes fields from an arrow record batch.
  caf::error
  add(type layout, std::shared_ptr<arrow::RecordBatch> batch) override;

  /// Retrieves the results of the delete transformation.
  /// @returns The batches with the new layout but without the deleted fields.
  caf::expected<std::vector<transform_batch>> finish() override;

private:
  /// Adjust the layout by deleting columns.
  /// @returns A pair containing the adjusted layout and the indices of the
  ///          columns keep.
  [[nodiscard]] caf::expected<std::pair<vast::type, std::vector<int>>>
  adjust_layout(const vast::type& layout) const;

  /// The slices being transformed.
  std::vector<transform_batch> transformed_;

  /// The underlying configuration of the transformation.
  delete_step_configuration config_;
};

} // namespace vast
