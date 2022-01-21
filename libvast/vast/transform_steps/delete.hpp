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

/// Deletes the specifed fields from the input
class delete_step : public transform_step {
public:
  /// @param fields The key suffixes of the fields to delete.
  delete_step(std::vector<std::string> fields);

  /// Deletes fields from an arrow record batch.
  caf::error
  add(type layout, std::shared_ptr<arrow::RecordBatch> batch) override;

  /// Retrieves the results of the delete transformation.
  /// @returns The batches with the new layout but without the deleted fields.
  caf::expected<batch_vector> finish() override;

private:
  /// Adjust the layout by deleting columns.
  /// @returns A pair containing the adjusted layout and the indices of the
  ///          columns keep.
  [[nodiscard]] caf::expected<std::pair<vast::type, std::vector<int>>>
  adjust_layout(const vast::type& layout) const;

  /// The key suffixes of the fields to delete.
  const std::vector<std::string> fields_;

  /// The slices being transformed.
  batch_vector transformed_;
};

} // namespace vast
