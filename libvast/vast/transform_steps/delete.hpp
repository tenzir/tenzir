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
class delete_step : public generic_transform_step, public arrow_transform_step {
public:
  /// @param fields The key suffixes of the fields to delete.
  delete_step(std::vector<std::string> fields);

  /// Deletes fields from a generic table slice.
  caf::expected<table_slice> operator()(table_slice&& slice) const override;

  /// Deletes fields from an arrow record batch.
  /// @returns The new layout and the record batch without the deleted fields.
  caf::expected<std::pair<type, std::shared_ptr<arrow::RecordBatch>>>
  operator()(type layout,
             std::shared_ptr<arrow::RecordBatch> batch) const override;

private:
  /// Adjust the layout by deleting columns.
  /// @returns A pair containing the adjusted layout and the indices of the
  ///          columns keep.
  [[nodiscard]] caf::expected<std::pair<vast::type, std::vector<int>>>
  adjust_layout(const vast::type& layout) const;

  /// The key suffixes of the fields to delete.
  const std::vector<std::string> fields_;
};

} // namespace vast
