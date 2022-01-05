//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/transform.hpp"

#include <unordered_set>
namespace vast {

/// Projects the input onto the specified fields(deletes unspecified fields).
class project_step : public transform_step {
public:
  /// @param fields The key suffixes of the fields to keep.
  explicit project_step(std::vector<std::string> fields);

  /// Projects an arrow record batch.
  /// @returns The new layout and the projected record batch.
  caf::error add(vast::id offset, type layout,
                 std::shared_ptr<arrow::RecordBatch> batch) override;
  caf::expected<batch_vector> finish() override;

private:
  /// Adjust the layout according to the projection.
  /// @returns A pair containing the adjusted layout and the indices of the
  ///          columns keep.
  [[nodiscard]] caf::expected<std::pair<vast::type, std::vector<int>>>
  adjust_layout(const vast::type& layout) const;

  /// The key suffixes of the fields to keep.
  const std::vector<std::string> fields_;
  batch_vector transformed_;
};

} // namespace vast
