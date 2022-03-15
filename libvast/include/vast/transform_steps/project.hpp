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

/// The configuration of a project transform step.
struct project_step_configuration {
  /// The key suffixes of the fields to keep.
  std::vector<std::string> fields = {};

  /// Support type inspection for easy parsing with convertible.
  template <class Inspector>
  friend auto inspect(Inspector& f, project_step_configuration& x) {
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

/// Projects the input onto the specified fields(deletes unspecified fields).
class project_step : public transform_step {
public:
  explicit project_step(project_step_configuration configuration);

  /// Projects an arrow record batch.
  /// @returns The new layout and the projected record batch.
  caf::error
  add(type layout, std::shared_ptr<arrow::RecordBatch> batch) override;
  caf::expected<std::vector<transform_batch>> finish() override;

private:
  /// Adjust the layout according to the projection.
  /// @returns A pair containing the adjusted layout and the indices of the
  ///          columns keep.
  [[nodiscard]] caf::expected<std::pair<vast::type, std::vector<int>>>
  adjust_layout(const vast::type& layout) const;

  /// The slices being transformed.
  std::vector<transform_batch> transformed_;

  /// The underlying configuration of the transformation.
  project_step_configuration config_;
};

} // namespace vast
