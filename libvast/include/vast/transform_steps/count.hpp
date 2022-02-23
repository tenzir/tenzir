//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/transform.hpp"

namespace vast {

// Counts the rows in the input
class count_step : public transform_step {
public:
  count_step() = default;

  /// Count is an aggregate transform step.
  [[nodiscard]] bool is_aggregate() const override {
    return true;
  }

  /// Applies the transformation to a record batch with a corresponding vast
  /// layout.
  [[nodiscard]] caf::error
  add(type layout, std::shared_ptr<arrow::RecordBatch> batch) override;

  /// Retrieves the result of the transformation.
  [[nodiscard]] caf::expected<std::vector<transform_batch>> finish() override;

private:
  size_t count_{};
};

} // namespace vast
