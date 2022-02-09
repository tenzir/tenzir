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

// Aggregates the input suricate flow
class aggregate_suricata_flow_step : public transform_step {
public:
  aggregate_suricata_flow_step(vast::duration bucket_size)
    : bucket_size_(bucket_size) {
  }

  /// Aggregate Suricate Flow is an aggregate transform step.
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
  /// The slices being transformed.
  std::deque<transform_batch> to_transform_;

  /// The duration of buckets to create time groups.
  vast::duration bucket_size_;
};

} // namespace vast
