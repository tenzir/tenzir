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

// Does nothing with the input.
class identity_step : public transform_step {
public:
  identity_step() = default;

  caf::error
  add(type layout, std::shared_ptr<arrow::RecordBatch> batch) override;
  caf::expected<std::vector<transform_batch>> finish() override;

private:
  /// The slices being transformed.
  std::vector<transform_batch> transformed_;
};

} // namespace vast
