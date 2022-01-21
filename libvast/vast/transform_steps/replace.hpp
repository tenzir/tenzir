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

class replace_step : public transform_step {
public:
  replace_step(const std::string& fieldname, const vast::data& value);

  /// Projects an arrow record batch.
  /// @returns The new layout and the projected record batch.
  caf::error
  add(type layout, std::shared_ptr<arrow::RecordBatch> batch) override;
  caf::expected<batch_vector> finish() override;

private:
  std::string field_;
  vast::data value_;

  /// The slices being transformed.
  batch_vector transformed_;
};

} // namespace vast
