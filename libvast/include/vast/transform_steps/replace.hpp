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
struct replace_step_configuration {
  std::string field;
  std::string value;

  /// Support type inspection for easy parsing with convertible.
  template <class Inspector>
  friend auto inspect(Inspector& f, replace_step_configuration& x) {
    return f(x.field, x.value);
  }

  /// Enable parsing from a record via convertible.
  static inline const record_type& layout() noexcept {
    static auto result = record_type{
      {"field", string_type{}},
      {"value", string_type{}},
    };
    return result;
  }
};

class replace_step : public transform_step {
public:
  explicit replace_step(replace_step_configuration configuration,
                        vast::data value);

  /// Projects an arrow record batch.
  /// @returns The new layout and the projected record batch.
  caf::error
  add(type layout, std::shared_ptr<arrow::RecordBatch> batch) override;
  caf::expected<std::vector<transform_batch>> finish() override;

private:
  vast::data value_;

  /// The slices being transformed.
  std::vector<transform_batch> transformed_;

  /// The underlying configuration of the transformation.
  replace_step_configuration config_;
};

} // namespace vast
