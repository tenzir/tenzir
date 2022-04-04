//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/expression.hpp"
#include "vast/transform.hpp"

namespace vast {

/// The configuration of a project transform step.
struct select_step_configuration {
  // The expression in the config file.
  std::string expression;

  // Whether to select or to filter.
  bool invert = false;

  /// Support type inspection for easy parsing with convertible.
  template <class Inspector>
  friend auto inspect(Inspector& f, select_step_configuration& x) {
    return f(x.expression, x.invert);
  }

  /// Enable parsing from a record via convertible.
  static inline const record_type& layout() noexcept {
    static auto result = record_type{
      {"expression", string_type{}},
      {"invert", bool_type{}},
    };
    return result;
  }
};

// Selects matching rows from the input
class select_step : public transform_step {
public:
  explicit select_step(select_step_configuration configuration);

  /// Applies the transformation to a record batch with a corresponding vast
  /// layout.
  [[nodiscard]] caf::error
  add(type layout, std::shared_ptr<arrow::RecordBatch> batch) override;

  /// Retrieves the result of the transformation.
  [[nodiscard]] caf::expected<std::vector<transform_batch>> finish() override;

private:
  caf::expected<vast::expression> expression_;

  /// Whether to select or to filter.
  bool invert_ = false;

  /// The slices being transformed.
  std::vector<transform_batch> transformed_;
};

} // namespace vast
