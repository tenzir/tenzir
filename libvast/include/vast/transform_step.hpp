//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/table_slice.hpp"

#include <arrow/record_batch.h>
#include <caf/expected.hpp>

#include <queue>

namespace vast {

struct transform_batch {
  transform_batch(type layout, std::shared_ptr<arrow::RecordBatch> batch)
    : layout(std::move(layout)), batch(std::move(batch)) {
  }
  vast::type layout;
  std::shared_ptr<arrow::RecordBatch> batch;
};

/// An individual transform step. This is mainly used in the plugin API,
/// later code deals with a complete `transform`.
class transform_step {
public:
  virtual ~transform_step() = default;

  /// Returns true for aggregate transform steps.
  /// @note Transform steps are not aggregate by default.
  [[nodiscard]] virtual bool is_aggregate() const {
    return false;
  }

  /// Starts applyings the transformation to a batch with a corresponding vast
  /// layout.
  [[nodiscard]] virtual caf::error
  add(type layout, std::shared_ptr<arrow::RecordBatch> batch)
    = 0;

  /// Retrieves the result of the transformation, resets the internal state.
  /// TODO: add another function abort() to free up internal resources.
  /// NOTE: If there is nothing to transform return an empty vector.
  [[nodiscard]] virtual caf::expected<std::vector<transform_batch>> finish()
    = 0;
};

caf::expected<std::unique_ptr<transform_step>>
make_transform_step(const std::string& name, const vast::record& options);

} // namespace vast
