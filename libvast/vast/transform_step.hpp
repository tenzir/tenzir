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

using arrow_batch
  = std::tuple<vast::id, vast::type, std::shared_ptr<arrow::RecordBatch>>;
using batch_queue = std::deque<arrow_batch>;
using slice_queue = std::deque<table_slice>;
using batch_vector = std::vector<arrow_batch>;

/// An individual transform step. This is mainly used in the plugin API,
/// later code deals with a complete `transform`.
class transform_step {
public:
  virtual ~transform_step() = default;

  /// Starts applyings the transformation to a batch with a corresponding vast
  /// layout.
  [[nodiscard]] virtual caf::error
  add(vast::id offset, type layout, std::shared_ptr<arrow::RecordBatch> batch)
    = 0;

  /// Retrieves the result of the transformation, resets the internal state.
  /// TODO: add another function abort() to free up internal resources.
  /// NOTE: If there is nothing to transform return an empty vector.
  [[nodiscard]] virtual caf::expected<batch_vector> finish() = 0;
};

using transform_step_ptr = std::unique_ptr<transform_step>;

caf::expected<transform_step_ptr>
make_transform_step(const std::string& name, const caf::settings& opts);

} // namespace vast
