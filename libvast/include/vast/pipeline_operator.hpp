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

struct pipeline_batch {
  pipeline_batch(type layout, std::shared_ptr<arrow::RecordBatch> batch)
    : layout(std::move(layout)), batch(std::move(batch)) {
  }
  vast::type layout;
  std::shared_ptr<arrow::RecordBatch> batch;
};

/// An individual pipeline operator. This is mainly used in the plugin API,
/// later code deals with a complete `transform`.
class pipeline_operator {
public:
  virtual ~pipeline_operator() = default;

  /// Returns true for aggregate pipeline operators.
  /// @note pipeline operators are not aggregate by default.
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
  [[nodiscard]] virtual caf::expected<std::vector<pipeline_batch>> finish() = 0;
};

caf::expected<std::unique_ptr<pipeline_operator>>
make_pipeline_operator(const std::string& name, const vast::record& options);

std::pair<std::string_view::iterator,
          caf::expected<std::unique_ptr<pipeline_operator>>>
parse_pipeline_operator(std::string_view name, std::string_view str);

} // namespace vast
