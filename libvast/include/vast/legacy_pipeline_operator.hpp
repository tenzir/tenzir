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

/// An individual pipeline operator. This is mainly used in the plugin API,
/// later code deals with a complete `transform`.
class legacy_pipeline_operator {
public:
  virtual ~legacy_pipeline_operator() = default;

  /// Returns true for pipeline operators for which are not incrementally
  /// usable.
  /// @note Operators are assumed to be non-blocking by default.
  [[nodiscard]] virtual bool is_blocking() const {
    return false;
  }

  /// Starts applyings the transformation to a batch with a corresponding vast
  /// schema.
  [[nodiscard]] virtual caf::error add(table_slice slice) = 0;

  /// Retrieves the result of the transformation, resets the internal state.
  /// TODO: add another function abort() to free up internal resources.
  /// NOTE: If there is nothing to transform return an empty vector.
  [[nodiscard]] virtual caf::expected<std::vector<table_slice>> finish() = 0;
};

caf::expected<std::unique_ptr<legacy_pipeline_operator>>
make_pipeline_operator(const std::string& name, const vast::record& options);

} // namespace vast
