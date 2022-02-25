//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include <arrow/type_fwd.h>

#include <cstdint>
#include <memory>

namespace vast::sketch {

class sketch;

/// An builder to construct a sketch from table slices.
/// @relates sketch
class builder {
public:
  virtual ~builder() = default;

  /// Adds all values from an array to the builder.
  /// @param xs The elements to add.
  /// @returns An error on failure.
  virtual caf::error add(const std::shared_ptr<arrow::Array>& xs) = 0;

  /// Finalizes the builder and constructs a sketch from it.
  /// @returns The sketch according to the current builder state.
  virtual caf::expected<sketch> finish() = 0;
};

} // namespace vast::sketch
