//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include <cstdint>

namespace vast::sketch {

class sketch;

/// An builder to construct a sketch from table slices.
/// @relates sketch
class builder {
public:
  virtual ~builder() = default;

  /// Adds a provided field of a table slice to the builder. Can be called
  /// multiple times for the same table slice.
  /// @param slice The table slice to index.
  /// @param off The offset corresponding to a field in the slice.
  /// @returns An error on failure.
  virtual caf::error add(table_slice slice, offset off) = 0;

  /// Finalizes the builder and constructs a sketch from it.
  /// @returns The sketch according to the current builder state.
  virtual caf::expected<sketch> finish() = 0;
};

} // namespace vast::sketch
