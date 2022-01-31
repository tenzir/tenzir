//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/chunk.hpp"

namespace vast {

/// A sparse index for a partition.
class partition_sketch {
public:
  /// Constructs a partition sketch from a flatbuffer.
  explicit partition_sketch(chunk_ptr flatbuffer) noexcept;

  /// Checks whether the partition for this sketch should be considered for a
  /// given predicate.
  /// @returns The probability that *pred* yields results in the partition for
  /// this sketch.
  double lookup(const predicate& pred);

  friend size_t mem_usage(const partition_sketch& x) noexcept;

private:
  chunk_ptr flatbuffer_;
};

} // namespace vast
