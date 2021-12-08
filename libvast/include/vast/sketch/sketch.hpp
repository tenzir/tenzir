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
#include "vast/fbs/sketch.hpp"
#include "vast/flatbuffer.hpp"

#include <cstddef>

namespace vast::sketch {

/// A type-erased sketch.
class sketch {
public:
  /// Constructs a partition sketch from a flatbuffer.
  explicit sketch(flatbuffer<fbs::Sketch> fb) noexcept;

  /// Checks whether the sketch fulfils a given predicate.
  /// @param pred The predicate to check.
  /// @returns A boolean that indicates whether *x* is in the sketch, or
  /// std::nullopt if the query cannot be answered.
  std::optional<bool>
  lookup(relational_operator op, const data& x) const noexcept;

  friend size_t mem_usage(const sketch& x);

private:
  flatbuffer<fbs::Sketch> flatbuffer_;
};

} // namespace vast::sketch
