//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <memory>

namespace vast {

/// The Taffy Block Filter (TBF).
class taffy_block_filter {
public:
  using digest_type = uint64_t;

  /// Constructs a filter for a given number of elements and false-positive
  /// probability.
  /// @param n The initial number of elements to support.
  /// @param p The false-positive probability.
  /// @pre `n > 0 && 0 < p < 1`
  taffy_block_filter(uint64_t n, double p);

  ~taffy_block_filter();

  /// Adds a hash digest.
  /// @param x The digest to add.
  void add(digest_type x);

  /// Adds a hash digest.
  /// @param x The digest to add.
  bool lookup(digest_type x);

private:
  struct impl;
  std::unique_ptr<impl> impl_;
};

} // namespace vast
