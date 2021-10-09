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

/// The Taffy Cuckoo Filter (TCF).
class taffy_cuckoo_filter {
public:
  using digest_type = uint64_t;

  /// Constructs a filter with a given number of bytes.
  /// @param m The number of bytes the filter shall occupy.
  taffy_cuckoo_filter(size_t m);

  ~taffy_cuckoo_filter();

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
