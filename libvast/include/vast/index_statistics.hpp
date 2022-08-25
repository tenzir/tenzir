//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/detail/heterogeneous_string_hash.hpp"

namespace vast {

/// Accumulates statistics for a given layout.
struct layout_statistics {
  uint64_t count = 0ull; ///< Number of events indexed.

  template <class Inspector>
  friend auto inspect(Inspector& f, layout_statistics& x) {
    f.object(x)
      .pretty_name("layout_statistics")
      .fields(f.field("count", x.count));
  }
};

/// Accumulates statistics about indexed data.
struct index_statistics {
  /// The number of events for a given layout.
  detail::heterogeneous_string_hashmap<layout_statistics> layouts;

  void merge_inplace(const index_statistics& other) {
    for (auto const& [field, layout] : other.layouts)
      layouts[field].count += layout.count;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, index_statistics& x) {
    f.object(x)
      .pretty_name("index_statistics")
      .fields(f.field("layouts", x.layouts));
  }
};

} // namespace vast
