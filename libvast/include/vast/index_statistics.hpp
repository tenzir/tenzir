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

/// Accumulates statistics for a given schema.
struct schema_statistics {
  uint64_t count = 0ull; ///< Number of events indexed.

  template <class Inspector>
  friend auto inspect(Inspector& f, schema_statistics& x) {
    f.object(x)
      .pretty_name("schema_statistics")
      .fields(f.field("count", x.count));
  }
};

/// Accumulates statistics about indexed data.
struct index_statistics {
  /// The number of events for a given schema.
  detail::heterogeneous_string_hashmap<schema_statistics> schemas;

  void merge_inplace(const index_statistics& other) {
    for (auto const& [field, schema] : other.schemas)
      schemas[field].count += schema.count;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, index_statistics& x) {
    f.object(x)
      .pretty_name("index_statistics")
      .fields(f.field("schemas", x.schemas));
  }
};

} // namespace vast
