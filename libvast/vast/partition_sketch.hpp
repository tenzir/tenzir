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
#include "vast/sketch/builder.hpp"

#include <caf/variant.hpp>

#include <cstdint>
#include <map>
#include <unordered_map>

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

  friend size_t mem_usage(const partition_sketch& x);

private:
  chunk_ptr flatbuffer_;
};

/// TODO: move into separate file
/// Builds a partition sketch by incrementally processing table slices.
class partition_sketch_builder {
public:
  explicit partition_sketch_builder(data config);

  /// Indexes a table slice.
  /// @param slice The table slice to index.
  caf::error add(const table_slice& slice);

  /// Creates an immutable partition sketch from the builder state.
  /// @returns The partition sketch.
  caf::expected<partition_sketch> finish();

  friend size_t mem_usage(const partition_sketch_builder& x);

private:
  /// Retrieves the set of relevant builders for a given table slice column.
  // std::vector<sketch::builder*> builders(record_type::field_view f);

  /// Hasher for the type builder unordered map.
  // static size_t type_hash(const type& x);

  /// Sketches for types.
  // std::unordered_map<type, std::unique_ptr<sketch::builder>, type_hash>
  //   type_builders_;

  /// Sketches for field suffixes.
  std::map<std::string, std::unique_ptr<sketch::builder>> field_builders_;
};

} // namespace vast
