//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/index_config.hpp"
#include "vast/partition_sketch.hpp"
#include "vast/qualified_record_field.hpp"
#include "vast/sketch/builder.hpp"
#include "vast/type.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <unordered_map>

namespace vast {

/// Builds a partition sketch by incrementally processing table slices.
class partition_sketch_builder {
public:
  /// Opaque factory to construct a concrete builder, based on the
  /// configuration.
  using builder_factory = std::function<std::unique_ptr<sketch::builder>()>;

  /// Construct a partion sketch builder from an index configuration.
  /// @param config The index configuration.
  /// @pre *config* is validated.
  explicit partition_sketch_builder(index_config config);

  /// Indexes a table slice.
  /// @param x The table slice to index.
  caf::error add(const table_slice& x);

  /// Creates an immutable partition sketch from the builder state.
  /// @returns The partition sketch.
  caf::expected<partition_sketch> finish();

private:
  /// Factory to create field sketch builders.
  std::unordered_map<std::string, builder_factory> field_factory_;

  /// Factory to create type sketch builders.
  std::unordered_map<type, builder_factory> type_factory_;

  /// Sketches for fields.
  std::unordered_map<qualified_record_field, std::unique_ptr<sketch::builder>>
    field_builders_;

  /// Sketches for types.
  std::unordered_map<type, std::unique_ptr<sketch::builder>> type_builders_;

  index_config config_;
};

} // namespace vast
