//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/detail/flat_map.hpp"
#include "vast/detail/generator.hpp"
#include "vast/detail/heterogeneous_string_hash.hpp"
#include "vast/fbs/partition_synopsis.hpp"
#include "vast/index_config.hpp"
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
  using builder_factory
    = std::function<std::unique_ptr<sketch::builder>(const type&)>;

  /// Construct a partition sketch builder from an index configuration.
  /// @param config The index configuration.
  /// @returns A partition sketch builder iff the configuration was correct.
  static caf::expected<partition_sketch_builder>
  make(vast::type layout, index_config config);

  /// Indexes a table slice.
  /// @param x The table slice to index.
  caf::error add(const table_slice& x);

  // Fill in the `field_sketches` and `type_sketches` of the partition
  // synopsis. Destroys the partition_sketch_builder.
  caf::error finish_into(partition_synopsis&) &&;

  /// Gets all field extractors.
  /// @returns the list of field extractors for which builders exists.
  detail::generator<std::string_view> fields() const;

  /// Gets all type extractors.
  /// @returns the list of type extractors for which builders exists.
  detail::generator<type> types() const;

private:
  /// Construct a partion sketch builder from an index configuration.
  /// @param config The index configuration.
  /// @pre *config* is validated.
  explicit partition_sketch_builder(index_config config);

  // TODO: Since we know the partition layout a priori we could just
  // create all the relevant builders directly insted of using these
  // factory maps.

  /// Factory to create field sketch builders, mapping field extractors to
  /// builder factories.
  detail::heterogeneous_string_hashmap<builder_factory> field_factory_;

  /// Factory to create type sketch builders.
  detail::flat_map<type, builder_factory> type_factory_;

  /// Sketches for fields, tracked by field extractor.
  std::unordered_map<vast::qualified_record_field,
                     std::unique_ptr<sketch::builder>>
    field_builders_;

  /// Sketches for types, tracked by type name.
  std::unordered_map<type, std::unique_ptr<sketch::builder>> type_builders_;

  index_config config_;

  // TODO: It may make sense to track the following data in a higher-level
  // `partition_synopsis_builder`, so this class can focus solely on building
  // sketches.

  /// Id of the first event.
  uint64_t offset_ = invalid_id;

  // Number of events.
  uint64_t events_ = 0;

  /// The minimum import timestamp of all contained table slices.
  time min_import_time_ = time::max();

  /// The maximum import timestamp of all contained table slices.
  time max_import_time_ = time::min();

  /// The schema of this partition.
  type schema_ = {};
};

} // namespace vast
