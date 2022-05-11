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
  using builder_factory
    = std::function<std::unique_ptr<sketch::builder>(const type&)>;

  /// Construct a partition sketch builder from an index configuration.
  /// @param config The index configuration.
  /// @returns A partition sketch builder iff the configuration was correct.
  // FIXME: Pass the type of the partition here, so that we only create builders
  // for fields and types that will actually appear.
  static caf::expected<partition_sketch_builder>
  make(vast::type layout, index_config config);

  /// Indexes a table slice.
  /// @param x The table slice to index.
  caf::error add(const table_slice& x);

  // Fill in the `field_sketches` and `type_sketches` of the partition
  // synopsis. Destroys the builder.
  // FIXME: It would probably be better to pass in a flatbuffer builder
  // here so we don't have to copy around the sketches so much.
  caf::error finish_into(partition_synopsis&) &&;

  /// Creates an immutable partition sketch from the builder state.
  /// @returns The partition sketch.
  // caf::expected<partition_sketch> finish();

  /// Checks whether the partition sketch fulfils an expression.
  /// @param expr The expression to check.
  /// @returns A probability in [0,1] that indicates the relevancy of this
  /// partition for *expr*. The semantics are as follows:
  /// - `= 0: guaranteed no results
  /// - 1: guaranteed results
  /// -
  /// and 1 means definitely results. A value between 0 and 1 means "maybe".
  // double lookup(const expression& expr) const noexcept;

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

  // TODO: Not completely sure if we even need the `{field,type}_factory_` maps.

  /// Factory to create field sketch builders, mapping field extractors to
  /// builder factories.
  detail::heterogeneous_string_hashmap<builder_factory> field_factory_;

  /// Factory to create type sketch builders.
  // detail::heterogeneous_string_hashmap<builder_factory> type_factory_;
  detail::flat_map<type, builder_factory> type_factory_;

  /// Sketches for fields, tracked by field extractor.
  // detail::heterogeneous_string_hashmap<std::unique_ptr<sketch::builder>>
  //   field_builders_;
  // TODO: Does it make more sense to keep string as a key and change
  // the other code instead?
  std::unordered_map<vast::qualified_record_field,
                     std::unique_ptr<sketch::builder>>
    field_builders_;

  /// Sketches for types, tracked by type name.
  std::unordered_map<type, std::unique_ptr<sketch::builder>> type_builders_;

  index_config config_;
};

} // namespace vast
