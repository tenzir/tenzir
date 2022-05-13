//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/detail/flat_map.hpp"
#include "vast/index_statistics.hpp"
#include "vast/segment_builder.hpp"
#include "vast/system/active_partition.hpp"
#include "vast/system/actors.hpp"

#include <caf/typed_event_based_actor.hpp>

#include <unordered_map>
#include <variant>
#include <vector>

namespace vast::system {

/// Helper class used to route table slices to the correct store.
struct partition_transformer_selector {
  bool
  operator()(const vast::type& filter, const vast::table_slice& column) const;
};

/// Similar to the active partition, but all contents come in a single
/// stream, a transform is applied and no queries need to be answered
/// while the partition is constructed.
struct partition_transformer_state {
  static constexpr const char* name = "partition-transformer";

  struct stream_data {
    caf::expected<std::vector<std::tuple<vast::uuid, vast::type, chunk_ptr>>>
      partition_chunks = caf::no_error;
    caf::expected<std::vector<std::tuple<vast::uuid, chunk_ptr>>> synopsis_chunks
      = caf::no_error;
  };

  struct path_data {
    caf::typed_response_promise<std::vector<augmented_partition_synopsis>> promise
      = {};
  };

  partition_transformer_state() = default;

  void add_slice(const table_slice& slice);
  void fulfill(
    partition_transformer_actor::stateful_pointer<partition_transformer_state>
      self,
    stream_data&&, path_data&&) const;

  /// Actor handle of the actor (usually the importer) where we reserve new ids
  /// for the transformed data.
  idspace_distributor_actor idspace_distributor = {};

  /// Actor handle of the type registry.
  type_registry_actor type_registry = {};

  /// Actor handle of the accountant.
  accountant_actor accountant = {};

  /// Actor handle of the store builder for this partition.
  detail::flat_map<type, store_builder_actor> store_builders = {};

  /// Actor handle of the filesystem actor.
  filesystem_actor fs = {};

  /// The transform to be applied to the data.
  transform_ptr transform = {};

  /// The stream stage to send table slices to the store(s).
  //  TODO: Use a specialized downstream manager that has
  //  a map from layout to store.
  using partition_transformer_stream_stage_ptr = caf::stream_stage_ptr<
    table_slice, caf::broadcast_downstream_manager<
                   table_slice, vast::type, partition_transformer_selector>>;

  partition_transformer_stream_stage_ptr stage = {};

  /// Cached stream error, if the stream terminated abnormally.
  caf::error stream_error = {};

  /// Cached transform error, if the transform returns one.
  caf::error transform_error = {};

  /// Cached table slices in this partition.
  std::vector<table_slice> slices = {};

  /// The maximum number of events per partition. (not really necessary, but
  /// required by the partition synopsis)
  size_t partition_capacity = 0ull;

  /// Total number of rows in all transformed `slices`.
  size_t events = 0ull;

  /// Number of rows per event type in the input and output.
  index_statistics stats_in;
  index_statistics stats_out;

  /// Oldest import timestamp of the input data.
  vast::time min_import_time = {};

  /// Newest import timestamp of the input data.
  vast::time max_import_time = {};

  /// The data of the newly created partition(s).
  detail::flat_map<type, active_partition_state::serialization_data> data = {};

  /// Stores the value index for each field.
  // Fields with a `#skip` attribute are stored as `nullptr`.
  using value_index_map
    = detail::stable_map<qualified_record_field, value_index_ptr>;
  detail::flat_map<vast::type, value_index_map> indexers = {};

  /// Store id for partitions.
  std::string store_id;

  /// Options for creating new synopses.
  index_config synopsis_opts = {};

  /// Options for creating new value indices.
  caf::settings index_opts = {};

  // Two format strings that can be formatted with a `vast::uuid`
  // as the single parameter. They give the
  std::string partition_path_template;
  std::string synopsis_path_template;

  /// The actor waits until both the stream is finished and an `atom::persist`
  /// has arrived. Depending on what happens first, a different set of
  /// variables need to be stored in the meantime.
  std::variant<std::monostate, stream_data, path_data> persist;

  /// The original import times of the added slices. The pointers to the `data`
  /// members of the slices are the identifier keys.
  std::unordered_map<const std::byte*, vast::time> original_import_times = {};
};

/// Spawns a PARTITION TRANSFORMER actor with the given parameters.
/// This actor
partition_transformer_actor::behavior_type partition_transformer(
  partition_transformer_actor::stateful_pointer<partition_transformer_state>,
  std::string store_id, const index_config& synopsis_opts,
  const caf::settings& index_opts, accountant_actor accountant,
  idspace_distributor_actor idspace_distributor,
  type_registry_actor type_registry, filesystem_actor fs,
  transform_ptr transform, std::string partition_path_template,
  std::string synopsis_path_template);

} // namespace vast::system
