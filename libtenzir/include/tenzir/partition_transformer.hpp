//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/active_partition.hpp"
#include "tenzir/actors.hpp"

#include <caf/typed_event_based_actor.hpp>

#include <unordered_map>
#include <variant>
#include <vector>

namespace tenzir {

/// Similar to the active partition, but all contents come in a single
/// stream, a transform is applied and no queries need to be answered
/// while the partition is constructed.
struct partition_transformer_state {
  static constexpr const char* name = "partition-transformer";

  using result_type = std::vector<partition_synopsis_pair>;
  using promise_type = caf::typed_response_promise<result_type>;
  using partition_tuple = std::tuple<tenzir::uuid, tenzir::type, chunk_ptr>;
  using synopsis_tuple = std::tuple<tenzir::uuid, chunk_ptr>;

  struct stream_data {
    caf::expected<std::vector<partition_tuple>> partition_chunks = caf::error{};
    caf::expected<std::vector<synopsis_tuple>> synopsis_chunks = caf::error{};
  };

  struct path_data {
    promise_type promise = {};
  };

  partition_transformer_state() = default;

  // Update the `type_ids` map with the information of the given slice.
  void
  update_type_ids_and_indexers(std::unordered_map<std::string, ids>& type_ids,
                               const tenzir::uuid& partition_id,
                               const table_slice& slice);

  // Returns the partition in which to insert this slice, maybe creating a new
  // partition.
  active_partition_state::serialization_data&
  create_or_get_partition(const table_slice& slice);

  void fulfill(
    partition_transformer_actor::stateful_pointer<partition_transformer_state>
      self,
    stream_data&&, path_data&&) const;

  /// Actor handle of the catalog.
  catalog_actor catalog = {};

  /// Actor handle of the filesystem actor.
  filesystem_actor fs = {};

  /// The transform to be applied to the data.
  pipeline transform = {};

  /// Collector for the received table slices.
  std::vector<table_slice> input = {};

  /// Cached stream error, if the stream terminated abnormally.
  caf::error stream_error = {};

  /// Cached transform error, if the transform returns one.
  caf::error transform_error = {};

  /// The maximum number of events per partition. (not really necessary, but
  /// required by the partition synopsis)
  size_t partition_capacity = 0ull;

  /// Total number of rows in all transformed `slices`.
  size_t events = 0ull;

  /// Oldest import timestamp of the input data.
  tenzir::time min_import_time = tenzir::time::max();

  /// Newest import timestamp of the input data.
  tenzir::time max_import_time = tenzir::time::min();

  /// The data of the newly created partition(s).
  std::multimap<type, active_partition_state::serialization_data> data = {};

  /// Auxiliary data required to create the final partition flatbuffer.
  struct buildup {
    /// The store builder.
    store_builder_actor builder = {};

    /// Cached table slices in this partition.
    std::vector<table_slice> slices = {};

    /// Stores the value index for each field.
    // Fields with a `#skip` attribute are stored as `nullptr`.
    using value_index_map
      = detail::stable_map<qualified_record_field, value_index_ptr>;
    value_index_map indexers = {};
  };

  std::unordered_map<uuid, buildup> partition_buildup;

  /// Store id for partitions.
  std::string store_id;

  /// Options for creating new synopses.
  index_config synopsis_opts = {};

  /// Options for creating new value indices.
  caf::settings index_opts = {};

  // Two format strings that can be formatted with a `tenzir::uuid`
  // as the single parameter. They give the
  std::string partition_path_template;
  std::string synopsis_path_template;

  /// The actor waits until both the stream is finished and an `atom::persist`
  /// has arrived. Depending on what happens first, a different set of
  /// variables need to be stored in the meantime.
  std::variant<std::monostate, stream_data, path_data> persist;

  /// Number of stores launched and finished.
  size_t stores_launched = 0ull;
  size_t stores_finished = 0ull;

  struct stores_are_finished {};
  struct transformer_is_finished {
    promise_type promise;
    result_type result;
  };

  /// This actor shuts down when both all stores it spawned have shut down,
  /// and its own result is ready.
  std::variant<std::monostate, stores_are_finished, transformer_is_finished>
    shutdown_state;
};

/// Spawns a PARTITION TRANSFORMER actor with the given parameters.
/// This actor
auto partition_transformer(
  partition_transformer_actor::stateful_pointer<partition_transformer_state>,
  std::string store_id, const index_config& synopsis_opts,
  const caf::settings& index_opts, catalog_actor catalog, filesystem_actor fs,
  pipeline transform, std::string partition_path_template,
  std::string synopsis_path_template)
  -> partition_transformer_actor::behavior_type;

} // namespace tenzir
