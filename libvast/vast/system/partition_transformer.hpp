//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/segment_builder.hpp"
#include "vast/system/active_partition.hpp"
#include "vast/system/actors.hpp"

#include <caf/typed_event_based_actor.hpp>

#include <variant>
#include <vector>

namespace vast::system {

/// Similar to the active partition, but all contents come in a single
/// stream, a transform is applied and no queries need to be answered
/// while the partition is constructed.
struct partition_transformer_state {
  struct persist_eagerly {
    chunk_ptr partition_chunk = {};
    chunk_ptr synopsis_chunk = {};
    caf::error error = {};
  };

  struct persist_lazily {
    std::filesystem::path partition_path = {};
    std::filesystem::path synopsis_path = {};
    caf::typed_response_promise<std::shared_ptr<partition_synopsis>> promise
      = {};
  };

  partition_transformer_state() = default;

  void add_slice(const table_slice& slice);
  void finalize_data();
  void fulfill(
    partition_transformer_actor::stateful_pointer<partition_transformer_state>
      self,
    persist_eagerly&&, persist_lazily&&) const;

  /// Actor handle of the importer to be able to reserve new ids
  /// for the transformed data.
  idspace_distributor_actor importer = {};

  /// Actor handle of the store builder for this partition.
  store_builder_actor store_builder = {};

  /// Actor handle of the filesystem actor.
  filesystem_actor fs = {};

  /// The transform to be applied to the data.
  transform_ptr transform = {};

  /// The stream stage to send table slices to the store.
  caf::stream_stage_ptr<table_slice,
                        caf::broadcast_downstream_manager<table_slice>>
    stage = {};

  /// Cached stream error, if the stream terminated abnormally.
  caf::error stream_error = {};

  /// Cached table slices in this partition.
  std::vector<table_slice> slices = {};

  /// Total number of rows in `slices`.
  size_t events = 0ull;

  /// The data of the newly created partition.
  active_partition_state::serialization_data data = {};

  /// Stores the value index for each field.
  // Fields with a `#skip` attribute are stored as `nullptr`.
  detail::stable_map<qualified_record_field, value_index_ptr> indexers = {};

  /// Options for creating new synopses.
  caf::settings synopsis_opts = {};

  /// Options for creating new value indices.
  caf::settings index_opts = {};

  /// The actor waits until both the stream is finished and an `atom::persist`
  /// has arrived. Depending on which one arrives first, a different set of
  /// variables need to be remembered in the meantime.
  std::variant<std::monostate, persist_eagerly, persist_lazily> persist;
};

partition_transformer_actor::behavior_type partition_transformer(
  partition_transformer_actor::stateful_pointer<partition_transformer_state>,
  uuid id, std::string store_id, const caf::settings& synopsis_opts,
  const caf::settings& index_opts, idspace_distributor_actor importer,
  filesystem_actor fs, transform_ptr transform);

} // namespace vast::system
