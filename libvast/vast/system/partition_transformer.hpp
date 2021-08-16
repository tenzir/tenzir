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

#include <vector>

namespace vast::system {

/// Similar to the active partition, but all contents come in a single
/// stream, a transform is applied and no queries need to be answered
/// while the partition is constructed.
struct partition_transformer_state {
  partition_transformer_state() = default;

  void add_slice(const table_slice& slice);
  void finalize_data();
  void fulfill(
    partition_transformer_actor::stateful_pointer<partition_transformer_state>
      self);

  idspace_distributor_actor importer = {};
  store_builder_actor store_builder = {};
  filesystem_actor fs = {};
  transform_ptr transform = {};
  caf::stream_stage_ptr<table_slice,
                        caf::broadcast_downstream_manager<table_slice>>
    stage = {};

  std::vector<table_slice> slices = {};
  size_t events = 0ull; // total number of rows in `slices`.
  active_partition_state::serialization_data data = {};
  // field -> [index, has_skip_attribute]
  detail::stable_map<qualified_record_field, std::pair<value_index_ptr, bool>>
    indexers = {};
  caf::settings synopsis_opts = {};
  caf::settings index_opts = {};

  // if stream finishes first
  chunk_ptr partition_chunk = {};
  chunk_ptr synopsis_chunk = {};
  caf::error error = {};
  // if atom::persist arrives first
  caf::typed_response_promise<std::shared_ptr<partition_synopsis>> promise = {};
  std::filesystem::path partition_path = {};
  std::filesystem::path synopsis_path = {};
};

partition_transformer_actor::behavior_type partition_transformer(
  partition_transformer_actor::stateful_pointer<partition_transformer_state>,
  uuid id, std::string store_id, const caf::settings& synopsis_opts,
  const caf::settings& index_opts, idspace_distributor_actor importer,
  filesystem_actor fs, transform_ptr transform);

} // namespace vast::system
