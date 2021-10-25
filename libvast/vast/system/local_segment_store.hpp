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
#include "vast/segment_builder.hpp"
#include "vast/system/actors.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

/// The STORE BUILDER actor interface.
using local_store_actor
  = typed_actor_fwd<caf::reacts_to<atom::internal, atom::persist>>::extend_with<
    store_builder_actor>::unwrap;

struct active_store_state {
  /// Defaulted constructor to make this a non-aggregate.
  active_store_state() = default;

  /// A pointer to the hosting actor.
  //  We intentionally store a strong pointer here: The store lifetime is
  //  ref-counted, it should exit after all currently active queries for this
  //  store have finished, its partition has dropped out of the cache, and it
  //  received all data from the incoming stream. This pointer serves to keep
  //  the ref-count alive for the last part, and is reset after the data has
  //  been written to disk.
  local_store_actor self = {};

  /// Actor handle of the filesystem actor.
  filesystem_actor fs = {};

  /// The path to where the store will be written.
  std::filesystem::path path = {};

  /// The builder preparing the store.
  //  TODO: Store a vector<table_slice> here and implement
  //  store/lookup/.. on that.
  std::unique_ptr<vast::segment_builder> builder = {};

  /// The serialized segment.
  std::optional<vast::segment> segment = {};

  /// Number of events in this store.
  size_t events = {};

  /// The name for this type of CAF actor.
  static constexpr const char* name = "active_local_store";
};

struct passive_store_state {
  /// Defaulted constructor to make this a non-aggregate.
  passive_store_state() = default;

  /// Holds requests that did arrive while the segment data
  /// was still being loaded from disk.
  using request
    = std::tuple<vast::query, caf::typed_response_promise<atom::done>>;
  std::vector<request> deferred_requests = {};

  /// The actor handle of the filesystem actor.
  filesystem_actor fs = {};

  /// The path where the segment is stored.
  std::filesystem::path path = {};

  /// The segment corresponding to this local store.
  caf::optional<vast::segment> segment = {};

  /// The name for this type of CAF actor.
  static constexpr const char* name = "passive_local_store";

private:
  caf::result<atom::done> erase(const vast::ids&);
};

std::filesystem::path store_path_for_partition(const vast::uuid&);

local_store_actor::behavior_type
active_local_store(local_store_actor::stateful_pointer<active_store_state>,
                   filesystem_actor fs, const std::filesystem::path& path);

store_actor::behavior_type
passive_local_store(store_actor::stateful_pointer<passive_store_state>,
                    filesystem_actor fs, const std::filesystem::path& path);

} // namespace vast::system
