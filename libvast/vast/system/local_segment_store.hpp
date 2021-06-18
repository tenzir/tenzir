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

struct active_store_state {
  /// The path to where the store will be written.
  std::filesystem::path path;

  /// The builder preparing the store.
  //  TODO: Just store a vector<table_slice> here and implement
  //  store/lookup/.. on that.
  std::unique_ptr<vast::segment_builder> builder;

  /// Number of events in this store.
  size_t events = {};

  /// Expected total size.
  std::optional<size_t> total = {};
};

struct passive_store_state {
  /// Holds requests that did arrive while the segment data
  /// was still being loaded from disk.
  using request = std::tuple<vast::query, vast::ids,
                             caf::typed_response_promise<atom::done>>;
  std::vector<request> deferred_requests;

  /// The actor handle of the filesystem actor.
  filesystem_actor fs;

  /// The path where the segment is stored.
  std::filesystem::path path;

  /// The segment corresponding to this local store.
  caf::optional<vast::segment> segment = {};

private:
  caf::result<atom::done> erase(const vast::ids&);
};

std::filesystem::path store_path_for_partition(const vast::uuid&);

store_builder_actor::behavior_type
active_local_store(store_builder_actor::stateful_pointer<active_store_state>,
                   filesystem_actor fs, const std::filesystem::path& path);

store_actor::behavior_type
passive_local_store(store_actor::stateful_pointer<passive_store_state>,
                    filesystem_actor fs, const std::filesystem::path& path);

} // namespace vast::system
