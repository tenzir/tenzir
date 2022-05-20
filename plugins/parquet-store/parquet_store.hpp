//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/data.hpp"

#include <vast/chunk.hpp>
#include <vast/error.hpp>
#include <vast/fwd.hpp>
#include <vast/plugin.hpp>
#include <vast/system/actors.hpp>
#include <vast/type.hpp>
#include <vast/uuid.hpp>

#include <arrow/table.h>
#include <arrow/type_fwd.h>
#include <caf/typed_event_based_actor.hpp>

namespace vast::plugins::parquet_store {

struct store_builder_state {
  static constexpr const char* name = "active-parquet-store";
  uuid id_ = {};
  system::store_builder_actor::pointer self_ = {};

  /// Actor handle of the accountant.
  system::accountant_actor accountant_ = {};

  /// Actor handle of the filesystem.
  system::filesystem_actor fs_ = {};

  /// The table slices added to this partition.
  std::vector<table_slice> table_slices_ = {};

  /// The layout of the first record batch.
  std::optional<type> vast_type_;
  /// Number of events in this store.
  size_t num_rows_ = {};
};

struct store_state {
  static constexpr const char* name = "passive-parquet-store";
  uuid id_ = {};
  system::store_actor::pointer self_ = {};

  std::shared_ptr<arrow::Table> table_ = {};

  std::filesystem::path path_ = {};

  /// Actor handle of the accountant.
  system::accountant_actor accountant_ = {};

  /// Actor handle of the filesystem.
  system::filesystem_actor fs_ = {};

  /// Holds requests that did arrive while the segment data
  /// was still being loaded from disk.
  using request
    = std::tuple<vast::query, caf::typed_response_promise<uint64_t>>;
  std::vector<request> deferred_requests_ = {};
};

system::store_builder_actor::behavior_type store_builder(
  system::store_builder_actor::stateful_pointer<store_builder_state> self,
  system::accountant_actor accountant, system::filesystem_actor fs,
  const uuid& id);

system::store_actor::behavior_type
store(system::store_actor::stateful_pointer<store_state> self,
      const system::accountant_actor& accountant,
      const system::filesystem_actor& fs, const uuid& id);

/// The plugin entrypoint for the parquet store plugin.
class plugin final : public store_plugin {
public:
  /// Initializes the aggregate plugin. This plugin has no general
  /// configuration, and is configured per instantiation as part of the
  /// transforms definition. We only check whether there's no unexpected
  /// configuration here.
  caf::error initialize(data options) override;

  [[nodiscard]] const char* name() const override;

  /// Create a store builder actor that accepts incoming table slices.
  /// @param accountant The actor handle of the accountant.
  /// @param fs The actor handle of a filesystem.
  /// @param id The partition id for which we want to create a store. Can
  ///           be used as a unique key by the implementation.
  /// @returns A store_builder actor and a chunk called the "header". The
  ///          contents of the header will be persisted on disk, and should
  ///          allow the plugin to retrieve the correct store actor when
  ///          `make_store()` below is called.
  [[nodiscard]] caf::expected<builder_and_header>
  make_store_builder(system::accountant_actor accountant,
                     system::filesystem_actor fs,
                     const vast::uuid& id) const override;

  /// Create a store actor from the given header. Called when deserializing a
  /// partition that uses this partition as a store backend.
  /// @param accountant The actor handle the accountant.
  /// @param fs The actor handle of a filesystem.
  /// @param header The store header as found in the partition flatbuffer.
  /// @returns A new store actor.
  [[nodiscard]] caf::expected<system::store_actor>
  make_store(system::accountant_actor accountant, system::filesystem_actor fs,
             std::span<const std::byte> header) const override;
};

} // namespace vast::plugins::parquet_store
