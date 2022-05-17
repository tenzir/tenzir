//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/fwd.hpp>
#include <vast/system/actors.hpp>
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

  /// The record batches added to this partition
  arrow::RecordBatchVector record_batches_ = {};

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

} // namespace vast::plugins::parquet_store
