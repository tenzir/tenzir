//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/generator.hpp"
#include "tenzir/resource.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/uuid.hpp"

#include <caf/typed_event_based_actor.hpp>

#include <variant>

namespace tenzir {

/// A base class for store implementations that provides shared functionality
/// between passive and active stores.
class base_store {
public:
  virtual ~base_store() noexcept = default;

  /// Retrieve the slices of the store.
  /// @returns The contained slices.
  [[nodiscard]] virtual generator<table_slice> slices() const = 0;

  /// Retrieve the number of contained events.
  /// @returns The number of rows in all contained slices.
  [[nodiscard]] virtual uint64_t num_events() const = 0;

  /// Retrieve the schema associated with the data in the store.
  /// @returns The Tenzir schema of the stored data.
  [[nodiscard]] virtual type schema() const;

  /// Execute a count query against the store.
  /// @param expr The expression to filter events.
  /// @param selection Pre-filtered ids to consider.
  /// @return The results of applying the count query to each table slice.
  [[nodiscard]] virtual generator<uint64_t>
  count(expression expr, ids selection) const;

  /// Execute an extract query against the store.
  /// @param expr The expression to filter events.
  /// @param selection Pre-filtered ids to consider.
  /// @return The results of applying the extract query to each table slice.
  [[nodiscard]] virtual generator<table_slice>
  extract(expression expr, ids selection) const;
};

/// A base class for passive stores used by the store plugin.
class passive_store : public base_store {
public:
  /// Load the store contents from the given chunk.
  /// @param chunk The chunk pointing to the store's persisted data.
  /// @returns An error on failure.
  [[nodiscard]] virtual caf::error load(chunk_ptr chunk) = 0;
};

/// A base class for active stores used by the store plugin.
class active_store : public base_store {
public:
  /// Add a set of slices to the store.
  /// @returns An error on failure.
  [[nodiscard]] virtual caf::error add(std::vector<table_slice> slices) = 0;

  /// Persist the store contents to a contiguous buffer.
  /// @returns A chunk containing the serialized store contents, or an error on
  /// failure.
  [[nodiscard]] virtual caf::expected<chunk_ptr> finish() = 0;
};

/// Shared state for in-flight queries for both count and extract operations.
template <class ResultType>
struct base_query_state {
  /// Generator producing results per stored table slice.
  generator<ResultType> result_generator = {};
  /// Iterator for result of processing current table lsice.
  typename generator<ResultType>::iterator result_iterator = {};
  /// Aggregator for number of matching events.
  uint64_t num_hits = {};
  /// Actor to send the final / intermediate results to.
  receiver_actor<ResultType> sink = {};
  /// Start time for metrics tracking.
  std::chrono::steady_clock::time_point start
    = std::chrono::steady_clock::now();
};

/// Keeps track of all relevant state for an in-progress count query.
struct count_query_state : public base_query_state<uint64_t> {};

/// Keeps track of all relevant state for an in-progress extract query.
struct extract_query_state : public base_query_state<table_slice> {};

/// The state of the default passive store actor implementation.
struct default_passive_store_state {
  static constexpr auto name = "passive-store";

  default_passive_store_actor::pointer self = {};
  filesystem_actor filesystem = {};
  std::unique_ptr<passive_store> store = {};
  std::filesystem::path path = {};
  std::string store_type = {};

  std::unordered_map<uuid, extract_query_state> running_extractions = {};
  std::unordered_map<uuid, count_query_state> running_counts = {};
};

/// Spawns a store actor for a passive store.
/// @param self A pointer to the hosting actor.
/// @param store The passive store to use.
/// @param filesystem A handle to the filesystem actor.
/// @param path The path to load the store from.
/// @param store_type The unique store identifier of the used store plugin.
default_passive_store_actor::behavior_type default_passive_store(
  default_passive_store_actor::stateful_pointer<default_passive_store_state>
    self,
  std::unique_ptr<passive_store> store, filesystem_actor filesystem,
  std::filesystem::path path, std::string store_type);

/// The state of the default active store actor implementation.
struct default_active_store_state {
  static constexpr auto name = "active-store";

  std::variant<std::monostate, resource, caf::typed_response_promise<resource>>
    file = {};
  default_active_store_actor::pointer self = {};
  filesystem_actor filesystem = {};
  std::unique_ptr<active_store> store = {};
  std::filesystem::path path = {};
  std::string store_type = {};
  std::unordered_map<uuid, extract_query_state> running_extractions = {};
  std::unordered_map<uuid, count_query_state> running_counts = {};
  bool erased = false;
};

/// Spawns a store builder actor for an active store.
/// @param self A pointer to the hosting actor.
/// @param store The active store to use.
/// @param filesystem A handle to the filesystem actor.
/// @param path The path to persist the store at.
/// @param store_type The unique store identifier of the used store plugin.
default_active_store_actor::behavior_type default_active_store(
  default_active_store_actor::stateful_pointer<default_active_store_state> self,
  std::unique_ptr<active_store> store, filesystem_actor filesystem,
  std::filesystem::path path, std::string store_type);

} // namespace tenzir
