//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/detail/generator.hpp"
#include "vast/system/actors.hpp"
#include "vast/table_slice.hpp"
#include "vast/uuid.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace vast {

/// A base class for store implementations that provides shared functionality
/// between passive and active stores.
class base_store {
public:
  virtual ~base_store() noexcept = default;

  /// Retrieve the slices of the store.
  /// @returns The contained slices.
  [[nodiscard]] virtual detail::generator<table_slice> slices() const = 0;

  /// Retrieve the number of contained events.
  /// @returns The number of rows in all contained slices.
  [[nodiscard]] virtual uint64_t num_events() const = 0;

  /// Retrieve the schema associated with the data in the store.
  /// @returns The VAST schema of the stored data.
  [[nodiscard]] virtual type schema() const;

  /// Execute a count query against the store.
  /// @param expr The expression to filter events.
  /// @param selection Pre-filtered ids to consider.
  /// @return
  [[nodiscard]] virtual detail::generator<uint64_t>
  count(const expression& expr, const ids& selection) const;

  /// Execute an extract query against the store.
  /// @param expr The expression to filter events.
  /// @param selection Pre-filtered ids to consider.
  [[nodiscard]] virtual detail::generator<table_slice>
  extract(const expression& expr, const ids& selection) const;
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

struct count_query_state {
  detail::generator<uint64_t> count_generator = {};
  detail::generator<uint64_t>::iterator count_iterator = {};
  uint64_t num_hits = {};
  system::receiver_actor<uint64_t> sink = {};
  std::chrono::steady_clock::time_point start
    = std::chrono::steady_clock::now();
};

struct extract_query_state {
  detail::generator<table_slice> extract_generator = {};
  detail::generator<table_slice>::iterator extract_iterator = {};
  uint64_t num_hits = {};
  system::receiver_actor<table_slice> sink = {};
  std::chrono::steady_clock::time_point start = {};
};

/// The state of the default passive store actor implementation.
struct default_passive_store_state {
  static constexpr auto name = "passive-store";

  system::default_passive_store_actor::pointer self = {};
  system::filesystem_actor filesystem = {};
  system::accountant_actor accountant = {};
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
/// @param accountant A handle to the accountant actor.
/// @param path The path to load the store from.
/// @param store_type The unique store identifier of the used store plugin.
system::default_passive_store_actor::behavior_type
default_passive_store(system::default_passive_store_actor::stateful_pointer<
                        default_passive_store_state>
                        self,
                      std::unique_ptr<passive_store> store,
                      system::filesystem_actor filesystem,
                      system::accountant_actor accountant,
                      std::filesystem::path path, std::string store_type);

/// The state of the default active store actor implementation.
struct default_active_store_state {
  static constexpr auto name = "active-store";

  system::default_active_store_actor::pointer self = {};
  system::filesystem_actor filesystem = {};
  system::accountant_actor accountant = {};
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
/// @param accountant A handle to the accountant actor.
/// @param path The path to persist the store at.
/// @param store_type The unique store identifier of the used store plugin.
system::default_active_store_actor::behavior_type default_active_store(
  system::default_active_store_actor::stateful_pointer<default_active_store_state>
    self,
  std::unique_ptr<active_store> store, system::filesystem_actor filesystem,
  system::accountant_actor accountant, std::filesystem::path path,
  std::string store_type);

} // namespace vast
