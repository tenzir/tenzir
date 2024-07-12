//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/aliases.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/heterogeneous_string_hash.hpp"
#include "tenzir/instrumentation.hpp"
#include "tenzir/table_slice.hpp"

#include <caf/broadcast_downstream_manager.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <caf/typed_response_promise.hpp>

#include <chrono>
#include <filesystem>
#include <vector>

namespace tenzir {

/// Receives chunks from SOURCEs, imbues them with an ID, and relays them to
///  INDEX and continuous queries.
struct importer_state {
  // -- member types -----------------------------------------------------------

  /// Used to signal how much information should be persisted in write_state().
  enum class write_mode : bool {
    /// Persist the next assignable id, used during a regular shutdown.
    with_next,
    /// Persist only the end of the block, used during regular operation to
    /// prevent state corruption if an irregular shutdown occurs.
    without_next
  };

  /// A helper structure to partition the id space into blocks.
  /// An importer uses one currently active block.
  struct id_block {
    /// The next available id of this block.
    id next;

    /// The last + 1 id of this block.
    id end;
  };

  explicit importer_state(importer_actor::pointer self);

  ~importer_state();

  void send_report();

  /// @returns various status metrics.
  [[nodiscard]] caf::typed_response_promise<record>
  status(status_verbosity v) const;

  void on_process(const table_slice& slice);

  /// The active id block.
  id_block current;

  /// The continous stage that moves data from all sources to all subscribers.
  caf::stream_stage_ptr<table_slice,
                        caf::broadcast_downstream_manager<table_slice>>
    stage;

  /// Pointer to the owning actor.
  importer_actor::pointer self;

  std::string inbound_description = "anonymous";

  std::unordered_map<caf::inbound_path*, std::string> inbound_descriptions;

  measurement measurement_ = {};
  stopwatch::time_point last_report = {};
  detail::heterogeneous_string_hashmap<uint64_t> schema_counters = {};

  /// The index actor.
  index_actor index;

  accountant_actor accountant;

  /// A list of subscribers for incoming events.
  std::vector<std::pair<receiver_actor<table_slice>, bool /*internal*/>>
    subscribers = {};

  /// Name of this actor in log events.
  static inline const char* name = "importer";
};

/// Spawns an IMPORTER.
/// @param self The actor handle.
/// @param dir The directory for persistent state.
/// @param index A handle to the INDEX.
/// @param accountant A handle to the ACCOUNTANT.
/// @param batch_size The initial number of IDs to request when replenishing.
importer_actor::behavior_type
importer(importer_actor::stateful_pointer<importer_state> self,
         const std::filesystem::path& dir, index_actor index,
         accountant_actor accountant);

} // namespace tenzir
