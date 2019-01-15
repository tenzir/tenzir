/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <chrono>
#include <vector>

#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>

#include "vast/aliases.hpp"
#include "vast/data.hpp"
#include "vast/filesystem.hpp"

#include "vast/system/accountant.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/consensus.hpp"
#include "vast/system/instrumentation.hpp"

namespace vast::system {

/// Receives chunks from SOURCEs, imbues them with an ID, and relays them to
/// ARCHIVE, INDEX and continuous queries.
struct importer_state {
  // -- member types -----------------------------------------------------------

  /// Type of incoming stream elements.
  using input_type = table_slice_ptr;

  /// Type of outgoing stream elements.
  using output_type = table_slice_ptr;

  /// Stream object for managing downstream actors.
  using downstream_manager = caf::broadcast_downstream_manager<output_type>;

  /// Base type for stream drivers implementing the importer.
  using driver_base = caf::stream_stage_driver<input_type, downstream_manager>;

  /// A simple generator for IDs.
  struct id_generator {
    /// The next available ID.
    id i;

    /// The first unavailable ID.
    id last;

    id_generator(id from, id to) : i(from), last(to) {
      // nop
    }

    /// @returns whether this generator is exhausted.
    bool at_end() const noexcept {
      return i == last;
    }

    /// @returns the next ID and advances the position in the range.
    id next(size_t num = 1) noexcept {
      VAST_ASSERT(static_cast<size_t>(remaining()) >= num);
      auto result = i;
      i += num;
      return result;
    }

    /// @returns how many more times `next` returns a valid ID.
    int32_t remaining() const noexcept {
      return static_cast<int32_t>(last - i);
    }
  };

  importer_state(caf::event_based_actor* self_ptr);

  ~importer_state();

  caf::error read_state();

  caf::error write_state();

  void send_report();

  /// Handle to the consensus module for obtaining more IDs.
  consensus_type consensus;

  /// Stores currently available IDs.
  std::vector<id_generator> id_generators;

  /// @returns the number of currently available IDs.
  int32_t available_ids() const noexcept;

  /// @returns the first ID for an ID block of size `max_table_slice_size`.
  /// @pre `available_ids() >= max_table_slice_size`
  id next_id_block();

  /// @returns various status metrics.
  caf::dictionary<caf::config_value> status() const;

  /// Stores how many slices inbound paths can still send us.
  int32_t in_flight_slices = 0;

  /// User-configured maximum for table slice sizes. This is the granularity
  /// for credit generation (each received slice consumes that many IDs).
  int32_t max_table_slice_size;

  /// Number of ID blocks we acquire per replenish, e.g., setting this to 10
  /// will acquire `max_table_slize * 10` IDs per replenish.
  size_t blocks_per_replenish = 100;

  /// Stores when we received new IDs for the last time.
  std::chrono::steady_clock::time_point last_replenish;

  /// State directory.
  path dir;

  /// Stores whether we've contacted the consensus module to obtain more IDs.
  bool awaiting_ids = false;

  /// The continous stage that moves data from all sources to all subscribers.
  caf::stream_stage_ptr<input_type, downstream_manager> stg;

  /// Pointer to the owning actor.
  caf::event_based_actor* self;

  /// List of actors that wait for the next flush event.
  std::vector<caf::actor> flush_listeners;

  measurement measurement_;
  stopwatch::time_point last_report;

  accountant_type accountant;

  /// Name of this actor in log events.
  static inline const char* name = "importer";
};

using importer_actor = caf::stateful_actor<importer_state>;

/// Spawns an IMPORTER.
/// @param self The actor handle.
/// @param dir The directory for persistent state.
/// @param batch_size The initial number of IDs to request when replenishing.
caf::behavior importer(importer_actor* self, path dir,
                       size_t max_table_slice_size);

} // namespace vast::system
