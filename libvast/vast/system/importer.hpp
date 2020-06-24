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

#include "vast/aliases.hpp"
#include "vast/data.hpp"
#include "vast/filesystem.hpp"
#include "vast/system/accountant.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/instrumentation.hpp"
#include "vast/system/type_registry.hpp"

#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>

#include <chrono>
#include <vector>

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

  importer_state(caf::event_based_actor* self_ptr);

  ~importer_state();

  caf::error read_state();

  caf::error write_state();

  void send_report();

  caf::error bump_boundary();

  /// @returns the next unused id and increments the position by its argument.
  id next_id(uint64_t advance);

  /// @returns the number of currently available IDs.
  id available_ids() const noexcept;

  /// @returns various status metrics.
  caf::dictionary<caf::config_value> status() const;

  /// Stores the id offset for the next slice.
  id next_id_ = 0u;

  /// Stores the id block boundary for persisting the id_space.
  /// When next_id_ reaches this value, the boundary gets bumped and synchronized
  /// to the state file on disk. When the importer spawns, it reads the value,
  /// initializes next_id_ with it, and bumps the boundary immediately.
  id id_boundary;

  /// State directory.
  path dir;

  /// The continous stage that moves data from all sources to all subscribers.
  caf::stream_stage_ptr<input_type, downstream_manager> stg;

  /// Pointer to the owning actor.
  caf::event_based_actor* self;

  measurement measurement_;
  stopwatch::time_point last_report;

  /// Stores all actor handles of connected INDEX actors.
  std::vector<caf::actor> index_actors;

  accountant_type accountant;

  /// Name of this actor in log events.
  static inline const char* name = "importer";
};

using importer_actor = caf::stateful_actor<importer_state>;

/// Spawns an IMPORTER.
/// @param self The actor handle.
/// @param dir The directory for persistent state.
/// @param max_table_slice_size The suggested maximum size for table slices.
/// @param archive A handle to the ARCHIVE.
/// @param index A handle to the INDEX.
/// @param batch_size The initial number of IDs to request when replenishing.
/// @param type_registry A handle to the type-registry module.
caf::behavior importer(importer_actor* self, path dir, archive_type archive,
                       caf::actor index, type_registry_type type_registry);

} // namespace vast::system
