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

#include "vast/fwd.hpp"

#include "vast/path.hpp"
#include "vast/system/actors.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

struct disk_monitor_state {
  /// The path to the database directory.
  path dbdir;

  /// When current disk space is above the high water mark, stuff
  /// is deleted until we get below the low water mark.
  size_t high_water_mark;
  size_t low_water_mark;

  /// Whether an erasing run is currently in progress.
  bool purging;

  /// Node handle of the ARCHIVE.
  archive_actor archive;

  /// Node handle of the INDEX.
  index_actor index;

  constexpr static const char* name = "disk_monitor";
};

/// Periodically scans the size of the database directory and deletes data
/// once it exceeds some threshold.
/// @param self The actor handle.
/// @param high_water Start erasing data if this limit is exceeded.
/// @param low_water Erase until this limit is no longer exceeded.
/// @param db_dir The path to the database directory.
/// @param archive The actor handle of the ARCHIVE.
/// @param index The actor handle of the INDEX.
disk_monitor_actor::behavior_type
disk_monitor(disk_monitor_actor::stateful_pointer<disk_monitor_state> self,
             size_t high_water, size_t low_water,
             std::chrono::seconds scan_interval, const path& db_dir,
             archive_actor archive, index_actor index);

} // namespace vast::system
