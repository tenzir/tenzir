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
#include "vast/system/filesystem_statistics.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

/// The state for the POSIX filesystem.
/// @relates posix_filesystem
struct posix_filesystem_state {
  /// Statistics about filesystem operations.
  filesystem_statistics stats;

  /// The filesystem root.
  path root;

  /// The actor name.
  static inline const char* name = "posix-filesystem";
};

/// A filesystem implemented with POSIX system calls.
/// @param self The actor handle.
/// @param root The filesystem root. The actor prepends this path to all
///             operations that include a path parameter.
/// @returns The actor behavior.
filesystem_actor::behavior_type posix_filesystem(
  filesystem_actor::stateful_pointer<posix_filesystem_state> self, path root);

} // namespace vast::system
