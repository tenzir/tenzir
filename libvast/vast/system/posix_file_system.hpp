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

#include "vast/filesystem.hpp"
#include "vast/system/file_system.hpp"

#include <caf/stateful_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

/// The state for the POSIX filesystem.
/// @relates posix_file_system
struct posix_file_system_state {
  /// The actor name.
  static inline const char* name = "posix-file-system";
};

/// A filesystem implemented with POSIX system calls.
/// @param self The actor handle.
/// @param root The filesystem root.
/// @returns The actor behavior.
file_system_type::behavior_type posix_file_system(
  file_system_type::stateful_pointer<posix_file_system_state> self, path root);

} // namespace vast::system
