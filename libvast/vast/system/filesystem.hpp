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
#include "vast/status.hpp"

#include <caf/fwd.hpp>
#include <caf/replies_to.hpp>
#include <caf/typed_actor.hpp>

#include <cstdint>

namespace vast::system {

/// Statistics about filesystem operations.
struct filesystem_statistics {
  struct ops_statistics {
    uint64_t successful = 0;
    uint64_t failed = 0;
    uint64_t bytes = 0;
  };

  ops_statistics writes;
  ops_statistics reads;
  ops_statistics mmaps;
};

// clang-format off

/// The interface for file system I/O. The filesystem actor implementation must
/// interpret all operations that contain paths *relative* to its own root
/// directory.
using filesystem_type = caf::typed_actor<
  // Writes a chunk of data to a given path. Creates intermediate directories
  // if needed.
  caf::replies_to<atom::write, path, chunk_ptr>::with<atom::ok>,
  // Reads a chunk of data from a given path and returns the chunk.
  caf::replies_to<atom::read, path>::with<chunk_ptr>,
  // Memory-maps a file.
  caf::replies_to<atom::mmap, path>::with<chunk_ptr>,
  // Reports statistics of filesystem interactions.
  caf::replies_to<atom::status, status_verbosity>::with<caf::dictionary<caf::config_value>>
>;

// clang-format on

} // namespace vast::system
