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

#include "vast/path.hpp"

// clang-format off

// The version number defined here describes the structure, sequence and
// organization of the database directory itself, independent of the binary
// content of the individual files.
//
// Currently we only know one possible layout, as described below.
//
// # Layout v0
//
// vast.db/
//   VERSION                                  - File containing the version of the db layout
//   pid.lock                                 - File containing the PID of the current process
//   index/
//     index.bin                              - Index flatbuffer
//     8bb0caba-eedc-41ce-b8c9-3419e99c9ff3   - Partition flatbuffers
//     [...]
//   archive/
//     segments/
//       d9513a65-9349-4c47-8e6d-4dcecf99327d - Segment flatbuffers
//       [...]
//   importer/
//     current_id_block                       - The persisted current id block.
//   type-registry/
//     type-registry                          - Serialized state of the type
//     registry

// clang-format on

namespace vast {

enum class layout_version : uint8_t {
  invalid,
  v0,
  count, // Number of enum values
};

std::ostream& operator<<(std::ostream& str, const layout_version& version);

/// Read the layout version from a database directory.
layout_version read_layout_version(const vast::path& dbdir);

/// Write the current layout version if `dbdir/VERSION` does not
/// exist yet.
caf::error initialize_layout_version(const vast::path& dbdir);

} // namespace vast
