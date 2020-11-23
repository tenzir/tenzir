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

namespace vast {

/// This version number defines compatibility of persistent state with with
/// prior directory layouts and contents. Breaking changes shall bump the
/// version number. A breaking change includes the structure, sequence and
/// organization of the database directory itself, as well irreconcilable
/// changes in binary content.
enum class layout_version : uint8_t {
  invalid,
  v0,
  v1,
  count, // Number of enum values
};

std::ostream& operator<<(std::ostream& str, const layout_version& version);

/// Read the layout version from a database directory.
layout_version read_layout_version(const vast::path& dbdir);

/// Write the current layout version if `dbdir/VERSION` does not
/// exist yet.
caf::error initialize_layout_version(const vast::path& dbdir);

} // namespace vast
