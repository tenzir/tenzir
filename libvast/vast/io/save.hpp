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

#include "vast/fwd.hpp"
#include "vast/span.hpp"

#include "caf/fwd.hpp"

#include <cstddef>

namespace vast::io {

/// Writes a buffer to a temporary file and atomically renames it to *filename*
/// afterwards.
/// @param filename The file to write to.
/// @param xs The buffer to read from.
/// @returns An error if the operation failed.
[[nodiscard]] caf::error save(const path& filename, span<const std::byte> xs);

} // namespace vast::io
