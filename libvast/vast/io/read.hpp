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

#include "vast/byte.hpp"
#include "vast/fwd.hpp"
#include "vast/span.hpp"

#include <vector>

#include "caf/fwd.hpp"

namespace vast::io {

/// Reads bytes from a file into a buffer in one shot.
/// @param filename The file to read from.
/// @param xs The buffer to write into.
/// @returns An error if the operation failed.
caf::error read(const path& filename, span<byte> xs);

/// Reads bytes from a file into a buffer in one shot.
/// @param filename The file to read from.
/// @returns The raw bytes of the buffer.
caf::expected<std::vector<byte>> read(const path& filename);

} // namespace vast::io
