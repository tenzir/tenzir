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

#include "vast/io/write.hpp"

#include "vast/error.hpp"
#include "vast/file.hpp"
#include "vast/path.hpp"

#include <cstddef>

namespace vast::io {

caf::error write(const path& filename, span<const std::byte> xs) {
  file f{filename};
  if (!f.open(file::write_only))
    return caf::make_error(ec::filesystem_error, "failed open file");
  return f.write(xs.data(), xs.size());
}

caf::error
write(const std::filesystem::path& filename, span<const std::byte> xs) {
  return write(vast::path{filename.string()}, xs);
}

} // namespace vast::io
