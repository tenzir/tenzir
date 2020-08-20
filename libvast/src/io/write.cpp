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

namespace vast::io {

caf::error write(const path& filename, span<const byte> xs) {
  file f{filename};
  if (!f.open(file::write_only))
    return make_error(ec::filesystem_error, "failed open file");
  size_t bytes_written = 0;
  if (auto err = f.write(xs.data(), xs.size(), &bytes_written))
    return err;
  if (bytes_written != xs.size())
    return make_error(ec::filesystem_error, "incomplete write");
  return caf::none;
}

} // namespace vast::io
