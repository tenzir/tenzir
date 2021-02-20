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

#include "vast/io/read.hpp"

#include "vast/error.hpp"
#include "vast/file.hpp"
#include "vast/path.hpp"

#include <cstddef>

namespace vast::io {

caf::error read(const path& filename, span<std::byte> xs) {
  file f{filename};
  if (!f.open(file::read_only))
    return caf::make_error(ec::filesystem_error, "failed open file");
  auto bytes_read = f.read(xs.data(), xs.size());
  if (!bytes_read)
    return bytes_read.error();
  if (*bytes_read != xs.size())
    return caf::make_error(ec::filesystem_error, "incomplete read of",
                           filename.str());
  return caf::none;
}

caf::expected<std::vector<std::byte>> read(const path& filename) {
  auto size = file_size(filename);
  if (!size)
    return size.error();
  std::vector<std::byte> buffer(*size);
  if (auto err = read(filename, span<std::byte>{buffer}))
    return err;
  return buffer;
}

} // namespace vast::io
