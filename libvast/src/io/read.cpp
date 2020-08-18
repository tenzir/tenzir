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

namespace vast::io {

caf::error read(const path& filename, span<byte> xs) {
  file f{filename};
  if (!f.open(file::read_only))
    return make_error(ec::filesystem_error, "failed open file");
  size_t bytes_read;
  auto ptr = reinterpret_cast<char*>(xs.data());
  if (!f.read(ptr, xs.size(), &bytes_read))
    return make_error(ec::filesystem_error, "failed to read chunk");
  if (bytes_read != xs.size())
    return make_error(ec::filesystem_error, "incomplete read");
  return caf::none;
}

caf::expected<std::vector<byte>> read(const path& filename) {
  auto size = file_size(filename);
  if (!size)
    return size.error();
  std::vector<byte> buffer(*size);
  if (auto err = read(filename, span<byte>{buffer}))
    return err;
  return buffer;
}

} // namespace vast::io
