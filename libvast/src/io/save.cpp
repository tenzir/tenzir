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

#include "vast/io/save.hpp"

#include "vast/error.hpp"
#include "vast/io/write.hpp"
#include "vast/path.hpp"

#include <cstdio>

namespace vast::io {

caf::error save(const path& filename, span<const byte> xs) {
  auto tmp = filename + ".tmp";
  if (auto err = write(tmp, xs)) {
    rm(tmp);
    return err;
  }
  if (std::rename(tmp.str().c_str(), filename.str().c_str()) != 0) {
    rm(tmp);
    return make_error(ec::filesystem_error, "rename(2)", std::strerror(errno));
  }
  return caf::none;
}

} // namespace vast::io
