
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

#include "vast/system/posix_file_system.hpp"

#include "vast/chunk.hpp"
#include "vast/io/read.hpp"
#include "vast/io/write.hpp"

#include <caf/config_value.hpp>
#include <caf/dictionary.hpp>
#include <caf/result.hpp>

namespace vast::system {

file_system_type::behavior_type posix_file_system(
  file_system_type::stateful_pointer<posix_file_system_state> self, path root) {
  return {
    [=](atom::write, const path& filename,
        chunk_ptr chk) -> caf::result<atom::ok> {
      if (auto err = io::write(root / filename, as_bytes(chk)))
        return err;
      return atom::ok_v;
    },
    [=](atom::read, const path& filename) -> caf::result<chunk_ptr> {
      if (auto bytes = io::read(root / filename))
        return chunk::make(std::move(*bytes));
      else
        return bytes.error();
    },
    [=](atom::mmap, const path& filename) {
      return chunk::mmap(root / filename);
    },
    [=](atom::status) {
      caf::dictionary<caf::config_value> result;
      // TODO: fill in reads, writes, mmaps, etc.
      return result;
    },
  };
}

} // namespace vast::system
