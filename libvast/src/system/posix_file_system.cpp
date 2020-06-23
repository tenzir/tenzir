
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
#include <caf/settings.hpp>

namespace vast::system {

file_system_type::behavior_type posix_file_system(
  file_system_type::stateful_pointer<posix_file_system_state> self, path root) {
  return {
    [=](atom::write, const path& filename,
        chunk_ptr chk) -> caf::result<atom::ok> {
      if (auto err = io::write(root / filename, as_bytes(chk))) {
        ++self->state.stats.writes.failed;
        return err;
      } else {
        ++self->state.stats.writes.successful;
        ++self->state.stats.writes.bytes += chk->size();
        return atom::ok_v;
      }
    },
    [=](atom::read, const path& filename) -> caf::result<chunk_ptr> {
      if (auto bytes = io::read(root / filename)) {
        ++self->state.stats.reads.successful;
        ++self->state.stats.reads.bytes += bytes->size();
        return chunk::make(std::move(*bytes));
      } else {
        ++self->state.stats.reads.failed;
        return bytes.error();
      }
    },
    [=](atom::mmap, const path& filename) -> caf::result<chunk_ptr> {
      if (auto chk = chunk::mmap(root / filename)) {
        ++self->state.stats.mmaps.successful;
        ++self->state.stats.mmaps.bytes += chk->size();
        return chk;
      } else {
        ++self->state.stats.mmaps.failed;
        return nullptr;
      }
    },
    [=](atom::status) {
      caf::dictionary<caf::config_value> result;
      result["type"] = "POSIX";
      auto& ops = put_dictionary(result, "operations");
      auto put = [&](auto& name, auto& stats) {
        auto& dict = put_dictionary(ops, name);
        caf::put(dict, "successful", stats.successful);
        caf::put(dict, "failed", stats.failed);
        caf::put(dict, "bytes", stats.bytes);
      };
      put("writes", self->state.stats.writes);
      put("reads", self->state.stats.reads);
      put("mmaps", self->state.stats.mmaps);
      return result;
    },
  };
}

} // namespace vast::system
