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

#include "vast/system/posix_filesystem.hpp"

#include "vast/chunk.hpp"
#include "vast/detail/assert.hpp"
#include "vast/io/read.hpp"
#include "vast/io/save.hpp"

#include <caf/config_value.hpp>
#include <caf/dictionary.hpp>
#include <caf/result.hpp>
#include <caf/settings.hpp>

namespace vast::system {

filesystem_type::behavior_type
posix_filesystem(filesystem_type::stateful_pointer<posix_filesystem_state> self,
                 path root) {
  return {
    [=](atom::write, const path& filename,
        chunk_ptr chk) -> caf::result<atom::ok> {
      VAST_ASSERT(chk != nullptr);
      auto path = filename.is_absolute() ? filename : root / filename;
      if (auto err = io::save(path, as_bytes(chk))) {
        ++self->state.stats.writes.failed;
        return err;
      } else {
        ++self->state.stats.writes.successful;
        ++self->state.stats.writes.bytes += chk->size();
        return atom::ok_v;
      }
    },
    [=](atom::read, const path& filename) -> caf::result<chunk_ptr> {
      auto path = filename.is_absolute() ? filename : root / filename;
      if (auto bytes = io::read(path)) {
        ++self->state.stats.reads.successful;
        ++self->state.stats.reads.bytes += bytes->size();
        return chunk::make(std::move(*bytes));
      } else {
        ++self->state.stats.reads.failed;
        return bytes.error();
      }
    },
    [=](atom::mmap, const path& filename) -> caf::result<chunk_ptr> {
      auto path = filename.is_absolute() ? filename : root / filename;
      if (auto chk = chunk::mmap(path)) {
        ++self->state.stats.mmaps.successful;
        ++self->state.stats.mmaps.bytes += chk->size();
        return chk;
      } else {
        ++self->state.stats.mmaps.failed;
        return nullptr;
      }
    },
    [=](atom::status, status_verbosity v) {
      auto result = caf::settings{};
      if (v >= status_verbosity::info)
        caf::put(result, "filesystem.type", "POSIX");
      if (v >= status_verbosity::debug) {
        auto& ops = put_dictionary(result, "filesystem.operations");
        auto add_stats = [&](auto& name, auto& stats) {
          auto& dict = put_dictionary(ops, name);
          caf::put(dict, "successful", stats.successful);
          caf::put(dict, "failed", stats.failed);
          caf::put(dict, "bytes", stats.bytes);
        };
        add_stats("writes", self->state.stats.writes);
        add_stats("reads", self->state.stats.reads);
        add_stats("mmaps", self->state.stats.mmaps);
      }
      return result;
    },
  };
}

} // namespace vast::system
