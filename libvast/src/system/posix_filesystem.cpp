//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/posix_filesystem.hpp"

#include "vast/chunk.hpp"
#include "vast/detail/assert.hpp"
#include "vast/io/read.hpp"
#include "vast/io/save.hpp"
#include "vast/system/status.hpp"

#include <caf/config_value.hpp>
#include <caf/dictionary.hpp>
#include <caf/result.hpp>
#include <caf/settings.hpp>

#include <filesystem>

namespace vast::system {

filesystem_actor::behavior_type
posix_filesystem(filesystem_actor::stateful_pointer<posix_filesystem_state> self,
                 const std::filesystem::path& root) {
  self->state.root = root;
  return {
    [self](atom::write, const std::filesystem::path& filename,
           chunk_ptr chk) -> caf::result<atom::ok> {
      VAST_ASSERT(chk != nullptr);
      const auto path
        = filename.is_absolute() ? filename : self->state.root / filename;
      if (auto err = io::save(path, as_bytes(chk))) {
        ++self->state.stats.writes.failed;
        return err;
      } else {
        ++self->state.stats.writes.successful;
        self->state.stats.writes.bytes += chk->size();
        return atom::ok_v;
      }
    },
    [self](atom::read,
           const std::filesystem::path& filename) -> caf::result<chunk_ptr> {
      const auto path
        = filename.is_absolute() ? filename : self->state.root / filename;
      std::error_code err;
      if (std::filesystem::exists(path, err)) {
        ++self->state.stats.checks.successful;
      } else {
        ++self->state.stats.checks.failed;
        return caf::make_error(ec::no_such_file, err.message());
      }
      if (auto bytes = io::read(path)) {
        ++self->state.stats.reads.successful;
        self->state.stats.reads.bytes += bytes->size();
        return chunk::make(std::move(*bytes));
      } else {
        ++self->state.stats.reads.failed;
        return bytes.error();
      }
    },
    [self](atom::mmap,
           const std::filesystem::path& filename) -> caf::result<chunk_ptr> {
      const auto path
        = filename.is_absolute() ? filename : self->state.root / filename;
      std::error_code err;
      if (std::filesystem::exists(path, err)) {
        ++self->state.stats.checks.successful;
      } else {
        ++self->state.stats.checks.failed;
        return caf::make_error(ec::no_such_file, err.message());
      }
      if (auto chk = chunk::mmap(path)) {
        ++self->state.stats.mmaps.successful;
        self->state.stats.mmaps.bytes += chk->get()->size();
        return chk;
      } else {
        ++self->state.stats.mmaps.failed;
        return chk.error();
      }
    },
    [self](atom::erase,
           const std::filesystem::path& filename) -> caf::result<atom::done> {
      const auto path
        = filename.is_absolute() ? filename : self->state.root / filename;
      std::error_code err;
      auto size = std::filesystem::file_size(path, err);
      if (err) {
        ++self->state.stats.checks.failed;
        return caf::make_error(ec::no_such_file,
                               fmt::format("{} failed to erase {}: {}", *self,
                                           path, err.message()));
      }
      ++self->state.stats.checks.successful;
      std::filesystem::remove_all(path, err);
      if (err) {
        ++self->state.stats.erases.failed;
        return caf::make_error(ec::system_error,
                               fmt::format("{} failed to erase {}: {}", *self,
                                           path, err.message()));
      }
      ++self->state.stats.erases.successful;
      self->state.stats.erases.bytes += size;
      return atom::done_v;
    },
    [self](atom::status, status_verbosity v) {
      auto result = record{};
      if (v >= status_verbosity::info)
        result["type"] = "POSIX";
      if (v >= status_verbosity::debug) {
        auto& ops = insert_record(result, "operations");
        auto add_stats = [&](auto& name, auto& stats) {
          auto& dict = insert_record(ops, name);
          dict["successful"] = count{stats.successful};
          dict["failed"] = count{stats.failed};
          dict["bytes"] = count{stats.bytes};
        };
        add_stats("checks", self->state.stats.checks);
        add_stats("writes", self->state.stats.writes);
        add_stats("reads", self->state.stats.reads);
        add_stats("mmaps", self->state.stats.mmaps);
        add_stats("erases", self->state.stats.erases);
      }
      return result;
    },
  };
}

} // namespace vast::system
