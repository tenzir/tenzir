//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/posix_filesystem.hpp"

#include "vast/chunk.hpp"
#include "vast/io/read.hpp"
#include "vast/io/save.hpp"
#include "vast/system/report.hpp"
#include "vast/system/status.hpp"

#include <caf/config_value.hpp>
#include <caf/detail/set_thread_name.hpp>
#include <caf/dictionary.hpp>
#include <caf/result.hpp>
#include <caf/settings.hpp>

#include <filesystem>

namespace vast::system {

caf::expected<atom::done>
posix_filesystem_state::rename_single_file(const std::filesystem::path& from,
                                           const std::filesystem::path& to) {
  const auto from_absolute = root / from;
  const auto to_absolute = root / to;
  if (from_absolute == to_absolute)
    return atom::done_v;
  std::error_code err;
  std::filesystem::rename(from, to, err);
  if (err) {
    ++stats.moves.failed;
    return caf::make_error(ec::system_error,
                           fmt::format("failed to move {} to {}: {}", from, to,
                                       err.message()));
  }
  ++stats.moves.successful;
  return atom::done_v;
}

filesystem_actor::behavior_type posix_filesystem(
  filesystem_actor::stateful_pointer<posix_filesystem_state> self,
  std::filesystem::path root, const accountant_actor& accountant) {
  if (self->getf(caf::local_actor::is_detached_flag))
    caf::detail::set_thread_name("vast.posix-filesystem");
  self->state.root = std::move(root);
  if (accountant) {
    self->state.accountant = accountant;
    self->send(self, atom::telemetry_v);
  }
  return {
    [self](atom::write, const std::filesystem::path& filename,
           const chunk_ptr& chk) -> caf::result<atom::ok> {
      const auto path
        = filename.is_absolute() ? filename : self->state.root / filename;
      if (chk == nullptr)
        return caf::make_error(ec::invalid_argument,
                               fmt::format("{} tried to write a nullptr to {}",
                                           *self, path));
      if (auto err = io::save(path, as_bytes(chk))) {
        ++self->state.stats.writes.failed;
        return err;
      }
      ++self->state.stats.writes.successful;
      self->state.stats.writes.bytes += chk->size();
      return atom::ok_v;
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
        return caf::make_error(ec::no_such_file,
                               fmt::format("{} no such file: {}", *self, path));
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
    [self](atom::move, const std::filesystem::path& from,
           const std::filesystem::path& to) -> caf::result<atom::done> {
      return self->state.rename_single_file(from, to);
    },
    [self](
      atom::move,
      const std::vector<std::pair<std::filesystem::path, std::filesystem::path>>&
        files) -> caf::result<atom::done> {
      std::error_code err;
      for (const auto& [from, to] : files) {
        auto result = self->state.rename_single_file(from, to);
        if (!result)
          return result.error();
      }
      return atom::done_v;
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
        return caf::make_error(ec::no_such_file,
                               fmt::format("{} {}: {}", *self, path,
                                           err.message()));
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
        auto ops = record{};
        auto add_stats = [&](auto& name, auto& stats) {
          auto dict = record{};
          dict["successful"] = count{stats.successful};
          dict["failed"] = count{stats.failed};
          dict["bytes"] = count{stats.bytes};
          ops[name] = std::move(dict);
        };
        add_stats("checks", self->state.stats.checks);
        add_stats("writes", self->state.stats.writes);
        add_stats("reads", self->state.stats.reads);
        add_stats("mmaps", self->state.stats.mmaps);
        // TODO: this should be called "deletes" or "erasures".
        add_stats("erases", self->state.stats.erases);
        add_stats("moves", self->state.stats.moves);
        result["operations"] = std::move(ops);
      }
      return result;
    },
    [self](atom::telemetry) {
      auto accountant = self->state.accountant.lock();
      if (!accountant)
        return;
      self->delayed_send(self, defaults::system::telemetry_rate,
                         atom::telemetry_v);
      auto msg = report{
        .data = {
          {"posix-filesystem.checks.sucessful", self->state.stats.checks.successful},
          {"posix-filesystem.checks.failed", self->state.stats.checks.failed},
          {"posix-filesystem.writes.sucessful", self->state.stats.writes.successful},
          {"posix-filesystem.writes.failed", self->state.stats.writes.failed},
          {"posix-filesystem.writes.bytes", self->state.stats.writes.bytes},
          {"posix-filesystem.reads.sucessful", self->state.stats.reads.successful},
          {"posix-filesystem.reads.failed", self->state.stats.reads.failed},
          {"posix-filesystem.reads.bytes", self->state.stats.reads.bytes},
          {"posix-filesystem.mmaps.sucessful", self->state.stats.mmaps.successful},
          {"posix-filesystem.mmaps.failed", self->state.stats.mmaps.failed},
          {"posix-filesystem.mmaps.bytes", self->state.stats.mmaps.bytes},
          {"posix-filesystem.erases.sucessful", self->state.stats.erases.successful},
          {"posix-filesystem.erases.failed", self->state.stats.erases.failed},
          {"posix-filesystem.erases.bytes", self->state.stats.erases.bytes},
          {"posix-filesystem.moves.sucessful", self->state.stats.moves.successful},
          {"posix-filesystem.moves.failed", self->state.stats.moves.failed},
        },
        .metadata = {},
      };
      self->send(accountant, atom::metrics_v, std::move(msg));
    },
  };
}

} // namespace vast::system
