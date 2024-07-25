//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/posix_filesystem.hpp"

#include "tenzir/chunk.hpp"
#include "tenzir/detail/weak_run_delayed.hpp"
#include "tenzir/io/read.hpp"
#include "tenzir/io/save.hpp"
#include "tenzir/report.hpp"
#include "tenzir/status.hpp"

#include <caf/config_value.hpp>
#include <caf/detail/set_thread_name.hpp>
#include <caf/dictionary.hpp>
#include <caf/result.hpp>
#include <caf/settings.hpp>

#include <filesystem>

namespace tenzir {

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

auto read_recursive(const std::filesystem::path& root,
                    size_t& total_size) -> caf::expected<record> {
  constexpr size_t MAX_TOTAL_SIZE = 64 * 1024 * 1024; // 64 MiB
  auto result = record{};
  for (const auto& entry : std::filesystem::directory_iterator(root)) {
    auto name = entry.path().filename();
    if (entry.is_directory()) {
      auto recursive_result = read_recursive(entry.path(), total_size);
      if (!recursive_result) {
        return recursive_result.error();
      }
      result[name] = *recursive_result;
    } else if (entry.is_regular_file()) {
      auto size = entry.file_size();
      if (total_size + size > MAX_TOTAL_SIZE) {
        return diagnostic::error("max size exceeded")
          .note("for file {}", entry.path())
          .to_error();
      }
      auto contents = io::read(entry.path());
      if (!contents) {
        return diagnostic::error(contents.error())
          .note("while trying to read file {}", entry.path())
          .to_error();
      }
      total_size += contents->size();
      result[name] = blob{contents->begin(), contents->end()};
    }
  }
  return result;
}

filesystem_actor::behavior_type posix_filesystem(
  filesystem_actor::stateful_pointer<posix_filesystem_state> self,
  std::filesystem::path root, const accountant_actor& accountant) {
  if (self->getf(caf::local_actor::is_detached_flag))
    caf::detail::set_thread_name("tenzir.posix-filesystem");
  self->state.root = std::move(root);
  if (accountant) {
    self->state.accountant = accountant;
    self->send(accountant, atom::announce_v, self->name());
    detail::weak_run_delayed_loop(self, defaults::telemetry_rate, [self] {
      auto accountant = self->state.accountant.lock();
      if (!accountant)
        return;
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
    });
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
    [self](atom::read, atom::recursive,
           const std::vector<std::filesystem::path>& filenames)
      -> caf::result<std::vector<record>> {
      auto result = std::vector<record>{};
      for (auto const& path : filenames) {
        const auto full_path
          = path.is_absolute() ? path : self->state.root / path;
        auto err = std::error_code{};
        if (!std::filesystem::exists(full_path, err)) {
          ++self->state.stats.checks.failed;
          result.emplace_back(record{});
          continue;
        } else {
          ++self->state.stats.checks.successful;
        }
        auto total_size = size_t{0};
        auto contents = read_recursive(full_path, total_size);
        if (!contents) {
          return diagnostic::error("failed to read directory")
            .note("trying to read {}", path)
            .note("encountered error {}", contents.error())
            .to_error();
        }
        result.push_back(std::move(*contents));
      }
      return result;
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
      TENZIR_DEBUG("{} got request to erase {}", *self, filename);
      const auto path
        = filename.is_absolute() ? filename : self->state.root / filename;
      auto size_error = std::error_code{};
      auto size = std::filesystem::file_size(path, size_error);
      if (size_error) {
        ++self->state.stats.checks.failed;
      } else {
        ++self->state.stats.checks.successful;
      }
      auto err = std::error_code{};
      std::filesystem::remove_all(path, err);
      if (err) {
        ++self->state.stats.erases.failed;
        return caf::make_error(ec::system_error,
                               fmt::format("{} failed to erase {}: {}", *self,
                                           path, err.message()));
      }
      ++self->state.stats.erases.successful;
      if (!size_error) {
        self->state.stats.erases.bytes += size;
      }
      return atom::done_v;
    },
    [self](atom::status, status_verbosity v, duration) {
      auto result = record{};
      if (v >= status_verbosity::info)
        result["type"] = "POSIX";
      if (v >= status_verbosity::debug) {
        auto ops = record{};
        auto add_stats = [&](auto& name, auto& stats) {
          auto dict = record{};
          dict["successful"] = uint64_t{stats.successful};
          dict["failed"] = uint64_t{stats.failed};
          dict["bytes"] = uint64_t{stats.bytes};
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
  };
}

} // namespace tenzir
