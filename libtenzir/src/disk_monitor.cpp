//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/disk_monitor.hpp"

#include "tenzir/fwd.hpp"

#include "tenzir/concept/parseable/tenzir/si.hpp"
#include "tenzir/concept/parseable/tenzir/uuid.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/process.hpp"
#include "tenzir/detail/recursive_size.hpp"
#include "tenzir/detail/scope_guard.hpp"
#include "tenzir/error.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/status.hpp"
#include "tenzir/uuid.hpp"

#include <caf/typed_event_based_actor.hpp>

#include <filesystem>
#include <system_error>

namespace tenzir {

namespace {

struct partition_diskstate {
  uuid id;
  std::uintmax_t filesize;
  std::filesystem::file_time_type mtime;
};

struct diskstate_blacklist_comparator {
  bool operator()(const partition_diskstate& lhs,
                  const disk_monitor_state::blacklist_entry& rhs) const {
    return lhs.id < rhs.id;
  }

  bool operator()(const disk_monitor_state::blacklist_entry& lhs,
                  const partition_diskstate& rhs) const {
    return lhs.id < rhs.id;
  }
};

template <typename Fun>
std::shared_ptr<detail::scope_guard<Fun>> make_shared_guard(Fun f) {
  return std::make_shared<detail::scope_guard<Fun>>(std::forward<Fun>(f));
}

} // namespace

bool operator<(const disk_monitor_state::blacklist_entry& lhs,
               const disk_monitor_state::blacklist_entry& rhs) {
  return lhs.id < rhs.id;
}

caf::error validate(const disk_monitor_config& config) {
  if (config.step_size < 1) {
    return caf::make_error(ec::invalid_configuration, "step size must be "
                                                      "greater than zero");
  }
  if (config.low_water_mark > config.high_water_mark) {
    return caf::make_error(ec::invalid_configuration, "low-water mark greater "
                                                      "than high-water mark");
  }
  if (config.scan_binary) {
    if (config.scan_binary->empty()) {
      return caf::make_error(ec::invalid_configuration,
                             "scan binary path cannot be "
                             "empty");
    }
    if (config.scan_binary->at(0) != '/') {
      return caf::make_error(ec::invalid_configuration,
                             "scan binary path must be "
                             "an absolute");
    }
    if (! std::filesystem::exists(*config.scan_binary)) {
      return caf::make_error(ec::invalid_configuration, "scan binary doesn't "
                                                        "exist");
    }
  }
  return {};
}

caf::expected<size_t> compute_dbdir_size(std::filesystem::path state_directory,
                                         const disk_monitor_config& config) {
  caf::expected<size_t> result = 0;
  if (! config.scan_binary) {
    return detail::recursive_size(state_directory);
  }
  const auto& command
    = fmt::format("{} {}", *config.scan_binary, state_directory);
  TENZIR_VERBOSE("executing command '{}' to determine size of state_directory",
                 command);
  auto cmd_output = detail::execute_blocking(command);
  if (! cmd_output) {
    return cmd_output.error();
  }
  if (cmd_output->back() == '\n') {
    cmd_output->pop_back();
  }
  if (! parsers::count(*cmd_output, result.value())) {
    result = caf::make_error(ec::parse_error,
                             fmt::format("failed to interpret output "
                                         "'{}' of command '{}'",
                                         *cmd_output, command));
  }
  return result;
}

bool disk_monitor_state::purging() const {
  return pending_partitions != 0;
}

disk_monitor_actor::behavior_type
disk_monitor(disk_monitor_actor::stateful_pointer<disk_monitor_state> self,
             const disk_monitor_config& config,
             const std::filesystem::path& db_dir, index_actor index) {
  TENZIR_TRACE("disk_monitor {} {} {} {}", TENZIR_ARG(self->id()),
               TENZIR_ARG(config.high_water_mark),
               TENZIR_ARG(config.low_water_mark), TENZIR_ARG(db_dir));
  using namespace std::string_literals;
  if (auto error = validate(config); error.valid()) {
    self->quit(error);
    return disk_monitor_actor::behavior_type::make_empty_behavior();
  }
  self->state().config = config;
  self->state().state_directory = db_dir;
  self->state().index = std::move(index);
  self->mail(atom::ping_v).send(self);
  return {
    [self](atom::ping) {
      self->mail(atom::ping_v)
        .delay(self->state().config.scan_interval)
        .send(self);
      if (self->state().purging()) {
        TENZIR_DEBUG("{} ignores ping because a deletion is still in "
                     "progress",
                     *self);
        return;
      }
      // TODO: This is going to do one syscall per file in the database
      // directory. This feels a bit wasteful, but in practice we didn't
      // see noticeable overhead even on large-ish databases.
      // Nonetheless, if this becomes relevant we should switch to using
      // `inotify()` or similar to do real-time tracking of the db size.
      auto size = compute_dbdir_size(self->state().state_directory,
                                     self->state().config);
      if (! size) {
        TENZIR_WARN("{} failed to calculate recursive size of {}: {}", *self,
                    self->state().state_directory, size.error());
        return;
      }
      TENZIR_VERBOSE("{} checks state-directory of size {}", *self, *size);
      if (*size > self->state().config.high_water_mark) {
        self->mail(atom::erase_v)
          .request(static_cast<disk_monitor_actor>(self), caf::infinite)
          .then(
            [=] {
              // nop
            },
            [=](const caf::error& err) {
              TENZIR_ERROR("{} failed to purge state-directory: {}", *self,
                           err);
            });
      }
    },
    [self](atom::erase) -> caf::result<void> {
      auto err = std::error_code{};
      const auto index_dir = std::filesystem::directory_iterator(
        self->state().state_directory / "index", err);
      if (err) {
        return caf::make_error(ec::filesystem_error, //
                               fmt::format("failed to find index in "
                                           "state-directory at {}: {}",
                                           self->state().state_directory, err));
      }
      // TODO(ch20006): Add some check on the overall structure on the db dir.
      std::vector<partition_diskstate> partitions;
      for (const auto& entry : index_dir) {
        auto partition = entry.path().filename().string();
        if (partition == "index.bin") {
          continue;
        }
        if (entry.path().extension() == ".mdx") {
          continue;
        }
        uuid id = {};
        if (! parsers::uuid(partition, id)) {
          TENZIR_VERBOSE("{} failed to find partition {}", *self, partition);
          continue;
        }
        if (entry.is_regular_file()) {
          std::error_code err{};
          const auto file_size = entry.file_size(err);
          const auto mtime = entry.last_write_time(err);
          if (! err && file_size != static_cast<std::uintmax_t>(-1)) {
            partitions.push_back({id, file_size, mtime});
          } else {
            TENZIR_WARN("{} failed to get file size and last write time for "
                        "partition {}",
                        *self, partition);
          }
        }
      }
      if (partitions.empty()) {
        TENZIR_VERBOSE("{} failed to find any partitions to delete", *self);
        return {};
      }
      TENZIR_DEBUG("{} found {} partitions on disk", *self, partitions.size());
      auto& blacklist = self->state().blacklist;
      if (! blacklist.empty()) {
        // Sort partitions so they're usable for `std::set_difference`.
        std::sort(partitions.begin(), partitions.end(),
                  [](const auto& lhs, const auto& rhs) {
                    return lhs.id < rhs.id;
                  });
        TENZIR_ASSERT_EXPENSIVE(
          std::is_sorted(blacklist.begin(), blacklist.end(),
                         [](const auto& lhs, const auto& rhs) {
                           return lhs.id < rhs.id;
                         }));
        std::vector<partition_diskstate> good_partitions;
        std::set_difference(partitions.begin(), partitions.end(),
                            blacklist.begin(), blacklist.end(),
                            std::back_inserter(good_partitions),
                            diskstate_blacklist_comparator{});
        partitions = std::move(good_partitions);
      }
      // Sort partitions by their mtime.
      std::sort(partitions.begin(), partitions.end(),
                [](const auto& lhs, const auto& rhs) {
                  return lhs.mtime < rhs.mtime;
                });
      // Delete up to `step_size` partitions at once.
      auto idx = std::min(partitions.size(), self->state().config.step_size);
      self->state().pending_partitions += idx;
      constexpr auto erase_timeout = std::chrono::seconds{60};
      auto continuation = [=] {
        if (--self->state().pending_partitions == 0) {
          if (const auto size = compute_dbdir_size(
                self->state().state_directory, self->state().config);
              ! size) {
            TENZIR_WARN("{} failed to calculate size of {}: {}", *self,
                        self->state().state_directory, size.error());
          } else {
            TENZIR_VERBOSE("{} erased ids from index; leftover size is {}",
                           *self, *size);
            if (*size > self->state().config.low_water_mark) {
              // Repeat until we're below the low water mark
              self->mail(atom::erase_v).send(self);
            }
          }
        }
      };
      for (size_t i = 0; i < idx; ++i) {
        auto& partition = partitions.at(i);
        TENZIR_VERBOSE("{} erases partition {} from index", *self,
                       partition.id);
        self->mail(atom::erase_v, partition.id)
          .request(self->state().index, erase_timeout)
          .then(
            [=](atom::done) {
              continuation();
            },
            [=, id = partition.id](caf::error& e) {
              TENZIR_WARN("{} failed to erase partition {} within {}: {}",
                          *self, id, erase_timeout, e);
              self->state().blacklist.insert(
                disk_monitor_state::blacklist_entry{id, std::move(e)});
              continuation();
            });
      }
      return {};
    },
    [self](atom::status, status_verbosity sv, duration) {
      auto result = record{};
      auto disk_monitor = record{};
      disk_monitor["blacklist-size"] = self->state().blacklist.size();
      if (sv >= status_verbosity::debug) {
        auto blacklist = list{};
        for (auto& blacklisted : self->state().blacklist) {
          auto entry = record{};
          entry["id"] = fmt::format("{}", blacklisted.id);
          entry["error"] = fmt::format("{}", blacklisted.error);
          blacklist.emplace_back(entry);
        }
        disk_monitor["blacklist"] = std::move(blacklist);
      }
      result["disk-monitor"] = std::move(disk_monitor);
      return result;
    },
  };
}

} // namespace tenzir
