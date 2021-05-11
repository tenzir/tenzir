//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/disk_monitor.hpp"

#include "vast/fwd.hpp"

#include "vast/concept/parseable/from_string.hpp"
#include "vast/concept/parseable/vast/si.hpp"
#include "vast/concept/parseable/vast/uuid.hpp"
#include "vast/detail/process.hpp"
#include "vast/detail/recursive_size.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/system/archive.hpp"
#include "vast/uuid.hpp"

#include <caf/detail/scope_guard.hpp>
#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <filesystem>
#include <system_error>

namespace vast::system {

namespace {

struct partition_diskstate {
  uuid id;
  std::uintmax_t filesize;
  std::filesystem::file_time_type mtime;
};

template <typename Fun>
std::shared_ptr<caf::detail::scope_guard<Fun>> make_shared_guard(Fun f) {
  return std::make_shared<caf::detail::scope_guard<Fun>>(std::forward<Fun>(f));
}

} // namespace

caf::error validate(const disk_monitor_config& config) {
  if (config.step_size < 1)
    return caf::make_error(ec::invalid_configuration, "step size must be "
                                                      "greater than zero");
  if (config.low_water_mark > config.high_water_mark)
    return caf::make_error(ec::invalid_configuration, "low-water mark greater "
                                                      "than high-water mark");
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
    if (!std::filesystem::exists(*config.scan_binary)) {
      return caf::make_error(ec::invalid_configuration, "scan binary doesn't "
                                                        "exist");
    }
  }
  return {};
}

caf::expected<size_t> disk_monitor_state::compute_dbdir_size() const {
  caf::expected<size_t> result = 0;
  if (!scan_command) {
    return detail::recursive_size(dbdir);
  }
  const auto& command = *scan_command;
  VAST_VERBOSE("{} executing command '{}' to determine size of dbdir", name,
               command);
  auto cmd_output = detail::execute_blocking(command);
  if (!cmd_output)
    return cmd_output.error();
  if (cmd_output->back() == '\n')
    cmd_output->pop_back();
  if (!parsers::count(*cmd_output, result.value())) {
    result = caf::make_error(ec::parse_error,
                             fmt::format("{} failed to interpret output "
                                         "'{}' of command '{}'",
                                         name, *cmd_output, command));
  }
  return result;
}

disk_monitor_actor::behavior_type
disk_monitor(disk_monitor_actor::stateful_pointer<disk_monitor_state> self,
             const disk_monitor_config& config,
             const std::filesystem::path& dbdir, archive_actor archive,
             index_actor index) {
  VAST_TRACE_SCOPE("{} {} {}", VAST_ARG(config.high_water_mark),
                   VAST_ARG(config.low_water_mark), VAST_ARG(dbdir));
  using namespace std::string_literals;
  if (auto error = validate(config)) {
    self->quit(error);
    return disk_monitor_actor::behavior_type::make_empty_behavior();
  }
  self->state.high_water_mark = config.high_water_mark;
  self->state.low_water_mark = config.low_water_mark;
  self->state.step_size = config.step_size;
  self->state.scan_interval = config.scan_interval;
  self->state.dbdir = dbdir;
  self->state.archive = archive;
  self->state.index = index;
  self->send(self, atom::ping_v);
  if (config.scan_binary)
    self->state.scan_command = fmt::format("{} {}", *config.scan_binary, dbdir);
  return {
    [self](atom::ping) {
      self->delayed_send(self, self->state.scan_interval, atom::ping_v);
      if (self->state.purging) {
        VAST_DEBUG("{} ignores ping because a deletion is still in "
                   "progress",
                   self);
        return;
      }
      auto size = self->state.compute_dbdir_size();
      // TODO: This is going to do one syscall per file in the database
      // directory. This feels a bit wasteful, but in practice we didn't
      // see noticeable overhead even on large-ish databases.
      // Nonetheless, if this becomes relevant we should switch to using
      // `inotify()` or similar to do real-time tracking of the db size.
      if (!size) {
        VAST_WARN("{} failed to calculate recursive size of {}: {}", self,
                  self->state.dbdir, size.error());
        return;
      }
      VAST_VERBOSE("{} checks db-directory of size {}", self, *size);
      if (*size > self->state.high_water_mark && !self->state.purging) {
        self->state.purging = true;
        // TODO: Remove the static_cast when switching to CAF 0.18.
        self
          ->request(static_cast<disk_monitor_actor>(self), caf::infinite,
                    atom::erase_v)
          .then(
            [] {
              // nop
            },
            [=](const caf::error& err) {
              VAST_ERROR("{} failed to purge db-directory: {}", self, err);
            });
      }
    },
    [self](atom::erase) -> caf::result<void> {
      // Make sure the `purging` state will be reset once all continuations
      // have finished or we encountered an error.
      auto shared_guard
        = make_shared_guard([=] { self->state.purging = false; });
      auto err = std::error_code{};
      const auto index_dir
        = std::filesystem::directory_iterator(self->state.dbdir / "index", err);
      if (err)
        return caf::make_error(ec::filesystem_error, //
                               fmt::format("failed to find index in "
                                           "db-directory at {}: {}",
                                           self->state.dbdir, err));
      // TODO(ch20006): Add some check on the overall structure on the db dir.
      std::vector<partition_diskstate> partitions;
      for (const auto& entry : index_dir) {
        auto partition = entry.path().filename().string();
        if (partition == "index.bin")
          continue;
        if (entry.path().extension() == ".mdx")
          continue;
        uuid id;
        if (!parsers::uuid(partition, id)) {
          VAST_VERBOSE("{} failed to find partition {}", self, partition);
          continue;
        }
        if (entry.is_regular_file()) {
          std::error_code err{};
          const auto file_size = entry.file_size(err);
          const auto mtime = entry.last_write_time(err);
          if (!err && file_size != static_cast<std::uintmax_t>(-1))
            partitions.push_back({id, file_size, mtime});
          else
            VAST_WARN("{} failed to get file size and last write time for "
                      "partition {}",
                      self, partition);
        }
      }
      if (partitions.empty()) {
        VAST_VERBOSE("{} failed to find any partitions to delete", self);
        return {};
      }
      VAST_DEBUG("{} found {} partitions on disk", self, partitions.size());
      std::sort(partitions.begin(), partitions.end(),
                [](const auto& lhs, const auto& rhs) {
                  return lhs.mtime < rhs.mtime;
                });
      // Delete up to `step_size` partitions at once.
      auto idx = std::min(partitions.size(), self->state.step_size);
      for (size_t i = 0; i < idx; ++i) {
        auto& partition = partitions.at(i);
        VAST_VERBOSE("{} erases partition {} from index", self, partition.id);
        self
          ->request(self->state.index, caf::infinite, atom::erase_v,
                    partition.id)
          .then(
            [=, sg = shared_guard](ids erased_ids) {
              // TODO: It would be more natural if we could chain these futures,
              // instead of nesting them.
              VAST_VERBOSE("{} erases removed ids from archive", self);
              self
                ->request(self->state.archive, caf::infinite, atom::erase_v,
                          erased_ids)
                .then(
                  [=, sg = shared_guard](atom::done) {
                    // TODO: There's a race condition here: We calculate the
                    // size of the database directory while we might be deleting
                    // files from it.
                    if (const auto size = self->state.compute_dbdir_size();
                        !size) {
                      VAST_WARN("{} failed to calculate size of {}: {}", self,
                                self->state.dbdir, size.error());
                    } else {
                      VAST_VERBOSE("{} erased ids from index; leftover size is "
                                   "{}",
                                   self, *size);
                      if (*size > self->state.low_water_mark) {
                        // Repeat until we're below the low water mark
                        self->send(self, atom::erase_v);
                      }
                    }
                  },
                  [=, sg = shared_guard](caf::error err) {
                    VAST_WARN("{} failed to erase from archive: {}", self, err);
                  });
            },
            [=, sg = shared_guard](caf::error e) {
              VAST_WARN("{} failed to erase from index: {}", self, render(e));
            });
      }
      return {};
    },
    [](atom::status, status_verbosity) {
      // TODO: Return some useful information here.
      return caf::settings{};
    },
  };
}

} // namespace vast::system
