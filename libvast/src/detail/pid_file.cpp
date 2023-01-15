//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/pid_file.hpp"

#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/printable/numeric.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/load_contents.hpp"
#include "vast/detail/posix.hpp"
#include "vast/detail/system.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"

#include <sys/file.h>

#include <fcntl.h>
#include <system_error>
#include <unistd.h>

namespace vast::detail {

#if VAST_LINUX

namespace {

bool pid_belongs_to_vast(pid_t pid) {
  const auto proc_pid_path = fmt::format("/proc/{}/status", pid);
  const auto proc_pid_contents = detail::load_contents(proc_pid_path);
  if (!proc_pid_contents) {
    VAST_DEBUG("failed to read {}: {}", proc_pid_path,
               proc_pid_contents.error());
    return false;
  }
  // The maximum length of the "vast" process name is 4, so including the
  // null-terminator we just need to read up to 4 bytes here.
  auto process_name = std::array<char, 5>{};
  ::sscanf(proc_pid_contents->c_str(), "%*s %4s", process_name.data());
  return std::string_view{process_name.begin(), process_name.end()} == "vast";
}

} // namespace

#endif // VAST_LINUX

caf::error acquire_pid_file(const std::filesystem::path& filename) {
  auto pid = process_id();
  std::error_code err{};
  // Check if the db directory is owned by an existing VAST process.
  const auto exists = std::filesystem::exists(filename, err);
  if (err)
    VAST_WARN("failed to check if the db directory {} exists: {}", filename,
              err.message());
  if (exists) {
    // Attempt to read file to display an actionable error message.
    auto contents = detail::load_contents(filename);
    if (!contents)
      return contents.error();
    auto other_pid = to<int32_t>(*contents);
    if (!other_pid)
      return caf::make_error(ec::parse_error,
                             "unable to parse pid_file:", *contents);
    // Safeguard in case the pid_file already belongs to this process.
    if (*other_pid == pid)
      return caf::none;
    // Safeguard in case the pid_file contains a PID of a non-VAST process.
    if (::getpgid(*other_pid) >= 0) {
#if VAST_LINUX
      // In deployments with containers it's rather likely that the PID in the
      // PID file belongs to a different, non-VAST process after a crash,
      // because after a restart of the container the PID may be assigned to
      // another process. If it does, we ignore the PID in the PID file and
      // don't stop execution.
      const auto ignore_pid = !pid_belongs_to_vast(*other_pid);
#else
      const auto ignore_pid = false;
#endif
      if (!ignore_pid)
        return caf::make_error(ec::filesystem_error,
                               fmt::format("PID file found: {}, terminate "
                                           "process {}",
                                           filename, *contents));
      VAST_DEBUG("ignores conflicting PID file because contained PID does not "
                 "belong to a VAST process");
    }
    // The previous owner is deceased, print a warning an assume ownership.
    VAST_WARN("node detected an irregular shutdown of the previous "
              "process on the database directory");
  }
  // Open the file.
  auto fd = ::open(filename.c_str(), O_WRONLY | O_CREAT, 0600);
  if (fd < 0)
    return caf::make_error(ec::filesystem_error,
                           fmt::format("failed in open(2) with filename {} : "
                                       "{}",
                                       filename, strerror(errno)));
  // Lock the file handle.
  if (::flock(fd, LOCK_EX | LOCK_NB) < 0) {
    ::close(fd);
    return caf::make_error(ec::filesystem_error,
                           "failed in flock(2):", strerror(errno));
  }
  // Write the PID in human readable form into the file.
  auto pid_string = to_string(pid);
  VAST_ASSERT(!pid_string.empty());
  if (::write(fd, pid_string.data(), pid_string.size()) < 0) {
    ::close(fd);
    return caf::make_error(ec::filesystem_error,
                           "failed in write(2):", strerror(errno));
  }
  // Relinquish the lock implicitly by closing the descriptor.
  ::close(fd);
  return caf::none;
}

} // namespace vast::detail
