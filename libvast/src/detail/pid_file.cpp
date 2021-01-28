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

#include "vast/detail/pid_file.hpp"

#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/printable/numeric.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/posix.hpp"
#include "vast/detail/system.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <sys/file.h>

namespace vast::detail {

caf::error acquire_pid_file(const path& filename) {
  auto pid = process_id();
  // Check if the db directory is owned by an existing VAST process.
  if (exists(filename)) {
    // Attempt to read file to display an actionable error message.
    auto contents = load_contents(filename);
    if (!contents)
      return contents.error();
    auto other_pid = to<int32_t>(*contents);
    if (!other_pid)
      return caf::make_error(ec::parse_error,
                             "unable to parse pid_file:", *contents);
    // Safeguard in case the pid_file already belongs to this process.
    if (*other_pid == pid)
      return caf::none;
    if (::getpgid(*other_pid) >= 0)
      return caf::make_error(ec::filesystem_error,
                             "PID file found: ", filename.str(),
                             "terminate process", *contents);
    // The previous owner is deceased, print a warning an assume ownership.
    VAST_LOG_SPD_WARN("node detected an irregular shutdown of the previous "
                      "process on the database directory");
  }
  // Open the file.
  auto fd = ::open(filename.str().c_str(), O_WRONLY | O_CREAT, 0600);
  if (fd < 0)
    return caf::make_error(ec::filesystem_error,
                           "failed in open(2):", strerror(errno));
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
