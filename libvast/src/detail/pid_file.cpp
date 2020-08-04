
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

#include "vast/concept/printable/numeric.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/posix.hpp"
#include "vast/detail/system.hpp"
#include "vast/error.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <sys/file.h>

namespace vast::detail {

caf::error acquire_pid_file(const path& filename) {
  // If the file exists, we cannot continue.
  if (exists(filename)) {
    // Attempt to read file to display an actionable error message.
    auto contents = load_contents(filename);
    if (!contents)
      return contents.error();
    return caf::make_error(ec::filesystem_error,
                           "stale PID file found: ", filename.str(),
                           "terminate process", *contents,
                           "or remove PID file manually");
  }
  // Open the file.
  auto fd = ::open(filename.str().c_str(), O_WRONLY | O_CREAT, 0600);
  if (fd < 0)
    return caf::make_error(ec::filesystem_error, "open(2):", strerror(errno));
  // Lock the file handle.
  if (::flock(fd, LOCK_EX | LOCK_NB) < 0) {
    ::close(fd);
    return caf::make_error(ec::filesystem_error, "flock(2):", strerror(errno));
  }
  // Write the PID in human readable form into the file.
  auto pid = to_string(process_id());
  VAST_ASSERT(!pid.empty());
  if (::write(fd, pid.data(), pid.size()) < 0) {
    ::close(fd);
    return caf::make_error(ec::filesystem_error, "write(2):", strerror(errno));
  }
  // Relinquish the lock implicitly by closing the descriptor.
  ::close(fd);
  return caf::none;
}

} // namespace vast::detail
