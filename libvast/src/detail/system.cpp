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

#include <unistd.h> // gethostname, sysconf, getpid

#include <cerrno>

#include "vast/detail/assert.hpp"
#include "vast/detail/system.hpp"

namespace vast {
namespace detail {

std::string hostname() {
  char buf[256];
  if (::gethostname(buf, sizeof(buf)) == 0)
    return buf;
  // if (errno == EFAULT)
  //  VAST_ERROR("failed to get hostname: invalid address");
  // else if (errno == ENAMETOOLONG)
  //  VAST_ERROR("failed to get hostname: longer than 256 characters");
  return {};
}

size_t page_size() {
  auto bytes = sysconf(_SC_PAGESIZE);
  VAST_ASSERT(bytes >= 1);
  return bytes;
}

int32_t process_id() {
  return ::getpid();
}

} // namespace detail
} // namespace vast
