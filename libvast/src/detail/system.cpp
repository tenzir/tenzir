//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/system.hpp"

#include "vast/detail/assert.hpp"
#include "vast/error.hpp"

#include <caf/expected.hpp>

#include <cerrno>
#include <cstdio>
#include <unistd.h> // gethostname, sysconf, getpid

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
