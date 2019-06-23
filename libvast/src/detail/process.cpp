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

#include "vast/detail/process.hpp"

#include <dlfcn.h>
#if __linux__
#  include <limits.h>
#  include <sys/stat.h>
#  include <unistd.h>
#endif

namespace vast::detail {

namespace {

caf::optional<vast::path> objectpath_dynamic(const void* addr) {
  Dl_info info;
  if (!dladdr(addr, &info))
    return caf::none;
  if (!info.dli_fname)
    return caf::none;
  return info.dli_fname;
}

caf::optional<vast::path> objectpath_static() {
#if __linux__
  struct stat sb;
  auto self = "/proc/self/exe";
  if (lstat(self, &sb) == -1)
    return caf::none;
  auto size = sb.st_size ? sb.st_size + 1 : PATH_MAX;
  std::vector<char> buf(size);
  if (readlink(self, buf.data(), size) == -1) {
    return caf::none;
  }
  return path{buf.data()};
#else
  return caf::none;
#endif
}

} // namespace

caf::optional<vast::path> objectpath(const void* addr) {
  auto result = objectpath_dynamic(addr);
  if (!result)
    result = objectpath_static();
  return result;
}

} // namespace vast::detail
