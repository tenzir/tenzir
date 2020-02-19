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
#include "vast/error.hpp"

#include <dlfcn.h>
#if __linux__
#  include <limits.h>
#  include <sys/stat.h>
#  include <unistd.h>
#endif

namespace vast::detail {

namespace {

caf::expected<path> objectpath_dynamic(const void* addr) {
  Dl_info info;
  if (!dladdr(addr, &info))
    return make_error(ec::unspecified, "failed to execute dladdr()");
  if (!info.dli_fname)
    return make_error(ec::unspecified, "addr not in an mmapped region");
  // FIXME: On Linux, if addr is inside the main executable,
  // dli_fname seems to be the same argv[0] instead of the full path.
  // We should detect and return an error in that case.
  return info.dli_fname;
}

caf::expected<path> objectpath_static() {
#ifdef VAST_LINUX
  struct stat sb;
  auto self = "/proc/self/exe";
  if (lstat(self, &sb) == -1)
    return make_error(ec::unspecified, "lstat() returned with error");
  auto size = sb.st_size ? sb.st_size + 1 : PATH_MAX;
  std::vector<char> buf(size);
  if (readlink(self, buf.data(), size) == -1)
    return make_error(ec::unspecified, "readlink() returned with error");
  return path{buf.data()};
#else
  return make_error(ec::unimplemented);
#endif
}

} // namespace

caf::expected<path> objectpath(const void* addr) {
  if (addr == nullptr)
    addr = reinterpret_cast<const void*>(objectpath_dynamic);
  if (auto result = objectpath_dynamic(addr))
    return result;
  return objectpath_static();
}

} // namespace vast::detail
